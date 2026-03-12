//! Parse OCI index.json + manifest, resolve layer blobs.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct OciIndex {
    pub manifests: Vec<OciDescriptor>,
}

#[derive(Debug, Deserialize)]
pub struct OciDescriptor {
    pub digest: String,
    #[serde(rename = "mediaType", default)]
    pub media_type: String,
}

#[derive(Debug, Deserialize)]
pub struct OciManifest {
    pub layers: Vec<OciDescriptor>,
}

/// One layer blob ready for processing.
#[derive(Debug)]
pub struct LayerBlob {
    pub path: PathBuf,
    pub media_type: String,
    pub index: usize,
}

const MEDIA_TYPE_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
const MEDIA_TYPE_INDEX: &str = "application/vnd.oci.image.index.v1+json";

/// Detect compression format by magic bytes. Used as a fallback when the
/// manifest does not carry a mediaType for a layer (e.g. minimal Docker save
/// layouts that omit LayerSources).
pub fn detect_media_type(path: &Path) -> Result<&'static str> {
    let mut f = std::fs::File::open(path)?;
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic)?;
    Ok(match magic {
        [0x1f, 0x8b, ..] => "application/vnd.oci.image.layer.v1.tar+gzip",
        [0x28, 0xb5, 0x2f, 0xfd] => "application/vnd.oci.image.layer.v1.tar+zstd",
        [0x42, 0x5a, 0x68, ..] => "application/vnd.oci.image.layer.v1.tar+bzip2",
        [0xfd, 0x37, 0x7a, 0x58] => "application/vnd.oci.image.layer.v1.tar+xz",
        _ => "application/vnd.oci.image.layer.v1.tar", // uncompressed
    })
}

/// Load the OCI manifest from index.json → manifest blob.
pub fn load_manifest(image_dir: &Path) -> Result<OciManifest> {
    // Try index.json first (OCI layout), fall back to manifest.json (Docker save).
    let index_path = image_dir.join("index.json");
    if index_path.exists() {
        let data = std::fs::read_to_string(&index_path)
            .with_context(|| format!("reading {}", index_path.display()))?;
        let index: OciIndex = serde_json::from_str(&data).context("parsing index.json")?;
        let desc = index
            .manifests
            .into_iter()
            .next()
            .context("index.json has no manifests")?;
        return load_manifest_blob(image_dir, &desc).context("loading manifest from index.json");
    }

    // Docker save manifest.json
    let manifest_path = image_dir.join("manifest.json");
    if manifest_path.exists() {
        #[derive(Deserialize)]
        struct LayerSource {
            #[serde(rename = "mediaType")]
            media_type: String,
        }

        #[derive(Deserialize)]
        struct DockerManifest {
            #[serde(rename = "Layers")]
            layers: Vec<String>,
            /// Present in Docker save layouts produced by newer Docker versions
            /// and by skopeo. Maps digest ("sha256:<hex>") → layer descriptor.
            #[serde(rename = "LayerSources", default)]
            layer_sources: HashMap<String, LayerSource>,
        }

        let data = std::fs::read_to_string(&manifest_path).context("reading manifest.json")?;
        let manifests: Vec<DockerManifest> =
            serde_json::from_str(&data).context("parsing manifest.json")?;
        let dm = manifests
            .into_iter()
            .next()
            .context("manifest.json is empty")?;

        let layers = dm
            .layers
            .into_iter()
            .map(|l| {
                // Layer paths look like "blobs/sha256/<hex>".
                // LayerSources keys look like "sha256:<hex>".
                // Reconstruct the digest key from the path's final component.
                let digest = l
                    .rsplit('/')
                    .next()
                    .map(|hex| format!("sha256:{hex}"))
                    .unwrap_or_default();
                let media_type = dm
                    .layer_sources
                    .get(&digest)
                    .map(|s| s.media_type.clone())
                    // No LayerSources entry — will be resolved by magic bytes
                    // in resolve_layers.
                    .unwrap_or_default();
                OciDescriptor {
                    digest: l,
                    media_type,
                }
            })
            .collect();

        return Ok(OciManifest { layers });
    }

    bail!(
        "no index.json or manifest.json found in {}",
        image_dir.display()
    );
}

/// Resolve a single `OciDescriptor` to an `OciManifest`, following one level
/// of nested index indirection if necessary.
///
/// containerd and Docker Desktop commonly produce a two-level structure for
/// multi-platform images:
///
///   index.json → platform index (mediaType: ...image.index...)
///              → per-platform manifest (mediaType: ...image.manifest...)
///              → layers
///
/// When `desc` points at a nested index we pick the first entry whose
/// mediaType is a single-image manifest and load that.  Entries that are
/// themselves indexes are skipped.
fn load_manifest_blob(image_dir: &Path, desc: &OciDescriptor) -> Result<OciManifest> {
    let hex = strip_digest_prefix(&desc.digest)?;
    let path = image_dir.join("blobs").join("sha256").join(hex);
    let data = std::fs::read_to_string(&path)
        .with_context(|| format!("reading manifest blob {}", path.display()))?;

    // If this blob is a nested index, follow it one level deeper.
    if desc.media_type == MEDIA_TYPE_INDEX {
        let nested: OciIndex = serde_json::from_str(&data)
            .with_context(|| format!("parsing nested index blob {}", path.display()))?;

        let inner = nested
            .manifests
            .into_iter()
            .find(|d| d.media_type == MEDIA_TYPE_MANIFEST)
            .with_context(|| {
                format!(
                    "nested index at {} contains no single-image manifest entry \
                     (mediaType {MEDIA_TYPE_MANIFEST})",
                    path.display()
                )
            })?;

        let inner_hex = strip_digest_prefix(&inner.digest)?;
        let inner_path = image_dir.join("blobs").join("sha256").join(inner_hex);
        let inner_data = std::fs::read_to_string(&inner_path)
            .with_context(|| format!("reading inner manifest blob {}", inner_path.display()))?;
        return serde_json::from_str(&inner_data)
            .with_context(|| format!("parsing inner manifest blob {}", inner_path.display()));
    }

    // Direct single-image manifest.
    serde_json::from_str(&data).with_context(|| format!("parsing manifest blob {}", path.display()))
}

/// Resolve layer descriptors to actual file paths.
pub fn resolve_layers(image_dir: &Path, manifest: &OciManifest) -> Result<Vec<LayerBlob>> {
    manifest
        .layers
        .iter()
        .enumerate()
        .map(|(i, desc)| {
            let path = if desc.digest.contains(':') {
                // OCI digest: sha256:<hex>
                let hex = strip_digest_prefix(&desc.digest)?;
                image_dir.join("blobs").join("sha256").join(hex)
            } else {
                // Docker save: relative path like "blobs/sha256/<hex>"
                image_dir.join(&desc.digest)
            };
            // Some layouts store each blob as <hash>/<manifest-order>
            // rather than as a flat file named <hash>.
            let path = if path.is_dir() {
                path.join(i.to_string())
            } else {
                path
            };
            if !path.exists() {
                bail!("layer blob not found: {}", path.display());
            }
            // If the manifest didn't carry a mediaType (e.g. a Docker save
            // layout without LayerSources), fall back to magic byte detection.
            let media_type = if desc.media_type.is_empty() {
                detect_media_type(&path)
                    .with_context(|| format!("detecting media type for {}", path.display()))?
                    .to_string()
            } else {
                desc.media_type.clone()
            };
            Ok(LayerBlob {
                path,
                media_type,
                index: i,
            })
        })
        .collect()
}

pub fn strip_digest_prefix(digest: &str) -> Result<&str> {
    digest
        .strip_prefix("sha256:")
        .with_context(|| format!("unsupported digest algorithm in: {digest}"))
}
