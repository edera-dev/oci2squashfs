//! Parse OCI index.json + manifest, resolve layer blobs.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
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

/// Load the OCI manifest from index.json → manifest blob.
pub fn load_manifest(image_dir: &Path) -> Result<OciManifest> {
    // Try index.json first (OCI layout), fall back to manifest.json (Docker save).
    let index_path = image_dir.join("index.json");
    if index_path.exists() {
        let data = std::fs::read_to_string(&index_path)
            .with_context(|| format!("reading {}", index_path.display()))?;
        // index.json may be an OCI image index (points to a manifest blob) or,
        // when produced by tools that skip the two-level layout, a bare OCI
        // image manifest (has "layers" directly).
        if let Ok(index) = serde_json::from_str::<OciIndex>(&data) {
            if !index.manifests.is_empty() {
                let desc = index.manifests.into_iter().next().unwrap();
                let digest = strip_digest_prefix(&desc.digest)?;
                let manifest_path = image_dir.join("blobs").join("sha256").join(digest);
                let mdata = std::fs::read_to_string(&manifest_path)
                    .with_context(|| format!("reading manifest blob {}", manifest_path.display()))?;
                let manifest: OciManifest =
                    serde_json::from_str(&mdata).context("parsing manifest blob")?;
                return Ok(manifest);
            }
        }
        // Fall through: try parsing index.json directly as an image manifest.
        let manifest: OciManifest =
            serde_json::from_str(&data).context("parsing index.json as image manifest")?;
        return Ok(manifest);
    }

    // Docker save manifest.json
    let manifest_path = image_dir.join("manifest.json");
    if manifest_path.exists() {
        #[derive(Deserialize)]
        struct DockerManifest {
            #[serde(rename = "Layers")]
            layers: Vec<String>,
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
            .map(|l| OciDescriptor {
                digest: l,
                media_type: "application/vnd.docker.image.rootfs.diff.tar.gzip".into(),
            })
            .collect();
        return Ok(OciManifest { layers });
    }

    bail!(
        "no index.json or manifest.json found in {}",
        image_dir.display()
    );
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
                // image_dir may be the blobs/ directory itself; check that first
                let flat = image_dir.join("sha256").join(hex);
                if flat.exists() {
                    flat
                } else {
                    image_dir.join("blobs").join("sha256").join(hex)
                }
            } else {
                // Docker save: relative path like "abc123.../layer.tar"
                image_dir.join(&desc.digest)
            };
            // Some layouts store each blob as <hash>/<manifest-order>
            // rather than as a flat file named <hash>.
            let path = if path.is_dir() { path.join(i.to_string()) } else { path };
            if !path.exists() {
                bail!("layer blob not found: {}", path.display());
            }
            Ok(LayerBlob {
                path,
                media_type: desc.media_type.clone(),
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
