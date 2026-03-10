pub mod canonical;
pub mod dir;
pub mod image;
pub mod layers;
pub mod overlay;
pub mod squashfs;
pub mod tar;
pub mod tracker;
pub mod verify;

use anyhow::Result;
use std::path::Path;

/// Convert an extracted OCI image directory into a squashfs file.
///
/// Layer content is streamed directly into mksquashfs's stdin — the merged
/// tar is never fully materialised in memory.
pub async fn convert_mksquashfs(
    image_dir: &Path,
    output_squashfs: &Path,
    squashfs_binpath: Option<&Path>,
) -> Result<()> {
    let image_dir = image_dir.to_path_buf();
    let output_squashfs = output_squashfs.to_path_buf();
    let squashfs_binpath = squashfs_binpath.map(Path::to_path_buf);

    tokio::task::spawn_blocking(move || {
        let manifest = image::load_manifest(&image_dir)?;
        let layers = image::resolve_layers(&image_dir, &manifest)?;
        squashfs::write_squashfs(layers, &output_squashfs, squashfs_binpath.as_deref())
    })
    .await?
}

/// Convert an extracted OCI image directory into a plain tar file.
pub async fn convert_tar(image_dir: &Path, output_tar: &Path) -> Result<()> {
    let image_dir = image_dir.to_path_buf();
    let output_tar = output_tar.to_path_buf();

    tokio::task::spawn_blocking(move || {
        let manifest = image::load_manifest(&image_dir)?;
        let layers = image::resolve_layers(&image_dir, &manifest)?;
        tar::write_tar(layers, &output_tar)
    })
    .await?
}

/// Extract an OCI image directory directly into `output_dir`.
///
/// Skips both tar archiving and mksquashfs — files are written straight to
/// the filesystem using the same overlay merge logic.
pub async fn convert_dir(image_dir: &Path, output_dir: &Path) -> Result<()> {
    let image_dir = image_dir.to_path_buf();
    let output_dir = output_dir.to_path_buf();

    tokio::task::spawn_blocking(move || {
        let manifest = image::load_manifest(&image_dir)?;
        let layers = image::resolve_layers(&image_dir, &manifest)?;
        dir::write_dir(layers, &output_dir)
    })
    .await?
}
