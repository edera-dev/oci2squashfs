//! Extract merged OCI layers directly into a directory.

use anyhow::{Context, Result};
use std::{os::unix::net::UnixStream, path::Path, sync::mpsc, thread};

use crate::{image::LayerBlob, overlay::merge_layers_into_streaming};

/// Extract the merged OCI layers into `output_dir`.
/// On error the partially populated directory is left in place — callers
/// are responsible for cleanup.
pub fn write_dir(
    receiver: mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output_dir: &Path,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("creating output directory {}", output_dir.display()))?;

    let (reader, writer) = UnixStream::pair().context("creating socket pair for tar pipe")?;

    let merge_handle =
        thread::spawn(move || merge_layers_into_streaming(receiver, total_layers, writer, None));

    let mut archive = tar::Archive::new(reader);
    archive.set_preserve_permissions(true);
    archive.set_preserve_mtime(true);
    archive
        .unpack(output_dir)
        .context("unpacking merged tar into output directory")?;

    merge_handle
        .join()
        .expect("merge thread panicked")
        .context("merging layers")?;

    Ok(())
}
