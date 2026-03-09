//! Extract merged OCI layers directly into a directory.

use anyhow::{Context, Result};
use std::{os::unix::net::UnixStream, path::Path, thread};

use crate::{image::LayerBlob, overlay::merge_layers_into};

/// Extract the merged OCI layers into `output_dir`.
pub fn write_dir(layers: Vec<LayerBlob>, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("creating output directory {}", output_dir.display()))?;

    let (reader, writer) = UnixStream::pair().context("creating socket pair for tar pipe")?;

    let merge_handle = thread::spawn(move || merge_layers_into(layers, writer));

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
