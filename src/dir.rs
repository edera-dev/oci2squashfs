//! Extract merged OCI layers directly into a directory.

use anyhow::{Context, Result};
use std::{os::unix::net::UnixStream, path::Path, sync::mpsc, thread};

use crate::{PackerProgress, image::LayerBlob, overlay::merge_layers_into_streaming};

/// Extract the merged OCI layers into `output_dir`, emitting progress events
/// on `progress_tx` as each layer is processed.
///
/// Internally this pipes the merged tar through a `UnixStream` pair: the merge
/// thread writes to the write end, and `tar::Archive::unpack` consumes the
/// read end on the calling thread. The two run concurrently; errors from either
/// side are surfaced after both complete.
///
/// On error the partially populated directory is left in place — callers are
/// responsible for cleanup.
pub fn write_dir_with_progress(
    receiver: mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output_dir: &Path,
    progress_tx: Option<std::sync::mpsc::SyncSender<PackerProgress>>,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("creating output directory {}", output_dir.display()))?;

    let (reader, writer) = UnixStream::pair().context("creating socket pair for tar pipe")?;

    let merge_handle = thread::spawn(move || {
        merge_layers_into_streaming(receiver, total_layers, writer, progress_tx.as_ref())
    });

    let unpack_result = (|| {
        let mut archive = tar::Archive::new(reader);
        archive.set_preserve_permissions(true);
        archive.set_preserve_mtime(true);
        archive
            .unpack(output_dir)
            .context("unpacking merged tar into output directory")
    })();

    let merge_result = merge_handle.join().expect("merge thread panicked");

    // Prefer the merge error if both fail: it's more likely to be the root
    // cause (e.g. a corrupt layer blob) rather than a downstream consequence
    // of the pipe closing unexpectedly.
    merge_result.and(unpack_result)
}

/// Extract the merged OCI layers into `output_dir`.
///
/// On error the partially populated directory is left in place — callers are
/// responsible for cleanup.
pub fn write_dir(
    receiver: mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output_dir: &Path,
) -> Result<()> {
    write_dir_with_progress(receiver, total_layers, output_dir, None)
}
