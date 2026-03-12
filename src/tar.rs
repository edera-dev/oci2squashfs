//! Write merged OCI layers directly to a tar file.

use anyhow::{Context, Result};
use std::{path::Path, sync::mpsc};

use crate::{PackerProgress, image::LayerBlob, overlay::merge_layers_into_streaming};

/// Stream the merged tar of `layers` into a plain tar file at `output`,
/// emitting progress events on `progress_tx` as each layer is processed.
/// On error the partially written file is removed before returning.
pub fn write_tar_with_progress(
    receiver: mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output: &Path,
    progress_tx: Option<std::sync::mpsc::SyncSender<PackerProgress>>,
) -> Result<()> {
    let file = std::fs::File::create(output)
        .with_context(|| format!("creating tar output {}", output.display()))?;

    let result = merge_layers_into_streaming(receiver, total_layers, file, progress_tx.as_ref());

    if result.is_err() {
        let _ = std::fs::remove_file(output);
    }

    result
}

/// Stream the merged tar of `layers` into a plain tar file at `output`.
/// On error the partially written file is removed before returning.
pub fn write_tar(
    receiver: mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output: &Path,
) -> Result<()> {
    write_tar_with_progress(receiver, total_layers, output, None)
}
