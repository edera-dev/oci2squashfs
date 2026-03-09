//! Write merged OCI layers directly to a tar file.

use anyhow::{Context, Result};
use std::{path::Path, sync::mpsc};

use crate::{image::LayerBlob, overlay::merge_layers_into_streaming};

/// Stream the merged tar of `layers` into a plain tar file at `output`.
/// On error the partially written file is removed before returning.
pub fn write_tar(
    receiver: mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output: &Path,
) -> Result<()> {
    let file = std::fs::File::create(output)
        .with_context(|| format!("creating tar output {}", output.display()))?;

    let result = merge_layers_into_streaming(receiver, total_layers, file, None);

    if result.is_err() {
        // Remove the partial file so callers don't see a corrupt tar.
        let _ = std::fs::remove_file(output);
    }

    result
}
