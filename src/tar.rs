//! Write merged OCI layers directly to a tar file.

use anyhow::Result;
use std::path::Path;

use crate::{image::LayerBlob, overlay::merge_layers_into};

/// Stream the merged tar of `layers` into a plain tar file at `output`.
pub fn write_tar(layers: Vec<LayerBlob>, output: &Path) -> Result<()> {
    let file = std::fs::File::create(output)?;
    merge_layers_into(layers, file)
}
