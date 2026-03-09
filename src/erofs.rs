//! Spawn mkfs.erofs and stream the merged tar directly into its stdin.

use anyhow::{bail, Context, Result};
use std::{
    path::Path,
    process::{Command, Stdio},
};

use crate::{image::LayerBlob, overlay::merge_layers_into};

/// Convert `layers` into a erofs image at `output` by streaming a merged
/// tar directly into mkfs.erofs's stdin. No full tar buffer is held in memory.
pub fn write_erofs(layers: Vec<LayerBlob>, output: &Path, mkfs_erofs_binpath: Option<&Path>) -> Result<()> {
    if output.exists() {
        std::fs::remove_file(output)
            .with_context(|| format!("removing existing {}", output.display()))?;
    }

    let mut child = match mkfs_erofs_binpath {
        Some(path) => Command::new(path),
        None => Command::new("mkfs.erofs"),
    }
    .args([
        "-L",
        "root",
        "--tar=f",
        output.to_str().context("output path is not UTF-8")?,
        "/dev/stdin",
    ])
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .context("spawning mkfs.erofs — is it installed?")?;

    let stdin = child.stdin.take().context("child stdin")?;

    // Drive the merge on the current thread, writing directly into the pipe.
    // mkfs.erofs reads and compresses concurrently in its own process, so
    // the pipe provides natural backpressure without a helper thread.
    let merge_result = merge_layers_into(layers, stdin);

    // Wait for mkfs.erofs regardless of whether the merge succeeded, so we
    // don't leave a zombie process behind.
    let exit = child.wait_with_output().context("waiting for mkfs.erofs")?;

    // Surface merge errors before subprocess errors — they're more actionable.
    merge_result.context("merging layers into mkfs.erofs stdin")?;

    if !exit.status.success() {
        let stderr = String::from_utf8_lossy(&exit.stderr);
        bail!("mkfs.erofs failed:\n{stderr}");
    }

    Ok(())
}
