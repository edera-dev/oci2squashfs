//! Spawn mksquashfs and stream the merged tar directly into its stdin.

use anyhow::{Context, Result, bail};
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
};

use crate::{PackerProgress, image::LayerBlob, overlay::merge_layers_into_streaming};

/// Convert `layers` into a squashfs image at `output` by streaming a merged
/// tar directly into mksquashfs's stdin.
pub fn write_squashfs(
    receiver: mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output: &Path,
    squashfs_binpath: Option<&Path>,
) -> Result<()> {
    write_squashfs_with_progress(receiver, total_layers, output, squashfs_binpath, None)
}

/// Like [`write_squashfs`] but emits progress events on `progress_tx` as the
/// merge thread processes each layer.
///
/// Uses `std::sync::mpsc::SyncSender` so the blocking merge thread never
/// needs to interact with the tokio runtime.  Send failures are silently
/// ignored.
pub fn write_squashfs_with_progress(
    receiver: mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output: &Path,
    squashfs_binpath: Option<&Path>,
    progress_tx: Option<std::sync::mpsc::SyncSender<PackerProgress>>,
) -> Result<()> {
    if output.exists() {
        std::fs::remove_file(output)
            .with_context(|| format!("removing existing {}", output.display()))?;
    }

    let mut child = spawn_mksquashfs(output, squashfs_binpath)?;
    let stdin = child.stdin.take().context("child stdin")?;

    // stdin is moved into merge_layers_into_streaming and dropped when it
    // returns, closing the write end of the pipe.  mksquashfs sees EOF and
    // exits cleanly regardless of whether the merge succeeded or failed.
    let merge_result =
        merge_layers_into_streaming(receiver, total_layers, stdin, progress_tx.as_ref());

    let exit = child.wait_with_output().context("waiting for mksquashfs")?;

    if merge_result.is_err() {
        // Remove the partial output so callers don't see a corrupt squashfs.
        let _ = std::fs::remove_file(output);
        // Surface the merge error — it's more actionable than the mksquashfs
        // exit status, which is usually just a consequence of the pipe closing.
        return merge_result.context("merging layers into mksquashfs stdin");
    }

    if !exit.status.success() {
        let _ = std::fs::remove_file(output);
        let stderr = String::from_utf8_lossy(&exit.stderr);
        bail!("mksquashfs failed:\n{stderr}");
    }

    Ok(())
}

fn spawn_mksquashfs(output: &Path, binpath: Option<&Path>) -> Result<Child> {
    let mut cmd = match binpath {
        Some(p) => Command::new(p),
        None => Command::new("mksquashfs"),
    };
    cmd.args([
        "-",
        output.to_str().context("output path is not UTF-8")?,
        "-tar",
        "-noappend",
        "-no-fragments",
        "-comp",
        "zstd",
        "-Xcompression-level",
        "2",
        "-quiet",
        // Ensure the root directory gets mode 0755 and uid/gid 0 regardless
        // of what the invoking user's identity is.  Without these, mksquashfs
        // uses the invoking user's uid/gid and 0777 for intermediate
        // directories that have no explicit tar entry.
        "-default-mode",
        "0755",
        "-default-uid",
        "0",
        "-default-gid",
        "0",
    ])
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .context("spawning mksquashfs — is it installed?")
}
