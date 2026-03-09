//! Core algorithm: merge OCI layers into a single flat tar stream.
//!
//! File content is never buffered in full — entries are streamed directly
//! into the provided `Write` sink (typically mksquashfs's stdin pipe).
//! The only in-memory state is the tracker data structures and the small
//! hard-link metadata structs deferred to the end.

use anyhow::{Context, Result};
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use tar::{Builder, EntryType};

use crate::{
    PackerProgress,
    canonical::CanonicalTarHeader,
    image::LayerBlob,
    layers::open_layer,
    tracker::{EmittedPathTracker, HardLinkTracker, WhiteoutTracker},
};

/// Process a single layer blob, updating trackers and streaming non-deferred
/// entries into `output`.  Called by both [`merge_layers_into`] and
/// [`merge_layers_into_streaming`].
fn process_layer<W: Write>(
    blob: &LayerBlob,
    whiteout: &mut WhiteoutTracker,
    emitted: &mut EmittedPathTracker,
    hardlinks: &mut HardLinkTracker,
    output: &mut Builder<W>,
) -> Result<()> {
    let mut archive = open_layer(&blob.path, &blob.media_type)
        .with_context(|| format!("opening layer {}", blob.path.display()))?;

    let entries = archive.entries().context("reading tar entries")?;
    for entry_result in entries {
        let mut entry = entry_result.context("reading tar entry")?;
        let raw_path = entry.path().context("entry path")?.into_owned();
        let path = normalize_path(&raw_path);

        // Skip the root directory entry (`./` or `/`), which normalizes to
        // an empty path and is meaningless in a merged tar.
        if path.as_os_str().is_empty() {
            continue;
        }

        // 1. Check whiteout suppression.
        if whiteout.is_suppressed(&path, blob.index) {
            continue;
        }

        // 2. Check already-emitted.
        if emitted.contains(&path) {
            continue;
        }

        // 3. Handle whiteout entries.
        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();

        if file_name == ".wh..wh..opq" {
            let parent = path.parent().unwrap_or(Path::new(""));
            whiteout.insert_opaque(parent, blob.index);
            continue;
        }
        if let Some(real_name) = file_name.strip_prefix(".wh.") {
            let parent = path.parent().unwrap_or(Path::new(""));
            whiteout.insert_simple(&parent.join(real_name), blob.index);
            continue;
        }

        // We capture the header + PAX extensions first (cheap), then
        // stream the entry body straight into the Builder without
        // buffering it in a Vec.
        let canonical =
            CanonicalTarHeader::from_entry(&mut entry).context("capturing entry header")?;

        // 4. Handle hard links — defer, emitting only metadata structs.
        if canonical.entry_type() == EntryType::Link {
            let link_target = canonical
                .link_name()
                .context("reading hard link target")?
                .context("hard link has no target")?;
            let target_path = normalize_path(&link_target);
            if !whiteout.is_suppressed(&target_path, blob.index) {
                hardlinks.record(path, target_path, blob.index, canonical);
            }
            continue;
        }

        // 5. Stream entry directly into the output tar.
        canonical
            .write_to_tar(&path, &mut entry, output)
            .with_context(|| format!("emitting {}", path.display()))?;
        emitted.insert(&path);
    }
    Ok(())
}

/// Merge layers, streaming the resulting tar into `sink`.
///
/// `sink` is typically the stdin pipe of a `mksquashfs` subprocess.
/// File data flows directly from the layer blobs into `sink` without
/// being accumulated in memory.
pub fn merge_layers_into<W: Write>(mut layers: Vec<LayerBlob>, sink: W) -> Result<()> {
    // Process in reverse (newest first).
    layers.sort_by_key(|l| std::cmp::Reverse(l.index));

    let mut whiteout = WhiteoutTracker::default();
    let mut emitted = EmittedPathTracker::default();
    let mut hardlinks = HardLinkTracker::default();

    let mut output = Builder::new(sink);
    output.mode(tar::HeaderMode::Complete);

    for blob in &layers {
        process_layer(
            &blob,
            &mut whiteout,
            &mut emitted,
            &mut hardlinks,
            &mut output,
        )?;
    }

    // Emit deferred hard links (oldest-layer first).
    // These are pure metadata — no file content to stream.
    for hl in hardlinks.drain_sorted() {
        if !emitted.contains(&hl.target_path) {
            continue;
        }
        hl.canonical
            .write_to_tar(&hl.link_path, &[] as &[u8], &mut output)
            .with_context(|| format!("emitting hard link {}", hl.link_path.display()))?;
        emitted.insert(&hl.link_path);
    }

    output.finish()?;
    // Flush and drop the Builder, closing the write end of the pipe so
    // mksquashfs sees EOF and knows the tar stream is complete.
    let mut sink = output.into_inner()?;
    sink.flush()?;
    Ok(())
}

/// Streaming variant of [`merge_layers_into`].
///
/// Layers arrive via `receiver` in arbitrary order as downloads complete.
/// `total_layers` must match the number of layers declared in the manifest.
///
/// After each layer is fully processed, its index is sent on `progress_tx`
/// if one was supplied.  Send failures are silently ignored.
pub fn merge_layers_into_streaming<W: Write>(
    receiver: std::sync::mpsc::Receiver<anyhow::Result<LayerBlob>>,
    total_layers: usize,
    sink: W,
    progress_tx: Option<&std::sync::mpsc::SyncSender<PackerProgress>>,
) -> Result<()> {
    let mut whiteout = WhiteoutTracker::default();
    let mut emitted = EmittedPathTracker::default();
    let mut hardlinks = HardLinkTracker::default();

    let mut output = Builder::new(sink);
    output.mode(tar::HeaderMode::Complete);

    // Resequencing buffer: index → LayerBlob.
    // We process layers newest-first, so next_index counts down from
    // total_layers-1 to 0.
    let mut buffer: std::collections::HashMap<usize, LayerBlob> = std::collections::HashMap::new();
    let mut next_index = total_layers.saturating_sub(1);
    let mut received = 0usize;

    while received < total_layers {
        // Block until the next blob (or error) arrives.
        let blob = match receiver.recv() {
            Ok(Ok(blob)) => blob,
            Ok(Err(e)) => return Err(e).context("download error received on streaming channel"),
            Err(_) => {
                anyhow::bail!(
                    "layer channel closed after {received} of {total_layers} layers; \
                     sender dropped without completing all layers"
                );
            }
        };
        received += 1;
        buffer.insert(blob.index, blob);

        // Drain any contiguous descending run we can now process.
        while let Some(blob) = buffer.remove(&next_index) {
            let idx = blob.index;
            // Notify progress listener that this layer has started processing.
            if let Some(tx) = progress_tx {
                let _ = tx.try_send(PackerProgress::LayerStarted(idx));
            }
            process_layer(
                &blob,
                &mut whiteout,
                &mut emitted,
                &mut hardlinks,
                &mut output,
            )?;
            // Notify progress listener that this layer has been fully processed.
            if let Some(tx) = progress_tx {
                let _ = tx.try_send(PackerProgress::LayerFinished(idx));
            }
            if next_index == 0 {
                break;
            }
            next_index -= 1;
        }
    }

    // Emit deferred hard links (oldest-layer first).
    for hl in hardlinks.drain_sorted() {
        if !emitted.contains(&hl.target_path) {
            continue;
        }
        hl.canonical
            .write_to_tar(&hl.link_path, &[] as &[u8], &mut output)
            .with_context(|| format!("emitting hard link {}", hl.link_path.display()))?;
        emitted.insert(&hl.link_path);
    }

    output.finish()?;
    let mut sink = output.into_inner()?;
    sink.flush()?;
    Ok(())
}

/// Strip leading `./` or `/` from paths.
pub fn normalize_path(p: &Path) -> PathBuf {
    let s = p.to_string_lossy();
    let s = s.trim_start_matches("./").trim_start_matches('/');
    PathBuf::from(s)
}
