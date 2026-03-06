//! Core algorithm: merge OCI layers into a single flat tar stream.
//!
//! Layers are decompressed in parallel (bounded by CPU count) on background
//! threads. The merge loop consumes them in strict layer-index order so that
//! whiteout and emitted-path tracking remain correct.

use anyhow::{Context, Result};
use std::{
    io::{Cursor, Write},
    path::{Path, PathBuf},
};
use tar::{Builder, EntryType, Header};

use crate::{
    decompress::ParallelDecompressor,
    image::LayerBlob,
    tracker::{EmittedPathTracker, HardLinkTracker, WhiteoutTracker},
};

/// Merge layers, streaming the resulting tar into `sink`.
///
/// Layers are decompressed in parallel; the merge loop processes them in
/// reverse index order (newest first) for correct whiteout semantics.
pub fn merge_layers_into<W: Write>(layers: Vec<LayerBlob>, sink: W) -> Result<()> {
    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    // We process newest-first, so reverse the layer order for the decompressor.
    // The decompressor sorts ascending internally, so we flip the indices by
    // re-mapping: effective_index = (num_layers - 1) - original_index.
    // That way the decompressor's ascending order gives us descending original
    // index, and whiteout comparisons still use original indices.
    let num_layers = layers.len();
    let reversed: Vec<LayerBlob> = layers
        .into_iter()
        .map(|mut b| {
            b.index = (num_layers - 1) - b.index;
            b
        })
        .collect();

    let mut decomp = ParallelDecompressor::new(reversed, concurrency);

    let mut whiteout = WhiteoutTracker::default();
    let mut emitted = EmittedPathTracker::default();
    let mut hardlinks = HardLinkTracker::default();
    let mut output = Builder::new(sink);

    // Consume layers in the decompressor's ascending order, which corresponds
    // to descending original index (newest layer first).
    while let Some(layer) = decomp.next_layer() {
        // Recover original layer index for whiteout comparisons.
        let original_index = (num_layers - 1) - layer.layer_index;

        for entry_result in layer.entries() {
            let entry = entry_result.context("decompressed entry")?;
            let path = normalize_path(&entry.path_raw);

            // 1. Whiteout suppression.
            if whiteout.is_suppressed(&path, original_index) {
                continue;
            }

            // 2. Already emitted.
            if emitted.contains(&path) {
                continue;
            }

            // 3. Whiteout entries.
            let file_name = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();

            if file_name == ".wh..wh..opq" {
                let parent = path.parent().unwrap_or(Path::new(""));
                whiteout.insert_opaque(parent, original_index);
                continue;
            }
            if let Some(real_name) = file_name.strip_prefix(".wh.") {
                let parent = path.parent().unwrap_or(Path::new(""));
                whiteout.insert_simple(&parent.join(real_name), original_index);
                continue;
            }

            // 4. Hard links — defer.
            if entry.canonical.entry_type() == EntryType::Link {
                let link_target = entry.canonical
                    .link_name()
                    .context("reading hard link target")?
                    .context("hard link has no target")?;
                let target_path = normalize_path(&link_target);
                if !whiteout.is_suppressed(&target_path, original_index) {
                    hardlinks.record(path, target_path, original_index, entry.canonical);
                }
                continue;
            }

            // 5. Stream entry into output tar.
            entry.canonical
                .write_to_tar(&path, Cursor::new(&entry.data), &mut output)
                .with_context(|| format!("emitting {}", path.display()))?;
            emitted.insert(&path);
        }
    }

    // Emit deferred hard links, oldest-layer first.
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
