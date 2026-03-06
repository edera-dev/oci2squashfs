//! Parallel layer decompression with bounded per-layer channels.
//!
//! Each layer is decompressed on its own thread. A semaphore limits how many
//! layers decompress concurrently. Each layer's entries are sent through a
//! bounded channel so that fast decompressors naturally back-pressure when the
//! consumer (the merge loop) is busy on an earlier layer.
//!
//! The consumer calls `ParallelDecompressor::next_layer()` to get an iterator
//! over a single layer's entries in order, then advances to the next layer.
//! Layers are always consumed in index order, preserving merge correctness.

use std::{
    sync::{Arc, Mutex},
    thread,
};

use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};

use crate::{
    canonical::CanonicalTarHeader,
    image::LayerBlob,
    layers::open_layer,
};

// How many entries to buffer per layer channel before the decompression
// thread blocks. Tune this to trade memory for parallelism.
const CHANNEL_BOUND: usize = 64;

// ─── Wire type ───────────────────────────────────────────────────────────────

/// One tar entry transported from a decompression thread to the merge loop.
pub struct DecompressedEntry {
    pub path_raw: std::path::PathBuf,
    pub canonical: CanonicalTarHeader,
    /// File content. Empty for dirs, symlinks, hard links, etc.
    pub data: Vec<u8>,
}

/// Sent as the final message on a layer's channel to report success or failure.
pub type LayerResult = Result<()>;

// ─── Per-layer receiver ───────────────────────────────────────────────────────

/// Receives entries for a single layer, then a terminal result.
pub struct LayerReceiver {
    pub layer_index: usize,
    pub media_type: String,
    entry_rx: Receiver<Result<DecompressedEntry>>,
}

impl LayerReceiver {
    /// Iterate over entries. Returns `Err` if the decompression thread failed.
    pub fn entries(&self) -> impl Iterator<Item = Result<DecompressedEntry>> + '_ {
        self.entry_rx.iter()
    }
}

// ─── Semaphore ────────────────────────────────────────────────────────────────

/// Simple counting semaphore for limiting concurrent decompression threads.
#[derive(Clone)]
struct Semaphore {
    inner: Arc<(Mutex<usize>, std::sync::Condvar)>,
}

impl Semaphore {
    fn new(n: usize) -> Self {
        Self {
            inner: Arc::new((Mutex::new(n), std::sync::Condvar::new())),
        }
    }

    fn acquire(&self) {
        let (lock, cvar) = &*self.inner;
        let mut count = lock.lock().unwrap();
        while *count == 0 {
            count = cvar.wait(count).unwrap();
        }
        *count -= 1;
    }

    fn release(&self) {
        let (lock, cvar) = &*self.inner;
        *lock.lock().unwrap() += 1;
        cvar.notify_one();
    }
}

// ─── ParallelDecompressor ────────────────────────────────────────────────────

/// Spawns one decompression thread per layer, gated by a semaphore.
/// Call `next_layer()` to consume layers in index order.
pub struct ParallelDecompressor {
    receivers: std::collections::VecDeque<LayerReceiver>,
}

impl ParallelDecompressor {
    /// Spawn decompression threads for all layers.
    /// `concurrency` controls how many decompress simultaneously.
    pub fn new(mut layers: Vec<LayerBlob>, concurrency: usize) -> Self {
        // Sort ascending by index — consumer processes them in this order.
        layers.sort_by_key(|l| l.index);

        let sem = Semaphore::new(concurrency);
        let mut receivers = std::collections::VecDeque::with_capacity(layers.len());

        for blob in layers {
            let (entry_tx, entry_rx) = bounded::<Result<DecompressedEntry>>(CHANNEL_BOUND);
            let sem = sem.clone();

            let layer_index = blob.index;
            let media_type = blob.media_type.clone();

            thread::spawn(move || {
                sem.acquire();
                decompress_layer(blob, entry_tx);
                sem.release();
            });

            receivers.push_back(LayerReceiver {
                layer_index,
                media_type,
                entry_rx,
            });
        }

        Self { receivers }
    }

    /// Return the next layer's receiver, in ascending index order.
    pub fn next_layer(&mut self) -> Option<LayerReceiver> {
        self.receivers.pop_front()
    }
}

// ─── Worker ───────────────────────────────────────────────────────────────────

fn decompress_layer(blob: LayerBlob, tx: Sender<Result<DecompressedEntry>>) {
    let result = do_decompress(&blob, &tx);
    if let Err(e) = result {
        // Send the error as the terminal message so the consumer sees it.
        let _ = tx.send(Err(e));
    }
    // Dropping `tx` closes the channel; the consumer's iterator will end.
}

fn do_decompress(
    blob: &LayerBlob,
    tx: &Sender<Result<DecompressedEntry>>,
) -> Result<()> {
    let mut archive = open_layer(&blob.path, &blob.media_type)
        .with_context(|| format!("opening layer {}", blob.path.display()))?;

    let entries = archive.entries().context("reading tar entries")?;
    for entry_result in entries {
        let mut entry = entry_result.context("reading tar entry")?;

        let path_raw = entry.path().context("entry path")?.into_owned();
        let canonical = CanonicalTarHeader::from_entry(&mut entry)
            .context("capturing entry header")?;

        // Only read data for entry types that carry a body.
        let data = if canonical.entry_type().is_file() {
            let mut buf = Vec::new();
            std::io::copy(&mut entry, &mut buf).context("reading entry data")?;
            buf
        } else {
            Vec::new()
        };

        if tx.send(Ok(DecompressedEntry { path_raw, canonical, data })).is_err() {
            // Consumer dropped the receiver (e.g. due to an error). Stop.
            break;
        }
    }
    Ok(())
}
