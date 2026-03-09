pub mod canonical;
pub mod dir;
pub mod image;
pub mod layers;
pub mod overlay;
pub mod squashfs;
pub mod tar;
pub mod tracker;
pub mod verify;

use anyhow::Result;
use image::LayerBlob;
use std::path::Path;

// ── internal helpers ──────────────────────────────────────────────────────────

/// Load manifest and resolve layer blobs from an OCI image directory,
/// returning them as a channel receiver ready for the streaming merge path.
fn layers_from_image_dir(
    image_dir: &Path,
) -> Result<(std::sync::mpsc::Receiver<Result<LayerBlob>>, usize)> {
    let manifest = image::load_manifest(image_dir)?;
    let layers = image::resolve_layers(image_dir, &manifest)?;
    let total = layers.len();
    let (tx, rx) = std::sync::mpsc::channel();
    for layer in layers {
        tx.send(Ok(layer)).unwrap();
    }
    Ok((rx, total))
}

/// Bridge a tokio mpsc receiver to a std mpsc receiver for handoff into a
/// `spawn_blocking` closure.  Runs as a detached tokio task; the std sender
/// is dropped when the tokio channel closes, signalling EOF to the merge thread.
pub async fn relay_to_blocking(
    mut tokio_rx: tokio::sync::mpsc::Receiver<Result<LayerBlob>>,
    std_tx: std::sync::mpsc::Sender<Result<LayerBlob>>,
) {
    while let Some(item) = tokio_rx.recv().await {
        if std_tx.send(item).is_err() {
            break;
        }
    }
}

/// Bridge a std mpsc receiver to a tokio mpsc sender.  Runs as a detached
/// tokio task.  Used to relay progress events from the blocking merge thread
/// back into the async world without requiring the blocking thread to interact
/// with the tokio runtime directly.
async fn relay_from_blocking(
    std_rx: std::sync::mpsc::Receiver<PackerProgress>,
    tokio_tx: tokio::sync::mpsc::Sender<PackerProgress>,
) {
    // recv() blocks until an item arrives or the sender is dropped.
    // We run this in a spawn_blocking so we don't stall the async runtime.
    let (bridge_tx, mut bridge_rx) = tokio::sync::mpsc::channel(1);
    tokio::task::spawn_blocking(move || {
        while let Ok(item) = std_rx.recv() {
            if bridge_tx.blocking_send(item).is_err() {
                break;
            }
        }
    });
    while let Some(item) = bridge_rx.recv().await {
        if tokio_tx.send(item).await.is_err() {
            break;
        }
    }
}

// ── batch entry points ────────────────────────────────────────────────────────

/// Convert an OCI image directory into a squashfs file.
pub async fn convert_mksquashfs(
    image_dir: &Path,
    output_squashfs: &Path,
    squashfs_binpath: Option<&Path>,
) -> Result<()> {
    let image_dir = image_dir.to_path_buf();
    let output_squashfs = output_squashfs.to_path_buf();
    let squashfs_binpath = squashfs_binpath.map(Path::to_path_buf);

    tokio::task::spawn_blocking(move || {
        let (rx, total) = layers_from_image_dir(&image_dir)?;
        squashfs::write_squashfs(rx, total, &output_squashfs, squashfs_binpath.as_deref())
    })
    .await?
}

/// Convert an OCI image directory into a plain tar file.
pub async fn convert_tar(image_dir: &Path, output_tar: &Path) -> Result<()> {
    let image_dir = image_dir.to_path_buf();
    let output_tar = output_tar.to_path_buf();

    tokio::task::spawn_blocking(move || {
        let (rx, total) = layers_from_image_dir(&image_dir)?;
        tar::write_tar(rx, total, &output_tar)
    })
    .await?
}

/// Extract an OCI image directory directly into `output_dir`.
pub async fn convert_dir(image_dir: &Path, output_dir: &Path) -> Result<()> {
    let image_dir = image_dir.to_path_buf();
    let output_dir = output_dir.to_path_buf();

    tokio::task::spawn_blocking(move || {
        let (rx, total) = layers_from_image_dir(&image_dir)?;
        dir::write_dir(rx, total, &output_dir)
    })
    .await?
}

// ── streaming entry points ────────────────────────────────────────────────────

/// Streaming variant of [`convert_mksquashfs`].
///
/// Layers are delivered via `receiver` as downloads complete, in any order.
/// `total_layers` must equal the number of layers in the manifest.
/// A download error sent as `Err` aborts the merge and terminates mksquashfs.
pub async fn convert_mksquashfs_streaming(
    receiver: tokio::sync::mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output_squashfs: &Path,
    squashfs_binpath: Option<&Path>,
) -> Result<()> {
    let output_squashfs = output_squashfs.to_path_buf();
    let squashfs_binpath = squashfs_binpath.map(Path::to_path_buf);
    let (std_tx, std_rx) = std::sync::mpsc::channel();
    tokio::spawn(relay_to_blocking(receiver, std_tx));

    tokio::task::spawn_blocking(move || {
        squashfs::write_squashfs(
            std_rx,
            total_layers,
            &output_squashfs,
            squashfs_binpath.as_deref(),
        )
    })
    .await?
}

/// Streaming variant of [`convert_tar`].
///
/// On download error the partially written tar file is removed.
pub async fn convert_tar_streaming(
    receiver: tokio::sync::mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output_tar: &Path,
) -> Result<()> {
    let output_tar = output_tar.to_path_buf();
    let (std_tx, std_rx) = std::sync::mpsc::channel();
    tokio::spawn(relay_to_blocking(receiver, std_tx));

    tokio::task::spawn_blocking(move || tar::write_tar(std_rx, total_layers, &output_tar)).await?
}

/// Streaming variant of [`convert_dir`].
///
/// On error the partially populated directory is left in place — callers
/// are responsible for cleanup.
pub async fn convert_dir_streaming(
    receiver: tokio::sync::mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output_dir: &Path,
) -> Result<()> {
    let output_dir = output_dir.to_path_buf();
    let (std_tx, std_rx) = std::sync::mpsc::channel();
    tokio::spawn(relay_to_blocking(receiver, std_tx));

    tokio::task::spawn_blocking(move || dir::write_dir(std_rx, total_layers, &output_dir)).await?
}

// ── PackerProgress ────────────────────────────────────────────────────────────

/// Progress events emitted by [`StreamingPacker`] as layers are processed.
///
/// Sent on the optional channel supplied to [`StreamingPacker::new`].
/// Send failures are silently ignored — the channel is purely advisory.
#[derive(Debug, Clone)]
pub enum PackerProgress {
    /// The merge thread has started processing this layer index.
    LayerStarted(usize),
    /// The merge thread has finished processing this layer index.
    LayerFinished(usize),
}

// ── LayerMeta / StreamingPacker ───────────────────────────────────────────────

/// Metadata about a single layer, captured from the manifest before any
/// downloading begins.  Used by [`StreamingPacker`] to reconstruct a
/// [`LayerBlob`] once the blob file is available on disk.
#[derive(Clone, Debug)]
pub struct LayerMeta {
    pub index: usize,
    pub media_type: String,
}

/// A streaming squashfs packer that accepts layers in any order as they
/// finish downloading.
///
/// Progress notifications are delivered via an optional
/// `tokio::sync::mpsc::Sender<PackerProgress>`.  Internally the blocking
/// merge thread uses a `std::sync::mpsc` channel so it never touches the
/// tokio runtime; a lightweight relay task bridges events back into async
/// land.  Send failures are silently ignored.
pub struct StreamingPacker {
    /// Sender side of the tokio→std relay channel.
    layer_tx: tokio::sync::mpsc::Sender<Result<LayerBlob>>,
    /// Layer metadata indexed by manifest order, for reconstructing LayerBlob.
    metas: Vec<LayerMeta>,
    /// Handle to the spawn_blocking task running mksquashfs + merge.
    squashfs_task: tokio::task::JoinHandle<Result<()>>,
}

impl StreamingPacker {
    /// Construct a `StreamingPacker` and immediately spawn mksquashfs.
    ///
    /// `layer_metas` must contain one entry per layer in manifest order.
    /// `progress_tx`, if supplied, receives each layer index as the merge
    /// thread finishes processing it.  Send errors are ignored.
    pub fn new(
        layer_metas: Vec<LayerMeta>,
        output_path: std::path::PathBuf,
        squashfs_binpath: Option<std::path::PathBuf>,
        progress_tx: Option<tokio::sync::mpsc::Sender<PackerProgress>>,
    ) -> Self {
        let total = layer_metas.len();

        // Layer delivery channel: tokio → std relay → blocking merge thread.
        let (tokio_tx, tokio_rx) = tokio::sync::mpsc::channel::<Result<LayerBlob>>(total.max(1));
        let (std_tx, std_rx) = std::sync::mpsc::channel::<Result<LayerBlob>>();

        tokio::spawn(relay_to_blocking(tokio_rx, std_tx));

        // Progress channel: std (written by blocking thread) → tokio relay → caller.
        // Using std::sync::mpsc here means the blocking merge thread never has
        // to interact with the tokio runtime when emitting progress events.
        let std_progress_tx = if progress_tx.is_some() {
            let (tx, rx) = std::sync::mpsc::sync_channel::<PackerProgress>(total.max(1) * 2);
            if let Some(async_tx) = progress_tx {
                tokio::spawn(relay_from_blocking(rx, async_tx));
            }
            Some(tx)
        } else {
            None
        };

        let squashfs_task = tokio::task::spawn_blocking(move || {
            squashfs::write_squashfs_with_progress(
                std_rx,
                total,
                &output_path,
                squashfs_binpath.as_deref(),
                std_progress_tx,
            )
        });

        StreamingPacker {
            layer_tx: tokio_tx,
            metas: layer_metas,
            squashfs_task,
        }
    }

    /// Notify the packer that a layer blob has finished downloading.
    ///
    /// May be called from any task in any order.  Returns an error only if
    /// the internal channel has closed, which means the packer has already
    /// encountered a fatal error — callers should propagate this and stop
    /// sending further notifications.
    pub async fn notify_layer_ready(&self, index: usize, path: std::path::PathBuf) -> Result<()> {
        let meta = self.metas.get(index).ok_or_else(|| {
            anyhow::anyhow!(
                "layer index {index} out of range (have {} layers)",
                self.metas.len()
            )
        })?;

        let blob = LayerBlob {
            path,
            media_type: meta.media_type.clone(),
            index,
        };

        self.layer_tx
            .send(Ok(blob))
            .await
            .map_err(|_| anyhow::anyhow!("packer channel closed unexpectedly"))?;

        Ok(())
    }

    /// Signal a download error to the packer, causing the merge to abort.
    ///
    /// After calling this, `finish()` will return an error.
    pub async fn notify_error(&self, err: anyhow::Error) {
        // Best-effort — if the channel is already closed the merge has
        // already failed anyway.
        let _ = self.layer_tx.send(Err(err)).await;
    }

    /// Wait for mksquashfs to finish and return its result.
    ///
    /// Must be called after all `notify_layer_ready` / `notify_error` calls.
    /// Dropping the `StreamingPacker` without calling `finish` will leave
    /// the squashfs task running until the internal channel closes.
    pub async fn finish(self) -> Result<()> {
        // Drop the sender so relay_to_blocking sees EOF and closes the std
        // channel, which causes merge_layers_into_streaming to return.
        drop(self.layer_tx);
        self.squashfs_task.await?
    }
}
