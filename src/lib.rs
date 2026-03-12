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
use std::path::{Path, PathBuf};

// ── ImageSpec ─────────────────────────────────────────────────────────────────

/// Describes the format, location, and any format-specific configuration of an
/// image. Direction-neutral: used both as a conversion output target and as a
/// verification input source.
#[derive(Clone, Debug)]
pub enum ImageSpec {
    /// A squashfs filesystem image. `binpath` overrides the `mksquashfs` binary
    /// location when writing; if `None`, `mksquashfs` is resolved from `PATH`.
    Squashfs {
        path: PathBuf,
        binpath: Option<PathBuf>,
    },
    /// A plain tar archive.
    Tar { path: PathBuf },
    /// A directory containing the extracted filesystem.
    Dir { path: PathBuf },
}

impl ImageSpec {
    /// The filesystem path this spec refers to, regardless of variant.
    pub fn path(&self) -> &Path {
        match self {
            Self::Squashfs { path, .. } | Self::Tar { path } | Self::Dir { path } => path,
        }
    }
}

// ── PackerProgress ────────────────────────────────────────────────────────────

/// Progress events emitted by [`StreamingPacker`] and the underlying merge
/// engine as layers are processed.
///
/// Applies equally to all output formats: the events describe progress through
/// the overlay merge, which is format-agnostic.
#[derive(Debug, Clone)]
pub enum PackerProgress {
    /// The merge thread has started processing this layer index.
    LayerStarted(usize),
    /// The merge thread has finished processing this layer index.
    LayerFinished(usize),
}

// ── LayerMeta ─────────────────────────────────────────────────────────────────

/// Metadata about a single layer, captured from the manifest before any
/// downloading begins. Used by [`StreamingPacker`] to reconstruct a
/// [`LayerBlob`] once the blob file is available on disk.
#[derive(Clone, Debug)]
pub struct LayerMeta {
    pub index: usize,
    pub media_type: String,
}

// ── internal helpers ──────────────────────────────────────────────────────────

/// Load manifest and resolve layer blobs from an OCI image directory,
/// pre-loading them into a std channel receiver so the batch path can reuse
/// the streaming merge implementation.
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

/// Create a paired tokio→std channel bridge for layer delivery.
///
/// Returns a tokio sender for async callers to push `LayerBlob`s into, and a
/// std receiver to hand off to a `spawn_blocking` merge thread. Spawns a
/// detached relay task; the std sender is dropped when the tokio channel
/// closes, which signals EOF to the merge thread.
fn make_layer_channel(
    cap: usize,
) -> (
    tokio::sync::mpsc::Sender<Result<LayerBlob>>,
    std::sync::mpsc::Receiver<Result<LayerBlob>>,
) {
    let (tokio_tx, tokio_rx) = tokio::sync::mpsc::channel(cap.max(1));
    let (std_tx, std_rx) = std::sync::mpsc::channel();
    tokio::spawn(relay_to_blocking(tokio_rx, std_tx));
    (tokio_tx, std_rx)
}

/// Forward items from a tokio mpsc receiver to a std mpsc sender.
/// Detached relay task; std sender drop signals EOF to the blocking side.
async fn relay_to_blocking(
    mut tokio_rx: tokio::sync::mpsc::Receiver<Result<LayerBlob>>,
    std_tx: std::sync::mpsc::Sender<Result<LayerBlob>>,
) {
    while let Some(item) = tokio_rx.recv().await {
        if std_tx.send(item).is_err() {
            break;
        }
    }
}

/// Forward progress events from a std mpsc receiver back into the async world.
/// Uses an intermediate spawn_blocking so the calling async task is never
/// blocked waiting on the std receiver.
async fn relay_from_blocking(
    std_rx: std::sync::mpsc::Receiver<PackerProgress>,
    tokio_tx: tokio::sync::mpsc::Sender<PackerProgress>,
) {
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

/// Dispatch a std layer receiver to the appropriate write function for the
/// given `ImageSpec`. This is the single point where format selection happens
/// in the blocking world.
fn write_for_spec(
    receiver: std::sync::mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    spec: ImageSpec,
    progress_tx: Option<std::sync::mpsc::SyncSender<PackerProgress>>,
) -> Result<()> {
    match spec {
        ImageSpec::Squashfs { path, binpath } => squashfs::write_squashfs_with_progress(
            receiver,
            total_layers,
            &path,
            binpath.as_deref(),
            progress_tx,
        ),
        ImageSpec::Tar { path } => {
            tar::write_tar_with_progress(receiver, total_layers, &path, progress_tx)
        }
        ImageSpec::Dir { path } => {
            dir::write_dir_with_progress(receiver, total_layers, &path, progress_tx)
        }
    }
}

// ── Unified convert entry point ───────────────────────────────────────────────

/// Convert an OCI image directory into the format and location described by
/// `spec`.
pub async fn convert(image_dir: &Path, spec: ImageSpec) -> Result<()> {
    let image_dir = image_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let (rx, total) = layers_from_image_dir(&image_dir)?;
        write_for_spec(rx, total, spec, None)
    })
    .await?
}

// ── Batch compatibility wrappers ──────────────────────────────────────────────

/// Convert an OCI image directory into a squashfs file.
pub async fn convert_mksquashfs(
    image_dir: &Path,
    output_squashfs: &Path,
    squashfs_binpath: Option<&Path>,
) -> Result<()> {
    convert(
        image_dir,
        ImageSpec::Squashfs {
            path: output_squashfs.to_path_buf(),
            binpath: squashfs_binpath.map(Path::to_path_buf),
        },
    )
    .await
}

/// Convert an OCI image directory into a plain tar file.
pub async fn convert_tar(image_dir: &Path, output_tar: &Path) -> Result<()> {
    convert(
        image_dir,
        ImageSpec::Tar {
            path: output_tar.to_path_buf(),
        },
    )
    .await
}

/// Extract an OCI image directory directly into `output_dir`.
pub async fn convert_dir(image_dir: &Path, output_dir: &Path) -> Result<()> {
    convert(
        image_dir,
        ImageSpec::Dir {
            path: output_dir.to_path_buf(),
        },
    )
    .await
}

// ── Streaming compatibility wrappers ─────────────────────────────────────────

/// Streaming variant of [`convert_mksquashfs`].
pub async fn convert_mksquashfs_streaming(
    receiver: tokio::sync::mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output_squashfs: &Path,
    squashfs_binpath: Option<&Path>,
) -> Result<()> {
    let spec = ImageSpec::Squashfs {
        path: output_squashfs.to_path_buf(),
        binpath: squashfs_binpath.map(Path::to_path_buf),
    };
    let (std_tx, std_rx) = std::sync::mpsc::channel();
    tokio::spawn(relay_to_blocking(receiver, std_tx));
    tokio::task::spawn_blocking(move || write_for_spec(std_rx, total_layers, spec, None)).await?
}

/// Streaming variant of [`convert_tar`].
pub async fn convert_tar_streaming(
    receiver: tokio::sync::mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output_tar: &Path,
) -> Result<()> {
    let spec = ImageSpec::Tar {
        path: output_tar.to_path_buf(),
    };
    let (std_tx, std_rx) = std::sync::mpsc::channel();
    tokio::spawn(relay_to_blocking(receiver, std_tx));
    tokio::task::spawn_blocking(move || write_for_spec(std_rx, total_layers, spec, None)).await?
}

/// Streaming variant of [`convert_dir`].
pub async fn convert_dir_streaming(
    receiver: tokio::sync::mpsc::Receiver<Result<LayerBlob>>,
    total_layers: usize,
    output_dir: &Path,
) -> Result<()> {
    let spec = ImageSpec::Dir {
        path: output_dir.to_path_buf(),
    };
    let (std_tx, std_rx) = std::sync::mpsc::channel();
    tokio::spawn(relay_to_blocking(receiver, std_tx));
    tokio::task::spawn_blocking(move || write_for_spec(std_rx, total_layers, spec, None)).await?
}

// ── StreamingPacker ───────────────────────────────────────────────────────────

/// A streaming packer that accepts layers in any order as they finish
/// downloading and writes to any output format supported by [`ImageSpec`].
///
/// Progress notifications are delivered via an optional
/// `tokio::sync::mpsc::Sender<PackerProgress>`. The blocking merge thread uses
/// a `std::sync::mpsc` channel internally and never touches the tokio runtime;
/// a relay task bridges events back into async land.
pub struct StreamingPacker {
    layer_tx: tokio::sync::mpsc::Sender<Result<LayerBlob>>,
    metas: Vec<LayerMeta>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl StreamingPacker {
    /// Construct a `StreamingPacker` and immediately begin processing.
    ///
    /// `spec` determines the output format, path, and any format-specific
    /// options (e.g. `mksquashfs` binary path).
    /// `layer_metas` must contain one entry per layer in manifest order.
    /// `progress_tx`, if supplied, receives layer events as the merge thread
    /// processes them. Send failures are silently ignored.
    pub fn new(
        layer_metas: Vec<LayerMeta>,
        spec: ImageSpec,
        progress_tx: Option<tokio::sync::mpsc::Sender<PackerProgress>>,
    ) -> Self {
        let total = layer_metas.len();
        let (tokio_tx, std_rx) = make_layer_channel(total);

        let std_progress_tx = if let Some(async_tx) = progress_tx {
            let (tx, rx) = std::sync::mpsc::sync_channel::<PackerProgress>(total.max(1) * 2);
            tokio::spawn(relay_from_blocking(rx, async_tx));
            Some(tx)
        } else {
            None
        };

        let task = tokio::task::spawn_blocking(move || {
            write_for_spec(std_rx, total, spec, std_progress_tx)
        });

        StreamingPacker {
            layer_tx: tokio_tx,
            metas: layer_metas,
            task,
        }
    }

    /// Notify the packer that a layer blob has finished downloading.
    ///
    /// May be called from any task in any order. Returns an error only if the
    /// internal channel has closed, indicating the packer has already hit a
    /// fatal error — callers should stop sending and propagate.
    pub async fn notify_layer_ready(&self, index: usize, path: PathBuf) -> Result<()> {
        let meta = self.metas.get(index).ok_or_else(|| {
            anyhow::anyhow!(
                "layer index {index} out of range (have {} layers)",
                self.metas.len()
            )
        })?;

        self.layer_tx
            .send(Ok(LayerBlob {
                path,
                media_type: meta.media_type.clone(),
                index,
            }))
            .await
            .map_err(|_| anyhow::anyhow!("packer channel closed unexpectedly"))
    }

    /// Signal a download error to the packer, causing the merge to abort.
    /// After calling this, `finish()` will return an error.
    pub async fn notify_error(&self, err: anyhow::Error) {
        let _ = self.layer_tx.send(Err(err)).await;
    }

    /// Wait for the output to be finalised and return the result.
    ///
    /// Must be called after all `notify_layer_ready` / `notify_error` calls.
    pub async fn finish(self) -> Result<()> {
        // Drop the sender so the relay task sees EOF and closes the std
        // channel, unblocking the merge thread's recv loop.
        drop(self.layer_tx);
        self.task.await?
    }
}
