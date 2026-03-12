//! Verification: diff a generated image against a reference directory.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    io::Read,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::TempDir;

use crate::ImageSpec;

// ─── RAII mount guard ─────────────────────────────────────────────────────────

/// Mounts a squashfs via squashfuse and unmounts it on drop, even if an error
/// occurs during the walk. The underlying `TempDir` is kept alive for the
/// lifetime of this guard so the mountpoint isn't deleted while still mounted.
struct SquashMount {
    mountpoint: TempDir,
}

impl SquashMount {
    fn new(squashfs: &Path) -> Result<Self> {
        let mountpoint = TempDir::new().context("creating temp mount dir")?;
        let status = Command::new("squashfuse")
            .arg(squashfs)
            .arg(mountpoint.path())
            .status()
            .context("spawning squashfuse — is it installed?")?;
        if !status.success() {
            anyhow::bail!("squashfuse failed with status {status}");
        }
        Ok(Self { mountpoint })
    }

    fn path(&self) -> &Path {
        self.mountpoint.path()
    }
}

impl Drop for SquashMount {
    fn drop(&mut self) {
        // Try fusermount first (Linux), fall back to umount (macOS/BSD).
        let ok = Command::new("fusermount")
            .args(["-u", self.mountpoint.path().to_str().unwrap_or("")])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        if !ok {
            let _ = Command::new("umount").arg(self.mountpoint.path()).status();
        }
        // TempDir::drop runs after this and removes the now-unmounted directory.
    }
}

// ─── Public API ───────────────────────────────────────────────────────────────

/// The result of comparing a generated image against a reference directory.
#[derive(Debug)]
pub struct VerifyReport {
    /// Paths present in the generated image but absent from the reference.
    pub only_in_generated: Vec<PathBuf>,
    /// Paths present in the reference but absent from the generated image.
    pub only_in_reference: Vec<PathBuf>,
    pub differences: Vec<FileDiff>,
}

impl VerifyReport {
    pub fn is_clean(&self) -> bool {
        self.only_in_generated.is_empty()
            && self.only_in_reference.is_empty()
            && self.differences.is_empty()
    }
}

#[derive(Debug)]
pub struct FileDiff {
    pub path: PathBuf,
    pub detail: String,
}

/// Verify a generated image described by `spec` against a `reference`
/// directory.
///
/// - `ImageSpec::Squashfs` — mounts via squashfuse, diffs the mount against
///   `reference`, then unmounts.
/// - `ImageSpec::Dir` — diffs the directory directly against `reference`.
/// - `ImageSpec::Tar` — returns `Err`: tar verification is not supported
///   directly. Extract to a directory first with `convert-dir`, then verify.
pub fn verify(spec: ImageSpec, reference: &Path) -> Result<VerifyReport> {
    match spec {
        ImageSpec::Squashfs { path, .. } => {
            let mount = SquashMount::new(&path)?;
            verify_dirs(mount.path(), reference)
        }
        ImageSpec::Dir { path } => verify_dirs(&path, reference),
        ImageSpec::Tar { .. } => anyhow::bail!(
            "tar verification is not supported directly; \
             extract to a directory with convert-dir first, then use --dir"
        ),
    }
}

/// Compare two directories and return a [`VerifyReport`] describing any
/// differences. This is the core primitive used by [`verify`].
pub(crate) fn verify_dirs(generated: &Path, reference: &Path) -> Result<VerifyReport> {
    let generated_tree = walk_tree(generated).context("walking generated directory")?;
    let reference_tree = walk_tree(reference).context("walking reference directory")?;

    let mut report = VerifyReport {
        only_in_generated: Vec::new(),
        only_in_reference: Vec::new(),
        differences: Vec::new(),
    };

    for (rel, gen_info) in &generated_tree {
        match reference_tree.get(rel) {
            None => report.only_in_generated.push(rel.clone()),
            Some(ref_info) => report
                .differences
                .extend(compare_entries(rel, gen_info, ref_info)),
        }
    }
    for rel in reference_tree.keys() {
        if !generated_tree.contains_key(rel) {
            report.only_in_reference.push(rel.clone());
        }
    }

    Ok(report)
}

// ─── Tree walking ─────────────────────────────────────────────────────────────

#[derive(Debug)]
struct EntryInfo {
    kind: EntryKind,
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    symlink_target: Option<PathBuf>,
    sha256: Option<String>,
}

#[derive(Debug, PartialEq)]
enum EntryKind {
    File,
    Dir,
    Symlink,
    Other,
}

fn walk_tree(root: &Path) -> Result<HashMap<PathBuf, EntryInfo>> {
    let mut map = HashMap::new();
    walk_dir(root, root, &mut map)?;
    Ok(map)
}

fn walk_dir(root: &Path, current: &Path, map: &mut HashMap<PathBuf, EntryInfo>) -> Result<()> {
    for entry in
        fs::read_dir(current).with_context(|| format!("reading dir {}", current.display()))?
    {
        let entry = entry?;
        let abs = entry.path();
        let rel = abs
            .strip_prefix(root)
            .context("strip prefix")?
            .to_path_buf();
        let meta = fs::symlink_metadata(&abs)
            .with_context(|| format!("metadata for {}", abs.display()))?;
        let ft = meta.file_type();

        let (kind, symlink_target, sha256) = if ft.is_symlink() {
            (EntryKind::Symlink, Some(fs::read_link(&abs)?), None)
        } else if ft.is_file() {
            (EntryKind::File, None, Some(hash_file(&abs)?))
        } else if ft.is_dir() {
            (EntryKind::Dir, None, None)
        } else {
            (EntryKind::Other, None, None)
        };

        // Mask to permission bits (rwxrwxrwx + setuid/setgid/sticky).
        // File type bits are captured separately in `kind`.
        const PERMISSION_BITS: u32 = 0o7777;

        map.insert(
            rel,
            EntryInfo {
                kind,
                mode: meta.permissions().mode() & PERMISSION_BITS,
                uid: meta.uid(),
                gid: meta.gid(),
                size: meta.len(),
                symlink_target,
                sha256,
            },
        );

        if ft.is_dir() {
            walk_dir(root, &abs, map)?;
        }
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

// ─── Comparison ───────────────────────────────────────────────────────────────

fn compare_entries(rel: &Path, generated: &EntryInfo, reference: &EntryInfo) -> Vec<FileDiff> {
    let mut diffs = Vec::new();
    macro_rules! diff {
        ($msg:expr) => {
            diffs.push(FileDiff {
                path: rel.to_path_buf(),
                detail: $msg,
            })
        };
    }

    if generated.kind != reference.kind {
        diff!(format!(
            "type mismatch: generated={:?} reference={:?}",
            generated.kind, reference.kind
        ));
        return diffs;
    }
    if generated.mode != reference.mode {
        diff!(format!(
            "mode: generated={:04o} reference={:04o}",
            generated.mode, reference.mode
        ));
    }
    if generated.uid != reference.uid {
        diff!(format!(
            "uid: generated={} reference={}",
            generated.uid, reference.uid
        ));
    }
    if generated.gid != reference.gid {
        diff!(format!(
            "gid: generated={} reference={}",
            generated.gid, reference.gid
        ));
    }
    if generated.symlink_target != reference.symlink_target {
        diff!(format!(
            "symlink target: generated={:?} reference={:?}",
            generated.symlink_target, reference.symlink_target
        ));
    }
    if generated.kind == EntryKind::File {
        if generated.size != reference.size {
            diff!(format!(
                "size: generated={} reference={}",
                generated.size, reference.size
            ));
        }
        if generated.sha256 != reference.sha256 {
            diff!("sha256 mismatch".into());
        }
    }
    diffs
}
