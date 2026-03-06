//! Verification helper: mount squashfs and diff against a reference directory.

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

// Mask to permission bits only (rwxrwxrwx + setuid/setgid/sticky).
// File type bits are captured separately in `kind`.
const PERMISSION_BITS: u32 = 0o7777;

#[derive(Debug)]
pub struct VerifyReport {
    pub only_in_squashfs: Vec<PathBuf>,
    pub only_in_reference: Vec<PathBuf>,
    pub differences: Vec<FileDiff>,
}

#[derive(Debug)]
pub struct FileDiff {
    pub path: PathBuf,
    pub detail: String,
}

pub fn verify(squashfs: &Path, reference: &Path) -> Result<VerifyReport> {
    let mount_dir = TempDir::new().context("creating temp mount dir")?;
    mount_squashfuse(squashfs, mount_dir.path())?;

    let squashfs_tree = walk_tree(mount_dir.path()).context("walking squashfs mount")?;
    let reference_tree = walk_tree(reference).context("walking reference directory")?;

    let mut report = VerifyReport {
        only_in_squashfs: Vec::new(),
        only_in_reference: Vec::new(),
        differences: Vec::new(),
    };

    for (rel, sq_info) in &squashfs_tree {
        match reference_tree.get(rel) {
            None => report.only_in_squashfs.push(rel.clone()),
            Some(ref_info) => {
                let diffs = compare_entries(rel, sq_info, ref_info);
                report.differences.extend(diffs);
            }
        }
    }
    for rel in reference_tree.keys() {
        if !squashfs_tree.contains_key(rel) {
            report.only_in_reference.push(rel.clone());
        }
    }

    // squashfuse will be unmounted when mount_dir is dropped (FUSE auto-unmounts).
    Ok(report)
}

fn mount_squashfuse(squashfs: &Path, mountpoint: &Path) -> Result<()> {
    let status = Command::new("squashfuse")
        .arg(squashfs)
        .arg(mountpoint)
        .status()
        .context("spawning squashfuse — is it installed?")?;
    if !status.success() {
        anyhow::bail!("squashfuse failed with status {status}");
    }
    Ok(())
}

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
            let target = fs::read_link(&abs)?;
            (EntryKind::Symlink, Some(target), None)
        } else if ft.is_file() {
            let hash = hash_file(&abs)?;
            (EntryKind::File, None, Some(hash))
        } else if ft.is_dir() {
            (EntryKind::Dir, None, None)
        } else {
            (EntryKind::Other, None, None)
        };

        let info = EntryInfo {
            kind,
            mode: meta.permissions().mode() & PERMISSION_BITS,
            uid: meta.uid(),
            gid: meta.gid(),
            size: meta.len(),
            symlink_target,
            sha256,
        };
        map.insert(rel, info);

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

fn compare_entries(rel: &Path, sq: &EntryInfo, rf: &EntryInfo) -> Vec<FileDiff> {
    let mut diffs = Vec::new();
    macro_rules! diff {
        ($msg:expr) => {
            diffs.push(FileDiff {
                path: rel.to_path_buf(),
                detail: $msg,
            })
        };
    }
    if sq.kind != rf.kind {
        diff!(format!(
            "type mismatch: squashfs={:?} ref={:?}",
            sq.kind, rf.kind
        ));
        return diffs;
    }
    if sq.mode != rf.mode {
        diff!(format!(
            "mode: squashfs={:04o} ref={:04o}",
            sq.mode, rf.mode
        ));
    }
    if sq.uid != rf.uid {
        diff!(format!("uid: squashfs={} ref={}", sq.uid, rf.uid));
    }
    if sq.gid != rf.gid {
        diff!(format!("gid: squashfs={} ref={}", sq.gid, rf.gid));
    }
    if sq.symlink_target != rf.symlink_target {
        diff!(format!(
            "symlink target: squashfs={:?} ref={:?}",
            sq.symlink_target, rf.symlink_target
        ));
    }
    if sq.kind == EntryKind::File {
        if sq.size != rf.size {
            diff!(format!("size: squashfs={} ref={}", sq.size, rf.size));
        }
        if sq.sha256 != rf.sha256 {
            diff!(format!("sha256 mismatch"));
        }
    }
    diffs
}
