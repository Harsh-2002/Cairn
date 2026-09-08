//! Bounded POSIX traversal of flat blobs and the approved two-hex-digit fanout grammar.
//! Directory descriptors anchor traversal and unlink; nested paths never select arbitrary trees.

use crate::{io_err, merge_report};
use cairn_types::blob::ReconcileReport;
use cairn_types::error::BlobError;
use cairn_types::id::StoragePath;
use cairn_types::time::Timestamp;
use cairn_types::traits::ReconcileOracle;
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, fstat, open, openat, statat, unlinkat};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

struct Directory {
    file: Arc<File>,
    entries: Dir,
}

struct Entry {
    name: String,
    kind: FileType,
    inode: u64,
}

impl Directory {
    fn from_file(file: File) -> std::io::Result<Self> {
        let entries = Dir::read_from(&file)?;
        Ok(Self {
            file: Arc::new(file),
            entries,
        })
    }

    fn child(parent: &File, name: &str) -> std::io::Result<Self> {
        let fd = openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let file: File = fd.into();
        if fstat(&file)?.st_dev != fstat(parent)?.st_dev {
            return Err(std::io::Error::other(
                "reconcile directory crosses filesystems",
            ));
        }
        Self::from_file(file)
    }

    fn page(&mut self, limit: usize) -> std::io::Result<(Vec<Entry>, usize, u64)> {
        let mut entries = Vec::new();
        let mut invalid = 0;
        let mut examined = 0;
        // Bound examined entries too: a directory full of invalid names must still yield.
        for _ in 0..limit {
            let Some(entry) = self.entries.next() else {
                break;
            };
            let entry = entry?;
            examined += 1;
            let bytes = entry.file_name().to_bytes();
            if matches!(bytes, b"." | b"..") {
                continue;
            }
            let Ok(name) = std::str::from_utf8(bytes) else {
                invalid += 1;
                continue;
            };
            entries.push(Entry {
                name: name.to_owned(),
                kind: entry.file_type(),
                inode: entry.ino(),
            });
        }
        Ok((entries, examined, invalid))
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, BlobError> + Send + 'static,
) -> Result<T, BlobError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| BlobError::Io(e.to_string()))?
}

fn lower_hex(name: &str, len: usize) -> bool {
    name.len() == len
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(super) async fn bucket(
    path: PathBuf,
    bucket: String,
    oracle: &dyn ReconcileOracle,
    batch_size: u32,
    margin_secs: i64,
    now: Timestamp,
) -> Result<ReconcileReport, BlobError> {
    let name = bucket.clone();
    let (parent, mut dir) = blocking(move || {
        let parent: File = open(
            path.parent()
                .ok_or_else(|| BlobError::Io("missing bucket parent".into()))?,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| io_err(e.into()))?
        .into();
        let dir = Directory::child(&parent, &name).map_err(io_err)?;
        Ok((parent, dir))
    })
    .await?;
    let mut report = ReconcileReport::default();
    loop {
        let (next, entries, examined, invalid) = blocking(move || {
            let (entries, examined, invalid) =
                dir.page(batch_size.max(1) as usize).map_err(io_err)?;
            Ok((dir, entries, examined, invalid))
        })
        .await?;
        dir = next;
        report.errors += invalid;
        if examined == 0 {
            break;
        }
        let mut files = Vec::new();
        let mut leaves = Vec::new();
        for entry in entries {
            if entry.kind == FileType::RegularFile && lower_hex(&entry.name, 32) {
                files.push(entry);
            } else if entry.kind == FileType::Directory && lower_hex(&entry.name, 2) {
                leaves.push(entry.name);
            } else {
                report.errors += 1;
            }
        }
        merge_report(
            &mut report,
            reclaim(dir.file.clone(), &bucket, files, oracle, margin_secs, now).await?,
        );
        for leaf in leaves {
            let parent = dir.file.clone();
            let name = leaf.clone();
            let child = blocking(move || Directory::child(&parent, &name).map_err(io_err)).await;
            let Ok(child) = child else {
                report.errors += 1;
                continue;
            };
            merge_report(
                &mut report,
                leaf_files(
                    child,
                    dir.file.clone(),
                    &bucket,
                    &leaf,
                    oracle,
                    batch_size,
                    margin_secs,
                    now,
                )
                .await?,
            );
        }
    }
    let file = dir.file.clone();
    report.dirs_pruned += u64::from(
        blocking(move || prune(&parent, &bucket, &file, File::sync_all).map_err(io_err)).await?,
    );
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn leaf_files(
    mut dir: Directory,
    parent: Arc<File>,
    bucket: &str,
    leaf: &str,
    oracle: &dyn ReconcileOracle,
    batch_size: u32,
    margin_secs: i64,
    now: Timestamp,
) -> Result<ReconcileReport, BlobError> {
    let mut report = ReconcileReport::default();
    let prefix = format!("{bucket}/{leaf}");
    loop {
        let (next, entries, examined, invalid) = blocking(move || {
            let (entries, examined, invalid) =
                dir.page(batch_size.max(1) as usize).map_err(io_err)?;
            Ok((dir, entries, examined, invalid))
        })
        .await?;
        dir = next;
        report.errors += invalid;
        if examined == 0 {
            break;
        }
        let mut files = Vec::new();
        for entry in entries {
            if entry.kind == FileType::RegularFile
                && lower_hex(&entry.name, 32)
                && entry.name.starts_with(leaf)
            {
                files.push(entry);
            } else {
                // Unknown depth, prefix, name, or symlink: preserve it, report it, never descend.
                report.errors += 1;
            }
        }
        merge_report(
            &mut report,
            reclaim(dir.file.clone(), &prefix, files, oracle, margin_secs, now).await?,
        );
    }
    let name = leaf.to_owned();
    report.dirs_pruned += u64::from(
        blocking(move || prune(&parent, &name, &dir.file, File::sync_all).map_err(io_err)).await?,
    );
    Ok(report)
}

async fn reclaim(
    dir: Arc<File>,
    prefix: &str,
    entries: Vec<Entry>,
    oracle: &dyn ReconcileOracle,
    margin_secs: i64,
    now: Timestamp,
) -> Result<ReconcileReport, BlobError> {
    if entries.is_empty() {
        return Ok(ReconcileReport::default());
    }
    let paths = entries
        .iter()
        .map(|e| StoragePath::from_string(format!("{prefix}/{}", e.name)))
        .collect::<Vec<_>>();
    let live = oracle
        .live_blobs(&paths)
        .await
        .map_err(|e| BlobError::Io(e.to_string()))?;
    if live.len() != entries.len() {
        return Err(BlobError::Io(
            "reconcile oracle returned incorrect membership count".into(),
        ));
    }
    blocking(move || {
        let mut report = ReconcileReport {
            blobs_scanned: entries.len() as u64,
            ..Default::default()
        };
        for (entry, live) in entries.into_iter().zip(live) {
            if live {
                continue;
            }
            let stat = match statat(&*dir, entry.name.as_str(), AtFlags::SYMLINK_NOFOLLOW) {
                Ok(stat) => stat,
                Err(rustix::io::Errno::NOENT) => continue,
                Err(_) => {
                    report.errors += 1;
                    continue;
                }
            };
            // Never follow a substituted symlink or reclaim a replacement observed under an old name.
            if stat.st_ino != entry.inode
                || FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
            {
                report.errors += 1;
                continue;
            }
            if margin_secs > 0 && now.as_secs().saturating_sub(stat.st_mtime) < margin_secs {
                continue;
            }
            match unlinkat(&*dir, entry.name.as_str(), AtFlags::empty()) {
                Ok(()) => report.orphans_reclaimed += 1,
                Err(rustix::io::Errno::NOENT) => (),
                Err(_) => report.errors += 1,
            }
        }
        if report.orphans_reclaimed > 0 {
            dir.sync_all().map_err(io_err)?;
        }
        Ok(report)
    })
    .await
}

/// rmdir is the emptiness check; no recursive delete, and a repopulated directory survives.
/// Successful removal is reported only after its parent is synchronized.
fn prune(
    parent: &File,
    name: &str,
    expected: &File,
    sync: impl FnOnce(&File) -> std::io::Result<()>,
) -> std::io::Result<bool> {
    let stat = match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => stat,
        Err(rustix::io::Errno::NOENT) => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let opened = fstat(expected)?;
    if stat.st_ino != opened.st_ino
        || stat.st_dev != opened.st_dev
        || FileType::from_raw_mode(stat.st_mode) != FileType::Directory
    {
        return Ok(false);
    }
    match unlinkat(parent, name, AtFlags::REMOVEDIR) {
        Ok(()) => {
            sync(parent)?;
            Ok(true)
        }
        Err(rustix::io::Errno::NOENT | rustix::io::Errno::NOTEMPTY | rustix::io::Errno::EXIST) => {
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::error::MetaError;
    use cairn_types::id::UploadId;
    use cairn_types::testing::SetReconcileOracle;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::symlink;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct BoundedOracle {
        maximum: AtomicUsize,
        wrong_count: bool,
    }

    #[async_trait::async_trait]
    impl ReconcileOracle for BoundedOracle {
        async fn live_blobs(&self, paths: &[StoragePath]) -> Result<Vec<bool>, MetaError> {
            self.maximum.fetch_max(paths.len(), Ordering::SeqCst);
            Ok(vec![false; if self.wrong_count { 0 } else { paths.len() }])
        }
        async fn live_session(&self, _: &UploadId) -> Result<bool, MetaError> {
            Ok(false)
        }
        async fn live_multipart_parts(
            &self,
            paths: &[StoragePath],
        ) -> Result<Vec<bool>, MetaError> {
            Ok(vec![false; paths.len()])
        }
    }

    fn empty_oracle() -> SetReconcileOracle {
        SetReconcileOracle {
            live_paths: Default::default(),
            live_uploads: Default::default(),
            live_multipart_paths: Default::default(),
        }
    }

    #[tokio::test]
    async fn bounded_pages_preserve_unknown_paths_and_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let bucket_dir = root.path().join("bucket");
        std::fs::create_dir_all(bucket_dir.join("ab")).unwrap();
        for id in 0..23 {
            std::fs::write(bucket_dir.join(format!("{id:032x}")), b"flat").unwrap();
            std::fs::write(bucket_dir.join(format!("ab/ab{id:030x}")), b"nested").unwrap();
        }
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(
            outside.path().join("cd000000000000000000000000000000"),
            b"outside",
        )
        .unwrap();
        symlink(outside.path(), bucket_dir.join("cd")).unwrap();
        symlink(
            outside.path().join("cd000000000000000000000000000000"),
            bucket_dir.join("ab/abffffffffffffffffffffffffffffff"),
        )
        .unwrap();
        std::fs::create_dir_all(bucket_dir.join("ab/deeper")).unwrap();
        std::fs::write(
            bucket_dir.join("ab/ac000000000000000000000000000000"),
            b"wrong prefix",
        )
        .unwrap();
        std::fs::write(
            bucket_dir.join(std::ffi::OsStr::from_bytes(b"\xff")),
            b"unknown",
        )
        .unwrap();
        let oracle = BoundedOracle {
            maximum: AtomicUsize::new(0),
            wrong_count: false,
        };
        let report = bucket(
            bucket_dir.clone(),
            "bucket".into(),
            &oracle,
            3,
            0,
            Timestamp::from_secs(0),
        )
        .await
        .unwrap();
        assert_eq!(report.blobs_scanned, 46);
        assert_eq!(report.orphans_reclaimed, 46);
        assert_eq!(report.errors, 5);
        assert_eq!(report.dirs_pruned, 0);
        assert!(oracle.maximum.load(Ordering::SeqCst) <= 3);
        assert_eq!(
            std::fs::read(outside.path().join("cd000000000000000000000000000000")).unwrap(),
            b"outside"
        );
        assert!(bucket_dir.join("ab/deeper").is_dir());
        assert!(
            bucket_dir
                .join("ab/ac000000000000000000000000000000")
                .is_file()
        );
    }

    #[tokio::test]
    async fn incorrect_oracle_count_cannot_authorize_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("bucket");
        std::fs::create_dir(&path).unwrap();
        let file = path.join("00000000000000000000000000000000");
        std::fs::write(&file, b"keep").unwrap();
        let oracle = BoundedOracle {
            maximum: AtomicUsize::new(0),
            wrong_count: true,
        };
        assert!(
            bucket(
                path,
                "bucket".into(),
                &oracle,
                2,
                0,
                Timestamp::from_secs(0)
            )
            .await
            .is_err()
        );
        assert_eq!(std::fs::read(file).unwrap(), b"keep");
    }

    #[tokio::test]
    async fn moved_leaf_descriptor_cannot_follow_replacement_symlink() {
        let root = tempfile::tempdir().unwrap();
        let parent = File::open(root.path()).unwrap();
        std::fs::create_dir(root.path().join("ab")).unwrap();
        let name = "ab000000000000000000000000000000";
        std::fs::write(root.path().join("ab").join(name), b"orphan").unwrap();
        let mut opened = Directory::child(&parent, "ab").unwrap();
        let (entries, _, _) = opened.page(10).unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join(name), b"outside").unwrap();
        std::fs::rename(root.path().join("ab"), root.path().join("old")).unwrap();
        symlink(outside.path(), root.path().join("ab")).unwrap();
        let report = reclaim(
            opened.file.clone(),
            "bucket/ab",
            entries,
            &empty_oracle(),
            0,
            Timestamp::from_secs(0),
        )
        .await
        .unwrap();
        assert_eq!(report.orphans_reclaimed, 1);
        assert_eq!(
            std::fs::read(outside.path().join(name)).unwrap(),
            b"outside"
        );
        assert!(
            !prune(&parent, "ab", &opened.file, |_| panic!(
                "must not sync a preserved symlink"
            ))
            .unwrap()
        );
    }

    #[test]
    fn prune_synchronizes_parent_after_removal_and_propagates_failure() {
        let root = tempfile::tempdir().unwrap();
        let parent = File::open(root.path()).unwrap();
        for fail in [false, true] {
            std::fs::create_dir(root.path().join("empty")).unwrap();
            let opened = Directory::child(&parent, "empty").unwrap();
            let result = prune(&parent, "empty", &opened.file, |actual_parent| {
                assert!(!root.path().join("empty").exists());
                assert_eq!(
                    fstat(actual_parent).unwrap().st_ino,
                    fstat(&parent).unwrap().st_ino
                );
                if fail {
                    Err(std::io::Error::other("injected parent fsync failure"))
                } else {
                    actual_parent.sync_all()
                }
            });
            if fail {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap());
            }
        }
        std::fs::create_dir(root.path().join("full")).unwrap();
        let full = Directory::child(&parent, "full").unwrap();
        // A writer repopulates the directory after it was opened.
        std::fs::write(root.path().join("full/live"), b"live").unwrap();
        assert!(
            !prune(&parent, "full", &full.file, |_| panic!(
                "nonempty directory was removed"
            ))
            .unwrap()
        );
        assert!(
            !prune(&parent, "missing", &full.file, |_| panic!(
                "missing directory was removed"
            ))
            .unwrap()
        );
    }
}
