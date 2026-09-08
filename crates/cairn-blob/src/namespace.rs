//! Exact admitted names, anchored to directory descriptors. No operation follows a path component
//! selected by a key, and no asynchronous namespace operation can outlive its storage lease.

use crate::io_err;
use cairn_types::storage::{StorageCreationPermit, StoragePathRole, io::StorageIoLease};
use cairn_types::{BlobError, StoragePath};
use rustix::fs::{AtFlags, FileType, Mode, OFlags, fstat, mkdirat, open, unlinkat};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Open below an already trusted directory without crossing even a same-device bind mount.
/// Device/inode comparisons alone cannot establish this boundary. Unsupported kernels or
/// platforms fail closed rather than quietly reverting to a weaker path walk.
pub(crate) fn open_beneath(
    parent: &File,
    name: &str,
    flags: OFlags,
    mode: Mode,
) -> std::io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::ResolveFlags;
        rustix::fs::openat2(
            parent,
            name,
            flags,
            mode,
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_XDEV,
        )
        .map(File::from)
        .map_err(|error| match error {
            rustix::io::Errno::NOSYS | rustix::io::Errno::INVAL => std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "storage requires openat2 with no-mount and no-symlink resolution support",
            ),
            error => error.into(),
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (parent, name, flags, mode);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "storage mount-safe directory resolution is unavailable on this platform",
        ))
    }
}

/// Initialize only the configured root and its known staging children. The caller retains
/// exclusive maintenance ownership through this synchronous operation. The configured root
/// may itself be a mount; none of its descendants may cross another mount or follow a symlink.
pub(crate) fn initialize(root: &Path) -> std::io::Result<()> {
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let directory: File = match open(root, flags, Mode::empty()) {
        Ok(file) => file.into(),
        Err(rustix::io::Errno::NOENT) => {
            let name = root
                .file_name()
                .ok_or_else(|| std::io::Error::other("missing storage root name"))?;
            let parent = root
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let parent: File = open(parent, flags, Mode::empty())?.into();
            match mkdirat(&parent, name, Mode::from_bits_truncate(0o700)) {
                Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                Err(error) => return Err(error.into()),
            }
            let directory: File = rustix::fs::openat(&parent, name, flags, Mode::empty())?.into();
            parent.sync_all()?;
            directory
        }
        Err(error) => return Err(error.into()),
    };
    let staging = ensure_directory(&directory, ".staging")?;
    ensure_directory(&staging, "multipart")?;
    Ok(())
}

fn ensure_directory(parent: &File, name: &str) -> std::io::Result<File> {
    match mkdirat(parent, name, Mode::from_bits_truncate(0o700)) {
        Ok(()) | Err(rustix::io::Errno::EXIST) => {}
        Err(error) => return Err(error.into()),
    }
    let directory = open_beneath(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    // EXIST can race a creator whose parent barrier has not completed yet.
    parent.sync_all()?;
    Ok(directory)
}

#[derive(Clone, Debug)]
pub(crate) struct AnchoredPath {
    pub(crate) parent: Arc<File>,
    name: String,
}

impl AnchoredPath {
    /// Must run inside a leased blocking operation. EXCL preserves unexpected existing artifacts.
    pub(crate) fn create_new(&self) -> std::io::Result<File> {
        let file = open_beneath(
            &self.parent,
            self.name.as_str(),
            OFlags::CREATE | OFlags::EXCL | OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )?;
        // io_uring references retain this open file description through kernel completion, even
        // after process death. Exact cleanup must acquire the same inode's lock before unlink.
        crate::try_lock_exclusive(&file)?;
        Ok(file)
    }

    pub(crate) fn rename_to(&self, destination: &Self) -> std::io::Result<()> {
        #[cfg(any(target_os = "linux", target_vendor = "apple"))]
        {
            rustix::fs::renameat_with(
                &*self.parent,
                self.name.as_str(),
                &*destination.parent,
                destination.name.as_str(),
                rustix::fs::RenameFlags::NOREPLACE,
            )?;
            Ok(())
        }
        #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
        {
            let _ = destination;
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "exclusive storage rename unavailable",
            ))
        }
    }

    /// Immediate scratch unlink is allowed only for the descriptor just created by this attempt.
    /// Its journal alias still needs a namespace barrier before cleanup debt can be retired.
    pub(crate) fn unlink_created(&self, file: &File) -> std::io::Result<()> {
        let expected = fstat(file)?;
        let actual =
            rustix::fs::statat(&*self.parent, self.name.as_str(), AtFlags::SYMLINK_NOFOLLOW)?;
        if expected.st_dev != actual.st_dev
            || expected.st_ino != actual.st_ino
            || FileType::from_raw_mode(actual.st_mode) != FileType::RegularFile
        {
            return Err(std::io::Error::other("storage scratch identity changed"));
        }
        unlinkat(&*self.parent, self.name.as_str(), AtFlags::empty())?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fixture(path: &Path) -> Self {
        Self {
            parent: Arc::new(crate::open_readonly_nofollow(path.parent().unwrap()).unwrap()),
            name: path.file_name().unwrap().to_str().unwrap().to_owned(),
        }
    }
}

pub(crate) struct AdmittedPaths {
    pub(crate) staging: AnchoredPath,
    pub(crate) final_file: AnchoredPath,
    pub(crate) spool: AnchoredPath,
    pub(crate) storage_path: StoragePath,
    pub(crate) lease: StorageIoLease,
}

impl AdmittedPaths {
    pub(crate) async fn open(
        root: &Path,
        permit: StorageCreationPermit,
    ) -> Result<Self, BlobError> {
        permit
            .plan()
            .validate()
            .map_err(|error| BlobError::Io(error.to_string()))?;
        let (plan, lease) = permit.into_parts();
        let operation = lease.try_child()?;
        let root = root.to_owned();
        tokio::task::spawn_blocking(move || {
            let _operation = operation;
            let root: File = open(
                &root,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| io_err(error.into()))?
            .into();
            let mut directories = HashMap::from([(String::new(), Arc::new(root))]);
            let mut staging = None;
            let mut final_file = None;
            let mut spool = None;
            for intent in &plan.paths {
                let path = prepare_path(&mut directories, intent.path.as_str()).map_err(io_err)?;
                match intent.role {
                    StoragePathRole::Temporary => staging = Some(path),
                    StoragePathRole::Final => final_file = Some(path),
                    StoragePathRole::IndexSpool => spool = Some(path),
                }
            }
            let final_file =
                final_file.ok_or_else(|| BlobError::Io("missing admitted final path".into()))?;
            Ok(Self {
                staging: staging.unwrap_or_else(|| final_file.clone()),
                final_file,
                spool: spool.ok_or_else(|| BlobError::Io("missing admitted spool path".into()))?,
                storage_path: plan
                    .final_path()
                    .map_err(|error| BlobError::Io(error.to_string()))?
                    .clone(),
                lease,
            })
        })
        .await
        .map_err(|error| BlobError::Io(error.to_string()))?
    }
}

fn prepare_path(
    directories: &mut HashMap<String, Arc<File>>,
    relative: &str,
) -> std::io::Result<AnchoredPath> {
    let mut components = relative.split('/').peekable();
    let mut prefix = String::new();
    let mut parent = directories.get("").expect("anchored root").clone();
    while let Some(name) = components.next() {
        if components.peek().is_none() {
            return Ok(AnchoredPath {
                parent,
                name: name.to_owned(),
            });
        }
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(name);
        if let Some(directory) = directories.get(&prefix) {
            parent = directory.clone();
            continue;
        }
        match mkdirat(&*parent, name, Mode::from_bits_truncate(0o700)) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        let child = open_beneath(
            &parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        if fstat(&child)?.st_dev != fstat(&*parent)?.st_dev {
            return Err(std::io::Error::other(
                "storage directory crosses filesystems",
            ));
        }
        // Existing does not imply durable: another creator may have failed its parent sync.
        // Complete this barrier in the same non-cancellable closure before dependent creation.
        parent.sync_all()?;
        parent = Arc::new(child);
        directories.insert(prefix.clone(), parent.clone());
    }
    Err(std::io::Error::other("empty admitted storage path"))
}

/// Open a referenced input through the same no-follow, same-filesystem directory boundary.
pub(crate) fn read_file(root: &Path, path: &StoragePath) -> std::io::Result<File> {
    let components: Vec<_> = path.as_str().split('/').collect();
    if components.len() > 4
        || components
            .iter()
            .any(|name| matches!(*name, "" | "." | ".."))
    {
        return Err(std::io::Error::other("unsafe storage input path"));
    }
    let mut directory: File = open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?
    .into();
    let device = fstat(&directory)?.st_dev;
    for (index, name) in components.iter().enumerate() {
        let last = index + 1 == components.len();
        let flags = OFlags::RDONLY
            | OFlags::NONBLOCK
            | OFlags::NOFOLLOW
            | OFlags::CLOEXEC
            | if last {
                OFlags::empty()
            } else {
                OFlags::DIRECTORY
            };
        let file = open_beneath(&directory, name, flags, Mode::empty())?;
        let stat = fstat(&file)?;
        if stat.st_dev != device
            || (last && FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile)
        {
            return Err(std::io::Error::other("unsupported storage input file"));
        }
        if last {
            return Ok(file);
        }
        directory = file;
    }
    Err(std::io::Error::other("empty storage input path"))
}

/// Exact unlink with durable absence, including an already-absent name. The Writer has already
/// proved that this immutable path has no authoritative reference or active intent owner.
pub(crate) fn cleanup(root: &Path, path: &StoragePath) -> std::io::Result<()> {
    let components: Vec<_> = path.as_str().split('/').collect();
    let root: File = open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?
    .into();
    let mut parent = Arc::new(root);
    let mut chain = Vec::new();
    for (index, name) in components[..components.len() - 1].iter().enumerate() {
        let child = match open_beneath(
            &parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                sync_absence(&parent)?;
                return prune(chain);
            }
            Err(error) => return Err(error),
        };
        if fstat(&child)?.st_dev != fstat(&*parent)?.st_dev {
            return Err(std::io::Error::other("storage cleanup crosses filesystems"));
        }
        let child = Arc::new(child);
        chain.push(DirectoryLink {
            parent,
            child: child.clone(),
            name: (*name).to_owned(),
            prunable: !(index == 0 && *name == ".staging")
                && !(index == 1 && components[0] == ".staging" && *name == "multipart"),
        });
        parent = child;
    }
    let name = components[components.len() - 1];
    let file = match open_beneath(
        &parent,
        name,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    if let Some(file) = &file {
        let stat = fstat(file)?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
            || stat.st_dev != fstat(&*parent)?.st_dev
            || stat.st_nlink != 1
        {
            return Err(std::io::Error::other("unsupported storage cleanup file"));
        }
        // Expired userspace ownership is never proof that optional kernel I/O has completed.
        // A live open-file-description lock keeps this cleanup retryable across process death.
        crate::try_lock_exclusive(file)?;
        let named = rustix::fs::statat(&*parent, name, AtFlags::SYMLINK_NOFOLLOW)?;
        if named.st_dev != stat.st_dev || named.st_ino != stat.st_ino {
            return Err(std::io::Error::other(
                "storage cleanup file identity changed",
            ));
        }
        unlinkat(&*parent, name, AtFlags::empty())?;
        fail::fail_point!("blob_after_storage_unlink");
    }
    sync_absence(&parent)?;
    let result = prune(chain);
    drop(file);
    result
}

fn sync_absence(directory: &File) -> std::io::Result<()> {
    fail::fail_point!("blob_before_storage_dirsync", |_| Err(
        std::io::Error::other("injected storage directory sync failure")
    ));
    directory.sync_all()
}

struct DirectoryLink {
    parent: Arc<File>,
    child: Arc<File>,
    name: String,
    prunable: bool,
}

fn prune(chain: Vec<DirectoryLink>) -> std::io::Result<()> {
    for link in chain.into_iter().rev() {
        if !link.prunable {
            break;
        }
        match rustix::fs::statat(&*link.parent, link.name.as_str(), AtFlags::SYMLINK_NOFOLLOW) {
            Ok(named) => {
                let expected = fstat(&*link.child)?;
                if named.st_dev != expected.st_dev
                    || named.st_ino != expected.st_ino
                    || FileType::from_raw_mode(named.st_mode) != FileType::Directory
                {
                    return Err(std::io::Error::other(
                        "storage cleanup directory identity changed",
                    ));
                }
                match unlinkat(&*link.parent, link.name.as_str(), AtFlags::REMOVEDIR) {
                    Ok(()) | Err(rustix::io::Errno::NOENT) => {}
                    Err(rustix::io::Errno::NOTEMPTY | rustix::io::Errno::EXIST) => break,
                    Err(error) => return Err(error.into()),
                }
            }
            Err(rustix::io::Errno::NOENT) => {}
            Err(error) => return Err(error.into()),
        }
        sync_absence(&link.parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::storage::{
        PlannedStorageWrite, StorageAdmission, StorageCleanup, StorageToken, StorageWritePlan,
        StorageWriteTarget, io::StorageIoWatch,
    };
    use cairn_types::{BucketName, ObjectKey, Timestamp, VersionId, traits::BlobStore};

    #[cfg(target_os = "linux")]
    #[test]
    fn anchored_open_rejects_symlinks_and_parent_escape() {
        let tree = tempfile::tempdir().unwrap();
        let root = tree.path().join("root");
        let outside = tree.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("sentinel"), b"preserve").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        let directory = crate::open_readonly_nofollow(&root).unwrap();
        for path in ["link/sentinel", "../outside/sentinel"] {
            assert!(open_beneath(&directory, path, OFlags::RDONLY, Mode::empty()).is_err());
        }
        assert_eq!(
            std::fs::read(outside.join("sentinel")).unwrap(),
            b"preserve"
        );
    }

    #[tokio::test]
    async fn initialization_rejects_symlinks_without_mutating_their_targets() {
        for component in ["root", ".staging", ".staging/multipart"] {
            let tree = tempfile::tempdir().unwrap();
            let root = tree.path().join("root");
            let outside = tree.path().join("outside");
            std::fs::create_dir(&outside).unwrap();
            std::fs::write(outside.join("sentinel"), b"preserve").unwrap();
            let link = if component == "root" {
                root.clone()
            } else {
                let link = root.join(component);
                std::fs::create_dir_all(link.parent().unwrap()).unwrap();
                link
            };
            std::os::unix::fs::symlink(&outside, &link).unwrap();
            assert!(
                crate::LocalBlobStore::open(&root, cairn_types::testing::fixture_storage_io())
                    .await
                    .is_err(),
                "accepted symlink at {component}"
            );
            assert!(
                std::fs::symlink_metadata(link)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1);
            assert_eq!(
                std::fs::read(outside.join("sentinel")).unwrap(),
                b"preserve"
            );
        }
    }

    #[tokio::test]
    async fn initialization_creates_only_the_final_root_and_known_staging_children() {
        let tree = tempfile::tempdir().unwrap();
        let root = tree.path().join("root");
        let store = crate::LocalBlobStore::open(&root, cairn_types::testing::fixture_storage_io())
            .await
            .unwrap();
        assert!(root.join(".staging/multipart").is_dir());
        drop(store);
        let missing_ancestor = tree.path().join("missing/root");
        assert!(
            crate::LocalBlobStore::open(
                &missing_ancestor,
                cairn_types::testing::fixture_storage_io()
            )
            .await
            .is_err()
        );
        assert!(!tree.path().join("missing").exists());
    }

    /// Run this test executable as root with `--ignored --exact` and this test's full name.
    /// It reexecutes in a private mount namespace before installing any temporary bind mount.
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires permission to create a private mount namespace and bind mounts"]
    fn same_device_bind_mounts_are_rejected_for_creation_reads_and_cleanup() {
        const CHILD: &str = "CAIRN_TEST_NAMESPACE_BIND_MOUNT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let result = std::process::Command::new("unshare")
                .args(["--mount", "--propagation", "private"])
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "namespace::tests::same_device_bind_mounts_are_rejected_for_creation_reads_and_cleanup",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(result.success(), "private bind-mount regression failed");
            return;
        }
        struct Mount(std::path::PathBuf);
        impl Drop for Mount {
            fn drop(&mut self) {
                assert!(
                    std::process::Command::new("umount")
                        .arg(&self.0)
                        .status()
                        .unwrap()
                        .success()
                );
            }
        }
        fn mount(source: &Path, target: &Path) -> Mount {
            assert!(
                std::process::Command::new("mount")
                    .arg("--bind")
                    .arg(source)
                    .arg(target)
                    .status()
                    .unwrap()
                    .success()
            );
            Mount(target.to_owned())
        }
        let tree = tempfile::tempdir().unwrap();
        let root = tree.path().join("root");
        let outside = tree.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let mounted = root.join("bucket");
        std::fs::create_dir(&mounted).unwrap();
        std::fs::write(outside.join("sentinel"), b"preserve").unwrap();
        let directory = Arc::new(crate::open_readonly_nofollow(&root).unwrap());
        {
            let _mount = mount(&outside, &mounted);
            let weak_open: File = rustix::fs::openat(
                &*directory,
                "bucket",
                OFlags::RDONLY | OFlags::DIRECTORY,
                Mode::empty(),
            )
            .unwrap()
            .into();
            assert_eq!(
                fstat(&weak_open).unwrap().st_dev,
                fstat(&*directory).unwrap().st_dev
            );
            let mut directories = HashMap::from([(String::new(), directory.clone())]);
            assert_eq!(
                prepare_path(&mut directories, "bucket/new-file")
                    .unwrap_err()
                    .raw_os_error(),
                Some(rustix::io::Errno::XDEV.raw_os_error())
            );
            let path = StoragePath::from_string("bucket/sentinel".into());
            assert_eq!(
                read_file(&root, &path).unwrap_err().raw_os_error(),
                Some(rustix::io::Errno::XDEV.raw_os_error())
            );
            assert_eq!(
                cleanup(&root, &path).unwrap_err().raw_os_error(),
                Some(rustix::io::Errno::XDEV.raw_os_error())
            );
            assert!(!outside.join("new-file").exists());
            assert_eq!(
                std::fs::read(outside.join("sentinel")).unwrap(),
                b"preserve"
            );
        }
        let leaf = mounted.join("sentinel");
        std::fs::write(&leaf, b"original").unwrap();
        {
            let _mount = mount(&outside.join("sentinel"), &leaf);
            let path = StoragePath::from_string("bucket/sentinel".into());
            assert_eq!(
                read_file(&root, &path).unwrap_err().raw_os_error(),
                Some(rustix::io::Errno::XDEV.raw_os_error())
            );
            assert_eq!(
                cleanup(&root, &path).unwrap_err().raw_os_error(),
                Some(rustix::io::Errno::XDEV.raw_os_error())
            );
            assert_eq!(
                std::fs::read(outside.join("sentinel")).unwrap(),
                b"preserve"
            );
        }
        assert_eq!(std::fs::read(leaf).unwrap(), b"original");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for component in [".staging", ".staging/multipart"] {
            let mounted = root.join(component);
            std::fs::create_dir_all(&mounted).unwrap();
            let _mount = mount(&outside, &mounted);
            assert!(
                runtime
                    .block_on(crate::LocalBlobStore::open(
                        &root,
                        cairn_types::testing::fixture_storage_io(),
                    ))
                    .is_err(),
                "initialization accepted bind mount at {component}"
            );
            assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1);
            assert_eq!(
                std::fs::read(outside.join("sentinel")).unwrap(),
                b"preserve"
            );
        }
        // A mount at the explicitly configured root remains a supported deployment layout.
        let configured_mount = tree.path().join("configured-mount");
        std::fs::create_dir(&configured_mount).unwrap();
        let _mount = mount(&outside, &configured_mount);
        let store = runtime
            .block_on(crate::LocalBlobStore::open(
                &configured_mount,
                cairn_types::testing::fixture_storage_io(),
            ))
            .unwrap();
        assert!(configured_mount.join(".staging/multipart").is_dir());
        drop(store);
    }

    fn admitted() -> (StorageWritePlan, StorageIoWatch, StorageCreationPermit) {
        let planned = PlannedStorageWrite::new(
            BucketName::parse("bucket").unwrap(),
            StorageToken::generate(),
            StorageWriteTarget::Object {
                key: ObjectKey::parse("key").unwrap(),
                version_id: VersionId::null(),
                row_id: StorageToken::generate().as_str().to_owned(),
            },
        )
        .unwrap();
        let plan = planned.plan().clone();
        let (watch, lease) =
            StorageIoWatch::new(plan.attempt.clone(), plan.generation.clone(), Arc::new(()));
        let permit = planned
            .admit(StorageAdmission::Granted(Box::new(plan.clone())), lease)
            .unwrap();
        (plan, watch, permit)
    }

    fn claim(plan: &StorageWritePlan, path: StoragePath) -> StorageCleanup {
        StorageCleanup {
            id: StorageToken::generate(),
            bucket: plan.bucket.clone(),
            path,
            quota_debt_id: None,
            claim_token: StorageToken::generate(),
            generation: plan.generation.clone(),
            lease_until: Timestamp(1000),
        }
    }

    fn cleanup_lease(claim: &StorageCleanup) -> StorageIoLease {
        StorageIoWatch::new(claim.id.clone(), claim.generation.clone(), Arc::new(())).1
    }

    fn probe_lease(plan: &StorageWritePlan) -> StorageIoLease {
        StorageIoWatch::new(plan.attempt.clone(), plan.generation.clone(), Arc::new(())).1
    }

    #[tokio::test]
    async fn cleanup_and_quiescence_refuse_a_lingering_open_file_description() {
        let root = tempfile::tempdir().unwrap();
        let store =
            crate::LocalBlobStore::open(root.path(), cairn_types::testing::fixture_storage_io())
                .await
                .unwrap();
        let (plan, mut watch, permit) = admitted();
        let paths = AdmittedPaths::open(root.path(), permit).await.unwrap();
        let file = paths.staging.create_new().unwrap();
        // Model the kernel retaining an open file description after userspace closes its handle.
        // This is an actual flock test, not a process-kill or power-loss test.
        let lingering = file.try_clone().unwrap();
        drop(file);
        drop(paths);
        watch.quiescent().await;
        let cleanup = claim(&plan, plan.paths[0].path.clone());
        assert!(
            store
                .confirm_storage_quiescence(&plan, probe_lease(&plan))
                .await
                .is_err()
        );
        assert!(
            store
                .cleanup_storage(&cleanup, cleanup_lease(&cleanup))
                .await
                .is_err()
        );
        assert!(root.path().join(cleanup.path.as_str()).exists());
        drop(lingering);
        store
            .confirm_storage_quiescence(&plan, probe_lease(&plan))
            .await
            .unwrap();
        store
            .cleanup_storage(&cleanup, cleanup_lease(&cleanup))
            .await
            .unwrap();
        assert!(!root.path().join(cleanup.path.as_str()).exists());
        store
            .cleanup_storage(&cleanup, cleanup_lease(&cleanup))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn symlinked_parent_is_rejected_without_creating_an_external_file() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join(".staging")).unwrap();
        let (_, mut watch, permit) = admitted();
        assert!(AdmittedPaths::open(root.path(), permit).await.is_err());
        watch.quiescent().await;
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
        assert!(
            std::fs::symlink_metadata(root.path().join(".staging"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[tokio::test]
    async fn rename_preserves_an_existing_destination_and_cleanup_preserves_unknown_entries() {
        let root = tempfile::tempdir().unwrap();
        let store =
            crate::LocalBlobStore::open(root.path(), cairn_types::testing::fixture_storage_io())
                .await
                .unwrap();
        let (plan, mut watch, permit) = admitted();
        let paths = AdmittedPaths::open(root.path(), permit).await.unwrap();
        let final_path = root.path().join(plan.final_path().unwrap().as_str());
        std::fs::write(&final_path, b"existing").unwrap();
        let file = paths.staging.create_new().unwrap();
        assert!(paths.staging.rename_to(&paths.final_file).is_err());
        assert_eq!(std::fs::read(&final_path).unwrap(), b"existing");
        drop(file);
        drop(paths);
        watch.quiescent().await;
        let unknown = root.path().join("bucket/operator-note");
        std::fs::write(&unknown, b"preserve").unwrap();
        let cleanup = claim(&plan, plan.final_path().unwrap().clone());
        store
            .cleanup_storage(&cleanup, cleanup_lease(&cleanup))
            .await
            .unwrap();
        assert!(unknown.exists());
        assert!(!final_path.exists());
    }

    #[tokio::test]
    async fn exact_cleanup_prunes_only_empty_supported_parents_and_retries_absence() {
        let root = tempfile::tempdir().unwrap();
        let store =
            crate::LocalBlobStore::open(root.path(), cairn_types::testing::fixture_storage_io())
                .await
                .unwrap();
        let (plan, _, permit) = admitted();
        drop(permit);
        let id = StorageToken::generate();
        let path =
            StoragePath::from_string(format!("bucket/{}/{}", &id.as_str()[..2], id.as_str()));
        let absolute = root.path().join(path.as_str());
        std::fs::create_dir_all(absolute.parent().unwrap()).unwrap();
        std::fs::write(&absolute, b"debt").unwrap();
        let cleanup = claim(&plan, path);
        store
            .cleanup_storage(&cleanup, cleanup_lease(&cleanup))
            .await
            .unwrap();
        assert!(!root.path().join("bucket").exists());
        store
            .cleanup_storage(&cleanup, cleanup_lease(&cleanup))
            .await
            .unwrap();
        assert!(root.path().join(".staging/multipart").exists());
    }

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn unlink_without_namespace_sync_is_retryable_even_when_the_name_is_absent() {
        let _scenario = fail::FailScenario::setup();
        let root = tempfile::tempdir().unwrap();
        let store =
            crate::LocalBlobStore::open(root.path(), cairn_types::testing::fixture_storage_io())
                .await
                .unwrap();
        let (plan, _, permit) = admitted();
        drop(permit);
        let path = plan.final_path().unwrap().clone();
        let absolute = root.path().join(path.as_str());
        std::fs::create_dir(absolute.parent().unwrap()).unwrap();
        std::fs::write(&absolute, b"debt").unwrap();
        let cleanup = claim(&plan, path);
        fail::cfg("blob_before_storage_dirsync", "return").unwrap();
        assert!(
            store
                .cleanup_storage(&cleanup, cleanup_lease(&cleanup))
                .await
                .is_err()
        );
        assert!(!absolute.exists());
        assert!(
            store
                .cleanup_storage(&cleanup, cleanup_lease(&cleanup))
                .await
                .is_err()
        );
        fail::remove("blob_before_storage_dirsync");
        store
            .cleanup_storage(&cleanup, cleanup_lease(&cleanup))
            .await
            .unwrap();
        assert!(!root.path().join("bucket").exists());
    }
}
