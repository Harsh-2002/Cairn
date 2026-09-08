//! Exclusive baseline classification and reverse coverage proof. No method here removes a name;
//! the ordinary exact-debt consumer remains the sole physical cleanup path.

use crate::{io_err, namespace::open_beneath};
use cairn_types::blob::{
    StorageBaselineOptions, StorageBaselineProof, StorageClassificationCounts,
    StorageClassificationReport,
};
use cairn_types::storage::{io::StorageIoLease, validate_storage_path};
use cairn_types::storage_baseline::{
    STORAGE_BASELINE_PAGE_LIMIT, StorageAuthority, StorageAuthorityCursor, StorageAuthorityKind,
    StorageBaselineDisposition,
};
use cairn_types::{BlobError, BucketName, MetadataStore, Mutation, MutationOutcome, StoragePath};
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, fstat, open, statat};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};
use std::sync::Arc;

fn invalid(message: impl Into<String>) -> BlobError {
    BlobError::Io(format!("storage baseline: {}", message.into()))
}

async fn blocking<T: Send + 'static>(
    lease: &StorageIoLease,
    work: impl FnOnce() -> Result<T, BlobError> + Send + 'static,
) -> Result<T, BlobError> {
    let lease = lease.try_child()?;
    let (result, _lease) = tokio::task::spawn_blocking(move || (work(), lease))
        .await
        .map_err(|error| invalid(error.to_string()))?;
    result
}

fn validate_options(opts: &StorageBaselineOptions) -> Result<(), BlobError> {
    if opts.batch_size == 0 || opts.batch_size as usize > STORAGE_BASELINE_PAGE_LIMIT {
        return Err(invalid("invalid bounded page size"));
    }
    if opts.root_artifacts.len() > STORAGE_BASELINE_PAGE_LIMIT {
        return Err(invalid("too many root file exemptions"));
    }
    for (index, name) in opts.root_artifacts.iter().enumerate() {
        let mut components = Path::new(name).components();
        if !matches!(components.next(), Some(Component::Normal(component)) if component == name)
            || components.next().is_some()
            || name == ".staging"
            || opts.root_artifacts[..index].contains(name)
        {
            return Err(invalid("invalid or duplicate root file exemption"));
        }
    }
    Ok(())
}

async fn require_hold(
    meta: &dyn MetadataStore,
    opts: &StorageBaselineOptions,
) -> Result<(), BlobError> {
    let states = meta
        .storage_baseline_states()
        .await
        .map_err(|error| invalid(error.to_string()))?;
    if states.is_empty()
        || states
            .iter()
            .any(|state| !state.matches(&opts.token) || state.legacy_release_authorized)
    {
        return Err(invalid("every shard must hold this unfinalized baseline"));
    }
    Ok(())
}

async fn require_drained(meta: &dyn MetadataStore) -> Result<(), BlobError> {
    if meta
        .storage_baseline_pending()
        .await
        .map_err(|error| invalid(error.to_string()))?
        .native_pending()
    {
        return Err(invalid("journal or native quota work remains"));
    }
    Ok(())
}

struct RootLifetime {
    _root: Arc<File>,
    _lease: StorageIoLease,
}

#[cfg(test)]
type SyncHook = dyn Fn(&str) -> Result<(), BlobError> + Send + Sync;

#[derive(Clone, Default)]
struct Hooks {
    #[cfg(test)]
    before_page: Option<Arc<dyn Fn() -> Result<(), BlobError> + Send + Sync>>,
    #[cfg(test)]
    before_sync: Option<Arc<SyncHook>>,
}

impl Hooks {
    fn page(&self) -> Result<(), BlobError> {
        #[cfg(test)]
        if let Some(hook) = &self.before_page {
            hook()?;
        }
        Ok(())
    }

    fn sync(&self, file: &File, path: &str) -> Result<(), BlobError> {
        #[cfg(test)]
        if let Some(hook) = &self.before_sync {
            hook(path)?;
        }
        let _ = path;
        file.sync_all().map_err(io_err)
    }
}

#[derive(Clone)]
enum DirectoryKind {
    Root,
    Staging,
    Multipart,
    Session,
    Bucket(BucketName),
    Leaf(BucketName),
}

impl DirectoryKind {
    fn child(&self, prefix: &str, name: &str) -> Result<Self, BlobError> {
        match self {
            Self::Root if name == ".staging" => Ok(Self::Staging),
            Self::Root => BucketName::parse(name)
                .map(Self::Bucket)
                .map_err(|_| invalid(format!("unknown root directory {name:?}"))),
            Self::Staging if name == "multipart" => Ok(Self::Multipart),
            Self::Multipart => {
                let path = StoragePath::from_string(format!("{prefix}/{name}/00001"));
                validate_storage_path(&orphan_bucket(), &path)
                    .map_err(|_| invalid(format!("unsupported upload directory {name:?}")))?;
                Ok(Self::Session)
            }
            Self::Bucket(bucket)
                if name.len() == 2
                    && name
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
            {
                Ok(Self::Leaf(bucket.clone()))
            }
            _ => Err(invalid(format!("unsupported directory {prefix}/{name}"))),
        }
    }

    fn file(&self, prefix: &str, name: &str) -> Result<StoragePath, BlobError> {
        let path = StoragePath::from_string(format!("{prefix}/{name}"));
        let bucket = match self {
            Self::Bucket(bucket) | Self::Leaf(bucket) => bucket.clone(),
            Self::Staging | Self::Session => orphan_bucket(),
            _ => return Err(invalid(format!("unsupported file {prefix}/{name}"))),
        };
        validate_storage_path(&bucket, &path)
            .map_err(|_| invalid(format!("unsupported storage file {}", path.as_str())))?;
        Ok(path)
    }
}

fn orphan_bucket() -> BucketName {
    BucketName::parse("cairn-storage-orphans").expect("valid retained orphan routing bucket")
}

struct Frame {
    file: Arc<File>,
    entries: Dir,
    parent: Option<(Arc<File>, OsString)>,
    kind: DirectoryKind,
    prefix: String,
}

impl Frame {
    fn new(
        file: Arc<File>,
        parent: Option<(Arc<File>, OsString)>,
        kind: DirectoryKind,
        prefix: String,
    ) -> Result<Self, BlobError> {
        let entries = Dir::read_from(&*file).map_err(|error| io_err(error.into()))?;
        Ok(Self {
            file,
            entries,
            parent,
            kind,
            prefix,
        })
    }

    fn validate_link(&self) -> Result<(), BlobError> {
        validate_directory_link(
            self.parent
                .as_ref()
                .map(|(parent, name)| (parent.as_ref(), name.as_os_str())),
            &self.file,
        )
    }
}

fn validate_directory_link(parent: Option<(&File, &OsStr)>, file: &File) -> Result<(), BlobError> {
    let opened = fstat(file).map_err(|error| io_err(error.into()))?;
    if opened.st_nlink == 0 {
        return Err(invalid("a walked directory was detached"));
    }
    if let Some((parent, name)) = parent {
        let named = statat(parent, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|error| io_err(error.into()))?;
        if opened.st_dev != named.st_dev || opened.st_ino != named.st_ino {
            return Err(invalid("a walked directory was replaced"));
        }
    }
    Ok(())
}

struct Walker {
    // The accepted grammar has at most four open frames. Each frame owns one bounded dir buffer.
    stack: Vec<Frame>,
    root_artifacts: Vec<OsString>,
    directories: u64,
    verify: bool,
    hooks: Hooks,
}

impl Walker {
    fn new(
        root: Arc<File>,
        opts: &StorageBaselineOptions,
        verify: bool,
        hooks: Hooks,
    ) -> Result<Self, BlobError> {
        Ok(Self {
            stack: vec![Frame::new(root, None, DirectoryKind::Root, String::new())?],
            root_artifacts: opts.root_artifacts.clone(),
            directories: 1,
            verify,
            hooks,
        })
    }

    fn page(&mut self, limit: usize) -> Result<Vec<StoragePath>, BlobError> {
        self.hooks.page()?;
        let mut files = Vec::with_capacity(limit);
        let mut examined = 0;
        while !self.stack.is_empty() && examined < limit {
            let frame = self.stack.last_mut().expect("nonempty bounded walk stack");
            let Some(entry) = frame.entries.next() else {
                let frame = self.stack.pop().expect("completed walk frame");
                frame.validate_link()?;
                if self.verify {
                    self.hooks.sync(&frame.file, &frame.prefix)?;
                }
                continue;
            };
            let entry = entry.map_err(|error| io_err(error.into()))?;
            examined += 1;
            let name = OsStr::from_bytes(entry.file_name().to_bytes());
            if name == "." || name == ".." {
                continue;
            }
            let stat = statat(&*frame.file, name, AtFlags::SYMLINK_NOFOLLOW)
                .map_err(|error| io_err(error.into()))?;
            match FileType::from_raw_mode(stat.st_mode) {
                FileType::Directory => {
                    if matches!(frame.kind, DirectoryKind::Root)
                        && self.root_artifacts.iter().any(|allowed| allowed == name)
                    {
                        return Err(invalid("a root file exemption names a directory"));
                    }
                    let text = name
                        .to_str()
                        .ok_or_else(|| invalid("non-UTF-8 directory"))?;
                    let kind = frame.kind.child(&frame.prefix, text)?;
                    let file = open_beneath(
                        &frame.file,
                        name,
                        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                        Mode::empty(),
                    )
                    .map_err(io_err)?;
                    let opened = fstat(&file).map_err(|error| io_err(error.into()))?;
                    if opened.st_dev != stat.st_dev
                        || opened.st_ino != stat.st_ino
                        || opened.st_nlink == 0
                    {
                        return Err(invalid("directory changed during open"));
                    }
                    rustix::fs::flock(&file, rustix::fs::FlockOperation::LockShared)
                        .map_err(|error| io_err(error.into()))?;
                    let prefix = if frame.prefix.is_empty() {
                        text.to_owned()
                    } else {
                        format!("{}/{text}", frame.prefix)
                    };
                    let child = Frame::new(
                        Arc::new(file),
                        Some((frame.file.clone(), name.to_owned())),
                        kind,
                        prefix,
                    )?;
                    child.validate_link()?;
                    self.stack.push(child);
                    self.directories += 1;
                }
                FileType::RegularFile => {
                    let exempt = matches!(frame.kind, DirectoryKind::Root)
                        && self.root_artifacts.iter().any(|allowed| allowed == name);
                    validate_file(&frame.file, name, Some((stat.st_dev, stat.st_ino)), !exempt)?;
                    if !exempt {
                        let text = name
                            .to_str()
                            .ok_or_else(|| invalid("non-UTF-8 storage file"))?;
                        files.push(frame.kind.file(&frame.prefix, text)?);
                    }
                }
                _ => return Err(invalid(format!("unsupported filesystem entry {name:?}"))),
            }
        }
        Ok(files)
    }
}

fn validate_file(
    parent: &File,
    name: &OsStr,
    expected: Option<(u64, u64)>,
    require_quiescence: bool,
) -> Result<(File, u64), BlobError> {
    let file = open_beneath(
        parent,
        name,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io_err)?;
    let opened = fstat(&file).map_err(|error| io_err(error.into()))?;
    if FileType::from_raw_mode(opened.st_mode) != FileType::RegularFile
        || opened.st_nlink != 1
        || expected.is_some_and(|identity| identity != (opened.st_dev, opened.st_ino))
    {
        return Err(invalid(
            "storage file changed, is not regular, or has multiple links",
        ));
    }
    // Node-lock exemptions are already locked by this caller. Every actual storage file,
    // including authoritative files, must exclude a remaining kernel writer.
    if require_quiescence {
        crate::try_lock_exclusive(&file).map_err(io_err)?;
    }
    let named =
        statat(parent, name, AtFlags::SYMLINK_NOFOLLOW).map_err(|error| io_err(error.into()))?;
    if named.st_dev != opened.st_dev || named.st_ino != opened.st_ino {
        return Err(invalid("storage file name changed during validation"));
    }
    let size =
        u64::try_from(opened.st_size).map_err(|_| invalid("negative storage file length"))?;
    Ok((file, size))
}

async fn open_root(root: &Path, lease: &StorageIoLease) -> Result<Arc<File>, BlobError> {
    let root = root.to_owned();
    blocking(lease, move || {
        let file: File = open(
            &root,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| io_err(error.into()))?
        .into();
        Ok(Arc::new(file))
    })
    .await
}

fn root_identity(root: &File) -> Result<(u64, u64), BlobError> {
    let stat = fstat(root).map_err(|error| io_err(error.into()))?;
    if stat.st_nlink == 0 {
        return Err(invalid("storage root was detached"));
    }
    Ok((stat.st_dev, stat.st_ino))
}

async fn owners(
    meta: &dyn MetadataStore,
    paths: &[StoragePath],
) -> Result<Vec<cairn_types::storage_baseline::StoragePathOwnership>, BlobError> {
    let owners = meta
        .storage_path_owners(paths)
        .await
        .map_err(|error| invalid(error.to_string()))?;
    if owners.len() != paths.len()
        || owners.iter().zip(paths).any(|(owner, path)| {
            &owner.path != path
                || ((owner.authoritative || owner.intent || owner.cleanup || owner.legacy_debt)
                    && owner.bucket.is_none())
        })
    {
        return Err(invalid("incorrect or unowned path classification response"));
    }
    Ok(owners)
}

pub(super) async fn classify(
    root: &Path,
    meta: &dyn MetadataStore,
    opts: &StorageBaselineOptions,
    lease: StorageIoLease,
) -> Result<StorageClassificationReport, BlobError> {
    classify_with(root, meta, opts, lease, Hooks::default()).await
}

async fn classify_with(
    root: &Path,
    meta: &dyn MetadataStore,
    opts: &StorageBaselineOptions,
    lease: StorageIoLease,
    hooks: Hooks,
) -> Result<StorageClassificationReport, BlobError> {
    validate_options(opts)?;
    require_hold(meta, opts).await?;
    let root_path = root.to_owned();
    let root = open_root(root, &lease).await?;
    let file = root.clone();
    let options = opts.clone();
    let (identity, mut walker) = blocking(&lease, move || {
        Ok((
            root_identity(&file)?,
            Walker::new(file, &options, false, hooks)?,
        ))
    })
    .await?;
    let mut counts = StorageClassificationCounts::default();
    while !walker.stack.is_empty() {
        let limit = opts.batch_size as usize;
        let (next, paths) = blocking(&lease, move || {
            let paths = walker.page(limit)?;
            Ok((walker, paths))
        })
        .await?;
        walker = next;
        if paths.is_empty() {
            continue;
        }
        require_hold(meta, opts).await?;
        let owners = owners(meta, &paths).await?;
        let mut batches: BTreeMap<BucketName, Vec<StoragePath>> = BTreeMap::new();
        for (path, owner) in paths.into_iter().zip(owners) {
            let bucket = if let Some(bucket) = owner.bucket {
                bucket
            } else if path.as_str().starts_with(".staging/") {
                orphan_bucket()
            } else {
                BucketName::parse(path.as_str().split('/').next().unwrap_or_default())
                    .map_err(|_| invalid("unroutable final storage path"))?
            };
            validate_storage_path(&bucket, &path)
                .map_err(|_| invalid("path owner conflicts with physical bucket"))?;
            batches.entry(bucket).or_default().push(path);
        }
        for (bucket, paths) in batches {
            let expected = paths.len();
            match meta
                .submit(Mutation::ClassifyStorageBaseline {
                    bucket,
                    token: opts.token.clone(),
                    paths,
                })
                .await
                .map_err(|error| invalid(error.to_string()))?
            {
                MutationOutcome::StorageBaselineClassified(dispositions)
                    if dispositions.len() == expected =>
                {
                    counts.files_scanned += expected as u64;
                    for disposition in dispositions {
                        match disposition {
                            StorageBaselineDisposition::Authoritative => {
                                counts.referenced_files += 1
                            }
                            StorageBaselineDisposition::IntentOwned => counts.journal_files += 1,
                            StorageBaselineDisposition::CleanupRecorded => {
                                counts.debts_recorded += 1
                            }
                        }
                    }
                }
                _ => return Err(invalid("incorrect durable classification acknowledgement")),
            }
        }
    }
    counts.directories_scanned = walker.directories;
    require_hold(meta, opts).await?;
    let named_root = open_root(&root_path, &lease).await?;
    if blocking(&lease, move || root_identity(&named_root)).await? != identity {
        return Err(invalid("classified root name changed"));
    }
    Ok(StorageClassificationReport::backend_completed(
        opts.clone(),
        identity.0,
        identity.1,
        counts,
        Arc::new(RootLifetime {
            _root: root,
            _lease: lease,
        }),
    ))
}

fn cursor_key(cursor: &StorageAuthorityCursor) -> (u32, u8, &str, u16) {
    (
        cursor.shard,
        match cursor.kind {
            StorageAuthorityKind::Objects => 0,
            StorageAuthorityKind::Parts => 1,
        },
        &cursor.last_id,
        cursor.last_part,
    )
}

fn verify_authority(root: &File, authority: StorageAuthority) -> Result<bool, BlobError> {
    let mut encrypted_part = None;
    let (bucket, path, size) = match authority {
        StorageAuthority::Object(row) => {
            if row.is_delete_marker {
                if row.storage_path.is_some() {
                    return Err(invalid("delete marker owns a physical file"));
                }
                return Ok(false);
            }
            let path = row
                .storage_path
                .ok_or_else(|| invalid("object has no storage path"))?;
            if path.as_str().split('/').next() != Some(row.bucket.as_str()) {
                return Err(invalid("object path does not belong to its bucket"));
            }
            (row.bucket, path, Some(row.size_physical))
        }
        StorageAuthority::Part {
            bucket,
            upload_id,
            part,
        } => {
            let prefix = format!(".staging/multipart/{upload_id}/");
            let name = part
                .storage_path
                .as_str()
                .strip_prefix(&prefix)
                .ok_or_else(|| invalid("part path does not belong to its upload"))?;
            if name
                .split('-')
                .next()
                .and_then(|number| number.parse::<u16>().ok())
                != Some(part.part_number)
            {
                return Err(invalid("part path does not match its part number"));
            }
            if let Some(sealed) = &part.part_dek {
                let authenticated =
                    if sealed.starts_with(cairn_types::sse::AUTHENTICATED_PART_DEK_PREFIX) {
                        true
                    } else if sealed.contains(':') {
                        return Err(invalid("unsupported encrypted part format marker"));
                    } else {
                        false
                    };
                encrypted_part = Some((part.size, authenticated));
            }
            (
                bucket,
                part.storage_path,
                part.part_dek.is_none().then_some(part.size),
            )
        }
    };
    validate_storage_path(&bucket, &path).map_err(|error| invalid(error.to_string()))?;
    // Keep each parent anchored and revalidate its single-component link after the probe. A
    // pathname stat following the complete relative name would reintroduce ancestor symlinks.
    let components: Vec<_> = path.as_str().split('/').collect();
    let mut parents: Vec<File> = Vec::with_capacity(components.len() - 1);
    for name in &components[..components.len() - 1] {
        let parent = parents.last().unwrap_or(root);
        let directory = open_beneath(
            parent,
            *name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io_err)?;
        rustix::fs::flock(&directory, rustix::fs::FlockOperation::LockShared)
            .map_err(|error| io_err(error.into()))?;
        validate_directory_link(Some((parent, OsStr::new(name))), &directory)?;
        parents.push(directory);
    }
    let (mut file, actual) = validate_file(
        parents.last().unwrap_or(root),
        OsStr::new(components.last().expect("validated storage path")),
        None,
        true,
    )?;
    if size.is_some_and(|expected| expected != actual) {
        return Err(invalid(format!(
            "physical length mismatch for {}",
            path.as_str()
        )));
    }
    if let Some((logical_len, authenticated)) = encrypted_part {
        crate::compress::verify_encrypted_part_geometry(
            &mut file,
            actual,
            logical_len,
            authenticated,
        )?;
    }
    for index in (0..parents.len()).rev() {
        let parent = if index == 0 {
            root
        } else {
            &parents[index - 1]
        };
        validate_directory_link(
            Some((parent, OsStr::new(components[index]))),
            &parents[index],
        )?;
    }
    Ok(true)
}

pub(super) async fn verify(
    root: &Path,
    meta: &dyn MetadataStore,
    opts: &StorageBaselineOptions,
    classification: &StorageClassificationReport,
    lease: StorageIoLease,
) -> Result<StorageBaselineProof, BlobError> {
    verify_with(root, meta, opts, classification, lease, Hooks::default()).await
}

async fn verify_with(
    root: &Path,
    meta: &dyn MetadataStore,
    opts: &StorageBaselineOptions,
    classification: &StorageClassificationReport,
    lease: StorageIoLease,
    hooks: Hooks,
) -> Result<StorageBaselineProof, BlobError> {
    validate_options(opts)?;
    if opts != classification.options() {
        return Err(invalid(
            "verification does not match its completed classification",
        ));
    }
    require_hold(meta, opts).await?;
    require_drained(meta).await?;
    let root_path = root.to_owned();
    let root = open_root(root, &lease).await?;
    let file = root.clone();
    let identity = blocking(&lease, move || root_identity(&file)).await?;
    if identity != classification.root_identity() {
        return Err(invalid("verification root differs from classification"));
    }
    let mut cursor: Option<StorageAuthorityCursor> = None;
    let mut authoritative_files = 0;
    loop {
        let page = meta
            .enumerate_storage_authority(cursor.as_ref(), opts.batch_size)
            .await
            .map_err(|error| invalid(error.to_string()))?;
        if page.items.len() > opts.batch_size as usize
            || page.next.as_ref().is_some_and(|next| {
                cursor
                    .as_ref()
                    .is_some_and(|prior| cursor_key(next) <= cursor_key(prior))
            })
        {
            return Err(invalid("invalid bounded authority page"));
        }
        let file = root.clone();
        authoritative_files += blocking(&lease, move || {
            let mut verified = 0;
            for authority in page.items {
                verified += u64::from(verify_authority(&file, authority)?);
            }
            Ok(verified)
        })
        .await?;
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    let file = root.clone();
    let options = opts.clone();
    let mut walker = blocking(&lease, move || Walker::new(file, &options, true, hooks)).await?;
    while !walker.stack.is_empty() {
        let limit = opts.batch_size as usize;
        let (next, paths) = blocking(&lease, move || {
            let paths = walker.page(limit)?;
            Ok((walker, paths))
        })
        .await?;
        walker = next;
        if !paths.is_empty()
            && owners(meta, &paths)
                .await?
                .iter()
                .any(|owner| !owner.authoritative)
        {
            return Err(invalid("unreferenced storage file remains after cleanup"));
        }
    }
    require_drained(meta).await?;
    require_hold(meta, opts).await?;
    let named_root = open_root(&root_path, &lease).await?;
    if blocking(&lease, move || root_identity(&named_root)).await? != classification.root_identity()
    {
        return Err(invalid("verified root identity changed"));
    }
    Ok(StorageBaselineProof::backend_verified(
        classification.clone(),
        authoritative_files,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::KeyInit;
    use cairn_types::storage::{StorageMutation, StorageToken, io::StorageIoWatch};
    use cairn_types::storage_baseline::{StorageBaselineToken, StorageBaselineTransition};
    use cairn_types::testing::{
        FixtureMetadataStore, InMemoryMetadataStore, PublicationFixture, fixture_storage_io,
    };
    use cairn_types::{
        BlobStore, Bucket, CompressionDescriptor, ETag, ObjectKey, ObjectVersionRow, OwnershipMode,
        PartRecord, Timestamp, UploadId, UserId, VersionId, VersioningState,
    };
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::symlink;
    use std::sync::Mutex;

    async fn begin(meta: &InMemoryMetadataStore, batch_size: u32) -> StorageBaselineOptions {
        let token = StorageBaselineToken {
            generation: StorageToken::generate(),
            baseline_id: StorageToken::generate(),
        };
        meta.submit(Mutation::BeginStorageGeneration {
            generation: token.generation.clone(),
        })
        .await
        .unwrap();
        assert!(matches!(
            meta.submit(Mutation::BeginStorageBaseline {
                token: token.clone()
            })
            .await
            .unwrap(),
            MutationOutcome::StorageBaselineUpdated(StorageBaselineTransition::Applied)
        ));
        StorageBaselineOptions {
            token,
            batch_size,
            root_artifacts: vec![".cairn-data.lock".into()],
        }
    }

    async fn local(root: &Path) -> crate::LocalBlobStore {
        crate::LocalBlobStore::open(root.to_owned(), fixture_storage_io())
            .await
            .unwrap()
    }

    fn write(root: &Path, path: &str) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"live").unwrap();
    }

    async fn drain(
        meta: &InMemoryMetadataStore,
        blob: &crate::LocalBlobStore,
        opts: &StorageBaselineOptions,
    ) {
        loop {
            let MutationOutcome::StorageCleanupBatch(batch) = meta
                .submit(Mutation::ClaimStorageCleanup {
                    generation: opts.token.generation.clone(),
                    limit: 7,
                    now: Timestamp(100),
                    lease_secs: 60,
                })
                .await
                .unwrap()
            else {
                panic!("cleanup batch expected")
            };
            if batch.is_empty() {
                break;
            }
            for cleanup in batch {
                let lease = StorageIoWatch::new(
                    cleanup.id.clone(),
                    cleanup.generation.clone(),
                    Arc::new(()),
                )
                .1;
                blob.cleanup_storage(&cleanup, lease).await.unwrap();
                assert_eq!(
                    meta.submit(Mutation::Storage {
                        bucket: cleanup.bucket.clone(),
                        operation: StorageMutation::FinishCleanup {
                            cleanup,
                            now: Timestamp(101)
                        },
                    })
                    .await
                    .unwrap(),
                    MutationOutcome::StorageUpdated { applied: true }
                );
            }
        }
        assert!(
            !meta
                .storage_baseline_pending()
                .await
                .unwrap()
                .native_pending()
        );
    }

    async fn bucket(meta: &InMemoryMetadataStore) -> BucketName {
        let name = BucketName::parse("baseline-live").unwrap();
        meta.submit(Mutation::CreateBucket(Box::new(Bucket {
            name: name.clone(),
            owner_id: UserId("owner".into()),
            created_at: Timestamp(1),
            versioning: VersioningState::Enabled,
            ownership_mode: OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".into(),
            compression: None,
        })))
        .await
        .unwrap();
        name
    }

    async fn object(
        meta: &InMemoryMetadataStore,
        fixture: &PublicationFixture,
        root: &Path,
        bucket: &BucketName,
    ) -> ObjectVersionRow {
        let id = StorageToken::generate().as_str().to_owned();
        let path = StoragePath::from_string(format!("{bucket}/{id}"));
        write(root, path.as_str());
        let row = ObjectVersionRow {
            id,
            bucket: bucket.clone(),
            key: ObjectKey::parse("history").unwrap(),
            version_id: VersionId::generate(),
            is_latest: true,
            is_delete_marker: false,
            size_logical: 4,
            size_physical: 4,
            etag: ETag::from_string("etag".into()),
            content_type: "application/octet-stream".into(),
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            storage_path: Some(path),
            compression: CompressionDescriptor::Uncompressed,
            storage_class: cairn_types::StorageClass::Standard,
            cold_locator: None,
            owner_id: UserId("owner".into()),
            user_metadata: vec![],
            acl: None,
            checksums: vec![],
            sse_descriptor: None,
            replication_status: None,
            internal_sha256: None,
            replicated_at: None,
            created_at: Timestamp(1),
            updated_at: Timestamp(1),
        };
        meta.submit_fixture(
            fixture,
            Mutation::PutObjectVersion {
                row: Box::new(row.clone()),
                precondition: Default::default(),
                initial_state: Default::default(),
                replication: vec![],
            },
        )
        .await
        .unwrap();
        row
    }

    async fn part(
        meta: &InMemoryMetadataStore,
        fixture: &PublicationFixture,
        root: &Path,
        bucket: &BucketName,
        encrypted: bool,
    ) -> (UploadId, PartRecord) {
        let upload = UploadId::generate();
        meta.submit(Mutation::CreateMultipart {
            session: Box::new(cairn_types::MultipartSession {
                upload_id: upload.clone(),
                bucket: bucket.clone(),
                key: ObjectKey::parse("upload").unwrap(),
                content_type: "application/octet-stream".into(),
                status: cairn_types::MultipartStatus::Active,
                owner_id: UserId("owner".into()),
                initiated_by: UserId("owner".into()),
                intended_acl: None,
                replica_intent: None,
                user_metadata: vec![],
                initial_tags: vec![],
                lock_intent: Default::default(),
                sse_requested: encrypted,
                encrypt_parts: encrypted,
                sse_kms_requested: false,
                sse_kms_key_id: None,
                sse_bucket_key_enabled: false,
                created_at: Timestamp(1),
                updated_at: Timestamp(1),
            }),
            limits: Default::default(),
        })
        .await
        .unwrap();
        let attempt = StorageToken::generate().as_str().to_owned();
        let plan = fixture.part_plan(bucket, &upload, 1, &attempt).unwrap();
        meta.submit(
            PublicationFixture::admission(
                plan.clone(),
                Mutation::ReserveMultipartPart {
                    upload_id: upload.clone(),
                    part_number: 1,
                    attempt_id: attempt.clone(),
                    reserved_bytes: 4,
                    max_parts_per_upload: 10000,
                    now: Timestamp(1),
                },
                Timestamp(1),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let dek = cairn_types::SecretKey32::new(
            aes_gcm::Aes256Gcm::generate_key(&mut aes_gcm::aead::OsRng).into(),
        );
        let part = PartRecord {
            part_number: 1,
            size: 4,
            etag: "etag".into(),
            storage_path: plan.final_path().unwrap().clone(),
            checksum: None,
            part_dek: encrypted.then(|| {
                cairn_types::sse::seal_part_blob_cipher(&cairn_types::testing::StubCrypto, &dek)
                    .unwrap()
            }),
        };
        write(root, part.storage_path.as_str());
        if encrypted {
            let mut encoder = crate::compress::BlockEncoder::new_encrypted(
                cairn_types::CompressionAlgorithm::None,
                crate::DEFAULT_ENCRYPTED_BLOCK_SIZE,
                dek,
            );
            let mut bytes = encoder.feed(b"live").unwrap();
            let tail = encoder.finish_parts().unwrap();
            bytes.extend(tail.payload);
            bytes.extend(tail.index);
            bytes.extend(tail.footer);
            std::fs::write(root.join(part.storage_path.as_str()), bytes).unwrap();
        }
        meta.submit(
            PublicationFixture::publication(
                plan,
                Mutation::RecordPart {
                    upload_id: upload.clone(),
                    attempt_id: attempt,
                    part: part.clone(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
        (upload, part)
    }

    #[tokio::test]
    async fn classification_pages_record_debt_before_any_physical_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let blob = local(root.path()).await;
        let meta = InMemoryMetadataStore::new();
        let opts = begin(&meta, 3).await;
        let mut paths = Vec::new();
        for index in 0..23 {
            let id = format!("{index:032x}");
            let path = if index % 2 == 0 {
                format!("deleted-bucket/{id}")
            } else {
                format!(".staging/{id}.index.tmp")
            };
            write(root.path(), &path);
            paths.push(StoragePath::from_string(path));
        }
        let report = blob
            .classify_storage_baseline(&meta, &opts, fixture_storage_io())
            .await
            .unwrap();
        assert_eq!(report.counts().files_scanned, 23);
        assert_eq!(report.counts().debts_recorded, 23);
        for path in &paths {
            assert!(root.path().join(path.as_str()).exists());
        }
        for owner in meta.storage_path_owners(&paths).await.unwrap() {
            let expected = if owner.path.as_str().starts_with(".staging/") {
                "cairn-storage-orphans"
            } else {
                "deleted-bucket"
            };
            assert_eq!(owner.bucket.unwrap().as_str(), expected);
            assert!(owner.cleanup);
        }
        assert!(
            blob.verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
                .await
                .is_err()
        );
        drain(&meta, &blob, &opts).await;
        let proof = blob
            .verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
            .await
            .unwrap();
        assert_eq!(proof.token(), &opts.token);
        assert_eq!(proof.authoritative_files(), 0);
        assert!(meta.storage_baseline_states().await.unwrap()[0].legacy_accounting_hold);
    }

    #[tokio::test]
    async fn reverse_proof_preserves_history_markers_and_active_parts() {
        let root = tempfile::tempdir().unwrap();
        let blob = local(root.path()).await;
        let meta = InMemoryMetadataStore::new();
        let fixture = meta.begin_fixture().await.unwrap();
        let bucket = bucket(&meta).await;
        let first = object(&meta, &fixture, root.path(), &bucket).await;
        let second = object(&meta, &fixture, root.path(), &bucket).await;
        let (upload, part) = part(&meta, &fixture, root.path(), &bucket, false).await;
        let mut marker = second.clone();
        marker.id = StorageToken::generate().as_str().to_owned();
        marker.version_id = VersionId::generate();
        marker.is_delete_marker = true;
        marker.storage_path = None;
        marker.size_logical = 0;
        marker.size_physical = 0;
        meta.submit_fixture(
            &fixture,
            Mutation::PutObjectVersion {
                row: Box::new(marker),
                precondition: Default::default(),
                initial_state: Default::default(),
                replication: vec![],
            },
        )
        .await
        .unwrap();
        let opts = begin(&meta, 1).await;
        let report = blob
            .classify_storage_baseline(&meta, &opts, fixture_storage_io())
            .await
            .unwrap();
        assert_eq!(report.counts().referenced_files, 3);
        drain(&meta, &blob, &opts).await;
        let proof = blob
            .verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
            .await
            .unwrap();
        assert_eq!(proof.authoritative_files(), 3);
        for row in [first, second] {
            assert!(
                meta.get_version(&bucket, &row.key, &row.version_id)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                std::fs::read(root.path().join(row.storage_path.unwrap().as_str())).unwrap(),
                b"live"
            );
        }
        assert_eq!(
            meta.list_parts(&upload, 0, 10).await.unwrap().items,
            vec![part]
        );
        assert!(meta.get_multipart(&upload).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn missing_or_truncated_authority_cannot_produce_a_proof() {
        for missing in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let blob = local(root.path()).await;
            let meta = InMemoryMetadataStore::new();
            let fixture = meta.begin_fixture().await.unwrap();
            let bucket = bucket(&meta).await;
            let row = object(&meta, &fixture, root.path(), &bucket).await;
            let opts = begin(&meta, 2).await;
            let report = blob
                .classify_storage_baseline(&meta, &opts, fixture_storage_io())
                .await
                .unwrap();
            drain(&meta, &blob, &opts).await;
            let path = root
                .path()
                .join(row.storage_path.as_ref().unwrap().as_str());
            if missing {
                std::fs::remove_file(&path).unwrap();
            } else {
                std::fs::write(&path, b"x").unwrap();
            }
            assert!(
                blob.verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
                    .await
                    .is_err()
            );
            assert!(
                meta.get_version(&bucket, &row.key, &row.version_id)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(meta.storage_baseline_states().await.unwrap()[0].legacy_accounting_hold);
        }
    }

    #[tokio::test]
    async fn encrypted_part_truncation_is_rejected_without_opening_its_key() {
        let root = tempfile::tempdir().unwrap();
        let blob = local(root.path()).await;
        let meta = InMemoryMetadataStore::new();
        let fixture = meta.begin_fixture().await.unwrap();
        let bucket = bucket(&meta).await;
        let (upload, part) = part(&meta, &fixture, root.path(), &bucket, true).await;
        let opts = begin(&meta, 1).await;
        let report = blob
            .classify_storage_baseline(&meta, &opts, fixture_storage_io())
            .await
            .unwrap();
        drain(&meta, &blob, &opts).await;
        let proof = blob
            .verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
            .await
            .unwrap();
        assert_eq!(proof.authoritative_files(), 1);
        let path = root.path().join(part.storage_path.as_str());
        let bytes = std::fs::read(&path).unwrap();
        for length in [0, bytes.len() - 1] {
            std::fs::write(&path, &bytes[..length]).unwrap();
            assert!(
                blob.verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
                    .await
                    .is_err()
            );
            assert_eq!(
                meta.list_parts(&upload, 0, 10).await.unwrap().items,
                vec![part.clone()]
            );
            assert!(meta.storage_baseline_states().await.unwrap()[0].legacy_accounting_hold);
        }
        std::fs::write(&path, bytes).unwrap();
        blob.verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn every_entry_is_validated_even_when_referenced_or_exactly_exempt() {
        for damage in [
            "symlink",
            "ancestor-symlink",
            "hardlink",
            "fifo",
            "unknown-root",
            "unknown-child",
            "exempt-symlink",
        ] {
            let root = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            let blob = local(root.path()).await;
            let meta = InMemoryMetadataStore::new();
            let fixture = meta.begin_fixture().await.unwrap();
            let bucket = bucket(&meta).await;
            let row = object(&meta, &fixture, root.path(), &bucket).await;
            let path = root
                .path()
                .join(row.storage_path.as_ref().unwrap().as_str());
            let victim = outside.path().join("victim");
            std::fs::write(&victim, b"live").unwrap();
            let opts = begin(&meta, 2).await;
            let report = blob
                .classify_storage_baseline(&meta, &opts, fixture_storage_io())
                .await
                .unwrap();
            match damage {
                "symlink" => {
                    std::fs::remove_file(&path).unwrap();
                    symlink(&victim, &path).unwrap();
                }
                "hardlink" => {
                    std::fs::hard_link(&path, outside.path().join("alias")).unwrap();
                }
                "ancestor-symlink" => {
                    let directory = root.path().join(bucket.as_str());
                    let retained = outside.path().join("retained");
                    std::fs::rename(&directory, &retained).unwrap();
                    symlink(&retained, &directory).unwrap();
                }
                "fifo" => {
                    std::fs::remove_file(&path).unwrap();
                    rustix::fs::mknodat(
                        rustix::fs::CWD,
                        &path,
                        FileType::Fifo,
                        Mode::from_bits_truncate(0o600),
                        0,
                    )
                    .unwrap();
                }
                "unknown-root" => {
                    std::fs::write(root.path().join(".unexpected.cairn-db.lock"), b"keep").unwrap();
                }
                "unknown-child" => {
                    write(root.path(), "baseline-live/unrecognized");
                }
                "exempt-symlink" => {
                    symlink(&victim, root.path().join(".cairn-data.lock")).unwrap();
                }
                _ => unreachable!(),
            }
            assert!(
                blob.classify_storage_baseline(&meta, &opts, fixture_storage_io())
                    .await
                    .is_err(),
                "{damage}"
            );
            assert!(
                blob.verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
                    .await
                    .is_err(),
                "reverse proof: {damage}"
            );
            assert!(std::fs::symlink_metadata(&path).is_ok());
            assert_eq!(std::fs::read(&victim).unwrap(), b"live");
            assert!(meta.storage_baseline_states().await.unwrap()[0].legacy_accounting_hold);
        }
    }

    #[tokio::test]
    async fn exact_root_exemptions_support_non_utf8_and_the_retained_node_lock() {
        let root = tempfile::tempdir().unwrap();
        let blob = local(root.path()).await;
        let meta = InMemoryMetadataStore::new();
        let mut opts = begin(&meta, 1).await;
        let database = OsString::from_vec(b"database-\xff".to_vec());
        std::fs::write(root.path().join(&database), b"database").unwrap();
        opts.root_artifacts.push(database);
        let lock = crate::open_lock_file_nofollow(&root.path().join(".cairn-data.lock")).unwrap();
        crate::try_lock_exclusive(&lock).unwrap();
        let report = blob
            .classify_storage_baseline(&meta, &opts, fixture_storage_io())
            .await
            .unwrap();
        let proof = blob
            .verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
            .await
            .unwrap();
        assert_eq!(proof.authoritative_files(), 0);
        assert_eq!(report.counts().files_scanned, 0);
    }

    #[tokio::test]
    async fn sync_is_child_first_root_last_and_failure_preserves_the_hold() {
        let root = tempfile::tempdir().unwrap();
        let blob = local(root.path()).await;
        std::fs::create_dir_all(root.path().join("empty-bucket/ab")).unwrap();
        let meta = InMemoryMetadataStore::new();
        let opts = begin(&meta, 1).await;
        let report = blob
            .classify_storage_baseline(&meta, &opts, fixture_storage_io())
            .await
            .unwrap();
        let synced = Arc::new(Mutex::new(Vec::new()));
        let observed = synced.clone();
        let hooks = Hooks {
            before_page: None,
            before_sync: Some(Arc::new(move |path| {
                observed.lock().unwrap().push(path.to_owned());
                Ok(())
            })),
        };
        verify_with(
            root.path(),
            &meta,
            &opts,
            &report,
            fixture_storage_io(),
            hooks,
        )
        .await
        .unwrap();
        {
            let synced = synced.lock().unwrap();
            assert_eq!(synced.last().unwrap(), "");
            for (child, parent) in [
                ("empty-bucket/ab", "empty-bucket"),
                (".staging/multipart", ".staging"),
            ] {
                assert!(
                    synced.iter().position(|path| path == child)
                        < synced.iter().position(|path| path == parent)
                );
            }
        }
        for failed in [".staging/multipart", "empty-bucket", ""] {
            let hooks = Hooks {
                before_page: None,
                before_sync: Some(Arc::new(move |path| {
                    if path == failed {
                        Err(invalid("injected directory sync failure"))
                    } else {
                        Ok(())
                    }
                })),
            };
            assert!(
                verify_with(
                    root.path(),
                    &meta,
                    &opts,
                    &report,
                    fixture_storage_io(),
                    hooks
                )
                .await
                .is_err()
            );
            let state = &meta.storage_baseline_states().await.unwrap()[0];
            assert!(state.legacy_accounting_hold);
            assert!(!state.legacy_release_authorized);
        }
    }

    #[tokio::test]
    async fn cancelled_classification_keeps_actual_io_owned_and_never_deletes() {
        let root = tempfile::tempdir().unwrap();
        let _blob = local(root.path()).await;
        for index in 0..2 {
            write(root.path(), &format!("orphan-bucket/{index:032x}"));
        }
        let meta = Arc::new(InMemoryMetadataStore::new());
        let opts = begin(&meta, 1).await;
        let (entered, mut pages) = tokio::sync::mpsc::channel(1);
        let (release, wait) = std::sync::mpsc::channel();
        let wait = Mutex::new(wait);
        let hooks = Hooks {
            before_sync: None,
            before_page: Some(Arc::new(move || {
                entered
                    .blocking_send(())
                    .map_err(|_| invalid("page observer dropped"))?;
                wait.lock()
                    .unwrap()
                    .recv()
                    .map_err(|_| invalid("page release dropped"))
            })),
        };
        let (mut watch, lease) = StorageIoWatch::new(
            StorageToken::generate(),
            opts.token.generation.clone(),
            Arc::new(()),
        );
        let path = root.path().to_owned();
        let store = meta.clone();
        let task =
            tokio::spawn(
                async move { classify_with(&path, store.as_ref(), &opts, lease, hooks).await },
            );
        loop {
            pages.recv().await.unwrap();
            if meta.storage_baseline_pending().await.unwrap().exact_debt {
                break;
            }
            release.send(()).unwrap();
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let mut quiescent = std::pin::pin!(watch.quiescent());
        assert!(
            std::future::Future::poll(
                quiescent.as_mut(),
                &mut std::task::Context::from_waker(std::task::Waker::noop()),
            )
            .is_pending()
        );
        release.send(()).unwrap();
        quiescent.await;
        for index in 0..2 {
            assert!(
                root.path()
                    .join(format!("orphan-bucket/{index:032x}"))
                    .exists()
            );
        }
        let state = &meta.storage_baseline_states().await.unwrap()[0];
        assert!(state.legacy_accounting_hold);
        assert!(!state.legacy_release_authorized);
    }

    #[tokio::test]
    async fn proof_rejects_different_root_run_or_exemption_set() {
        let root = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let blob = local(root.path()).await;
        let other_blob = local(other.path()).await;
        let meta = InMemoryMetadataStore::new();
        let opts = begin(&meta, 1).await;
        let report = blob
            .classify_storage_baseline(&meta, &opts, fixture_storage_io())
            .await
            .unwrap();
        assert!(
            other_blob
                .verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
                .await
                .is_err()
        );
        let mut changed = opts.clone();
        changed.root_artifacts.push("extra".into());
        assert!(
            blob.verify_storage_baseline(&meta, &changed, &report, fixture_storage_io())
                .await
                .is_err()
        );
        let fresh = begin(&meta, 1).await;
        assert!(
            blob.verify_storage_baseline(&meta, &fresh, &report, fixture_storage_io())
                .await
                .is_err()
        );
        assert!(
            blob.verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    #[ignore = "requires a private mount namespace and bind-mount permission"]
    async fn same_device_mounts_block_classification_and_reverse_proof() {
        const CHILD: &str = "CAIRN_TEST_BASELINE_BIND_MOUNT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new("unshare")
                .args(["--mount", "--propagation", "private"])
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "baseline::tests::same_device_mounts_block_classification_and_reverse_proof",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
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
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let blob = local(root.path()).await;
        let meta = InMemoryMetadataStore::new();
        let fixture = meta.begin_fixture().await.unwrap();
        let bucket = bucket(&meta).await;
        let row = object(&meta, &fixture, root.path(), &bucket).await;
        let path = row.storage_path.unwrap();
        let name = Path::new(path.as_str()).file_name().unwrap();
        let source = outside.path().join(name);
        std::fs::write(&source, b"live").unwrap();
        let target = root.path().join(path.as_str());
        let opts = begin(&meta, 1).await;
        let report = blob
            .classify_storage_baseline(&meta, &opts, fixture_storage_io())
            .await
            .unwrap();
        drain(&meta, &blob, &opts).await;
        for (source, target) in [
            (outside.path().to_owned(), root.path().join(bucket.as_str())),
            (source, target),
        ] {
            assert!(
                std::process::Command::new("mount")
                    .arg("--bind")
                    .arg(&source)
                    .arg(&target)
                    .status()
                    .unwrap()
                    .success()
            );
            let mount = Mount(target);
            assert!(
                blob.classify_storage_baseline(&meta, &opts, fixture_storage_io())
                    .await
                    .is_err()
            );
            assert!(
                blob.verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
                    .await
                    .is_err()
            );
            drop(mount);
        }
        blob.verify_storage_baseline(&meta, &opts, &report, fixture_storage_io())
            .await
            .unwrap();
    }
}
