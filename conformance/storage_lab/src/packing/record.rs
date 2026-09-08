//! Laboratory-only immutable artifacts. SQLite supplies every creation/cleanup capability;
//! these synchronous functions return proofs only after their filesystem work has finished.

use super::model::{
    ArtifactAdmission, ArtifactIdentity, ArtifactKind, ArtifactPlan, CleanupClaim, Location,
    PublishedRecord,
};
use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags, ResolveFlags};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Component, Path};
use std::sync::Arc;

pub use super::model::{
    MAX_RECORDS as MAX_SEGMENT_RECORDS, MAX_SEGMENT_LENGTH,
    RECORD_HEADER_LENGTH as RECORD_HEADER_LEN, SEGMENT_HEADER_LENGTH as SEGMENT_HEADER_LEN,
};
pub const MAX_PACKED_RECORD_LENGTH: u64 = 1024 * 1024;
const SCRATCH_LENGTH: usize = 64 * 1024;
const MAGIC: &[u8; 8] = b"CRNLPK01";

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub fn segment_length(lengths: impl IntoIterator<Item = u64>) -> io::Result<u64> {
    let mut total = SEGMENT_HEADER_LEN;
    let mut count = 0;
    for length in lengths {
        count += 1;
        if count > MAX_SEGMENT_RECORDS || length > MAX_PACKED_RECORD_LENGTH {
            return Err(invalid("laboratory segment record bound exceeded"));
        }
        total = total
            .checked_add(RECORD_HEADER_LEN)
            .and_then(|value| value.checked_add(length))
            .ok_or_else(|| invalid("laboratory segment length overflow"))?;
        if total > MAX_SEGMENT_LENGTH {
            return Err(invalid("laboratory segment byte bound exceeded"));
        }
    }
    if count == 0 {
        return Err(invalid("an empty builder cannot become a segment"));
    }
    Ok(total)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableSpan {
    pub offset: u64,
    pub length: u64,
    pub sha256: [u8; 32],
}

pub struct DurableArtifact {
    admission: Box<ArtifactAdmission>,
    physical_length: u64,
    sha256: [u8; 32],
    spans: Vec<DurableSpan>,
    // Publication has not reached SQLite yet. Keep both the root lifetime (in admission)
    // and the artifact's descriptor pin until the Writer resolves the exact operation.
    root: Arc<File>,
    file: Arc<File>,
}

impl std::fmt::Debug for DurableArtifact {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableArtifact")
            .field("plan", self.plan())
            .field("physical_length", &self.physical_length)
            .field("spans", &self.spans)
            .finish_non_exhaustive()
    }
}

impl DurableArtifact {
    pub fn plan(&self) -> &ArtifactPlan {
        self.admission.plan()
    }

    pub fn physical_length(&self) -> u64 {
        self.physical_length
    }

    pub fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    pub fn spans(&self) -> &[DurableSpan] {
        &self.spans
    }

    pub fn into_quiescent(self) -> QuiescentArtifact {
        let Self {
            admission,
            root,
            file,
            ..
        } = self;
        drop(file);
        drop(root);
        QuiescentArtifact { admission }
    }
}

pub struct QuiescentArtifact {
    admission: Box<ArtifactAdmission>,
}

impl QuiescentArtifact {
    pub fn plan(&self) -> &ArtifactPlan {
        self.admission.plan()
    }
}

pub fn abort(admission: ArtifactAdmission) -> QuiescentArtifact {
    QuiescentArtifact {
        admission: Box::new(admission),
    }
}

pub struct PublishError {
    error: io::Error,
    quiescent: QuiescentArtifact,
}

impl PublishError {
    pub fn into_parts(self) -> (io::Error, QuiescentArtifact) {
        (self.error, self.quiescent)
    }
}

impl std::fmt::Debug for PublishError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for PublishError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Barrier {
    Create,
    Reserve,
    CopyRead,
    Write,
    FileSync,
    Rename,
    DirectorySync,
    Validate,
    Unlink,
    CleanupSync,
}

#[derive(Default)]
struct Hooks {
    #[cfg(test)]
    fail: Option<(Barrier, i32)>,
    #[cfg(test)]
    trace: std::sync::Mutex<Vec<Barrier>>,
    #[cfg(test)]
    fail_write_after: Option<usize>,
    #[cfg(test)]
    writes: std::sync::atomic::AtomicUsize,
}

impl Hooks {
    fn at(&self, barrier: Barrier) -> io::Result<()> {
        #[cfg(test)]
        {
            self.trace.lock().unwrap().push(barrier);
            if barrier == Barrier::Write
                && self.fail_write_after.is_some_and(|after| {
                    self.writes
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                        >= after
                })
            {
                return Err(io::Error::from_raw_os_error(28));
            }
            if let Some((failed, error)) = self.fail
                && failed == barrier
            {
                return Err(io::Error::from_raw_os_error(error));
            }
        }
        let _ = barrier;
        Ok(())
    }
}

fn open_root(root: &Path) -> io::Result<File> {
    rustix::fs::open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(Into::into)
}

fn basename(path: &Path) -> io::Result<&std::ffi::OsStr> {
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(name)), None) if name == path.as_os_str() => Ok(name),
        _ => Err(invalid("artifact alias must be an exact single basename")),
    }
}

fn open_beneath(root: &File, path: &Path, flags: OFlags) -> io::Result<File> {
    let name = basename(path)?;
    rustix::fs::openat2(
        root,
        name,
        flags | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        if flags.contains(OFlags::CREATE) {
            Mode::from_bits_truncate(0o600)
        } else {
            Mode::empty()
        },
        ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_XDEV,
    )
    .map(File::from)
    .map_err(Into::into)
}

fn validate_link(root: &File, path: &Path, file: &File) -> io::Result<u64> {
    let opened = rustix::fs::fstat(file)?;
    let named = rustix::fs::statat(root, basename(path)?, AtFlags::SYMLINK_NOFOLLOW)?;
    if rustix::fs::FileType::from_raw_mode(opened.st_mode) != rustix::fs::FileType::RegularFile
        || opened.st_nlink != 1
        || opened.st_dev != named.st_dev
        || opened.st_ino != named.st_ino
    {
        return Err(invalid("artifact is not one unchanged regular file"));
    }
    u64::try_from(opened.st_size).map_err(|_| invalid("negative artifact length"))
}

fn validate_root_name(root_path: &Path, root: &File) -> io::Result<()> {
    let opened = rustix::fs::fstat(root)?;
    let named = rustix::fs::fstat(open_root(root_path)?)?;
    if opened.st_nlink == 0 || opened.st_dev != named.st_dev || opened.st_ino != named.st_ino {
        return Err(invalid("artifact root changed during filesystem work"));
    }
    Ok(())
}

fn write_hashed(file: &mut File, hash: &mut Sha256, bytes: &[u8], hooks: &Hooks) -> io::Result<()> {
    for chunk in bytes.chunks(SCRATCH_LENGTH) {
        hooks.at(Barrier::Write)?;
        file.write_all(chunk)?;
        hash.update(chunk);
    }
    Ok(())
}

fn finish(
    root_path: &Path,
    root: File,
    mut file: File,
    plan: &ArtifactPlan,
    physical_length: u64,
    expected_hash: [u8; 32],
    hooks: &Hooks,
) -> io::Result<(Arc<File>, Arc<File>)> {
    hooks.at(Barrier::FileSync)?;
    file.sync_all()?;
    if validate_link(&root, plan.temporary_path(), &file)? != physical_length {
        return Err(invalid("staged artifact length changed"));
    }
    hooks.at(Barrier::Rename)?;
    rustix::fs::renameat_with(
        &root,
        basename(plan.temporary_path())?,
        &root,
        basename(plan.final_path())?,
        RenameFlags::NOREPLACE,
    )?;
    hooks.at(Barrier::DirectorySync)?;
    root.sync_all()?;
    hooks.at(Barrier::Validate)?;
    if validate_link(&root, plan.final_path(), &file)? != physical_length {
        return Err(invalid("published artifact length changed"));
    }
    file.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut scratch = [0; SCRATCH_LENGTH];
    loop {
        let count = file.read(&mut scratch)?;
        if count == 0 {
            break;
        }
        hash.update(&scratch[..count]);
    }
    if <[u8; 32]>::from(hash.finalize()) != expected_hash {
        return Err(invalid("durable artifact hash differs from staged bytes"));
    }
    validate_root_name(root_path, &root)?;
    file.lock_shared()?;
    Ok((Arc::new(root), Arc::new(file)))
}

fn create(
    root_path: &Path,
    admission: &ArtifactAdmission,
    hooks: &Hooks,
) -> io::Result<(File, File)> {
    let root = open_root(root_path)?;
    let identity = rustix::fs::fstat(&root)?;
    if admission.root_identity() != (identity.st_dev, identity.st_ino) {
        return Err(invalid("artifact admission belongs to a different root"));
    }
    let plan = admission.plan();
    basename(plan.final_path())?;
    hooks.at(Barrier::Create)?;
    let file = open_beneath(
        &root,
        plan.temporary_path(),
        OFlags::CREATE | OFlags::EXCL | OFlags::RDWR,
    )?;
    file.try_lock().map_err(io::Error::other)?;
    validate_link(&root, plan.temporary_path(), &file)?;
    Ok((root, file))
}

pub fn publish_segment(
    root: &Path,
    admission: ArtifactAdmission,
    records: &[&[u8]],
) -> Result<DurableArtifact, PublishError> {
    publish_segment_with(root, admission, records, &Hooks::default())
}

fn publish_segment_with(
    root_path: &Path,
    admission: ArtifactAdmission,
    records: &[&[u8]],
    hooks: &Hooks,
) -> Result<DurableArtifact, PublishError> {
    let result = (|| {
        let plan = admission.plan();
        let physical_length = segment_length(records.iter().map(|bytes| bytes.len() as u64))?;
        if plan.kind() != ArtifactKind::Segment || physical_length > plan.max_length() {
            return Err(invalid("segment exceeds its durable artifact admission"));
        }
        let (root, mut file) = create(root_path, &admission, hooks)?;
        let mut header = [0; SEGMENT_HEADER_LEN as usize];
        header[..8].copy_from_slice(MAGIC);
        header[8..40].copy_from_slice(plan.artifact().id.as_str().as_bytes());
        header[40..72].copy_from_slice(plan.artifact().generation.as_str().as_bytes());
        header[72..76].copy_from_slice(&(records.len() as u32).to_le_bytes());
        let mut hash = Sha256::new();
        write_hashed(&mut file, &mut hash, &header, hooks)?;
        let mut offset = SEGMENT_HEADER_LEN;
        let mut spans = Vec::with_capacity(records.len());
        for bytes in records {
            let length = bytes.len() as u64;
            let sha256: [u8; 32] = Sha256::digest(bytes).into();
            let mut prefix = [0; RECORD_HEADER_LEN as usize];
            prefix[..8].copy_from_slice(&length.to_le_bytes());
            prefix[8..].copy_from_slice(&sha256);
            write_hashed(&mut file, &mut hash, &prefix, hooks)?;
            write_hashed(&mut file, &mut hash, bytes, hooks)?;
            offset += RECORD_HEADER_LEN;
            spans.push(DurableSpan {
                offset,
                length,
                sha256,
            });
            offset += length;
        }
        let sha256 = hash.finalize().into();
        let (root, file) = finish(root_path, root, file, plan, physical_length, sha256, hooks)?;
        Ok((root, file, physical_length, sha256, spans))
    })();
    match result {
        Ok((root, file, physical_length, sha256, spans)) => Ok(DurableArtifact {
            admission: Box::new(admission),
            physical_length,
            sha256,
            spans,
            root,
            file,
        }),
        Err(error) => Err(PublishError {
            error,
            quiescent: abort(admission),
        }),
    }
}

/// Copy already encoded records verbatim, reserving the entire replacement allocation before
/// reading even the first source payload. The caller owns admission and the source pin through
/// this synchronous job and the subsequent Writer relocation result.
pub fn copy_segment(
    root: &Path,
    admission: ArtifactAdmission,
    source: &PinnedSegment,
    records: &[PublishedRecord],
) -> Result<DurableArtifact, PublishError> {
    copy_segment_with(root, admission, source, records, &Hooks::default())
}

fn copy_segment_with(
    root_path: &Path,
    admission: ArtifactAdmission,
    source: &PinnedSegment,
    records: &[PublishedRecord],
    hooks: &Hooks,
) -> Result<DurableArtifact, PublishError> {
    let result = (|| {
        let plan = admission.plan();
        let physical_length =
            segment_length(records.iter().map(|record| record.location.length()))?;
        if plan.kind() != ArtifactKind::Segment
            || physical_length > plan.max_length()
            || source.root_identity()? != admission.root_identity()
        {
            return Err(invalid(
                "replacement does not match its admitted root or byte bound",
            ));
        }
        let (root, mut file) = create(root_path, &admission, hooks)?;
        hooks.at(Barrier::Reserve)?;
        // A sparse set_len or an ignored allocation error does not reserve replacement space.
        rustix::fs::fallocate(
            &file,
            rustix::fs::FallocateFlags::KEEP_SIZE,
            0,
            physical_length,
        )?;
        let mut header = [0; SEGMENT_HEADER_LEN as usize];
        header[..8].copy_from_slice(MAGIC);
        header[8..40].copy_from_slice(plan.artifact().id.as_str().as_bytes());
        header[40..72].copy_from_slice(plan.artifact().generation.as_str().as_bytes());
        header[72..76].copy_from_slice(&(records.len() as u32).to_le_bytes());
        let mut hash = Sha256::new();
        write_hashed(&mut file, &mut hash, &header, hooks)?;
        let mut offset = SEGMENT_HEADER_LEN;
        let mut spans = Vec::with_capacity(records.len());
        let mut scratch = [0; SCRATCH_LENGTH];
        for record in records {
            let length = record.location.length();
            if length != record.metadata.encoded_length {
                return Err(invalid("source record length differs from metadata"));
            }
            let expected = record.metadata.encoded_sha256;
            let mut reader = source.record(&record.location, expected)?;
            let mut prefix = [0; RECORD_HEADER_LEN as usize];
            prefix[..8].copy_from_slice(&length.to_le_bytes());
            prefix[8..].copy_from_slice(&expected);
            write_hashed(&mut file, &mut hash, &prefix, hooks)?;
            let mut record_hash = Sha256::new();
            let mut remaining = length;
            while remaining != 0 {
                let count = remaining.min(SCRATCH_LENGTH as u64) as usize;
                hooks.at(Barrier::CopyRead)?;
                reader.read_exact(&mut scratch[..count])?;
                record_hash.update(&scratch[..count]);
                write_hashed(&mut file, &mut hash, &scratch[..count], hooks)?;
                remaining -= count as u64;
            }
            if <[u8; 32]>::from(record_hash.finalize()) != expected {
                return Err(invalid("collection source hash differs from metadata"));
            }
            offset += RECORD_HEADER_LEN;
            spans.push(DurableSpan {
                offset,
                length,
                sha256: expected,
            });
            offset += length;
        }
        let sha256 = hash.finalize().into();
        let (root, file) = finish(root_path, root, file, plan, physical_length, sha256, hooks)?;
        Ok((root, file, physical_length, sha256, spans))
    })();
    match result {
        Ok((root, file, physical_length, sha256, spans)) => Ok(DurableArtifact {
            admission: Box::new(admission),
            physical_length,
            sha256,
            spans,
            root,
            file,
        }),
        Err(error) => Err(PublishError {
            error,
            quiescent: abort(admission),
        }),
    }
}

pub fn publish_file(
    root: &Path,
    admission: ArtifactAdmission,
    source: &mut impl Read,
) -> Result<DurableArtifact, PublishError> {
    publish_file_with(root, admission, source, &Hooks::default())
}

fn publish_file_with(
    root_path: &Path,
    admission: ArtifactAdmission,
    source: &mut impl Read,
    hooks: &Hooks,
) -> Result<DurableArtifact, PublishError> {
    let result = (|| {
        let plan = admission.plan();
        if plan.kind() != ArtifactKind::File {
            return Err(invalid("dedicated file requires file admission"));
        }
        let (root, mut file) = create(root_path, &admission, hooks)?;
        let mut scratch = [0; SCRATCH_LENGTH];
        let mut physical_length = 0_u64;
        let mut hash = Sha256::new();
        loop {
            let count = source.read(&mut scratch)?;
            if count == 0 {
                break;
            }
            physical_length = physical_length
                .checked_add(count as u64)
                .filter(|length| *length <= plan.max_length())
                .ok_or_else(|| invalid("dedicated file exceeds durable admission"))?;
            write_hashed(&mut file, &mut hash, &scratch[..count], hooks)?;
        }
        let sha256 = hash.finalize().into();
        let (root, file) = finish(root_path, root, file, plan, physical_length, sha256, hooks)?;
        Ok((root, file, physical_length, sha256))
    })();
    match result {
        Ok((root, file, physical_length, sha256)) => Ok(DurableArtifact {
            admission: Box::new(admission),
            physical_length,
            sha256,
            spans: vec![DurableSpan {
                offset: 0,
                length: physical_length,
                sha256,
            }],
            root,
            file,
        }),
        Err(error) => Err(PublishError {
            error,
            quiescent: abort(admission),
        }),
    }
}

/// A shared descriptor pin is the ownership, without a heap entry per stored artifact. Its
/// position is private to this reader even when multiple readers share a file description.
pub struct PinnedRecord {
    file: Arc<File>,
    root: Arc<File>,
    _lifetime: Arc<dyn Send + Sync>,
    base: u64,
    length: u64,
    cursor: u64,
    declared_sha256: Option<[u8; 32]>,
}

impl std::fmt::Debug for PinnedRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PinnedRecord")
            .field("base", &self.base)
            .field("length", &self.length)
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

impl PinnedRecord {
    pub fn root_identity(&self) -> io::Result<(u64, u64)> {
        let identity = rustix::fs::fstat(&*self.root)?;
        Ok((identity.st_dev, identity.st_ino))
    }

    pub fn verify_sha256(&self, expected: [u8; 32]) -> io::Result<()> {
        if self
            .declared_sha256
            .is_some_and(|declared| declared != expected)
        {
            return Err(invalid(
                "segment record hash declaration differs from metadata",
            ));
        }
        let mut scratch = [0; SCRATCH_LENGTH];
        let mut position = 0;
        let mut hash = Sha256::new();
        while position < self.length {
            let length = (self.length - position).min(SCRATCH_LENGTH as u64) as usize;
            self.file
                .read_exact_at(&mut scratch[..length], self.base + position)?;
            hash.update(&scratch[..length]);
            position += length as u64;
        }
        if <[u8; 32]>::from(hash.finalize()) != expected {
            return Err(invalid("record hash differs from metadata"));
        }
        Ok(())
    }
}

impl Read for PinnedRecord {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let length = (self.length - self.cursor)
            .min(buffer.len() as u64)
            .min(SCRATCH_LENGTH as u64) as usize;
        if length == 0 {
            return Ok(0);
        }
        let count = self
            .file
            .read_at(&mut buffer[..length], self.base + self.cursor)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "record was truncated",
            ));
        }
        self.cursor += count as u64;
        Ok(count)
    }
}

impl Seek for PinnedRecord {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let position = match position {
            SeekFrom::Start(value) => i128::from(value),
            SeekFrom::End(delta) => i128::from(self.length) + i128::from(delta),
            SeekFrom::Current(delta) => i128::from(self.cursor) + i128::from(delta),
        };
        if position < 0 || position > i128::from(self.length) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek exceeds record bounds",
            ));
        }
        self.cursor = position as u64;
        Ok(self.cursor)
    }
}

fn validate_segment(
    file: &File,
    identity: &ArtifactIdentity,
    offset: u64,
    length: u64,
    physical_length: u64,
    expected_sha256: [u8; 32],
) -> io::Result<[u8; 32]> {
    validate_segment_header(file, identity, physical_length)?;
    validate_record_prefix(file, offset, length, physical_length, expected_sha256)
}

fn validate_segment_header(
    file: &File,
    identity: &ArtifactIdentity,
    physical_length: u64,
) -> io::Result<()> {
    if !(SEGMENT_HEADER_LEN..=MAX_SEGMENT_LENGTH).contains(&physical_length) {
        return Err(invalid("segment physical length exceeds framing bounds"));
    }
    let mut header = [0; SEGMENT_HEADER_LEN as usize];
    file.read_exact_at(&mut header, 0)?;
    if &header[..8] != MAGIC
        || &header[8..40] != identity.id.as_str().as_bytes()
        || &header[40..72] != identity.generation.as_str().as_bytes()
        || header[76..] != [0; 4]
    {
        return Err(invalid("segment identity, generation or format differs"));
    }
    let records = u32::from_le_bytes(header[72..76].try_into().unwrap()) as usize;
    if !(1..=MAX_SEGMENT_RECORDS).contains(&records) {
        return Err(invalid("segment record count exceeds bounds"));
    }
    Ok(())
}

fn validate_record_prefix(
    file: &File,
    offset: u64,
    length: u64,
    physical_length: u64,
    expected_sha256: [u8; 32],
) -> io::Result<[u8; 32]> {
    // SQLite's exact span was matched to the private publication receipt. Read only the
    // selected prefix here; a directory/header inventory must never select a record for us.
    if length > MAX_PACKED_RECORD_LENGTH
        || offset < SEGMENT_HEADER_LEN + RECORD_HEADER_LEN
        || offset
            .checked_add(length)
            .is_none_or(|end| end > physical_length)
    {
        return Err(invalid("record location exceeds segment bounds"));
    }
    let mut prefix = [0; RECORD_HEADER_LEN as usize];
    file.read_exact_at(&mut prefix, offset - RECORD_HEADER_LEN)?;
    let declared: [u8; 32] = prefix[8..].try_into().unwrap();
    if u64::from_le_bytes(prefix[..8].try_into().unwrap()) != length || declared != expected_sha256
    {
        return Err(invalid("record prefix differs from its trusted metadata"));
    }
    Ok(declared)
}

/// Whole immutable bytes for an offline snapshot or bounded segment collection. The file is
/// pinned before its name and trusted physical length are validated, and is never reopened.
pub struct PinnedArtifact {
    identity: ArtifactIdentity,
    kind: ArtifactKind,
    reader: PinnedRecord,
}

impl PinnedArtifact {
    pub fn open(
        root_path: &Path,
        identity: &ArtifactIdentity,
        kind: ArtifactKind,
        physical_length: u64,
        lifetime: Arc<dyn Send + Sync>,
    ) -> io::Result<Self> {
        let root = open_root(root_path)?;
        let name = identity.file_name(kind);
        let file = open_beneath(&root, Path::new(&name), OFlags::RDONLY)?;
        file.lock_shared()?;
        if validate_link(&root, Path::new(&name), &file)? != physical_length {
            return Err(invalid("artifact length differs from trusted metadata"));
        }
        if kind == ArtifactKind::Segment {
            validate_segment_header(&file, identity, physical_length)?;
        }
        validate_root_name(root_path, &root)?;
        Ok(Self {
            identity: identity.clone(),
            kind,
            reader: PinnedRecord {
                file: Arc::new(file),
                root: Arc::new(root),
                _lifetime: lifetime,
                base: 0,
                length: physical_length,
                cursor: 0,
                declared_sha256: None,
            },
        })
    }

    pub fn root_identity(&self) -> io::Result<(u64, u64)> {
        self.reader.root_identity()
    }

    pub fn verify_sha256(&self, expected: [u8; 32]) -> io::Result<()> {
        self.reader.verify_sha256(expected)
    }
}

impl Read for PinnedArtifact {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buffer)
    }
}

impl Seek for PinnedArtifact {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.reader.seek(position)
    }
}

pub struct PinnedSegment {
    artifact: PinnedArtifact,
}

impl PinnedSegment {
    pub fn open(
        root: &Path,
        identity: &ArtifactIdentity,
        physical_length: u64,
        lifetime: Arc<dyn Send + Sync>,
    ) -> io::Result<Self> {
        Ok(Self {
            artifact: PinnedArtifact::open(
                root,
                identity,
                ArtifactKind::Segment,
                physical_length,
                lifetime,
            )?,
        })
    }

    pub fn root_identity(&self) -> io::Result<(u64, u64)> {
        self.artifact.root_identity()
    }

    pub fn record(
        &self,
        location: &Location,
        expected_sha256: [u8; 32],
    ) -> io::Result<PinnedRecord> {
        if location.artifact() != &self.artifact.identity || location.kind() != self.artifact.kind {
            return Err(invalid(
                "record does not belong to the pinned segment generation",
            ));
        }
        let pinned = &self.artifact.reader;
        let declared_sha256 = validate_record_prefix(
            &pinned.file,
            location.offset(),
            location.length(),
            pinned.length,
            expected_sha256,
        )?;
        Ok(PinnedRecord {
            file: pinned.file.clone(),
            root: pinned.root.clone(),
            _lifetime: pinned._lifetime.clone(),
            base: location.offset(),
            length: location.length(),
            cursor: 0,
            declared_sha256: Some(declared_sha256),
        })
    }
}

/// Only the SQLite actor admits stored locations, after matching publication against a private
/// durable receipt. This is not an artifact scanner or an API accepting untrusted offsets.
pub(super) fn open_pinned(
    root_path: &Path,
    location: &Location,
    expected_sha256: [u8; 32],
    lifetime: Arc<dyn Send + Sync>,
) -> io::Result<PinnedRecord> {
    let (identity, kind, base, length) = match location {
        Location::File { artifact, length } => (artifact, ArtifactKind::File, 0, *length),
        Location::Segment {
            artifact,
            offset,
            length,
        } => (artifact, ArtifactKind::Segment, *offset, *length),
    };
    base.checked_add(length)
        .ok_or_else(|| invalid("record location overflow"))?;
    let root = open_root(root_path)?;
    let name = identity.file_name(kind);
    let file = open_beneath(&root, Path::new(&name), OFlags::RDONLY)?;
    file.lock_shared()?;
    let physical_length = validate_link(&root, Path::new(&name), &file)?;
    let declared_sha256 = match kind {
        ArtifactKind::File if physical_length != length => {
            return Err(invalid("dedicated file length differs from metadata"));
        }
        ArtifactKind::Segment => Some(validate_segment(
            &file,
            identity,
            base,
            length,
            physical_length,
            expected_sha256,
        )?),
        ArtifactKind::File => None,
    };
    validate_root_name(root_path, &root)?;
    Ok(PinnedRecord {
        file: Arc::new(file),
        root: Arc::new(root),
        _lifetime: lifetime,
        base,
        length,
        cursor: 0,
        declared_sha256,
    })
}

pub enum CleanupResult {
    Removed(CleanupReceipt),
    Pinned(CleanupClaim),
}

pub struct CleanupReceipt {
    claim: CleanupClaim,
}

impl CleanupReceipt {
    pub fn into_claim(self) -> CleanupClaim {
        self.claim
    }
}

pub struct CleanupError {
    error: io::Error,
    claim: Box<CleanupClaim>,
}

impl CleanupError {
    pub fn into_parts(self) -> (io::Error, CleanupClaim) {
        (self.error, *self.claim)
    }
}

impl std::fmt::Debug for CleanupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::fmt::Display for CleanupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for CleanupError {}

pub fn cleanup(root_path: &Path, claim: CleanupClaim) -> Result<CleanupResult, CleanupError> {
    cleanup_with(root_path, claim, &Hooks::default())
}

fn cleanup_with(
    root_path: &Path,
    claim: CleanupClaim,
    hooks: &Hooks,
) -> Result<CleanupResult, CleanupError> {
    let result = (|| {
        let root = open_root(root_path)?;
        let identity = rustix::fs::fstat(&root)?;
        if claim.root_identity() != (identity.st_dev, identity.st_ino) {
            return Err(invalid("cleanup claim belongs to a different root"));
        }
        let file = match open_beneath(&root, claim.path(), OFlags::RDONLY) {
            Ok(file) => Some(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        if let Some(file) = file {
            match file.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => return Ok(false),
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
            validate_link(&root, claim.path(), &file)?;
            hooks.at(Barrier::Unlink)?;
            rustix::fs::unlinkat(&root, basename(claim.path())?, AtFlags::empty())?;
        }
        hooks.at(Barrier::CleanupSync)?;
        root.sync_all()?;
        validate_root_name(root_path, &root)?;
        Ok(true)
    })();
    match result {
        Ok(true) => Ok(CleanupResult::Removed(CleanupReceipt { claim })),
        Ok(false) => Ok(CleanupResult::Pinned(claim)),
        Err(error) => Err(CleanupError {
            error,
            claim: Box::new(claim),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::super::model::{
        CipherFormat, EncodedFormat, ExpectedCurrent, PublishRecord, RecordMetadata,
    };
    use super::super::store::Store;
    use super::*;
    use cairn_types::testing::{FixtureBlobStore, fixture_storage_io};
    use cairn_types::{
        BlobCipher, BucketName, CompressionAlgorithm, CompressionPolicy, StageOptions,
    };
    use std::io::Cursor;

    fn store(root: &Path) -> Store {
        Store::open(
            super::super::node::Node::open(root).unwrap(),
            Default::default(),
        )
        .unwrap()
    }

    async fn segment(root: &Path, store: &Store, records: &[&[u8]]) -> DurableArtifact {
        let length = segment_length(records.iter().map(|bytes| bytes.len() as u64)).unwrap();
        let admission = store.plan(ArtifactKind::Segment, length).await.unwrap();
        publish_segment(root, admission, records).unwrap()
    }

    fn location(artifact: &DurableArtifact, index: usize) -> Location {
        let span = &artifact.spans()[index];
        match artifact.plan().kind() {
            ArtifactKind::File => Location::File {
                artifact: artifact.plan().artifact().clone(),
                length: span.length,
            },
            ArtifactKind::Segment => Location::Segment {
                artifact: artifact.plan().artifact().clone(),
                offset: span.offset,
                length: span.length,
            },
        }
    }

    async fn drain(root: &Path, store: &Store) -> usize {
        let mut pinned = 0;
        for claim in store.claim_cleanup(32).await.unwrap() {
            match cleanup(root, claim).unwrap() {
                CleanupResult::Removed(receipt) => {
                    assert!(store.finish_cleanup(receipt).await.unwrap());
                }
                CleanupResult::Pinned(claim) => {
                    pinned += 1;
                    assert!(store.release_cleanup(claim).await.unwrap());
                }
            }
        }
        pinned
    }

    fn source_records(artifact: &DurableArtifact) -> Vec<PublishedRecord> {
        artifact
            .spans()
            .iter()
            .enumerate()
            .map(|(index, span)| PublishedRecord {
                metadata: RecordMetadata {
                    row_id: cairn_types::storage::StorageToken::generate(),
                    key: format!("copy-{index}"),
                    encoded_sha256: span.sha256,
                    encoded_length: span.length,
                    logical_size: span.length,
                    format: EncodedFormat::Raw,
                    compression: cairn_types::CompressionDescriptor::Uncompressed,
                    cipher: CipherFormat::Plaintext,
                    locked: false,
                },
                location: location(artifact, index),
                is_current: true,
            })
            .collect()
    }

    #[tokio::test]
    async fn collection_reserves_before_reading_and_streams_unchanged_records() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let data = vec![31; 200_017];
        let artifact = segment(root.path(), &store, &[&data, b"tail-CRNB"]).await;
        let records = source_records(&artifact);
        let source = PinnedSegment::open(
            root.path(),
            artifact.plan().artifact(),
            artifact.physical_length(),
            artifact.admission.lifetime(),
        )
        .unwrap();
        let length = artifact.physical_length();
        let admission = store.plan(ArtifactKind::Segment, length).await.unwrap();
        let hooks = Hooks::default();
        let replacement =
            copy_segment_with(root.path(), admission, &source, &records, &hooks).unwrap();
        assert_eq!(replacement.spans(), artifact.spans());
        assert_eq!(replacement.physical_length(), artifact.physical_length());
        {
            let trace = hooks.trace.lock().unwrap();
            assert!(
                trace.iter().position(|stage| *stage == Barrier::Reserve)
                    < trace.iter().position(|stage| *stage == Barrier::CopyRead)
            );
            assert!(
                trace
                    .iter()
                    .filter(|stage| **stage == Barrier::CopyRead)
                    .count()
                    >= 5
            );
        }
        let mut pinned = PinnedArtifact::open(
            root.path(),
            replacement.plan().artifact(),
            ArtifactKind::Segment,
            length,
            replacement.admission.lifetime(),
        )
        .unwrap();
        pinned.verify_sha256(replacement.sha256()).unwrap();
        pinned
            .seek(SeekFrom::Start(replacement.spans()[0].offset))
            .unwrap();
        let mut copied = vec![0; data.len()];
        pinned.read_exact(&mut copied).unwrap();
        assert_eq!(copied, data);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn collection_allocation_copy_and_barrier_failures_keep_source_bytes() {
        // ENOSPC here is injected at the real allocation/write boundary. It is a command-error
        // model and does not claim a filled-device, process-kill or power-loss experiment.
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let data = vec![29; 200_017];
        let artifact = segment(root.path(), &store, &[&data]).await;
        let records = source_records(&artifact);
        let source_path = root.path().join(artifact.plan().final_path());
        let original = std::fs::read(&source_path).unwrap();
        let source = PinnedSegment::open(
            root.path(),
            artifact.plan().artifact(),
            artifact.physical_length(),
            artifact.admission.lifetime(),
        )
        .unwrap();
        for stage in [
            Barrier::Reserve,
            Barrier::CopyRead,
            Barrier::FileSync,
            Barrier::Rename,
            Barrier::DirectorySync,
            Barrier::Validate,
        ] {
            let admission = store
                .plan(ArtifactKind::Segment, artifact.physical_length())
                .await
                .unwrap();
            let hooks = Hooks {
                fail: Some((stage, if stage == Barrier::Reserve { 28 } else { 5 })),
                ..Default::default()
            };
            let failure =
                copy_segment_with(root.path(), admission, &source, &records, &hooks).unwrap_err();
            if stage == Barrier::Reserve {
                assert!(!hooks.trace.lock().unwrap().contains(&Barrier::CopyRead));
                assert_eq!(failure.error.raw_os_error(), Some(28));
            }
            assert!(store.abort(failure.into_parts().1).await.unwrap());
            assert_eq!(drain(root.path(), &store).await, 0);
            assert_eq!(std::fs::read(&source_path).unwrap(), original);
        }
        let admission = store
            .plan(ArtifactKind::Segment, artifact.physical_length())
            .await
            .unwrap();
        let hooks = Hooks {
            fail_write_after: Some(3),
            ..Default::default()
        };
        let failure =
            copy_segment_with(root.path(), admission, &source, &records, &hooks).unwrap_err();
        assert_eq!(failure.error.raw_os_error(), Some(28));
        assert!(hooks.trace.lock().unwrap().contains(&Barrier::CopyRead));
        assert!(store.abort(failure.into_parts().1).await.unwrap());
        assert_eq!(drain(root.path(), &store).await, 0);
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .open(&source_path)
            .unwrap();
        writer
            .write_all_at(b"corrupt", artifact.spans()[0].offset)
            .unwrap();
        let admission = store
            .plan(ArtifactKind::Segment, artifact.physical_length())
            .await
            .unwrap();
        let failure = copy_segment(root.path(), admission, &source, &records).unwrap_err();
        assert!(failure.error.to_string().contains("source hash"));
        assert!(store.abort(failure.into_parts().1).await.unwrap());
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_unlink_and_directory_sync_errors_retain_exact_retryable_debt() {
        for failed in [Barrier::Unlink, Barrier::CleanupSync] {
            let root = tempfile::tempdir().unwrap();
            let store = store(root.path());
            let artifact = segment(root.path(), &store, &[b"obsolete"]).await;
            let path = artifact.plan().final_path().to_owned();
            assert!(store.abort(artifact.into_quiescent()).await.unwrap());
            let mut final_claim = None;
            for claim in store.claim_cleanup(32).await.unwrap() {
                if claim.path() == path {
                    final_claim = Some(claim);
                } else if let CleanupResult::Removed(receipt) = cleanup(root.path(), claim).unwrap()
                {
                    assert!(store.finish_cleanup(receipt).await.unwrap());
                }
            }
            let failure = cleanup_with(
                root.path(),
                final_claim.unwrap(),
                &Hooks {
                    fail: Some((failed, 5)),
                    ..Default::default()
                },
            )
            .err()
            .unwrap();
            assert!(store.release_cleanup(failure.into_parts().1).await.unwrap());
            assert_eq!(root.path().join(&path).exists(), failed == Barrier::Unlink);
            assert_eq!(store.stats().await.unwrap().cleanup, 1);
            assert_eq!(drain(root.path(), &store).await, 0);
            assert_eq!(store.stats().await.unwrap().cleanup, 0);
            assert!(!root.path().join(path).exists());
            store.close().await.unwrap();
        }
    }

    #[test]
    fn segment_geometry_counts_all_framing_without_overflow() {
        let first = MAX_PACKED_RECORD_LENGTH;
        let final_length =
            MAX_SEGMENT_LENGTH - SEGMENT_HEADER_LEN - 4 * RECORD_HEADER_LEN - 3 * first;
        assert_eq!(
            segment_length([first, first, first, final_length]).unwrap(),
            MAX_SEGMENT_LENGTH
        );
        assert!(segment_length([first, first, first, final_length + 1]).is_err());
        assert!(segment_length([MAX_PACKED_RECORD_LENGTH + 1]).is_err());
        assert!(segment_length([u64::MAX]).is_err());
        assert!(segment_length([]).is_err());
        assert_eq!(
            segment_length([0; MAX_SEGMENT_RECORDS]).unwrap(),
            SEGMENT_HEADER_LEN + RECORD_HEADER_LEN * MAX_SEGMENT_RECORDS as u64
        );
        assert!(segment_length([0; MAX_SEGMENT_RECORDS + 1]).is_err());
    }

    #[tokio::test]
    async fn raw_records_are_unchanged_and_cannot_read_their_neighbor() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let records: &[&[u8]] = &[b"first-CRNB", b"", b"second-record"];
        let artifact = segment(root.path(), &store, records).await;
        assert_eq!(artifact.physical_length(), 80 + 3 * 40 + 10 + 13);
        let bytes = std::fs::read(root.path().join(artifact.plan().final_path())).unwrap();
        assert_eq!(<[u8; 32]>::from(Sha256::digest(&bytes)), artifact.sha256());
        for (index, expected) in records.iter().enumerate() {
            let mut pin = open_pinned(
                root.path(),
                &location(&artifact, index),
                artifact.spans()[index].sha256,
                Arc::new(()),
            )
            .unwrap();
            pin.verify_sha256(artifact.spans()[index].sha256).unwrap();
            assert!(pin.seek(SeekFrom::End(1)).is_err());
            assert!(pin.seek(SeekFrom::Current(-1)).is_err());
            let mut result = [0; 64];
            let count = pin.read(&mut result).unwrap();
            assert_eq!(&result[..count], *expected);
            assert_eq!(pin.read(&mut result).unwrap(), 0);
            assert_eq!(pin.seek(SeekFrom::End(0)).unwrap(), expected.len() as u64);
        }
        let mut first = open_pinned(
            root.path(),
            &location(&artifact, 0),
            artifact.spans()[0].sha256,
            Arc::new(()),
        )
        .unwrap();
        let mut second = PinnedRecord {
            file: first.file.clone(),
            root: first.root.clone(),
            _lifetime: first._lifetime.clone(),
            base: first.base,
            length: first.length,
            cursor: 0,
            declared_sha256: first.declared_sha256,
        };
        first.seek(SeekFrom::Start(6)).unwrap();
        let mut independent = [0; 5];
        second.read_exact(&mut independent).unwrap();
        assert_eq!(&independent, b"first");
        assert_eq!(first.stream_position().unwrap(), 6);
        let mut wrong = location(&artifact, 0);
        if let Location::Segment { offset, .. } = &mut wrong {
            *offset += 1;
        }
        assert!(
            open_pinned(
                root.path(),
                &wrong,
                artifact.spans()[0].sha256,
                Arc::new(())
            )
            .is_err()
        );
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn dedicated_streaming_files_have_no_added_framing_and_obey_admission() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let data = vec![41; MAX_PACKED_RECORD_LENGTH as usize + 8193];
        let admission = store
            .plan(ArtifactKind::File, data.len() as u64)
            .await
            .unwrap();
        let artifact = publish_file(root.path(), admission, &mut Cursor::new(&data)).unwrap();
        assert_eq!(artifact.spans()[0].offset, 0);
        assert_eq!(
            std::fs::read(root.path().join(artifact.plan().final_path())).unwrap(),
            data
        );
        let mut pin = open_pinned(
            root.path(),
            &location(&artifact, 0),
            artifact.spans()[0].sha256,
            Arc::new(()),
        )
        .unwrap();
        pin.seek(SeekFrom::End(-17)).unwrap();
        let mut tail = [0; 32];
        assert_eq!(pin.read(&mut tail).unwrap(), 17);
        assert_eq!(tail[..17], [41; 17]);
        let admission = store.plan(ArtifactKind::File, 4).await.unwrap();
        let plan = admission.plan().clone();
        let error = publish_file(root.path(), admission, &mut Cursor::new(b"exceeds")).unwrap_err();
        let (_, quiescent) = error.into_parts();
        assert!(!root.path().join(plan.final_path()).exists());
        assert!(store.abort(quiescent).await.unwrap());
        assert_eq!(drain(root.path(), &store).await, 0);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn real_crnb_plaintext_and_encrypted_records_preserve_interpretation() {
        let key = super::super::model::encryption_test_key();
        let source = tempfile::tempdir().unwrap();
        let blobs = cairn_blob::LocalBlobStore::open(source.path(), fixture_storage_io())
            .await
            .unwrap();
        let bucket = BucketName::parse("packing-fixture").unwrap();
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let plaintext = vec![b'a'; 160_123];
        for encrypted in [false, true] {
            let body = bytes::Bytes::from(plaintext.clone());
            let staged = blobs
                .stage_fixture(
                    &bucket,
                    Box::pin(futures_util::stream::once(async move { Ok(body) })),
                    StageOptions {
                        compression: Some(CompressionPolicy {
                            algorithm: CompressionAlgorithm::Zstd,
                            block_size: 64 * 1024,
                        }),
                        encryption: encrypted.then(|| key.clone()),
                        size_ceiling: plaintext.len() as u64,
                        content_type: "text/plain".into(),
                        content_length: Some(plaintext.len() as u64),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let encoded = std::fs::read(source.path().join(staged.storage_path.as_str())).unwrap();
            assert_eq!(&encoded[encoded.len() - 34..encoded.len() - 30], b"CRNB");
            let artifact = segment(root.path(), &store, &[b"neighbor", &encoded, b"after"]).await;
            let exact = location(&artifact, 1);
            let expected_hash = artifact.spans()[1].sha256;
            let metadata_cipher = if encrypted {
                CipherFormat::AuthenticatedV3
            } else {
                CipherFormat::Plaintext
            };
            let publications: Vec<_> = artifact
                .spans()
                .iter()
                .enumerate()
                .map(|(index, span)| PublishRecord {
                    metadata: RecordMetadata {
                        row_id: cairn_types::storage::StorageToken::generate(),
                        key: format!("fixture-{encrypted}-{index}"),
                        encoded_sha256: span.sha256,
                        encoded_length: span.length,
                        logical_size: if index == 1 {
                            staged.size_logical
                        } else {
                            span.length
                        },
                        format: if index == 1 {
                            EncodedFormat::Crnb
                        } else {
                            EncodedFormat::Raw
                        },
                        compression: if index == 1 {
                            staged.compression.clone()
                        } else {
                            cairn_types::CompressionDescriptor::Uncompressed
                        },
                        cipher: if index == 1 {
                            metadata_cipher
                        } else {
                            CipherFormat::Plaintext
                        },
                        locked: false,
                    },
                    location: location(&artifact, index),
                    expected: ExpectedCurrent::Absent,
                    preserve_previous: false,
                })
                .collect();
            assert!(matches!(
                store.publish(artifact, publications).await.unwrap(),
                super::super::store::PublicationOutcome::Applied { records: 3 }
            ));
            let pinned = store
                .pin(&format!("fixture-{encrypted}-1"))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pinned.record.metadata.compression, staged.compression);
            assert_eq!(pinned.record.metadata.cipher, metadata_cipher);
            let pin = pinned.pin;
            pin.verify_sha256(expected_hash).unwrap();
            let cipher = if encrypted {
                BlobCipher::AuthenticatedV3(key.clone())
            } else {
                BlobCipher::KnownPlaintext
            };
            let mut reader = cairn_blob::compress::CompressedReader::open_with_dek(
                pin,
                cipher,
                &staged.compression,
                staged.size_logical,
            )
            .unwrap();
            assert_eq!(
                reader.read_range(0, staged.size_logical).unwrap(),
                plaintext
            );
            assert_eq!(
                reader.read_range(65_530, 31).unwrap(),
                plaintext[65_530..65_561]
            );
            if encrypted {
                for declaration in [
                    BlobCipher::AuthenticatedV3(super::super::model::encryption_test_key()),
                    BlobCipher::LegacyV2(key.clone()),
                    BlobCipher::KnownPlaintext,
                ] {
                    let pin =
                        open_pinned(root.path(), &exact, expected_hash, Arc::new(())).unwrap();
                    assert!(
                        cairn_blob::compress::CompressedReader::open_with_dek(
                            pin,
                            declaration,
                            &staged.compression,
                            staged.size_logical,
                        )
                        .is_err()
                    );
                }
            }
            let mut truncated = encoded.clone();
            truncated.truncate(truncated.len() - 1);
            let damaged = segment(root.path(), &store, &[&truncated, b"unread-neighbor"]).await;
            let pin = open_pinned(
                root.path(),
                &location(&damaged, 0),
                damaged.spans()[0].sha256,
                Arc::new(()),
            )
            .unwrap();
            let cipher = if encrypted {
                BlobCipher::AuthenticatedV3(key.clone())
            } else {
                BlobCipher::KnownPlaintext
            };
            assert!(
                cairn_blob::compress::CompressedReader::open_with_dek(
                    pin,
                    cipher,
                    &staged.compression,
                    staged.size_logical,
                )
                .is_err()
            );
        }
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn every_publication_barrier_and_injected_enospc_preserves_pending_ownership() {
        // These are deterministic command-error seams, not process-kill or power-loss tests.
        for barrier in [
            Barrier::Create,
            Barrier::Write,
            Barrier::FileSync,
            Barrier::Rename,
            Barrier::DirectorySync,
            Barrier::Validate,
        ] {
            let root = tempfile::tempdir().unwrap();
            let store = store(root.path());
            let admission = store.plan(ArtifactKind::Segment, 124).await.unwrap();
            let plan = admission.plan().clone();
            let hooks = Hooks {
                fail: Some((barrier, if barrier == Barrier::Write { 28 } else { 5 })),
                ..Default::default()
            };
            let error =
                publish_segment_with(root.path(), admission, &[b"data"], &hooks).unwrap_err();
            let (error, quiescent) = error.into_parts();
            if barrier == Barrier::Write {
                assert_eq!(error.raw_os_error(), Some(28));
            }
            assert_eq!(quiescent.plan(), &plan);
            assert_eq!(hooks.trace.lock().unwrap().last(), Some(&barrier));
            assert!(store.abort(quiescent).await.unwrap());
            assert_eq!(drain(root.path(), &store).await, 0);
            assert!(!root.path().join(plan.temporary_path()).exists());
            assert!(!root.path().join(plan.final_path()).exists());
            store.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn durable_receipt_and_reader_retain_root_lifetime_after_store_close() {
        let root = tempfile::tempdir().unwrap();
        let lifetime = super::super::node::Node::open(root.path()).unwrap();
        let observed = Arc::downgrade(&lifetime);
        let store = Store::open(lifetime.clone(), Default::default()).unwrap();
        drop(lifetime);
        let admission = store.plan(ArtifactKind::Segment, 124).await.unwrap();
        let hooks = Hooks::default();
        let artifact = publish_segment_with(root.path(), admission, &[b"data"], &hooks).unwrap();
        assert_eq!(
            hooks
                .trace
                .lock()
                .unwrap()
                .iter()
                .copied()
                .filter(|barrier| *barrier != Barrier::Write)
                .collect::<Vec<_>>(),
            [
                Barrier::Create,
                Barrier::FileSync,
                Barrier::Rename,
                Barrier::DirectorySync,
                Barrier::Validate
            ]
        );
        store.close().await.unwrap();
        drop(store);
        assert!(observed.upgrade().is_some());
        let pin = open_pinned(
            root.path(),
            &location(&artifact, 0),
            artifact.spans()[0].sha256,
            artifact.admission.lifetime(),
        )
        .unwrap();
        drop(artifact.into_quiescent());
        assert!(observed.upgrade().is_some());
        drop(pin);
        assert!(observed.upgrade().is_none());
    }

    #[tokio::test]
    async fn publisher_cannot_replace_existing_final_or_use_another_root() {
        let root = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let admission = store.plan(ArtifactKind::File, 4).await.unwrap();
        let plan = admission.plan().clone();
        std::fs::write(root.path().join(plan.final_path()), b"keep").unwrap();
        let error = publish_file(root.path(), admission, &mut Cursor::new(b"data")).unwrap_err();
        assert_eq!(
            std::fs::read(root.path().join(plan.final_path())).unwrap(),
            b"keep"
        );
        drop(error);
        let admission = store.plan(ArtifactKind::File, 4).await.unwrap();
        let plan = admission.plan().clone();
        let error = publish_file(other.path(), admission, &mut Cursor::new(b"data")).unwrap_err();
        assert!(!other.path().join(plan.temporary_path()).exists());
        assert!(!other.path().join(plan.final_path()).exists());
        assert!(store.abort(error.into_parts().1).await.unwrap());
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_cancelled_reader_job_keeps_the_shared_pin_until_actual_work_finishes() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let artifact = segment(root.path(), &store, &[b"read-data"]).await;
        let final_path = root.path().join(artifact.plan().final_path());
        let mut pin = open_pinned(
            root.path(),
            &location(&artifact, 0),
            artifact.spans()[0].sha256,
            Arc::new(()),
        )
        .unwrap();
        let (entered, wait_entered) = tokio::sync::oneshot::channel();
        let (release, wait_release) = std::sync::mpsc::channel();
        let (finished, wait_finished) = tokio::sync::oneshot::channel();
        let reader = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let mut data = [0; 4];
                pin.read_exact(&mut data).unwrap();
                entered.send(()).unwrap();
                wait_release.recv().unwrap();
                pin.read_exact(&mut data).unwrap();
                drop(pin);
                finished.send(()).unwrap();
            })
            .await
            .unwrap();
        });
        wait_entered.await.unwrap();
        reader.abort();
        assert!(reader.await.unwrap_err().is_cancelled());
        assert!(store.abort(artifact.into_quiescent()).await.unwrap());
        assert_eq!(drain(root.path(), &store).await, 1);
        assert!(final_path.exists());
        release.send(()).unwrap();
        wait_finished.await.unwrap();
        assert_eq!(drain(root.path(), &store).await, 0);
        assert!(!final_path.exists());
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn framed_identity_hash_and_physical_truncation_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        for position in [
            8_u64,
            SEGMENT_HEADER_LEN + 8,
            SEGMENT_HEADER_LEN + RECORD_HEADER_LEN,
        ] {
            let artifact = segment(root.path(), &store, &[b"payload"]).await;
            let location = location(&artifact, 0);
            let expected = artifact.spans()[0].sha256;
            let path = root.path().join(artifact.plan().final_path());
            let writer = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            writer.write_all_at(b"!", position).unwrap();
            match open_pinned(root.path(), &location, expected, Arc::new(())) {
                Err(_) => assert!(position == 8 || position == SEGMENT_HEADER_LEN + 8),
                Ok(pin) => assert!(pin.verify_sha256(expected).is_err()),
            }
        }
        let artifact = segment(root.path(), &store, &[b"payload"]).await;
        let exact = location(&artifact, 0);
        let mut pin = open_pinned(
            root.path(),
            &exact,
            artifact.spans()[0].sha256,
            Arc::new(()),
        )
        .unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(root.path().join(artifact.plan().final_path()))
            .unwrap()
            .set_len(SEGMENT_HEADER_LEN + RECORD_HEADER_LEN + 1)
            .unwrap();
        assert!(
            open_pinned(
                root.path(),
                &exact,
                artifact.spans()[0].sha256,
                Arc::new(())
            )
            .is_err()
        );
        assert!(pin.read_exact(&mut [0; 7]).is_err());
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn symlinks_hardlinks_and_nonregular_artifacts_are_refused_without_touching_targets() {
        for damage in ["symlink", "hardlink", "fifo"] {
            let root = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            let store = store(root.path());
            let artifact = segment(root.path(), &store, &[b"payload"]).await;
            let exact = location(&artifact, 0);
            let path = root.path().join(artifact.plan().final_path());
            let target = outside.path().join("protected");
            std::fs::write(&target, b"protected").unwrap();
            match damage {
                "symlink" => {
                    std::fs::remove_file(&path).unwrap();
                    std::os::unix::fs::symlink(&target, &path).unwrap();
                }
                "hardlink" => std::fs::hard_link(&path, outside.path().join("alias")).unwrap(),
                "fifo" => {
                    std::fs::remove_file(&path).unwrap();
                    rustix::fs::mknodat(
                        rustix::fs::CWD,
                        &path,
                        rustix::fs::FileType::Fifo,
                        Mode::from_bits_truncate(0o600),
                        0,
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            assert!(
                open_pinned(
                    root.path(),
                    &exact,
                    artifact.spans()[0].sha256,
                    Arc::new(())
                )
                .is_err(),
                "{damage}"
            );
            assert_eq!(std::fs::read(&target).unwrap(), b"protected");
            store.close().await.unwrap();
        }
    }
}
