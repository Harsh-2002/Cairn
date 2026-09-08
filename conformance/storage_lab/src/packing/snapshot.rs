//! Offline, manifest-last laboratory snapshots. No production backup or S3 parity claim.
use super::model::{ArtifactIdentity, ArtifactKind, Result};
use super::node::{Node, OfflineRoot};
use super::record::{self, PinnedArtifact};
use super::store::{ArtifactSnapshot, ImageView, OfflineView};
use cairn_types::storage::StorageToken;
use rustix::fs::{AtFlags, FallocateFlags, Mode, OFlags, RenameFlags, ResolveFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const DATABASE: &str = "metadata.sqlite3";
const CATALOG: &str = "artifacts.jsonl";
const MANIFEST: &str = "manifest.json";
const PAGE: usize = 256;
const SCRATCH: usize = 64 * 1024;
const MANIFEST_LIMIT: u64 = 16 * 1024;
const CATALOG_LINE_LIMIT: usize = 4096;
const FIXED_HEADROOM: u64 = 1024 * 1024;
// Reserve allocation rounding and directory growth for every possible copied artifact,
// the database, catalog and manifest. This is conservative accounting, not a measurement
// of filesystem-specific metadata: larger reported allocation units are added at admission.
const ENTRY_RESERVATION: u64 = 16 * 1024;
const CONTROL_ENTRIES: u64 = 3;
// The isolated 4A/4B driver admits dedicated files and segments of at most 4 MiB.
const MAX_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024;

/// Caller reservation, separate from the campaign's combined source/snapshot/restore charge.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_artifacts: usize,
    pub max_records: usize,
    pub max_database_bytes: u64,
    pub max_catalog_bytes: u64,
    pub max_total_bytes: u64,
}
impl Limits {
    fn validate(&self) -> Result<()> {
        if self.max_artifacts == 0
            || self.max_artifacts > 16_384
            || self.max_records == 0
            || self.max_records > 16_384
            || self.max_database_bytes == 0
            || self.max_database_bytes > 1024 * 1024 * 1024
            || self.max_catalog_bytes == 0
            || self.max_catalog_bytes > 64 * 1024 * 1024
            || self.max_total_bytes == 0
            || self.max_total_bytes > 100_000_000_000
        {
            return Err(
                "snapshot requires explicit bounded count/database/catalog/total reservations"
                    .into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    protocol: u32,
    generation: StorageToken,
    database_bytes: u64,
    database_sha256: [u8; 32],
    catalog_bytes: u64,
    catalog_sha256: [u8; 32],
    artifacts: usize,
    records: usize,
    artifact_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    artifact: ArtifactIdentity,
    kind: ArtifactKind,
    physical_length: u64,
    sha256: [u8; 32],
}
impl Entry {
    fn from_artifact(artifact: &ArtifactSnapshot) -> Result<Self> {
        if artifact.references == 0 || artifact.state != "live" {
            return Err("snapshot reference does not resolve to a live immutable artifact".into());
        }
        if artifact
            .physical_length
            .is_some_and(|length| length > MAX_ARTIFACT_BYTES)
        {
            return Err("snapshot artifact exceeds the 4-MiB laboratory bound".into());
        }
        Ok(Self {
            artifact: artifact.plan.artifact().clone(),
            kind: artifact.plan.kind(),
            physical_length: artifact
                .physical_length
                .ok_or("referenced artifact lacks physical length")?,
            sha256: artifact
                .sha256
                .ok_or("referenced artifact lacks physical hash")?,
        })
    }
    fn name(&self) -> String {
        self.artifact.file_name(self.kind)
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Summary {
    pub artifacts: usize,
    pub records: usize,
    pub artifact_bytes: u64,
    pub database_bytes: u64,
    pub catalog_bytes: u64,
}
impl Manifest {
    fn summary(&self) -> Summary {
        Summary {
            artifacts: self.artifacts,
            records: self.records,
            artifact_bytes: self.artifact_bytes,
            database_bytes: self.database_bytes,
            catalog_bytes: self.catalog_bytes,
        }
    }
    fn check(&self, limits: Limits) -> Result<()> {
        if self.protocol != 1
            || self.artifacts > limits.max_artifacts
            || self.records > limits.max_records
            || self.database_bytes == 0
            || self.database_bytes > limits.max_database_bytes
            || self.catalog_bytes > limits.max_catalog_bytes
        {
            return Err("unsupported or oversized snapshot manifest".into());
        }
        checked_total(
            self.database_bytes,
            self.catalog_bytes,
            self.artifact_bytes,
            limits,
        )?;
        Ok(())
    }
}

struct Directory {
    path: PathBuf,
    file: File,
    identity: (u64, u64),
}
impl Directory {
    fn open(path: &Path) -> Result<Self> {
        let parent = path.parent().ok_or("snapshot path needs a parent")?;
        let name = path.file_name().ok_or("snapshot path needs a basename")?;
        if !path.is_absolute()
            || parent.canonicalize()? != parent
            || path
                .components()
                .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
        {
            return Err("snapshot directory must have an absolute canonical parent".into());
        }
        let parent = rustix::fs::open(
            parent,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let file = File::from(rustix::fs::openat2(
            parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_XDEV,
        )?);
        let stat = rustix::fs::fstat(&file)?;
        Ok(Self {
            path: path.to_owned(),
            file,
            identity: (stat.st_dev, stat.st_ino),
        })
    }
    fn create(path: &Path) -> Result<Self> {
        let parent = path.parent().ok_or("snapshot needs parent")?;
        let name = path.file_name().ok_or("snapshot needs basename")?;
        if !path.is_absolute() || parent.canonicalize()? != parent || parent.join(name) != path {
            return Err("fresh canonical snapshot directory required".into());
        }
        let parent = File::from(rustix::fs::open(
            parent,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        rustix::fs::mkdirat(&parent, name, Mode::from_bits_truncate(0o700))?;
        parent.sync_all()?;
        Self::open(path)
    }
    fn validate(&self) -> Result<()> {
        let named = Self::open(&self.path)?;
        if named.identity != self.identity || rustix::fs::fstat(&self.file)?.st_nlink == 0 {
            return Err("snapshot directory identity changed".into());
        }
        Ok(())
    }
    fn open_file(&self, name: &str, create: bool) -> Result<File> {
        basename(name)?;
        let flags = if create {
            OFlags::RDWR | OFlags::CREATE | OFlags::EXCL
        } else {
            OFlags::RDONLY
        };
        let file = File::from(rustix::fs::openat2(
            &self.file,
            name,
            flags | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            if create {
                Mode::from_bits_truncate(0o600)
            } else {
                Mode::empty()
            },
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_XDEV,
        )?);
        self.check_file(name, &file)?;
        Ok(file)
    }
    fn check_file(&self, name: &str, file: &File) -> Result<u64> {
        let opened = rustix::fs::fstat(file)?;
        let named = rustix::fs::statat(&self.file, name, AtFlags::SYMLINK_NOFOLLOW)?;
        if rustix::fs::FileType::from_raw_mode(opened.st_mode) != rustix::fs::FileType::RegularFile
            || opened.st_nlink != 1
            || (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
        {
            return Err("snapshot entry is not one unchanged regular file".into());
        }
        u64::try_from(opened.st_size).map_err(Into::into)
    }
    fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        self.validate()
    }
    fn free_reservation(&self, bytes: u64, limits: Limits) -> Result<()> {
        let space = rustix::fs::fstatvfs(&self.file)?;
        let extra_per_entry = space
            .f_frsize
            .max(space.f_bsize)
            .saturating_sub(ENTRY_RESERVATION);
        let extra = extra_per_entry
            .checked_mul(limits.max_artifacts as u64 + CONTROL_ENTRIES)
            .ok_or("snapshot allocation reservation overflow")?;
        let bytes = bytes
            .checked_add(extra)
            .filter(|bytes| *bytes <= limits.max_total_bytes)
            .ok_or("snapshot filesystem allocation exceeds total reservation")?;
        if bytes
            .checked_add(FIXED_HEADROOM)
            .ok_or("snapshot space overflow")?
            > space.f_bavail.saturating_mul(space.f_frsize)
        {
            return Err("snapshot cannot reserve destination bytes and cleanup headroom".into());
        }
        Ok(())
    }
}

fn basename(name: &str) -> Result<()> {
    let mut parts = Path::new(name).components();
    if !matches!((parts.next(), parts.next()), (Some(Component::Normal(part)), None) if part == name)
    {
        return Err("snapshot entry must be one exact basename".into());
    }
    Ok(())
}
fn deadline(until: Instant) -> Result<()> {
    if Instant::now() >= until {
        Err("snapshot deadline reached".into())
    } else {
        Ok(())
    }
}
fn checked_total(database: u64, catalog: u64, artifacts: u64, limits: Limits) -> Result<u64> {
    database
        .checked_add(catalog)
        .and_then(|n| n.checked_add(artifacts))
        .and_then(|n| n.checked_add(MANIFEST_LIMIT + FIXED_HEADROOM))
        .and_then(|n| {
            n.checked_add((limits.max_artifacts as u64 + CONTROL_ENTRIES) * ENTRY_RESERVATION)
        })
        .filter(|n| *n <= limits.max_total_bytes)
        .ok_or_else(|| "snapshot exceeds total reservation".into())
}

fn hash(reader: &mut impl Read, expected_length: u64, until: Instant) -> Result<[u8; 32]> {
    let mut scratch = [0; SCRATCH];
    let mut digest = Sha256::new();
    let mut length = 0_u64;
    loop {
        deadline(until)?;
        let count = reader.read(&mut scratch)?;
        if count == 0 {
            break;
        }
        length = length
            .checked_add(count as u64)
            .filter(|n| *n <= expected_length)
            .ok_or("snapshot source exceeds declared length")?;
        digest.update(&scratch[..count]);
    }
    if length != expected_length {
        return Err("snapshot source is truncated".into());
    }
    Ok(digest.finalize().into())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Barrier {
    CopyChunk,
    FileSync,
    BeforeManifest,
    BeforeDatabasePublication,
}
#[derive(Default)]
struct Hooks {
    #[cfg(test)]
    fail: Option<(Barrier, usize)>,
    #[cfg(test)]
    visits: std::cell::Cell<usize>,
}
impl Hooks {
    fn at(&self, barrier: Barrier) -> Result<()> {
        #[cfg(test)]
        if let Some((fail, after)) = self.fail
            && fail == barrier
        {
            let visit = self.visits.get();
            self.visits.set(visit + 1);
            if visit == after {
                return Err(std::io::Error::from_raw_os_error(28).into());
            }
        }
        let _ = barrier;
        Ok(())
    }
}

fn copy(
    reader: &mut impl Read,
    destination: &Directory,
    name: &str,
    length: u64,
    expected: [u8; 32],
    until: Instant,
    hooks: &Hooks,
) -> Result<()> {
    deadline(until)?;
    let mut target = destination.open_file(name, true)?;
    if length != 0 {
        // Refuse ENOSPC before copying source payload. Unsupported reservations fail closed.
        rustix::fs::fallocate(&target, FallocateFlags::KEEP_SIZE, 0, length)?;
    }
    let mut scratch = [0; SCRATCH];
    let mut digest = Sha256::new();
    let mut written = 0_u64;
    loop {
        deadline(until)?;
        let count = reader.read(&mut scratch)?;
        if count == 0 {
            break;
        }
        written = written
            .checked_add(count as u64)
            .filter(|n| *n <= length)
            .ok_or("snapshot copy exceeds declared length")?;
        hooks.at(Barrier::CopyChunk)?;
        target.write_all(&scratch[..count])?;
        digest.update(&scratch[..count]);
    }
    if written != length || <[u8; 32]>::from(digest.finalize()) != expected {
        return Err("snapshot copy length/hash mismatch".into());
    }
    hooks.at(Barrier::FileSync)?;
    target.sync_all()?;
    if destination.check_file(name, &target)? != length {
        return Err("copied snapshot length changed".into());
    }
    target.seek(SeekFrom::Start(0))?;
    if hash(&mut target, length, until)? != expected {
        return Err("durable snapshot copy hash mismatch".into());
    }
    destination.sync()
}

struct CatalogReader {
    reader: BufReader<File>,
    bytes: u64,
    limit: u64,
}
impl CatalogReader {
    fn new(file: File, limit: u64) -> Self {
        Self {
            reader: BufReader::with_capacity(CATALOG_LINE_LIMIT, file),
            bytes: 0,
            limit,
        }
    }
    fn next(&mut self) -> Result<Option<Entry>> {
        let mut line = Vec::new();
        self.reader
            .by_ref()
            .take((CATALOG_LINE_LIMIT + 1) as u64)
            .read_until(b'\n', &mut line)?;
        if line.is_empty() {
            return Ok(None);
        }
        self.bytes = self
            .bytes
            .checked_add(line.len() as u64)
            .filter(|n| *n <= self.limit)
            .ok_or("snapshot catalog exceeds reservation")?;
        if line.len() > CATALOG_LINE_LIMIT || line.last() != Some(&b'\n') {
            return Err("snapshot catalog record is oversized or incomplete".into());
        }
        Ok(Some(serde_json::from_slice(&line)?))
    }
}

/// The owned offline proof remains alive through every actual synchronous copy and validation.
pub fn create(
    source: OfflineRoot,
    destination: &Path,
    limits: Limits,
    until: Instant,
) -> Result<Summary> {
    create_with(source, destination, limits, until, &Hooks::default())
}
fn create_with(
    source: OfflineRoot,
    destination: &Path,
    limits: Limits,
    until: Instant,
    hooks: &Hooks,
) -> Result<Summary> {
    limits.validate()?;
    deadline(until)?;
    if destination.starts_with(source.root()) || source.root().starts_with(destination) {
        return Err("snapshot and source directories must not overlap".into());
    }
    source.validate()?;
    let view = OfflineView::open(source)?;
    let source = Directory::open(view.root())?;
    let mut database = source.open_file("packing.sqlite3", false)?;
    let database_bytes = source.check_file("packing.sqlite3", &database)?;
    if database_bytes == 0 || database_bytes > limits.max_database_bytes {
        return Err("snapshot database exceeds reservation".into());
    }
    let database_sha256 = hash(&mut database, database_bytes, until)?;
    let mut artifacts = 0;
    let mut metadata_artifacts = 0;
    let mut artifact_bytes = 0_u64;
    let mut after = None;
    loop {
        deadline(until)?;
        let page = view.artifact_page(after.as_ref(), PAGE)?;
        if page.is_empty() {
            break;
        }
        for artifact in &page {
            after = Some(artifact.plan.artifact().id.clone());
            metadata_artifacts += 1;
            if metadata_artifacts > limits.max_artifacts {
                return Err("snapshot metadata artifact count exceeds reservation".into());
            }
            if artifact.references == 0 {
                continue;
            }
            let entry = Entry::from_artifact(artifact)?;
            artifacts += 1;
            if artifacts > limits.max_artifacts {
                return Err("snapshot artifact count exceeds reservation".into());
            }
            artifact_bytes = artifact_bytes
                .checked_add(entry.physical_length)
                .ok_or("snapshot artifact size overflow")?;
            checked_total(
                database_bytes,
                limits.max_catalog_bytes,
                artifact_bytes,
                limits,
            )?;
        }
    }
    let reserved = checked_total(
        database_bytes,
        limits.max_catalog_bytes,
        artifact_bytes,
        limits,
    )?;
    let destination = Directory::create(destination)?;
    destination.free_reservation(reserved, limits)?;
    database.seek(SeekFrom::Start(0))?;
    copy(
        &mut database,
        &destination,
        DATABASE,
        database_bytes,
        database_sha256,
        until,
        hooks,
    )?;
    let mut catalog = destination.open_file(CATALOG, true)?;
    let mut catalog_bytes = 0_u64;
    let mut catalog_hash = Sha256::new();
    let mut after = None;
    loop {
        deadline(until)?;
        let page = view.artifact_page(after.as_ref(), PAGE)?;
        if page.is_empty() {
            break;
        }
        for artifact in &page {
            after = Some(artifact.plan.artifact().id.clone());
            if artifact.references == 0 {
                continue;
            }
            let entry = Entry::from_artifact(artifact)?;
            let mut pinned = PinnedArtifact::open(
                &source.path,
                &entry.artifact,
                entry.kind,
                entry.physical_length,
                view.lifetime(),
            )?;
            copy(
                &mut pinned,
                &destination,
                &entry.name(),
                entry.physical_length,
                entry.sha256,
                until,
                hooks,
            )?;
            let mut line = serde_json::to_vec(&entry)?;
            line.push(b'\n');
            catalog_bytes = catalog_bytes
                .checked_add(line.len() as u64)
                .filter(|n| *n <= limits.max_catalog_bytes)
                .ok_or("snapshot catalog exceeds reservation")?;
            if line.len() > CATALOG_LINE_LIMIT {
                return Err("snapshot catalog line exceeds bound".into());
            }
            catalog.write_all(&line)?;
            catalog_hash.update(&line);
        }
    }
    hooks.at(Barrier::FileSync)?;
    catalog.sync_all()?;
    destination.sync()?;
    let records = verify_records(&view, &destination, limits, until, view.lifetime())?;
    let manifest = Manifest {
        protocol: 1,
        generation: view.generation().clone(),
        database_bytes,
        database_sha256,
        catalog_bytes,
        catalog_sha256: catalog_hash.finalize().into(),
        artifacts,
        records,
        artifact_bytes,
    };
    manifest.check(limits)?;
    source.validate()?;
    hooks.at(Barrier::BeforeManifest)?;
    let bytes = serde_json::to_vec(&manifest)?;
    if bytes.len() as u64 > MANIFEST_LIMIT {
        return Err("manifest exceeds fixed bound".into());
    }
    let temporary = format!(".manifest-{}.tmp", StorageToken::generate().as_str());
    let mut file = destination.open_file(&temporary, true)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    rustix::fs::renameat_with(
        &destination.file,
        temporary.as_str(),
        &destination.file,
        MANIFEST,
        RenameFlags::NOREPLACE,
    )?;
    destination.sync()?;
    Ok(manifest.summary())
}

fn verify_records(
    view: &ImageView,
    directory: &Directory,
    limits: Limits,
    until: Instant,
    lifetime: Arc<dyn Send + Sync>,
) -> Result<usize> {
    let mut records = 0;
    let mut after = None;
    loop {
        deadline(until)?;
        let page = view.record_page(after.as_ref(), PAGE)?;
        if page.is_empty() {
            break;
        }
        for record in page {
            after = Some(record.metadata.row_id.clone());
            records += 1;
            if records > limits.max_records {
                return Err("snapshot record count exceeds reservation".into());
            }
            let pin = record::open_pinned(
                &directory.path,
                &record.location,
                record.metadata.encoded_sha256,
                lifetime.clone(),
            )?;
            pin.verify_sha256(record.metadata.encoded_sha256)?;
        }
    }
    Ok(records)
}

struct Validated {
    source: Directory,
    manifest: Manifest,
    view: ImageView,
}
fn validate_source(path: &Path, limits: Limits, until: Instant) -> Result<Validated> {
    limits.validate()?;
    deadline(until)?;
    let source = Directory::open(path)?;
    let mut file = source.open_file(MANIFEST, false)?;
    let manifest_length = source.check_file(MANIFEST, &file)?;
    if manifest_length > MANIFEST_LIMIT {
        return Err("snapshot manifest exceeds fixed bound".into());
    }
    let mut bytes = Vec::with_capacity(manifest_length as usize);
    Read::by_ref(&mut file)
        .take(MANIFEST_LIMIT + 1)
        .read_to_end(&mut bytes)?;
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    manifest.check(limits)?;
    let mut database = source.open_file(DATABASE, false)?;
    if source.check_file(DATABASE, &database)? != manifest.database_bytes
        || hash(&mut database, manifest.database_bytes, until)? != manifest.database_sha256
    {
        return Err("snapshot metadata length/hash mismatch".into());
    }
    let mut catalog_file = source.open_file(CATALOG, false)?;
    if source.check_file(CATALOG, &catalog_file)? != manifest.catalog_bytes
        || hash(&mut catalog_file, manifest.catalog_bytes, until)? != manifest.catalog_sha256
    {
        return Err("snapshot catalog length/hash mismatch".into());
    }
    catalog_file.seek(SeekFrom::Start(0))?;
    let mut catalog = CatalogReader::new(catalog_file, limits.max_catalog_bytes);
    let view = ImageView::open(&source.path.join(DATABASE))?;
    if view.generation() != &manifest.generation {
        return Err("snapshot generation differs from metadata image".into());
    }
    let mut after = None;
    let mut artifacts = 0;
    let mut metadata_artifacts = 0;
    let mut artifact_bytes = 0_u64;
    loop {
        deadline(until)?;
        let page = view.artifact_page(after.as_ref(), PAGE)?;
        if page.is_empty() {
            break;
        }
        for artifact in &page {
            after = Some(artifact.plan.artifact().id.clone());
            metadata_artifacts += 1;
            if metadata_artifacts > limits.max_artifacts {
                return Err("snapshot metadata artifact count exceeds reservation".into());
            }
            if artifact.references == 0 {
                continue;
            }
            let expected = Entry::from_artifact(artifact)?;
            if catalog.next()?.as_ref() != Some(&expected) {
                return Err(
                    "snapshot catalog does not exactly match referenced generations".into(),
                );
            }
            artifacts += 1;
            if artifacts > limits.max_artifacts {
                return Err("snapshot artifact count exceeds reservation".into());
            }
            artifact_bytes = artifact_bytes
                .checked_add(expected.physical_length)
                .ok_or("snapshot size overflow")?;
            let mut pinned = PinnedArtifact::open(
                &source.path,
                &expected.artifact,
                expected.kind,
                expected.physical_length,
                Arc::new(()),
            )?;
            if hash(&mut pinned, expected.physical_length, until)? != expected.sha256 {
                return Err("snapshot artifact hash mismatch".into());
            }
        }
    }
    if catalog.next()?.is_some()
        || artifacts != manifest.artifacts
        || artifact_bytes != manifest.artifact_bytes
    {
        return Err("snapshot catalog counts or coverage differ".into());
    }
    let records = verify_records(&view, &source, limits, until, Arc::new(()))?;
    if records != manifest.records {
        return Err("snapshot record count differs".into());
    }
    // Exactly three control files and one file per referenced immutable artifact. Reading
    // names is bounded; no arbitrary directory tree or unreferenced debt bytes are copied.
    let mut count = 0;
    for entry in std::fs::read_dir(&source.path)? {
        deadline(until)?;
        let entry = entry?;
        count += 1;
        if count > artifacts + 3 {
            return Err("snapshot contains an unexpected entry".into());
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or("non-UTF8 snapshot entry")?;
        source.open_file(name, false)?;
    }
    if count != artifacts + 3 {
        return Err("snapshot entries are incomplete".into());
    }
    source.validate()?;
    Ok(Validated {
        source,
        manifest,
        view,
    })
}

pub fn validate(source: &Path, limits: Limits, until: Instant) -> Result<Summary> {
    Ok(validate_source(source, limits, until)?.manifest.summary())
}

/// Validate the complete input before writing any target artifact. Publish target metadata
/// only after durable, verified copies; startup then creates a fresh process generation.
pub fn restore(
    source: &Path,
    target: Arc<Node>,
    limits: Limits,
    until: Instant,
) -> Result<Summary> {
    restore_with(source, target, limits, until, &Hooks::default())
}
fn restore_with(
    source: &Path,
    target: Arc<Node>,
    limits: Limits,
    until: Instant,
    hooks: &Hooks,
) -> Result<Summary> {
    let validated = validate_source(source, limits, until)?;
    if source.starts_with(target.root()) || target.root().starts_with(source) {
        return Err("snapshot and restore target must not overlap".into());
    }
    let proof = target.offline()?;
    let destination = Directory::open(proof.root())?;
    let mut entries = std::fs::read_dir(proof.root())?;
    let only_lock = entries
        .next()
        .transpose()?
        .ok_or("fresh target lock missing")?;
    if only_lock.file_name() != ".packing.lock" || entries.next().is_some() {
        return Err("restore requires a fresh target containing only its node lock".into());
    }
    let manifest = &validated.manifest;
    destination.free_reservation(
        checked_total(
            manifest.database_bytes,
            manifest.catalog_bytes,
            manifest.artifact_bytes,
            limits,
        )?,
        limits,
    )?;
    let mut after = None;
    loop {
        deadline(until)?;
        let page = validated.view.artifact_page(after.as_ref(), PAGE)?;
        if page.is_empty() {
            break;
        }
        for artifact in &page {
            after = Some(artifact.plan.artifact().id.clone());
            if artifact.references == 0 {
                continue;
            }
            let entry = Entry::from_artifact(artifact)?;
            let mut pinned = PinnedArtifact::open(
                &validated.source.path,
                &entry.artifact,
                entry.kind,
                entry.physical_length,
                proof.lifetime(),
            )?;
            copy(
                &mut pinned,
                &destination,
                &entry.name(),
                entry.physical_length,
                entry.sha256,
                until,
                hooks,
            )?;
        }
    }
    let temporary = format!(".restore-{}.sqlite3", StorageToken::generate().as_str());
    let mut database = validated.source.open_file(DATABASE, false)?;
    copy(
        &mut database,
        &destination,
        &temporary,
        manifest.database_bytes,
        manifest.database_sha256,
        until,
        hooks,
    )?;
    let copied = ImageView::open(&destination.path.join(&temporary))?;
    if copied.generation() != &manifest.generation
        || verify_records(&copied, &destination, limits, until, proof.lifetime())?
            != manifest.records
    {
        return Err("restored metadata or record coverage differs before publication".into());
    }
    drop(copied);
    proof.validate()?;
    hooks.at(Barrier::BeforeDatabasePublication)?;
    deadline(until)?;
    rustix::fs::renameat_with(
        &destination.file,
        temporary.as_str(),
        &destination.file,
        "packing.sqlite3",
        RenameFlags::NOREPLACE,
    )?;
    proof.mark_dirty();
    destination.sync()?;
    Ok(manifest.summary())
}

#[cfg(test)]
mod tests {
    use super::super::model::PhysicalBudget;
    use super::super::model::{
        CipherFormat, EncodedFormat, ExpectedCurrent, Location, PublishRecord, PublishedRecord,
        RecordMetadata,
    };
    use super::super::record::{CleanupResult, DurableArtifact};
    use super::super::store::{PublicationOutcome, Store};
    use super::*;
    use cairn_types::CompressionDescriptor;
    use std::io::Cursor;
    use std::time::Duration;

    fn limits() -> Limits {
        Limits {
            max_artifacts: 64,
            max_records: 64,
            max_database_bytes: 8 * 1024 * 1024,
            max_catalog_bytes: 1024 * 1024,
            max_total_bytes: 32 * 1024 * 1024,
        }
    }
    fn until() -> Instant {
        Instant::now() + Duration::from_secs(20)
    }
    fn open(node: Arc<Node>) -> Store {
        Store::open(
            node,
            PhysicalBudget {
                limit_bytes: 32 * 1024 * 1024,
            },
        )
        .unwrap()
    }
    fn metadata(artifact: &DurableArtifact, key: &str) -> PublishRecord {
        let span = &artifact.spans()[0];
        let identity = artifact.plan().artifact().clone();
        PublishRecord {
            metadata: RecordMetadata {
                row_id: StorageToken::generate(),
                key: key.into(),
                encoded_sha256: span.sha256,
                encoded_length: span.length,
                logical_size: span.length,
                format: EncodedFormat::Raw,
                compression: CompressionDescriptor::Uncompressed,
                cipher: CipherFormat::Plaintext,
                locked: false,
            },
            location: if artifact.plan().kind() == ArtifactKind::File {
                Location::File {
                    artifact: identity,
                    length: span.length,
                }
            } else {
                Location::Segment {
                    artifact: identity,
                    offset: span.offset,
                    length: span.length,
                }
            },
            expected: ExpectedCurrent::Absent,
            preserve_previous: false,
        }
    }
    async fn stage(store: &Store, root: &Path, bytes: &[u8], packed: bool) -> DurableArtifact {
        if packed {
            let admission = store
                .plan(
                    ArtifactKind::Segment,
                    record::segment_length([bytes.len() as u64]).unwrap(),
                )
                .await
                .unwrap();
            record::publish_segment(root, admission, &[bytes]).unwrap()
        } else {
            let admission = store
                .plan(ArtifactKind::File, bytes.len() as u64)
                .await
                .unwrap();
            record::publish_file(root, admission, &mut Cursor::new(bytes)).unwrap()
        }
    }
    async fn apply(
        store: &Store,
        artifact: DurableArtifact,
        update: PublishRecord,
    ) -> PublishedRecord {
        let key = update.metadata.key.clone();
        assert!(matches!(
            store.publish(artifact, vec![update]).await.unwrap(),
            PublicationOutcome::Applied { records: 1 }
        ));
        store.lookup(&key).await.unwrap().unwrap()
    }
    async fn publish(
        store: &Store,
        node: &Node,
        key: &str,
        bytes: &[u8],
        packed: bool,
    ) -> PublishedRecord {
        let artifact = stage(store, node.root(), bytes, packed).await;
        let update = metadata(&artifact, key);
        apply(store, artifact, update).await
    }
    async fn drain(store: &Store, root: &Path) {
        loop {
            let claims = store.claim_cleanup(PAGE).await.unwrap();
            if claims.is_empty() {
                break;
            }
            for claim in claims {
                let CleanupResult::Removed(receipt) = record::cleanup(root, claim).unwrap() else {
                    panic!("unexpected pinned fixture debt")
                };
                assert!(store.finish_cleanup(receipt).await.unwrap());
            }
        }
    }
    fn tree_digest(path: &Path) -> Vec<(String, [u8; 32])> {
        let mut values: Vec<_> = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (
                    entry.file_name().to_string_lossy().into_owned(),
                    Sha256::digest(std::fs::read(entry.path()).unwrap()).into(),
                )
            })
            .collect();
        values.sort();
        values
    }
    async fn basic(temporary: &Path) -> (Arc<Node>, PathBuf, PublishedRecord) {
        let node = Node::create(&temporary.join("source")).unwrap();
        let store = open(node.clone());
        let record = publish(&store, &node, "survivor", b"immutable survivor", true).await;
        store.close().await.unwrap();
        let snapshot = temporary.join("snapshot");
        create(node.offline().unwrap(), &snapshot, limits(), until()).unwrap();
        (node, snapshot, record)
    }

    #[tokio::test]
    async fn manifest_last_interruption_never_becomes_restorable_and_preserves_source() {
        let temporary = tempfile::tempdir().unwrap();
        let node = Node::create(&temporary.path().join("source")).unwrap();
        let store = open(node.clone());
        publish(&store, &node, "a", &vec![7; SCRATCH + 19], true).await;
        store.close().await.unwrap();
        let original = tree_digest(node.root());
        for (index, (barrier, after)) in [
            (Barrier::CopyChunk, 0),
            (Barrier::CopyChunk, 1),
            (Barrier::FileSync, 0),
            (Barrier::BeforeManifest, 0),
        ]
        .into_iter()
        .enumerate()
        {
            let path = temporary.path().join(format!("interrupted-{index}"));
            let hooks = Hooks {
                fail: Some((barrier, after)),
                visits: std::cell::Cell::new(0),
            };
            assert!(
                create_with(node.offline().unwrap(), &path, limits(), until(), &hooks).is_err()
            );
            assert!(!path.join(MANIFEST).exists());
            assert!(validate(&path, limits(), until()).is_err());
            assert_eq!(tree_digest(node.root()), original);
        }
    }

    #[tokio::test]
    async fn corrupted_input_refuses_before_any_fresh_target_write() {
        for corruption in [
            "artifact-hash",
            "artifact-truncated",
            "metadata",
            "catalog",
            "catalog-rebound",
            "metadata-rebound",
            "manifest",
            "extra",
            "symlink",
            "hardlink",
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let (node, snapshot, record) = basic(temporary.path()).await;
            let artifact = snapshot.join(record.location.path());
            match corruption {
                "artifact-hash" => {
                    let mut bytes = std::fs::read(&artifact).unwrap();
                    let last = bytes.len() - 1;
                    bytes[last] ^= 1;
                    std::fs::write(&artifact, bytes).unwrap();
                }
                "artifact-truncated" => File::options()
                    .write(true)
                    .open(&artifact)
                    .unwrap()
                    .set_len(81)
                    .unwrap(),
                "metadata" => std::fs::write(snapshot.join(DATABASE), b"invalid metadata").unwrap(),
                "catalog" => std::fs::write(snapshot.join(CATALOG), b"{}\n").unwrap(),
                "catalog-rebound" => {
                    let mut entry: Entry =
                        serde_json::from_slice(&std::fs::read(snapshot.join(CATALOG)).unwrap())
                            .unwrap();
                    entry.artifact.generation = StorageToken::generate();
                    let mut encoded = serde_json::to_vec(&entry).unwrap();
                    encoded.push(b'\n');
                    std::fs::write(snapshot.join(CATALOG), &encoded).unwrap();
                    let mut manifest: Manifest =
                        serde_json::from_slice(&std::fs::read(snapshot.join(MANIFEST)).unwrap())
                            .unwrap();
                    manifest.catalog_bytes = encoded.len() as u64;
                    manifest.catalog_sha256 = Sha256::digest(&encoded).into();
                    std::fs::write(
                        snapshot.join(MANIFEST),
                        serde_json::to_vec(&manifest).unwrap(),
                    )
                    .unwrap();
                }
                "metadata-rebound" => {
                    let connection = rusqlite::Connection::open(snapshot.join(DATABASE)).unwrap();
                    connection
                        .execute(
                            "UPDATE records SET encoded_sha256=?1",
                            [Sha256::digest(b"forged record hash").to_vec()],
                        )
                        .unwrap();
                    connection.close().unwrap();
                    let encoded = std::fs::read(snapshot.join(DATABASE)).unwrap();
                    let mut manifest: Manifest =
                        serde_json::from_slice(&std::fs::read(snapshot.join(MANIFEST)).unwrap())
                            .unwrap();
                    manifest.database_bytes = encoded.len() as u64;
                    manifest.database_sha256 = Sha256::digest(&encoded).into();
                    std::fs::write(
                        snapshot.join(MANIFEST),
                        serde_json::to_vec(&manifest).unwrap(),
                    )
                    .unwrap();
                }
                "manifest" => {
                    std::fs::remove_file(snapshot.join(MANIFEST)).unwrap();
                }
                "extra" => std::fs::write(snapshot.join("unlisted"), b"unlisted").unwrap(),
                "symlink" => {
                    std::fs::remove_file(&artifact).unwrap();
                    std::os::unix::fs::symlink(node.root().join(record.location.path()), &artifact)
                        .unwrap();
                }
                "hardlink" => {
                    std::fs::remove_file(&artifact).unwrap();
                    std::fs::hard_link(node.root().join(record.location.path()), &artifact)
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let source_manifest = std::fs::read(snapshot.join(MANIFEST)).ok();
            let target = Node::create(&temporary.path().join("target")).unwrap();
            let before = tree_digest(target.root());
            assert!(
                restore(&snapshot, target.clone(), limits(), until()).is_err(),
                "{corruption}"
            );
            assert_eq!(tree_digest(target.root()), before, "{corruption}");
            assert!(!target.root().join("packing.sqlite3").exists());
            assert_eq!(std::fs::read(snapshot.join(MANIFEST)).ok(), source_manifest);
        }
    }

    #[tokio::test]
    async fn restore_copies_before_metadata_publication_and_never_reuses_a_partial_target() {
        let temporary = tempfile::tempdir().unwrap();
        let (_node, snapshot, _record) = basic(temporary.path()).await;
        let input = tree_digest(&snapshot);
        for (index, barrier) in [Barrier::CopyChunk, Barrier::BeforeDatabasePublication]
            .into_iter()
            .enumerate()
        {
            let target = Node::create(&temporary.path().join(format!("target-{index}"))).unwrap();
            let hooks = Hooks {
                fail: Some((barrier, 0)),
                visits: std::cell::Cell::new(0),
            };
            assert!(restore_with(&snapshot, target.clone(), limits(), until(), &hooks).is_err());
            assert!(!target.root().join("packing.sqlite3").exists());
            assert!(restore(&snapshot, target.clone(), limits(), until()).is_err());
            assert_eq!(tree_digest(&snapshot), input);
        }
    }

    #[tokio::test]
    async fn exact_image_pending_claims_and_history_survive_until_fresh_generation_recovery() {
        let temporary = tempfile::tempdir().unwrap();
        let source = Node::create(&temporary.path().join("source")).unwrap();
        let store = open(source.clone());
        let first = publish(&store, &source, "versioned", b"historical bytes", true).await;
        let artifact = stage(&store, source.root(), b"current bytes", false).await;
        let mut update = metadata(&artifact, "versioned");
        update.expected = ExpectedCurrent::Exact {
            row_id: first.metadata.row_id.clone(),
            location: first.location.clone(),
        };
        update.preserve_previous = true;
        let current = apply(&store, artifact, update).await;
        let uncommitted = stage(&store, source.root(), b"unpublished bytes", false).await;
        let uncommitted_path = uncommitted.plan().final_path().to_owned();
        drop(uncommitted); // Actual physical work ended; durable pending identity remains.
        let abort = store.plan(ArtifactKind::File, 13).await.unwrap();
        assert!(store.abort(record::abort(abort)).await.unwrap());
        let claimed = store.claim_cleanup(1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        drop(claimed); // Abandoned reply is durable, and no syscall still owns the claim.
        let old_generation = store.generation().clone();
        let original_stats = store.stats().await.unwrap();
        assert_eq!(original_stats.pending, 1);
        assert!(original_stats.cleanup > 0);
        store.close().await.unwrap();
        let original = tree_digest(source.root());
        let snapshot = temporary.path().join("snapshot");
        let summary = create(source.offline().unwrap(), &snapshot, limits(), until()).unwrap();
        assert_eq!(summary.records, 2);
        assert_eq!(summary.artifacts, 2);
        assert!(!snapshot.join(&uncommitted_path).exists());
        assert_eq!(
            std::fs::read(snapshot.join(DATABASE)).unwrap(),
            std::fs::read(source.root().join("packing.sqlite3")).unwrap()
        );
        assert_eq!(tree_digest(source.root()), original);
        let input = tree_digest(&snapshot);
        let target = Node::create(&temporary.path().join("restored")).unwrap();
        restore(&snapshot, target.clone(), limits(), until()).unwrap();
        assert_eq!(
            std::fs::read(target.root().join("packing.sqlite3")).unwrap(),
            std::fs::read(snapshot.join(DATABASE)).unwrap()
        );
        assert!(target.offline().is_err()); // Restored generation must first be fenced by an actor.
        let restored = open(target.clone());
        assert_ne!(restored.generation(), &old_generation);
        assert_eq!(restored.stats().await.unwrap(), original_stats);
        assert!(
            restored
                .debt(None, PAGE)
                .await
                .unwrap()
                .iter()
                .all(|debt| !debt.claimed)
        );
        let pending = restored.pending(None, PAGE).await.unwrap();
        assert_eq!(pending.len(), 1);
        for pending in pending {
            assert!(restored.recover_pending(pending).await.unwrap());
        }
        drain(&restored, target.root()).await;
        let stats = restored.stats().await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.cleanup, 0);
        for expected in [first, current] {
            let pinned = restored
                .pin_version(&expected.metadata.row_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pinned.record.metadata, expected.metadata);
            assert_eq!(pinned.record.location, expected.location);
            pinned
                .pin
                .verify_sha256(expected.metadata.encoded_sha256)
                .unwrap();
        }
        restored.close().await.unwrap();
        assert!(target.offline().is_ok());
        assert_eq!(tree_digest(&snapshot), input);
    }

    #[tokio::test]
    async fn encrypted_locked_history_roundtrip_uses_external_keys_and_trusted_metadata() {
        use cairn_types::testing::{FixtureBlobStore, fixture_storage_io};
        use cairn_types::{
            BlobCipher, BucketName, CompressionAlgorithm, CompressionPolicy, StageOptions,
        };
        let key = super::super::model::encryption_test_key();
        let temporary = tempfile::tempdir().unwrap();
        let blobs = cairn_blob::LocalBlobStore::open(
            &temporary.path().join("encoder"),
            fixture_storage_io(),
        )
        .await
        .unwrap();
        let plaintext = vec![b'z'; 65_537];
        let body = bytes::Bytes::from(plaintext.clone());
        let staged = blobs
            .stage_fixture(
                &BucketName::parse("snapshot-history").unwrap(),
                Box::pin(futures_util::stream::once(async move { Ok(body) })),
                StageOptions {
                    compression: Some(CompressionPolicy {
                        algorithm: CompressionAlgorithm::Zstd,
                        block_size: 64 * 1024,
                    }),
                    encryption: Some(key.clone()),
                    size_ceiling: plaintext.len() as u64,
                    content_type: "text/plain".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let encoded = std::fs::read(
            temporary
                .path()
                .join("encoder")
                .join(staged.storage_path.as_str()),
        )
        .unwrap();
        let source = Node::create(&temporary.path().join("source")).unwrap();
        let store = open(source.clone());
        let artifact = stage(&store, source.root(), &encoded, true).await;
        let mut update = metadata(&artifact, "encrypted");
        update.metadata.format = EncodedFormat::Crnb;
        update.metadata.compression = staged.compression;
        update.metadata.cipher = CipherFormat::AuthenticatedV3;
        update.metadata.logical_size = plaintext.len() as u64;
        update.metadata.locked = true;
        let history = apply(&store, artifact, update).await;
        let artifact = stage(&store, source.root(), b"replacement", false).await;
        let mut update = metadata(&artifact, "encrypted");
        update.expected = ExpectedCurrent::Exact {
            row_id: history.metadata.row_id.clone(),
            location: history.location.clone(),
        };
        update.preserve_previous = true;
        apply(&store, artifact, update).await;
        store.close().await.unwrap();
        let snapshot = temporary.path().join("snapshot");
        create(source.offline().unwrap(), &snapshot, limits(), until()).unwrap();
        let input = tree_digest(&snapshot);
        let target = Node::create(&temporary.path().join("restored")).unwrap();
        restore(&snapshot, target.clone(), limits(), until()).unwrap();
        let restored = open(target.clone());
        let pinned = restored
            .pin_version(&history.metadata.row_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!pinned.record.is_current);
        assert!(pinned.record.metadata.locked);
        assert_eq!(pinned.record.metadata, history.metadata);
        assert_eq!(pinned.record.location, history.location);
        let mut reader = cairn_blob::compress::CompressedReader::open_with_dek(
            pinned.pin,
            BlobCipher::AuthenticatedV3(key),
            &pinned.record.metadata.compression,
            pinned.record.metadata.logical_size,
        )
        .unwrap();
        assert_eq!(
            reader.read_range(0, plaintext.len() as u64).unwrap(),
            plaintext
        );
        drop(reader);
        let wrong = super::super::model::encryption_test_key();
        let pinned = restored
            .pin_version(&history.metadata.row_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            cairn_blob::compress::CompressedReader::open_with_dek(
                pinned.pin,
                BlobCipher::AuthenticatedV3(wrong),
                &pinned.record.metadata.compression,
                pinned.record.metadata.logical_size
            )
            .is_err()
        );
        drain(&restored, target.root()).await;
        restored.close().await.unwrap();
        assert_eq!(tree_digest(&snapshot), input);
    }

    #[tokio::test]
    async fn caps_and_expired_deadline_refuse_before_snapshot_or_restore_writes() {
        let temporary = tempfile::tempdir().unwrap();
        let (node, snapshot, _record) = basic(temporary.path()).await;
        let mut tiny = limits();
        tiny.max_total_bytes = 1;
        let destination = temporary.path().join("oversized");
        assert!(create(node.offline().unwrap(), &destination, tiny, until()).is_err());
        assert!(!destination.exists());
        let destination = temporary.path().join("expired");
        assert!(
            create(
                node.offline().unwrap(),
                &destination,
                limits(),
                Instant::now() - Duration::from_secs(1)
            )
            .is_err()
        );
        assert!(!destination.exists());
        let target = Node::create(&temporary.path().join("target")).unwrap();
        let original = tree_digest(target.root());
        assert!(restore(&snapshot, target.clone(), tiny, until()).is_err());
        assert_eq!(tree_digest(target.root()), original);
    }

    #[tokio::test]
    async fn active_store_clone_and_pinned_job_cannot_manufacture_an_offline_snapshot() {
        let temporary = tempfile::tempdir().unwrap();
        let node = Node::create(&temporary.path().join("source")).unwrap();
        let store = open(node.clone());
        publish(&store, &node, "a", b"pinned", true).await;
        let clone = store.clone();
        assert!(node.offline().is_err());
        let pin = store.pin("a").await.unwrap().unwrap();
        store.close().await.unwrap();
        assert!(node.offline().is_err());
        assert!(clone.plan(ArtifactKind::File, 1).await.is_err());
        assert!(Store::open(node.clone(), PhysicalBudget::default()).is_err());
        drop(pin);
        let proof = node.offline().unwrap();
        assert!(Store::open(node.clone(), PhysicalBudget::default()).is_err());
        let snapshot = temporary.path().join("snapshot");
        create(proof, &snapshot, limits(), until()).unwrap();
        assert_eq!(validate(&snapshot, limits(), until()).unwrap().records, 1);
    }

    #[tokio::test]
    async fn tiny_artifact_allocation_is_reserved_before_any_copy() {
        let temporary = tempfile::tempdir().unwrap();
        let (node, snapshot, _record) = basic(temporary.path()).await;
        let mut bounded = limits();
        bounded.max_artifacts = 16_384;
        bounded.max_total_bytes = 8 * 1024 * 1024;
        // Payload-only accounting would admit 16,384 one-byte files, despite at least
        // 64 MiB of allocated blocks on a 4-KiB filesystem, plus control files/directories.
        assert!(checked_total(4096, 0, 16_384, bounded).is_err());
        let mut one = limits();
        one.max_artifacts = 1;
        let reserved = checked_total(4096, 0, 1, one).unwrap();
        assert_eq!(
            reserved,
            4096 + 1 + MANIFEST_LIMIT + FIXED_HEADROOM + 4 * ENTRY_RESERVATION
        );
        let destination = temporary.path().join("under-reserved-snapshot");
        assert!(create(node.offline().unwrap(), &destination, bounded, until()).is_err());
        assert!(!destination.exists());
        let target = Node::create(&temporary.path().join("under-reserved-restore")).unwrap();
        let before = tree_digest(target.root());
        assert!(restore(&snapshot, target.clone(), bounded, until()).is_err());
        assert_eq!(tree_digest(target.root()), before);
    }
}
