//! Actual root locking and in-process quiescence for the isolated laboratory.
//!
//! A closed SQLite handle is insufficient: admissions, detached filesystem jobs and pins
//! retain the same session. Only its last drop makes an offline proof obtainable.
use super::model::Result;
use rustix::fs::{AtFlags, Mode, OFlags, ResolveFlags};
use std::fs::File;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const LOCK_NAME: &str = ".packing.lock";

#[derive(Clone, Copy)]
enum State {
    Idle { clean: bool },
    Active,
    Offline,
}

pub struct Node {
    root: PathBuf,
    directory: File,
    lock: File,
    identity: (u64, u64),
    state: Mutex<State>,
}

impl Node {
    /// Create only a fresh canonical directory; never adopt an existing dataset.
    pub fn create(root: &Path) -> Result<Arc<Self>> {
        let (parent, name) = canonical_parent(root)?;
        rustix::fs::mkdirat(&parent, name, Mode::from_bits_truncate(0o700))?;
        parent.sync_all()?;
        Self::acquire(root, parent, name, true)
    }

    /// Existing nodes must establish a successful SQLite close before snapshotting.
    pub fn open(root: &Path) -> Result<Arc<Self>> {
        let (parent, name) = canonical_parent(root)?;
        Self::acquire(root, parent, name, false)
    }

    fn acquire(
        root: &Path,
        parent: File,
        name: &std::ffi::OsStr,
        fresh: bool,
    ) -> Result<Arc<Self>> {
        let directory = File::from(rustix::fs::openat2(
            &parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_XDEV,
        )?);
        let flags = OFlags::RDWR
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC
            | if fresh {
                OFlags::CREATE | OFlags::EXCL
            } else {
                OFlags::CREATE
            };
        let lock = File::from(rustix::fs::openat2(
            &directory,
            LOCK_NAME,
            flags,
            Mode::from_bits_truncate(0o600),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_XDEV,
        )?);
        let lock_stat = rustix::fs::fstat(&lock)?;
        if rustix::fs::FileType::from_raw_mode(lock_stat.st_mode)
            != rustix::fs::FileType::RegularFile
            || lock_stat.st_nlink != 1
        {
            return Err("node lock must be one regular, unaliased file".into());
        }
        lock.try_lock()?;
        // Existing roots may be acquired for the first time. CREATE always names the same
        // exact lock, and both the file and parent are durable before any actor starts.
        lock.sync_all()?;
        directory.sync_all()?;
        let stat = rustix::fs::fstat(&directory)?;
        let node = Arc::new(Self {
            root: root.to_owned(),
            directory,
            lock,
            identity: (stat.st_dev, stat.st_ino),
            state: Mutex::new(State::Idle { clean: fresh }),
        });
        node.validate()?;
        Ok(node)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn identity(&self) -> (u64, u64) {
        self.identity
    }

    pub fn validate(&self) -> Result<()> {
        let (parent, name) = canonical_parent(&self.root)?;
        let named = rustix::fs::openat2(
            &parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_XDEV,
        )?;
        let named = rustix::fs::fstat(named)?;
        let root = rustix::fs::fstat(&self.directory)?;
        let lock = rustix::fs::fstat(&self.lock)?;
        let named_lock = rustix::fs::statat(&self.directory, LOCK_NAME, AtFlags::SYMLINK_NOFOLLOW)?;
        if root.st_nlink == 0
            || (named.st_dev, named.st_ino) != self.identity
            || (root.st_dev, root.st_ino) != self.identity
            || lock.st_nlink != 1
            || (lock.st_dev, lock.st_ino) != (named_lock.st_dev, named_lock.st_ino)
        {
            return Err("node root or lock identity changed".into());
        }
        Ok(())
    }

    /// Dirty idle state may reopen for recovery. Simultaneous actors and offline reads may not.
    pub(super) fn active(self: &Arc<Self>) -> Result<Arc<ActiveSession>> {
        self.validate()?;
        let mut state = self.state.lock().map_err(|_| "node gate poisoned")?;
        if !matches!(*state, State::Idle { .. }) {
            return Err("node already has an actor, physical owner or offline reader".into());
        }
        *state = State::Active;
        Ok(Arc::new(ActiveSession {
            node: self.clone(),
            clean: AtomicBool::new(false),
        }))
    }

    pub fn offline(self: &Arc<Self>) -> Result<OfflineRoot> {
        self.validate()?;
        let mut state = self.state.lock().map_err(|_| "node gate poisoned")?;
        match *state {
            State::Idle { clean: true } => {}
            State::Idle { clean: false } => {
                return Err("successful SQLite close required before offline access".into());
            }
            _ => return Err("node still has an actor, physical owner or offline reader".into()),
        }
        *state = State::Offline;
        Ok(OfflineRoot {
            session: Arc::new(OfflineSession {
                node: self.clone(),
                clean: AtomicBool::new(true),
            }),
        })
    }
}

pub struct ActiveSession {
    node: Arc<Node>,
    clean: AtomicBool,
}

impl ActiveSession {
    /// Called only after the successful checkpoint, connection destruction and actor join.
    pub(super) fn mark_clean(&self) -> Result<()> {
        self.node.validate()?;
        self.clean.store(true, Ordering::Release);
        Ok(())
    }
    pub fn root(&self) -> &Path {
        self.node.root()
    }
    pub fn identity(&self) -> (u64, u64) {
        self.node.identity()
    }
}

impl Drop for ActiveSession {
    fn drop(&mut self) {
        if let Ok(mut state) = self.node.state.lock() {
            *state = State::Idle {
                clean: self.clean.load(Ordering::Acquire),
            };
        }
    }
}

struct OfflineSession {
    node: Arc<Node>,
    clean: AtomicBool,
}
impl Drop for OfflineSession {
    fn drop(&mut self) {
        if let Ok(mut state) = self.node.state.lock() {
            *state = State::Idle {
                clean: self.clean.load(Ordering::Acquire),
            };
        }
    }
}

/// Move-only proof. Detached readers may retain its lifetime without manufacturing another proof.
pub struct OfflineRoot {
    session: Arc<OfflineSession>,
}
impl OfflineRoot {
    pub fn root(&self) -> &Path {
        self.session.node.root()
    }
    pub fn identity(&self) -> (u64, u64) {
        self.session.node.identity()
    }
    pub fn validate(&self) -> Result<()> {
        self.session.node.validate()
    }
    pub fn lifetime(&self) -> Arc<dyn Send + Sync> {
        self.session.clone()
    }
    /// Installing a restored database requires another actor recovery/clean-close cycle.
    pub(super) fn mark_dirty(&self) {
        self.session.clean.store(false, Ordering::Release);
    }
}

fn canonical_parent(root: &Path) -> Result<(File, &std::ffi::OsStr)> {
    if !root.is_absolute()
        || root
            .components()
            .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
    {
        return Err("absolute canonical node root required".into());
    }
    let parent = root.parent().ok_or("node root requires a parent")?;
    let name = root.file_name().ok_or("node root requires a basename")?;
    if parent.canonicalize()? != parent || parent.join(name) != root {
        return Err("node root parent must be canonical and nonsymlink".into());
    }
    let descriptor = rustix::fs::open(
        parent,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    Ok((File::from(descriptor), name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::time::Duration;

    #[test]
    fn independent_open_existing_and_offline_reopen_are_excluded() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("node");
        let node = Node::create(&path).unwrap();
        assert!(Node::create(&path).is_err());
        assert!(Node::open(&path).is_err());
        let offline = node.offline().unwrap();
        assert!(node.active().is_err());
        assert!(node.offline().is_err());
        let pinned = offline.lifetime();
        drop(offline);
        assert!(node.active().is_err());
        drop(pinned);
        let active = node.active().unwrap();
        assert!(node.offline().is_err());
        active.mark_clean().unwrap();
        drop(active);
        assert!(node.offline().is_ok());
        drop(node);
        let reopened = Node::open(&path).unwrap();
        assert!(reopened.offline().is_err());
        let active = reopened.active().unwrap();
        active.mark_clean().unwrap();
        drop(active);
        assert!(reopened.offline().is_ok());
    }

    #[test]
    fn dirty_close_and_restore_require_a_new_clean_actor_cycle() {
        let temporary = tempfile::tempdir().unwrap();
        let node = Node::create(&temporary.path().join("node")).unwrap();
        drop(node.active().unwrap());
        assert!(node.offline().is_err());
        let active = node.active().unwrap();
        active.mark_clean().unwrap();
        drop(active);
        let offline = node.offline().unwrap();
        offline.mark_dirty();
        drop(offline);
        assert!(node.offline().is_err());
        assert!(node.active().is_ok());
    }

    #[test]
    fn physical_owner_outlives_clean_actor_and_retains_real_root_lock() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("node");
        let node = Node::create(&path).unwrap();
        let active = node.active().unwrap();
        let physical = active.clone();
        active.mark_clean().unwrap();
        drop(active);
        assert!(node.offline().is_err());
        drop(node);
        assert!(Node::open(&path).is_err());
        drop(physical);
        assert!(Node::open(&path).is_ok());
    }

    #[test]
    fn malformed_root_or_lock_cannot_authorize_access() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("node");
        let node = Node::create(&path).unwrap();
        let alias = temporary.path().join("alias");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(Node::open(&alias).is_err());
        assert!(Node::create(&alias.join("child")).is_err());
        assert!(!path.join("child").exists());
        fs_remove_lock(&path);
        File::create(path.join(LOCK_NAME)).unwrap();
        assert!(node.validate().is_err());
        assert!(node.active().is_err());
    }

    fn fs_remove_lock(path: &Path) {
        std::fs::remove_file(path.join(LOCK_NAME)).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_offline_waiter_keeps_exclusion_through_actual_blocking_copy() {
        let temporary = tempfile::tempdir().unwrap();
        let node = Node::create(&temporary.path().join("node")).unwrap();
        let offline = node.offline().unwrap();
        let (started, entered) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new(Barrier::new(2));
        let (finished, done) = tokio::sync::oneshot::channel();
        let handle = tokio::task::spawn_blocking({
            let release = release.clone();
            move || {
                started.send(()).unwrap();
                release.wait();
                offline.validate().unwrap();
                drop(offline);
                let _ = finished.send(());
            }
        });
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        handle.abort(); // Already running work retains the proof until the real copy ends.
        let active_blocked = node.active().is_err();
        let second_blocked = Node::open(node.root()).is_err();
        release.wait();
        done.await.unwrap();
        handle.await.unwrap();
        assert!(active_blocked && second_blocked);
        assert!(node.active().is_ok());
    }
}
