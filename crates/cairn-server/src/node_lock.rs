//! Process-level exclusion for commands that directly access node-local state.
//!
//! SQLite's locks protect the database, but not the blob tree. Every Cairn node-local command
//! therefore cooperates on two advisory locks: one for the data root and one for the configured
//! database identity. Holding both makes `backup`/`restore` explicitly offline relative to
//! `serve`, while also preventing two differently-configured processes from sharing only one half
//! of the store.

use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::path::Path;

const DATA_LOCK_NAME: &str = ".cairn-data.lock";
const DB_LOCK_SUFFIX: &str = ".cairn-db.lock";

/// Retained ownership of all advisory node locks.
#[derive(Debug)]
pub(crate) struct NodeLock {
    _files: Vec<File>,
}

impl NodeLock {
    /// Acquire the data-root and database-identity locks without waiting.
    ///
    /// A busy lock is an operator action, not transient request traffic: the caller reports it and
    /// exits instead of waiting behind a live server for an unbounded time.
    pub(crate) fn acquire(data_dir: &Path, db_path: &Path) -> io::Result<Self> {
        reject_symlink(data_dir, "data directory")?;
        reject_symlink(db_path, "database")?;
        let db_parent = usable_parent(db_path);
        std::fs::create_dir_all(data_dir)?;
        std::fs::create_dir_all(db_parent)?;

        let canonical_data = std::fs::canonicalize(data_dir)?;
        let canonical_db_parent = std::fs::canonicalize(db_parent)?;
        if canonical_db_parent != canonical_data && canonical_db_parent.starts_with(&canonical_data)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "database path {} is nested below data directory {}; place it directly in the \
                     data directory or outside it",
                    db_path.display(),
                    data_dir.display()
                ),
            ));
        }

        let configured_name = db_path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("database path has no file name: {}", db_path.display()),
            )
        })?;
        let (canonical_db_parent, db_name) = if db_path.exists() {
            let metadata = std::fs::symlink_metadata(db_path)?;
            if !metadata.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("database path is not a regular file: {}", db_path.display()),
                ));
            }
            reject_hard_linked_database(db_path, &metadata)?;
            let canonical_db = std::fs::canonicalize(db_path)?;
            let parent = canonical_db
                .parent()
                .ok_or_else(|| io::Error::other("canonical database path has no parent"))?
                .to_owned();
            let name = canonical_db
                .file_name()
                .ok_or_else(|| io::Error::other("canonical database path has no file name"))?
                .to_owned();
            (parent, name)
        } else {
            (canonical_db_parent, configured_name.to_owned())
        };

        let mut db_lock_name = OsString::from(".");
        db_lock_name.push(&db_name);
        db_lock_name.push(DB_LOCK_SUFFIX);

        let mut paths = vec![
            canonical_data.join(DATA_LOCK_NAME),
            canonical_db_parent.join(db_lock_name),
        ];
        paths.sort();
        paths.dedup();

        let mut files = Vec::with_capacity(paths.len());
        for path in paths {
            let file = cairn_blob::open_lock_file_nofollow(&path).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "failed to open node-local lock without following symlinks {}: {error}",
                        path.display()
                    ),
                )
            })?;
            cairn_blob::try_lock_exclusive(&file).map_err(|error| {
                if error.kind() == io::ErrorKind::WouldBlock {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!(
                            "node-local state is already in use (lock {})",
                            path.display()
                        ),
                    )
                } else {
                    io::Error::new(
                        error.kind(),
                        format!(
                            "failed to lock node-local state {}: {error}",
                            path.display()
                        ),
                    )
                }
            })?;
            files.push(file);
        }

        Ok(Self { _files: files })
    }
}

fn reject_symlink(path: &Path, label: &str) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{label} must not be a symlink: {}", path.display()),
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn reject_hard_linked_database(path: &Path, metadata: &std::fs::Metadata) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    if metadata.nlink() > 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "database file has {} hard links and therefore has an ambiguous lock identity: {}",
                metadata.nlink(),
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn reject_hard_linked_database(_path: &Path, _metadata: &std::fs::Metadata) -> io::Result<()> {
    Ok(())
}

fn usable_parent(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

pub(crate) fn is_lock_file_name(name: &str) -> bool {
    name == DATA_LOCK_NAME || (name.starts_with('.') && name.ends_with(DB_LOCK_SUFFIX))
}

#[cfg(test)]
mod tests {
    use super::NodeLock;
    use std::io::ErrorKind;

    #[test]
    fn second_node_is_rejected_and_lock_releases_on_drop() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let db = root.path().join("meta/cairn.db");

        let first = NodeLock::acquire(&data, &db).unwrap();
        let error = NodeLock::acquire(&data, &db).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::WouldBlock);
        assert!(error.to_string().contains("already in use"));

        drop(first);
        NodeLock::acquire(&data, &db).expect("dropping the owner releases both locks");
    }

    #[test]
    fn sharing_either_data_or_database_conflicts() {
        let root = tempfile::tempdir().unwrap();
        let data_a = root.path().join("data-a");
        let data_b = root.path().join("data-b");
        let db_a = root.path().join("meta/a.db");
        let db_b = root.path().join("meta/b.db");

        let first = NodeLock::acquire(&data_a, &db_a).unwrap();
        assert_eq!(
            NodeLock::acquire(&data_a, &db_b).unwrap_err().kind(),
            ErrorKind::WouldBlock,
            "a shared blob root must conflict even with another database"
        );
        assert_eq!(
            NodeLock::acquire(&data_b, &db_a).unwrap_err().kind(),
            ErrorKind::WouldBlock,
            "a shared database must conflict even with another blob root"
        );
        drop(first);
    }

    #[test]
    fn nested_database_directory_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let error = NodeLock::acquire(&data, &data.join("metadata/cairn.db")).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.to_string().contains("nested below"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn database_symlink_and_parent_alias_cannot_split_the_lock_identity() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let real_parent = root.path().join("metadata");
        std::fs::create_dir_all(&real_parent).unwrap();
        let real_db = real_parent.join("cairn.db");
        std::fs::write(&real_db, b"sqlite placeholder").unwrap();

        let db_symlink = root.path().join("db-link");
        symlink(&real_db, &db_symlink).unwrap();
        let error = NodeLock::acquire(&root.path().join("data-a"), &db_symlink).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("must not be a symlink"),
            "{error}"
        );

        let parent_alias = root.path().join("metadata-alias");
        symlink(&real_parent, &parent_alias).unwrap();
        let first = NodeLock::acquire(&root.path().join("data-a"), &real_db).unwrap();
        let error = NodeLock::acquire(&root.path().join("data-b"), &parent_alias.join("cairn.db"))
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::WouldBlock);
        drop(first);
    }

    #[cfg(unix)]
    #[test]
    fn hard_linked_database_is_rejected_as_an_ambiguous_identity() {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("cairn.db");
        let alias = root.path().join("cairn-alias.db");
        std::fs::write(&database, b"sqlite placeholder").unwrap();
        std::fs::hard_link(&database, &alias).unwrap();

        let error = NodeLock::acquire(&root.path().join("data"), &database).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.to_string().contains("hard links"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_lock_file_is_rejected_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let meta = root.path().join("meta");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&meta).unwrap();
        let victim = root.path().join("victim");
        std::fs::write(&victim, b"unchanged").unwrap();
        symlink(&victim, data.join(".cairn-data.lock")).unwrap();

        let error = NodeLock::acquire(&data, &meta.join("cairn.db")).unwrap_err();
        assert_ne!(error.kind(), ErrorKind::WouldBlock);
        assert_eq!(std::fs::read(&victim).unwrap(), b"unchanged");
    }
}
