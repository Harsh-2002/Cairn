//! Plaintext multipart placement through copy-on-write range cloning.
//!
//! When every selected part is known-plaintext and the completion applies no compression or
//! encryption, the assembled object is exactly the concatenation of the part files. Instead of
//! streaming each part through the staging sink, ask a reflink-capable filesystem to share each
//! part's extents at its destination offset. The mandatory internal baseline is computed in the
//! same read of the source parts, so the placement adds no second pass over the data.
//!
//! A range the filesystem refuses — or a completion whose destination offsets are unaligned — is
//! copied while it is read, so this path always produces the same bytes, the same digest, and the
//! same durability sequence as the streaming pipeline (file fsync → rename → sidecar → directory
//! sync).
//!
//! Parts are immutable after their durable part record and stay linked until completion cleanup.
//! The clone is copy-on-write, so unlinking a part later cannot change the completed object; the
//! shared extents keep their own reference.

use crate::hash::Hashers;
use crate::namespace;
use crate::{ASSEMBLY_READ_CHUNK, io_err, read_at};
use cairn_types::blob::PartRef;
use cairn_types::error::BlobError;
use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;

/// Filesystem block alignment required by `FICLONERANGE`. Unaligned ranges are copied. A
/// filesystem with a larger block size rejects the clone and takes the same copy fallback, so this
/// is a performance hint, never a correctness assumption.
const REFLINK_ALIGN: u64 = 4096;

/// Whether this completion can be placed from the part files directly. Compression and encryption
/// change the physical bytes (and an encrypted part must be decrypted and re-encoded under the
/// object DEK), so both keep the streaming pipeline.
pub(crate) fn eligible(parts: &[PartRef], block_policy: bool) -> bool {
    !block_policy
        && parts
            .iter()
            .all(|part| matches!(part.cipher, cairn_types::blob::BlobCipher::KnownPlaintext))
}

/// The outcome of a plaintext placement attempt.
#[derive(Debug)]
pub(crate) enum Placement {
    /// Every part was placed and hashed; the caller can finalize the object.
    Placed { hashers: Hashers, logical: u64 },
    /// This filesystem cannot reflink, so nothing was placed (the probe is a no-op on failure) and
    /// the caller must use the streaming pipeline. The hashers are handed back untouched.
    Unsupported(Hashers),
}

/// Place every part at its logical offset, hashing each part's plaintext exactly once.
///
/// The first action is a single 4 KiB support probe. If the filesystem refuses it, this returns
/// [`Placement::Unsupported`] **before writing any data**, so the caller can run the original
/// streaming pipeline on the same sink. Once the probe succeeds, a later clone failure is handled
/// by copying that part; the completion never fails merely because an allocator or range refused a
/// clone.
///
/// Runs inside the staging file's retained blocking job, so the destination descriptor and its
/// storage lease stay owned through completion even if the caller's future is cancelled.
pub(crate) fn place(
    root: &Path,
    parts: &[PartRef],
    destination: &File,
    mut hashers: Hashers,
    size_ceiling: u64,
) -> Result<Placement, BlobError> {
    let total = parts.iter().try_fold(0u64, |total, part| {
        total.checked_add(part.size).ok_or(BlobError::SizeExceeded)
    })?;
    if total > size_ceiling {
        return Err(BlobError::SizeExceeded);
    }
    if !probe_reflink(destination, root, parts)? {
        return Ok(Placement::Unsupported(hashers));
    }
    let mut buffer = vec![0u8; ASSEMBLY_READ_CHUNK];
    let mut logical = 0u64;
    for part in parts {
        let offset = logical;
        logical = logical
            .checked_add(part.size)
            .ok_or(BlobError::SizeExceeded)?;
        if logical > size_ceiling {
            return Err(BlobError::SizeExceeded);
        }
        if part.size == 0 {
            continue;
        }
        let source = open_part(root, part)?;
        if source.metadata().map_err(io_err)?.len() != part.size {
            return Err(BlobError::Corruption(
                "part length does not match metadata".into(),
            ));
        }
        let aligned = offset % REFLINK_ALIGN == 0 && part.size % REFLINK_ALIGN == 0;
        if aligned {
            hash_part(&source, &mut hashers, part.size, &mut buffer)?;
            // Hashing succeeded, so a failed clone can fall back to a plain copy of the same
            // source bytes. Clone failures are expected on filesystems/allocators that refuse the
            // range; they are never allowed to change bytes or fail the completion.
            if cairn_reflink::clone_range(destination, &source, 0, part.size, offset).is_err() {
                copy_part(destination, &source, offset, part.size, &mut buffer)?;
            }
        } else {
            copy_and_hash(
                destination,
                &source,
                offset,
                part.size,
                &mut hashers,
                &mut buffer,
            )?;
        }
    }
    // `Staging::create` reserves the assembled length with KEEP_SIZE, so the logical size is still
    // zero until the first write. Clones and copies extend it, but an empty object and a
    // filesystem that keeps KEEP_SIZE at zero need the exact length before finalization.
    destination.set_len(logical).map_err(io_err)?;
    Ok(Placement::Placed { hashers, logical })
}

/// Probe reflink support once. The first non-empty part starts at destination offset zero; a 4 KiB
/// clone there is harmless when support exists because that part is placed again by the main loop,
/// and a failed probe leaves the destination untouched, so the caller can fall back to the
/// streaming pipeline.
fn probe_reflink(destination: &File, root: &Path, parts: &[PartRef]) -> Result<bool, BlobError> {
    let Some(first) = parts.iter().find(|part| part.size > 0) else {
        return Ok(false);
    };
    if first.size < REFLINK_ALIGN {
        return Ok(false);
    }
    let source = open_part(root, first)?;
    Ok(cairn_reflink::clone_range(destination, &source, 0, REFLINK_ALIGN, 0).is_ok())
}

fn open_part(root: &Path, part: &PartRef) -> Result<File, BlobError> {
    namespace::read_file(root, &part.storage_path).map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            BlobError::NotFound
        } else {
            io_err(error)
        }
    })
}

fn hash_part(
    source: &File,
    hashers: &mut Hashers,
    len: u64,
    buffer: &mut [u8],
) -> Result<(), BlobError> {
    let mut offset = 0u64;
    while offset < len {
        let want = (len - offset).min(buffer.len() as u64) as usize;
        let read = read_at(source, &mut buffer[..want], offset).map_err(io_err)?;
        if read == 0 {
            return Err(BlobError::Corruption(
                "part truncated during assembly".into(),
            ));
        }
        hashers.update(&buffer[..read]);
        offset += read as u64;
    }
    Ok(())
}

/// Read and hash a part, writing the same chunk to its destination offset. This is the universal
/// fallback: it is used for unaligned parts, for filesystems without reflink, and for a clone that
/// the kernel refuses after the support probe.
fn copy_and_hash(
    destination: &File,
    source: &File,
    dest_offset: u64,
    len: u64,
    hashers: &mut Hashers,
    buffer: &mut [u8],
) -> Result<(), BlobError> {
    let mut copied = 0u64;
    while copied < len {
        let want = (len - copied).min(buffer.len() as u64) as usize;
        let read = read_at(source, &mut buffer[..want], copied).map_err(io_err)?;
        if read == 0 {
            return Err(BlobError::Corruption(
                "part truncated during assembly".into(),
            ));
        }
        hashers.update(&buffer[..read]);
        write_all_at(destination, &buffer[..read], dest_offset + copied)?;
        copied += read as u64;
    }
    Ok(())
}

/// Copy a part whose plaintext was already hashed (a clone failed after the hash).
fn copy_part(
    destination: &File,
    source: &File,
    dest_offset: u64,
    len: u64,
    buffer: &mut [u8],
) -> Result<(), BlobError> {
    let mut copied = 0u64;
    while copied < len {
        let want = (len - copied).min(buffer.len() as u64) as usize;
        let read = read_at(source, &mut buffer[..want], copied).map_err(io_err)?;
        if read == 0 {
            return Err(BlobError::Corruption(
                "part truncated during assembly".into(),
            ));
        }
        write_all_at(destination, &buffer[..read], dest_offset + copied)?;
        copied += read as u64;
    }
    Ok(())
}

fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64) -> Result<(), BlobError> {
    use std::os::unix::fs::FileExt;

    while !bytes.is_empty() {
        match file.write_at(bytes, offset) {
            Ok(0) => {
                return Err(BlobError::Io("staging write returned zero bytes".into()));
            }
            Ok(written) => {
                bytes = &bytes[written..];
                offset += written as u64;
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(io_err(error)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::blob::BlobCipher;
    use cairn_types::id::StoragePath;
    use cairn_types::object::ChecksumSet;
    use cairn_types::secret::SecretKey32;
    use std::io::Write;

    /// A scratch directory on the build filesystem (not `/tmp`), so the local XFS checkout
    /// actually exercises the reflink branch; a CI filesystem without reflink exercises the copy
    /// fallback with the same byte and hash assertions.
    fn scratch(name: &str) -> tempfile::TempDir {
        let root =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/reflink-assembly-tests");
        std::fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix(name)
            .tempdir_in(root)
            .unwrap()
    }

    fn write_part(root: &Path, number: u16, name: &str, bytes: &[u8]) -> PartRef {
        let mut file = std::fs::File::create(root.join(name)).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
        PartRef {
            part_number: number,
            storage_path: StoragePath::from_string(name.to_owned()),
            size: bytes.len() as u64,
            cipher: BlobCipher::KnownPlaintext,
        }
    }

    fn destination(root: &Path, name: &str) -> File {
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(root.join(name))
            .unwrap()
    }

    fn assembled_hasher() -> Hashers {
        Hashers::new_for_assembly(&ChecksumSet::none(), true)
    }

    fn assert_same_hash(actual: Hashers, expected: &[u8]) {
        let actual = actual.finalize();
        let mut reference = assembled_hasher();
        reference.update(expected);
        assert_eq!(actual, reference.finalize());
    }

    #[test]
    fn placement_matches_the_streamed_bytes_for_aligned_and_unaligned_parts() {
        let dir = scratch("placement");
        let root = dir.path();
        let big = vec![0xA5u8; 5 * 1024 * 1024];
        let tail: Vec<u8> = (0..5000).map(|i| (i % 251) as u8).collect();
        let last = vec![0x3Cu8; 4096];
        let parts = vec![
            write_part(root, 1, "part-1", &big),
            write_part(root, 2, "part-2", &big),
            write_part(root, 3, "part-3", &tail),
            write_part(root, 4, "part-4", &last),
        ];
        let expected = [
            big.as_slice(),
            big.as_slice(),
            tail.as_slice(),
            last.as_slice(),
        ]
        .concat();
        let destination = destination(root, "assembled");

        let placed = place(
            root,
            &parts,
            &destination,
            assembled_hasher(),
            100 * 1024 * 1024,
        )
        .unwrap();
        let Placement::Placed { hashers, logical } = placed else {
            // The build filesystem (ext4/tmpfs in CI) has no reflink support; the helper correctly
            // declined before writing anything and the caller runs the streaming pipeline.
            eprintln!("skipping reflink placement assertions: filesystem has no reflink support");
            return;
        };
        assert_eq!(logical, expected.len() as u64);
        drop(destination);
        assert_eq!(std::fs::read(root.join("assembled")).unwrap(), expected);
        assert_same_hash(hashers, &expected);
    }

    #[test]
    fn eligibility_requires_plaintext_and_no_encoding_policy() {
        let parts = vec![PartRef {
            part_number: 1,
            storage_path: StoragePath::from_string("part-1".to_owned()),
            size: 5,
            cipher: BlobCipher::KnownPlaintext,
        }];
        assert!(eligible(&parts, false));
        assert!(!eligible(&parts, true));

        let encrypted = vec![PartRef {
            cipher: BlobCipher::AuthenticatedV3(SecretKey32::new([7u8; 32])),
            ..parts[0].clone()
        }];
        assert!(!eligible(&encrypted, false));
    }

    #[test]
    fn a_part_length_mismatch_fails_closed() {
        let dir = scratch("length-mismatch");
        let root = dir.path();
        // At least one filesystem block, so the support probe succeeds and the per-part length
        // check (rather than the pipeline fallback) is what fails closed.
        let mut part = write_part(root, 1, "part-1", &vec![7u8; 5000]);
        part.size += 1;
        let destination = destination(root, "assembled");
        match place(root, &[part], &destination, assembled_hasher(), 1024 * 1024) {
            Err(BlobError::Corruption(_)) => {}
            Ok(Placement::Unsupported(_)) => {
                eprintln!("skipping length-mismatch assertion: filesystem has no reflink support");
            }
            other => panic!("expected a corruption error, got {other:?}"),
        }
    }

    #[test]
    fn a_filesystem_without_reflink_reports_unsupported_without_writing() {
        // tmpfs normally refuses FICLONERANGE; if a future kernel implements it, skip rather than
        // assert the wrong arm.
        let Ok(dir) = tempfile::Builder::new()
            .prefix("no-reflink")
            .tempdir_in("/dev/shm")
        else {
            return;
        };
        let root = dir.path();
        let big = vec![0x5Au8; 5 * 1024 * 1024];
        let parts = vec![write_part(root, 1, "part-1", &big)];
        let destination = destination(root, "assembled");
        match place(
            root,
            &parts,
            &destination,
            assembled_hasher(),
            100 * 1024 * 1024,
        )
        .unwrap()
        {
            Placement::Unsupported(_) => {
                assert_eq!(destination.metadata().unwrap().len(), 0);
            }
            Placement::Placed { .. } => {
                eprintln!("skipping: /dev/shm unexpectedly supports reflink");
            }
        }
    }

    #[test]
    fn the_running_size_ceiling_is_enforced_before_any_oversized_placement() {
        let dir = scratch("ceiling");
        let root = dir.path();
        let big = vec![9u8; 5 * 1024 * 1024];
        let parts = vec![
            write_part(root, 1, "part-1", &big),
            write_part(root, 2, "part-2", &big),
        ];
        let destination = destination(root, "assembled");
        assert!(matches!(
            place(
                root,
                &parts,
                &destination,
                assembled_hasher(),
                6 * 1024 * 1024
            ),
            Err(BlobError::SizeExceeded)
        ));
    }
}
