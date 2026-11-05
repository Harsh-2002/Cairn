//! Integration coverage for the reflink seam.
//!
//! The success path is exercised only on a filesystem that implements `FICLONERANGE` (XFS or
//! btrfs); ext4 and tmpfs return an unsupported error and the test skips. The failure and
//! copy-on-write semantics are what the storage engine relies on, so they are asserted whenever a
//! clone succeeds.
//!
//! `CAIRN_REFLINK_TEST_DIR` places the scratch files on a chosen filesystem, and
//! `CAIRN_REQUIRE_REFLINK=1` turns every skip into a failure so a dedicated CI job cannot silently
//! pass on a filesystem that cannot clone.

use cairn_reflink::clone_range;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const BLOCK: usize = 4096;

fn scratch(name: &str) -> std::path::PathBuf {
    let root = match std::env::var_os("CAIRN_REFLINK_TEST_DIR") {
        Some(dir) => std::path::PathBuf::from(dir).join("reflink-tests"),
        None => Path::new(env!("CARGO_TARGET_TMPDIR")).join("reflink-tests"),
    };
    std::fs::create_dir_all(&root).unwrap();
    let dir = root.join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).unwrap();
    }
    std::fs::create_dir(&dir).unwrap();
    dir
}

fn write_source(path: &Path, len: usize, seed: u8) -> File {
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let bytes: Vec<u8> = (0..len).map(|i| seed.wrapping_add(i as u8)).collect();
    file.write_all(&bytes).unwrap();
    file
}

fn read_all(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap()
}

/// True only for errnos that mean "this filesystem/range cannot reflink", so a genuine kernel or
/// alignment failure on a supported filesystem still fails the test.
fn require_reflink() -> bool {
    std::env::var("CAIRN_REQUIRE_REFLINK").is_ok_and(|value| value == "1")
}

/// Record a skipped success-path assertion, or fail when the job requires a reflink filesystem.
fn skip(error: &std::io::Error, what: &str) {
    assert!(unavailable(error), "{what} failed unexpectedly: {error}");
    assert!(
        !require_reflink(),
        "CAIRN_REQUIRE_REFLINK=1 but {what} found no reflink support: {error}"
    );
    eprintln!("skipping reflink {what} assertions: {error}");
}

fn unavailable(error: &std::io::Error) -> bool {
    if !cairn_reflink::is_supported() {
        return true;
    }
    // EOPNOTSUPP=95, ENOTTY=25, ENOSYS=38, EXDEV=18.
    matches!(error.raw_os_error(), Some(95 | 25 | 38 | 18))
}

#[test]
fn clone_shares_bytes_and_isolates_later_writes() {
    let dir = scratch("cow");
    let src_path = dir.join("src");
    let dst_path = dir.join("dst");
    let src = write_source(&src_path, 5 * 1024 * 1024, 11);
    let dst = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&dst_path)
        .unwrap();

    if let Err(error) = clone_range(&dst, &src, 0, 5 * 1024 * 1024, 0) {
        skip(&error, "copy-on-write isolation");
        return;
    }
    dst.set_len(5 * 1024 * 1024).unwrap();
    dst.sync_data().unwrap();
    assert_eq!(read_all(&dst_path), read_all(&src_path));

    // Copy-on-write isolation: changing either side must not change the other.
    let mut src_writer = OpenOptions::new().write(true).open(&src_path).unwrap();
    src_writer.seek(SeekFrom::Start(0)).unwrap();
    src_writer.write_all(&[0xEE; BLOCK]).unwrap();
    src_writer.sync_data().unwrap();
    let mut dst_reader = File::open(&dst_path).unwrap();
    let mut after_source_write = vec![0; BLOCK];
    dst_reader.read_exact(&mut after_source_write).unwrap();
    assert_ne!(after_source_write, vec![0xEE; BLOCK]);

    let mut dst_writer = OpenOptions::new().write(true).open(&dst_path).unwrap();
    dst_writer.seek(SeekFrom::Start(BLOCK as u64)).unwrap();
    dst_writer.write_all(&[0xDD; BLOCK]).unwrap();
    dst_writer.sync_data().unwrap();
    let mut src_reader = File::open(&src_path).unwrap();
    src_reader.seek(SeekFrom::Start(BLOCK as u64)).unwrap();
    let mut after_dest_write = vec![0; BLOCK];
    src_reader.read_exact(&mut after_dest_write).unwrap();
    assert_ne!(after_dest_write, vec![0xDD; BLOCK]);
}

#[test]
fn clone_multiple_ranges_preserves_concatenation() {
    let dir = scratch("concat");
    let dst_path = dir.join("dst");
    let dst = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&dst_path)
        .unwrap();

    let mut expected = Vec::new();
    let mut sources = Vec::new();
    let mut first_error = None;
    for index in 0..4u8 {
        let path = dir.join(format!("part-{index}"));
        let file = write_source(&path, 1024 * 1024, index * 17);
        if let Err(error) = clone_range(&dst, &file, 0, 1024 * 1024, index as u64 * 1024 * 1024) {
            first_error = Some(error);
            break;
        }
        expected.extend_from_slice(&read_all(&path));
        sources.push(file);
    }
    if let Some(error) = first_error {
        skip(&error, "concatenation");
        return;
    }
    dst.set_len(expected.len() as u64).unwrap();
    dst.sync_data().unwrap();
    assert_eq!(read_all(&dst_path), expected);
}

#[test]
fn a_range_past_the_source_end_is_an_error_not_a_short_clone() {
    let dir = scratch("bounds");
    let src_path = dir.join("src");
    let dst_path = dir.join("dst");
    let src = write_source(&src_path, 2 * BLOCK, 3);
    let dst = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&dst_path)
        .unwrap();

    // Probe support with a valid aligned range first.
    if let Err(error) = clone_range(&dst, &src, 0, BLOCK as u64, 0) {
        skip(&error, "bounds");
        return;
    }
    assert!(
        clone_range(&dst, &src, 0, 4 * BLOCK as u64, 0).is_err(),
        "a clone past EOF must fail rather than silently short-copy"
    );
}
