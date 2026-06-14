//! Safe file-placement hints for the write fast path (ARCH §7.5): preallocation and access advice
//! through `rustix`, whose API borrows an `AsFd` so the crate keeps `#![forbid(unsafe_code)]`.
//!
//! Every call is **best-effort**: a filesystem that does not support a hint (tmpfs, some network
//! filesystems) returns an error that is deliberately ignored — a placement hint must never fail a
//! write. On non-Unix targets the functions compile to no-ops.

#![allow(unused_variables)]

use std::fs::File;

/// Objects at or above this size get preallocation + access advice. Below it the fixed syscall cost
/// outweighs the benefit for a tiny write, so the hints are skipped.
pub(crate) const HINT_THRESHOLD: u64 = 1 << 20; // 1 MiB

/// Reserve `len` bytes of blocks for `file` (without changing its logical size) and advise the
/// kernel that access will be sequential. Reserving up front lets the filesystem place the file
/// contiguously and surfaces an out-of-space condition **immediately and cleanly** rather than
/// partway through the streamed write (ARCH §7.5). `KEEP_SIZE` keeps the file's reported length
/// tracking the bytes actually written, so a short body never leaves a padded blob.
#[cfg(unix)]
pub(crate) fn preallocate_sequential(file: &File, len: u64) {
    use rustix::fs::{Advice, FallocateFlags, fadvise, fallocate};
    let _ = fallocate(file, FallocateFlags::KEEP_SIZE, 0, len);
    // `fadvise` takes the length as `Option<NonZeroU64>` (None = to end of file).
    let _ = fadvise(file, 0, std::num::NonZeroU64::new(len), Advice::Sequential);
}

#[cfg(not(unix))]
pub(crate) fn preallocate_sequential(file: &File, len: u64) {}

/// Advise the kernel that the just-written pages of `file` are no longer needed, so a stream of
/// large write-once uploads does not evict the page cache that hot reads depend on (ARCH §7.5).
/// Called after the data is fsynced. Best-effort.
#[cfg(unix)]
pub(crate) fn release_pages(file: &File, len: u64) {
    use rustix::fs::{Advice, fadvise};
    let _ = fadvise(file, 0, std::num::NonZeroU64::new(len), Advice::DontNeed);
}

#[cfg(not(unix))]
pub(crate) fn release_pages(file: &File, len: u64) {}
