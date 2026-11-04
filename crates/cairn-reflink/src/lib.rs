//! `cairn-reflink` — the one isolated unsafe seam for copy-on-write range cloning.
//!
//! Every other Cairn crate keeps `forbid(unsafe_code)`. The only operation the storage engine
//! needs that has no safe standard-library wrapper is `ioctl(FICLONERANGE)`: ask a reflink-capable
//! filesystem to share an extent range between two open files. Keeping it in this two-function
//! crate makes the unsafe surface auditable in one place and keeps `cairn-blob` free of `unsafe`.
//!
//! Cloning is an optimization, never a correctness requirement. Callers must treat every error as
//! "reflink unavailable for this range" and fall back to a byte copy: filesystems without reflink
//! return `EOPNOTSUPP`/`ENOTTY`, unaligned or overlapping ranges return `EINVAL`, and a
//! cross-device pair returns `EXDEV`.
//!
//! The clone is copy-on-write, not a hard link: writing either file afterwards does not change the
//! other one's bytes. The source must stay alive (and unmodified) until the destination is durable;
//! unlinking the source after that is safe because the shared extents keep their own reference.

#![deny(unsafe_code)]

use std::fs::File;
use std::io;

/// Whether this build can attempt a reflink at all.
pub const fn is_supported() -> bool {
    cfg!(target_os = "linux")
}

/// Share `src[src_offset .. src_offset + len]` with `dest[dest_offset .. dest_offset + len]`
/// using `FICLONERANGE`.
///
/// `src_offset`, `len` and `dest_offset` must each be aligned to the filesystem block size; a
/// caller that cannot align a range must copy it instead. `len` must be non-zero.
///
/// # Errors
///
/// Every `ioctl` failure, including an unsupported filesystem, an unaligned or overlapping range,
/// a cross-device pair, or a source shorter than the requested range. Callers must fall back to a
/// byte copy; the error kind is intentionally not classified because every failure has the same
/// safe response.
#[allow(unsafe_code)] // The one audited `ioctl` call in the workspace; see the SAFETY note.
pub fn clone_range(
    dest: &File,
    src: &File,
    src_offset: u64,
    len: u64,
    dest_offset: u64,
) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;

        let range = libc::file_clone_range {
            src_fd: src.as_raw_fd() as libc::__s64,
            src_offset: src_offset as libc::__u64,
            src_length: len as libc::__u64,
            dest_offset: dest_offset as libc::__u64,
        };
        // SAFETY: both descriptors are open and owned by the caller for the whole call, and
        // `range` is an initialised `struct file_clone_range` whose address stays valid for the
        // syscall. `FICLONERANGE` only reads that structure (it is a `_IOW`, not an output
        // buffer) and changes filesystem metadata for the caller-owned destination inode, exactly
        // the operation this function is documented to perform. The kernel validates offsets,
        // lengths, alignment and device identity and returns an error rather than touching memory.
        let result = unsafe { libc::ioctl(dest.as_raw_fd(), libc::FICLONERANGE, &range) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (dest, src, src_offset, len, dest_offset);
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
}
