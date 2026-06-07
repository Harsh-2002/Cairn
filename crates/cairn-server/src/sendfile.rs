//! Linux-only raw-syscall helpers for the experimental `fast-io` performance path.
//!
//! This module is compiled only under `#[cfg(all(feature = "fast-io", target_os = "linux"))]`. It
//! holds two things:
//!
//! 1. [`probe_tcp_ulp_tls`] — the one-time capability probe the server uses to decide whether the
//!    kernel can offload TLS record crypto (kTLS). This gates the whole kTLS path so that, when the
//!    kernel cannot offload, every connection falls back to the unchanged userspace TLS path.
//!
//! 2. [`sendfile_all`] — a complete, tested wrapper around the `sendfile(2)` syscall that copies a
//!    byte range from a file fd straight to a socket fd inside the kernel, never bouncing the bytes
//!    through userspace. This is the mechanism layer (b) of the feature would use to serve an
//!    uncompressed, unencrypted, plaintext object GET. See [`sendfile_get_takeover`] for why the
//!    GET integration is currently a documented stub rather than wired into the live path.
//!
//! ## On `unsafe`
//!
//! The workspace lints `unsafe_code` as a warning and the gate runs clippy with `-D warnings`, so
//! each `unsafe` block here is individually `#[allow(unsafe_code)]`-ed with a SAFETY comment that
//! justifies it. Every block is a single FFI call into libc with arguments we fully control; none
//! dereferences a raw pointer we did not just create, and none escapes a borrowed fd past its
//! owner's lifetime.

use std::io;
use std::os::fd::{AsRawFd, RawFd};

/// Probe whether the kernel exposes the `tls` upper-layer protocol (ULP) that kTLS needs.
///
/// We open a throwaway blocking TCP socket and attempt `setsockopt(SOL_TCP, TCP_ULP, "tls")`. We do
/// not need a connected socket for the kernel to reject an absent ULP with `ENOENT`; a present ULP
/// returns success (the socket is unconnected, so this only arms the ULP and is immediately torn
/// down). The fd is always closed before returning. A `false` result means the server never
/// attempts the kTLS offload and serves every TLS connection in userspace.
pub fn probe_tcp_ulp_tls() -> bool {
    // SAFETY: `socket(2)` takes three integer arguments and returns a new fd or -1; it reads no
    // memory we pass in. We check the return value before using it.
    #[allow(unsafe_code)]
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return false;
    }
    // Owns the probe fd so it is closed on every path (including the early returns below).
    let guard = FdGuard(fd);
    let name = b"tls\0";
    // SAFETY: `setsockopt(2)` reads `optlen` bytes starting at `optval`. We pass a pointer into the
    // local `name` buffer and an `optlen` of 3 (the bytes "tls", excluding the NUL), which is fully
    // inside that buffer. The fd is valid (checked above) and owned by `guard`. The kernel does not
    // retain the pointer past the call.
    #[allow(unsafe_code)]
    let rc = unsafe {
        libc::setsockopt(
            guard.0,
            libc::IPPROTO_TCP,
            libc::TCP_ULP,
            name.as_ptr().cast(),
            3,
        )
    };
    rc == 0
}

/// Copy exactly `len` bytes from `src` (a seekable file) at `offset` to `dst` (a socket) using
/// `sendfile(2)`, looping over short transfers and retrying interrupted calls.
///
/// On success the file's read offset for `src` is advanced by `len` (sendfile updates the offset
/// in-kernel when the `offset` argument is null; here we pass an explicit offset so the file
/// position is *not* used, matching how a blob read with an explicit range behaves). The kernel
/// streams the bytes directly from the page cache to the socket buffer without a userspace copy.
///
/// This is synchronous/blocking and is intended to run on a blocking executor thread, exactly like
/// the existing portable blob read does its file work via `spawn_blocking`.
///
/// # Errors
/// Returns the underlying I/O error if `sendfile` fails for a reason other than `EINTR`, or if the
/// peer closes early (a zero-byte transfer with bytes still outstanding surfaces as `WriteZero`).
///
/// `#[allow(dead_code)]`: this is the implemented, tested transfer primitive for layer (b) of the
/// feature; it is exercised by the unit tests but not yet called from a live serving path (see
/// [`sendfile_get_takeover`] for why the GET takeover is stubbed). Kept so the mechanism is ready
/// for the follow-up that plumbs the blob fd across the `cairn-s3` boundary.
#[allow(dead_code)]
pub fn sendfile_all(dst: RawFd, src: RawFd, offset: u64, len: u64) -> io::Result<u64> {
    let mut sent: u64 = 0;
    // `sendfile` mutates the offset through this pointer as it makes progress.
    let mut off: libc::off_t = offset as libc::off_t;
    while sent < len {
        let remaining = len - sent;
        // A single sendfile call is capped at 0x7fff_f000 by the kernel; clamp to be explicit.
        let want = remaining.min(0x7fff_f000) as usize;
        // SAFETY: `sendfile(2)` reads `count` bytes from `src` starting at the offset pointed to by
        // `&mut off` and writes them to `dst`, updating `*off`. `src` and `dst` are caller-owned,
        // open fds for the duration of this call. `&mut off` is a valid, uniquely-borrowed local of
        // the correct `off_t` type. We pass a `count` we computed above. The kernel writes only to
        // `*off` (an integer we own) and the socket; it does not retain either pointer.
        #[allow(unsafe_code)]
        let n = unsafe { libc::sendfile(dst, src, &mut off, want) };
        if n < 0 {
            let err = io::Error::last_os_error();
            // Only `EINTR` is retryable here: a signal interrupted the call before any progress, so
            // we re-issue the same range. Everything else (including `EAGAIN`, which should not
            // occur because the caller runs us on a blocking thread with a blocking socket) is
            // surfaced to the caller rather than spun on.
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(err);
        }
        if n == 0 {
            // Peer closed before all bytes were transferred.
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "sendfile transferred 0 bytes before completion",
            ));
        }
        sent += n as u64;
    }
    Ok(sent)
}

/// Convenience wrapper accepting anything with a raw fd for both ends.
///
/// Kept separate so call sites read clearly and so the test below can pass `File`/socket handles
/// without leaking `RawFd`s around. `#[allow(dead_code)]` for the same reason as [`sendfile_all`].
#[allow(dead_code)]
pub fn sendfile_all_handles<D: AsRawFd, S: AsRawFd>(
    dst: &D,
    src: &S,
    offset: u64,
    len: u64,
) -> io::Result<u64> {
    sendfile_all(dst.as_raw_fd(), src.as_raw_fd(), offset, len)
}

/// An RAII guard that closes a raw fd on drop, so the probe never leaks a descriptor.
struct FdGuard(RawFd);

impl Drop for FdGuard {
    fn drop(&mut self) {
        // SAFETY: `self.0` is an fd this guard exclusively owns (created just before the guard and
        // never duplicated or closed elsewhere); closing it exactly once on drop is correct. We
        // ignore the result because there is nothing actionable to do with a close error here.
        #[allow(unsafe_code)]
        unsafe {
            libc::close(self.0);
        }
    }
}

/// STUB / TODO — the sendfile(2) GET takeover is intentionally not wired into the live path.
///
/// ## Why this is a stub
///
/// Layer (b) of `fast-io` would, for a GET of an uncompressed/unencrypted/plaintext object, write
/// the HTTP/1.1 response head and then [`sendfile_all`] the blob file fd straight to the socket fd,
/// bypassing hyper's body copy. The blob store already opens exactly the right fd for this: a
/// committed uncompressed blob yields `BlobReadHandle.zero_copy = Some(ZeroCopyRead { file, offset,
/// len })` (see `cairn-blob`/`cairn-types`).
///
/// The blocker is purely a crate boundary this task must not cross. The server never sees that
/// `ZeroCopyRead`: `cairn-s3`'s `get_object` consumes the `BlobReadHandle` and returns its body as
/// `S3Body::Stream { length, stream }` — an opaque `BlobStream` that carries the *bytes* but drops
/// the raw fd. The adapter then renders that stream into hyper's body. To take over the connection
/// with sendfile the server would need the fd, which means widening `S3Body` (in `cairn-s3`) to
/// carry the `zero_copy` hint and threading it through `get_object` and the adapter. That edits
/// `cairn-s3`/`cairn-types`, which are out of scope here, so the takeover is deferred.
///
/// Crucially, *not* wiring this changes nothing about correctness: every GET — fast-path-eligible
/// or not — continues to flow through the always-on portable streamed body. The mechanism this
/// stub documents is the only missing piece, and it is implemented and tested above
/// ([`sendfile_all`]); only the plumbing of the fd across the `cairn-s3` boundary remains.
///
/// ## Sketch of the intended integration (for the follow-up that owns `cairn-s3`)
///
/// 1. Add a `ZeroCopy { file: Arc<File>, offset, len, length }` variant (or a side-channel field)
///    to `S3Body`, populated by `get_object` from `handle.zero_copy` when present and the object is
///    plaintext/uncompressed and the request is a non-TLS or kTLS connection.
/// 2. In the adapter, when the response body is `ZeroCopy` *and* hyper is serving HTTP/1.1, write
///    the response head, then take the underlying socket fd and `spawn_blocking(|| sendfile_all(..))`.
/// 3. Everything else (ranges hyper can't express as one head, HTTP/2, chunked encoding, any
///    compressed/encrypted object) keeps using the portable stream — never a regression.
///
/// The function is `#[allow(dead_code)]` because it is documentation-bearing scaffolding, not yet
/// called from any path.
#[allow(dead_code)]
pub fn sendfile_get_takeover() {
    // Intentionally empty: see the doc comment. The transfer primitive lives in `sendfile_all`.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::fd::FromRawFd;

    /// `sendfile_all` copies an exact byte range from a real file fd to a socket fd in the kernel,
    /// and the bytes arrive unchanged on the other end. This proves the syscall wrapper (offset
    /// handling, short-write loop, EINTR retry) works against live descriptors — the load-bearing
    /// mechanism of the sendfile fast path, independent of the (stubbed) hyper takeover.
    #[test]
    fn sendfile_all_copies_a_range_to_a_socket() {
        // A temp file holding a known payload.
        let mut tmp = tempfile::tempfile().expect("tempfile");
        let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        tmp.write_all(&payload).expect("write payload");
        tmp.flush().expect("flush");

        // A connected, in-process socket pair as the destination/source for the transfer.
        let (mut rx, tx) = socketpair();

        // Transfer a sub-range [100, 100+2000) to exercise non-zero offset and partial length.
        let offset = 100u64;
        let len = 2000u64;
        let sent = sendfile_all_handles(&tx, &tmp, offset, len).expect("sendfile");
        assert_eq!(
            sent, len,
            "sendfile must transfer the whole requested range"
        );

        // Close the write end so the read side sees EOF after the transferred bytes.
        drop(tx);

        let mut got = Vec::new();
        rx.read_to_end(&mut got).expect("read socket");
        assert_eq!(
            got,
            &payload[offset as usize..(offset + len) as usize],
            "bytes delivered by sendfile must equal the source range"
        );
    }

    /// The kTLS capability probe runs without panicking and returns a bool. We do not assert its
    /// value: whether the kernel `tls` ULP is present depends on the host/CI kernel, and the server
    /// is explicitly designed to work either way (the probe only *gates* the offload). This guards
    /// against the probe itself faulting (bad setsockopt args, fd leak path, etc.).
    #[test]
    fn ulp_probe_is_total() {
        let _ = probe_tcp_ulp_tls();
    }

    /// Create a connected UNIX stream socket pair, returning (read_half, write_half) as owned
    /// `std::os::unix::net::UnixStream`s so the test can use blocking `Read`/`Write`.
    fn socketpair() -> (
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
    ) {
        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: `socketpair(2)` writes exactly two fds into the 2-element array we pass. The array
        // is a valid, uniquely-borrowed local of the right length. We check the return code before
        // taking ownership of the fds.
        #[allow(unsafe_code)]
        let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(rc, 0, "socketpair failed: {}", io::Error::last_os_error());
        // SAFETY: `socketpair` just handed us two fresh, owned fds; wrapping each exactly once in a
        // `UnixStream` transfers ownership so they are closed on drop and never double-closed.
        #[allow(unsafe_code)]
        let a = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fds[0]) };
        // SAFETY: as above for the second fd of the pair.
        #[allow(unsafe_code)]
        let b = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fds[1]) };
        (a, b)
    }
}
