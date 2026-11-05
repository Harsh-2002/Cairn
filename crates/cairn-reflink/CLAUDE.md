# cairn-reflink

The workspace's only dedicated `unsafe` crate. It exists so every other crate can keep
`forbid(unsafe_code)`: `cairn-blob` calls one safe function and never sees the ioctl.

## Layout
- `src/lib.rs` — `clone_range(dest, src, src_offset, len, dest_offset)`, the single
  `ioctl(FICLONERANGE)` wrapper, and `is_supported()`. The `SAFETY` note is the audit point: both
  descriptors are caller-owned and alive for the call, the range struct is stack-initialised and
  read-only to the kernel, and every failure returns `io::Error`.
- `tests/reflink.rs` — copy-on-write isolation, multi-range concatenation, and past-EOF rejection.
  Success arms skip on filesystems without reflink; `CAIRN_REQUIRE_REFLINK=1` turns a skip into a
  failure for the dedicated CI job, and `CAIRN_REFLINK_TEST_DIR` selects the scratch filesystem.

## Rules
- Cloning is an optimization, never a correctness requirement. Every error means "copy this range";
  do not classify or retry errors inside the wrapper, and do not remove a caller's copy fallback.
- The range contract is the caller's: aligned offsets/length, same filesystem, non-zero length.
  Keep the wrapper's documentation explicit and keep the unsafe surface to this file.
- Run the strict XFS CI job when touching the wrapper or its callers; ext4 only exercises the
  fallback. See `crates/cairn-blob/CLAUDE.md` for the shared-extent cleanup invariant.
