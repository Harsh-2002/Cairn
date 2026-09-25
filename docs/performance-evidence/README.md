# Performance evidence

The dated subdirectories hold compact, non-secret JSON reports retained by the
performance decision records in `docs/`. These are not temporary server stores or
build artifacts: the reports link to the exact files and record their SHA-256 hashes
so that measured results and rejected candidates remain auditable.

Keep new raw evidence here rather than adding more JSON files to the `docs/` root.
Remove task-owned binaries, databases, logs, and generated build output after a
campaign; do not remove a referenced report merely because its candidate was
rejected.
