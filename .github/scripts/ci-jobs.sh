#!/usr/bin/env bash
set -euo pipefail

case "${1:?CI task required}" in
  storage-lab)
    chmod +x target/debug/cairn
    shellcheck -s sh conformance/storage_lab/run.sh
    cargo fmt --manifest-path conformance/storage_lab/Cargo.toml --check
    cargo clippy --locked --manifest-path conformance/storage_lab/Cargo.toml --all-targets -- -D warnings
    cargo test --locked --manifest-path conformance/storage_lab/Cargo.toml
    cargo build --locked --manifest-path conformance/storage_lab/Cargo.toml --bins
    LAB_TEST_SERVER="$PWD/target/debug/cairn" LAB_TEST_RECOVERY_SERVER="$PWD/target/debug/cairn" LAB_TEST_FANOUT_DRIVER="$PWD/conformance/storage_lab/target/debug/cairn-fanout-lab" LAB_TEST_PACKING_DRIVER="$PWD/conformance/storage_lab/target/debug/cairn-packing-lab" LAB_TEST_METADATA_DRIVER="$PWD/conformance/storage_lab/target/debug/cairn-metadata-capacity-lab" LAB_TEST_METADATA_COMPARISON_DRIVER="$PWD/conformance/storage_lab/target/debug/cairn-metadata-comparison-lab" python3 -m unittest discover -s conformance/storage_lab -p 'test_*.py' -v
    ;;
  rust-audit-1)
    set -euo pipefail
    archive="${RUNNER_TEMP}/cargo-audit-v0.22.2.tgz"
    curl --proto '=https' --tlsv1.2 -fsSL -o "$archive" "$AUDIT_URL"
    printf '%s  %s\n' "$AUDIT_SHA256" "$archive" | sha256sum -c -
    tar -xzf "$archive" -C "$RUNNER_TEMP"
    ;;
  rust-audit-2)
    "${RUNNER_TEMP}/cargo-audit-x86_64-unknown-linux-musl-v0.22.2/cargo-audit" audit
    "${RUNNER_TEMP}/cargo-audit-x86_64-unknown-linux-musl-v0.22.2/cargo-audit" audit --file conformance/storage_lab/Cargo.lock
    ;;
  workspace-tests)
    EXTRA=()
    case "$TARGET" in
      *musl) EXTRA=(--exclude cairn-meta-async) ;;
    esac
    cargo nextest run --workspace "${EXTRA[@]}" --target "$TARGET"
    if [ "$TARGET" = "x86_64-unknown-linux-gnu" ]; then
      cargo test --workspace --doc
    fi
    ;;
  fuzz-smoke)
    (cd crates/cairn-protocol   && cargo fuzz run chunked_decoder  -- -max_total_time=45)
    (cd crates/cairn-xml  && cargo fuzz run request_parsers  -- -max_total_time=30)
    (cd crates/cairn-authz && cargo fuzz run parse_policy    -- -max_total_time=30)
    (cd crates/cairn-blob  && cargo fuzz run compress_reader -- -max_total_time=45)
    (cd crates/cairn-types && cargo fuzz run parse_ids       -- -max_total_time=30)
    ;;
  cleanup)
    if [ "$IS_FORK" = "true" ]; then
      echo "Fork artifacts expire after one day."
      exit 0
    fi
    for name in cairn-bin web-dist; do
      id=$(gh api "/repos/${REPOSITORY}/actions/runs/${RUN_ID}/artifacts" --jq ".artifacts[] | select(.name==\"${name}\") | .id")
      if [ -n "$id" ]; then
        gh api -X DELETE "/repos/${REPOSITORY}/actions/artifacts/$id"
      fi
    done
    ;;
  actionlint)
    archive="${RUNNER_TEMP}/actionlint.tar.gz"
    curl --proto '=https' --tlsv1.2 -fsSL -o "$archive" https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_linux_amd64.tar.gz
    printf '%s  %s\n' 8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8 "$archive" | sha256sum -c -
    tar -xzf "$archive" -C "$RUNNER_TEMP" actionlint
    "$RUNNER_TEMP/actionlint"
    ;;
  *) echo "Unknown CI task" >&2; exit 2 ;;
esac
