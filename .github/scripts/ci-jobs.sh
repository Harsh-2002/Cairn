#!/usr/bin/env bash
set -euo pipefail

case "${1:?CI task required}" in
  musl-tools)
    # musl comes from Ubuntu; unrelated runner repositories must not block its installation.
    apt_sources=(-o Dir::Etc::sourcelist=/etc/apt/sources.list.d/ubuntu.sources -o Dir::Etc::sourceparts=- -o APT::Update::Error-Mode=any)
    sudo apt-get "${apt_sources[@]}" update
    sudo apt-get "${apt_sources[@]}" install -y musl-tools=1.2.4-2
    ;;
  upgrade-drill)
    chmod +x target/debug/cairn
    mkdir -p /var/tmp/cairn-upgrade
    gh release download v2026.09.13 --repo "$GITHUB_REPOSITORY" \
      --pattern cairn-linux-amd64 --pattern SHA256SUMS \
      --dir /var/tmp/cairn-upgrade --clobber
    (cd /var/tmp/cairn-upgrade && sha256sum -c SHA256SUMS --ignore-missing && chmod +x cairn-linux-amd64)
    python3 conformance/upgrade_drill.py \
      --root /var/tmp/cairn-upgrade/run \
      --old-bin /var/tmp/cairn-upgrade/cairn-linux-amd64 \
      --new-bin target/debug/cairn --port 9090
    ;;
  console-browser)
    chmod +x target/debug/cairn
    BIN=target/debug/cairn BROWSER=1 python3 conformance/api_console.py
    ;;
  storage-lab)
    chmod +x target/debug/cairn
    shellcheck -s sh conformance/storage_lab/run.sh
    cargo fmt --manifest-path conformance/storage_lab/Cargo.toml --check
    cargo clippy --locked --manifest-path conformance/storage_lab/Cargo.toml --all-targets -- -D warnings
    cargo test --locked --manifest-path conformance/storage_lab/Cargo.toml
    cargo build --locked --manifest-path conformance/storage_lab/Cargo.toml --bins
    LAB_TEST_SERVER="$PWD/target/debug/cairn" LAB_TEST_RECOVERY_SERVER="$PWD/target/debug/cairn" LAB_TEST_FANOUT_DRIVER="$PWD/conformance/storage_lab/target/debug/cairn-fanout-lab" LAB_TEST_PACKING_DRIVER="$PWD/conformance/storage_lab/target/debug/cairn-packing-lab" LAB_TEST_METADATA_DRIVER="$PWD/conformance/storage_lab/target/debug/cairn-metadata-capacity-lab" LAB_TEST_METADATA_COMPARISON_DRIVER="$PWD/conformance/storage_lab/target/debug/cairn-metadata-comparison-lab" python3 -m unittest discover -s conformance/storage_lab -p 'test_*.py' -v
    ;;
  reflink-filesystem)
    # Hosted runners use ext4, so mount a loop XFS image with reflink to exercise the clone path.
    apt_sources=(-o Dir::Etc::sourcelist=/etc/apt/sources.list.d/ubuntu.sources -o Dir::Etc::sourceparts=- -o APT::Update::Error-Mode=any)
    sudo apt-get "${apt_sources[@]}" update
    sudo apt-get "${apt_sources[@]}" install -y xfsprogs=6.6.0-1ubuntu2.1
    image="${RUNNER_TEMP}/reflink.img"
    mount_dir="${RUNNER_TEMP}/reflink"
    truncate -s 1G "$image"
    sudo mkfs.xfs -q -m reflink=1 "$image"
    mkdir -p "$mount_dir"
    sudo mount -o loop "$image" "$mount_dir"
    trap 'sudo umount "$mount_dir"' EXIT
    sudo chown "$(id -u):$(id -g)" "$mount_dir"
    findmnt --noheadings --output FSTYPE --target "$mount_dir" | grep -qx xfs
    export CAIRN_REFLINK_TEST_DIR="$mount_dir" CAIRN_REQUIRE_REFLINK=1
    cargo test -p cairn-reflink -- --test-threads=1
    cargo test -p cairn-blob reflink -- --test-threads=1
    ;;
  rust-audit-1)
    set -euo pipefail
    name=cargo-audit-x86_64-unknown-linux-musl-v0.22.2.tgz
    archive="${RUNNER_TEMP}/${name}"
    gh release download 'cargo-audit/v0.22.2' --repo rustsec/rustsec --pattern "$name" --dir "$RUNNER_TEMP" --clobber
    printf '%s  %s\n' "$AUDIT_SHA256" "$archive" | sha256sum -c -
    tar -xzf "$archive" -C "$RUNNER_TEMP"
    ;;
  rust-audit-2)
    audit="${RUNNER_TEMP}/cargo-audit-x86_64-unknown-linux-musl-v0.22.2/cargo-audit"
    "$audit" audit
    "$audit" audit --file conformance/storage_lab/Cargo.lock
    # Fuzz crates detach into their own workspaces, so their lockfiles need explicit coverage.
    for lock in crates/*/fuzz/Cargo.lock; do
      "$audit" audit --file "$lock"
    done
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
  codeql-bundle)
    set -euo pipefail
    name=codeql-bundle-linux64.tar.zst
    archive="${RUNNER_TEMP}/${name}"
    gh release download codeql-bundle-v2.26.4 --repo github/codeql-action --pattern "$name" --dir "$RUNNER_TEMP" --clobber
    printf '%s  %s\n' a9872c9075f85374a8d03546263c6dc01fde50a29baea9fc08dcc6b25cc2efd5 "$archive" | sha256sum -c -
    ;;
  actionlint)
    # GitHub's unauthenticated release-asset CDN has been returning intermittent 504s; the
    # authenticated release API is both pinned by this checksum and independent of that path.
    name=actionlint_1.7.12_linux_amd64.tar.gz
    archive="${RUNNER_TEMP}/${name}"
    gh release download v1.7.12 --repo rhysd/actionlint --pattern "$name" --dir "$RUNNER_TEMP" --clobber
    printf '%s  %s\n' 8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8 "$archive" | sha256sum -c -
    tar -xzf "$archive" -C "$RUNNER_TEMP" actionlint
    "$RUNNER_TEMP/actionlint"
    ;;
  *) echo "Unknown CI task" >&2; exit 2 ;;
esac
