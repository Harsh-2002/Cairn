#!/usr/bin/env python3
"""Deterministic release-workflow trust-boundary policy.

This intentionally uses only the Python standard library so the check can run before any
third-party package installer. It complements (rather than replaces) GitHub's workflow parser:
the assertions below encode Cairn's repository-specific release authority and immutable-input
contract.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GITHUB = ROOT / ".github"
RELEASE = GITHUB / "workflows" / "release.yml"
HEX40 = r"[0-9a-f]{40}"
SHA256 = r"sha256:[0-9a-f]{64}"

errors: list[str] = []


def fail(path: Path, line: int, message: str) -> None:
    errors.append(f"{path.relative_to(ROOT)}:{line}: {message}")


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


yaml_files = sorted(
    path
    for path in GITHUB.rglob("*")
    if path.is_file() and path.suffix in {".yml", ".yaml"}
)

# Every referenced action is immutable. Local composite actions are repository content and are
# therefore bound by github.sha; docker:// actions must use a content digest.
uses_re = re.compile(r"^\s*(?:-\s*)?uses:\s*([^\s#]+)", re.MULTILINE)
for path in yaml_files:
    text = path.read_text(encoding="utf-8")
    for match in re.finditer(r"^\s*runs-on:\s*([^\s#]+)", text, re.MULTILINE):
        if match.group(1) == "ubuntu-latest":
            fail(path, line_number(text, match.start()), "mutable ubuntu-latest runner is forbidden")
    for match in uses_re.finditer(text):
        ref = match.group(1)
        if ref.startswith("./"):
            continue
        if ref.startswith("docker://"):
            if not re.fullmatch(rf"docker://[^@\s]+@{SHA256}", ref):
                fail(path, line_number(text, match.start()), f"mutable docker action: {ref}")
            continue
        if not re.fullmatch(rf"[^@\s]+@{HEX40}", ref):
            fail(path, line_number(text, match.start()), f"action is not pinned to a full commit: {ref}")

# Docker build inputs are content-addressed, including heredoc Dockerfiles in workflows. The
# syntax frontend directive is intentionally absent because docker/dockerfile:<tag> is mutable too.
docker_sources = [ROOT / "Dockerfile", *yaml_files]
from_re = re.compile(r"^\s*FROM\s+(?:--platform=\S+\s+)?(\S+)", re.MULTILINE)
for path in docker_sources:
    text = path.read_text(encoding="utf-8")
    for match in from_re.finditer(text):
        image = match.group(1)
        if image != "scratch" and not re.fullmatch(rf"[^@\s]+@{SHA256}", image):
            fail(path, line_number(text, match.start()), f"base image is not digest-pinned: {image}")
    if path.name == "Dockerfile" and re.search(r"^#\s*syntax=", text, re.MULTILINE):
        fail(path, 1, "mutable Dockerfile frontend directives are forbidden")

for path in yaml_files:
    text = path.read_text(encoding="utf-8")
    for match in re.finditer(r"^\s*(?:container|image):\s*([^\s#]+)", text, re.MULTILINE):
        image = match.group(1)
        # `image` is also a legitimate output key; expressions are checked at their material source.
        if image.startswith("$"):
            continue
        if not re.fullmatch(rf"[^@\s]+@{SHA256}", image):
            fail(path, line_number(text, match.start()), f"container image is not digest-pinned: {image}")
    for match in re.finditer(r"driver-opts:\s*image=([^\s#]+)", text):
        if not re.fullmatch(rf"[^@\s]+@{SHA256}", match.group(1)):
            fail(path, line_number(text, match.start()), "BuildKit driver image is not digest-pinned")

# Installer inputs must resolve to an exact tool/package version. Registry locks and explicit
# wheel/archive checksums provide the content binding.
for path in [*yaml_files, ROOT / "Dockerfile"]:
    text = path.read_text(encoding="utf-8")
    for match in re.finditer(r"python-version:\s*[\"']?([^\"'\s#]+)", text):
        if not re.fullmatch(r"\d+\.\d+\.\d+", match.group(1)):
            fail(path, line_number(text, match.start()), f"non-exact Python version: {match.group(1)}")
    for match in re.finditer(r"node-version:\s*[\"']?([^\"'\s#]+)", text):
        if not re.fullmatch(r"\d+\.\d+\.\d+", match.group(1)):
            fail(path, line_number(text, match.start()), f"non-exact Node version: {match.group(1)}")
    for match in re.finditer(r"toolchain:\s*[\"']?([^\"'\s#]+)", text):
        toolchain = match.group(1)
        if not (
            re.fullmatch(r"\d+\.\d+\.\d+", toolchain)
            or re.fullmatch(r"nightly-\d{4}-\d{2}-\d{2}", toolchain)
        ):
            fail(path, line_number(text, match.start()), f"non-exact Rust toolchain: {toolchain}")
    for match in re.finditer(r"RUSTUP_TOOLCHAIN:\s*[\"']?([^\"'\s#]+)", text):
        if not re.fullmatch(r"(?:\d+\.\d+\.\d+|nightly-\d{4}-\d{2}-\d{2})", match.group(1)):
            fail(path, line_number(text, match.start()), f"non-exact RUSTUP_TOOLCHAIN: {match.group(1)}")
    for match in re.finditer(r"^\s*(?:-\s*)?(?:run:\s*)?.*cargo install\b([^\n]*)", text, re.MULTILINE):
        if "--version" not in match.group(1):
            fail(path, line_number(text, match.start()), "cargo install requires --version")
    for match in re.finditer(r"^\s*(?:-\s*)?(?:run:\s*)?.*\bpip install\b([^\n]*)", text, re.MULTILINE):
        command = match.group(0)
        if "--require-hashes" not in command and "--no-index" not in command:
            fail(path, line_number(text, match.start()), "pip install requires hashes or a verified local wheel")
    for match in re.finditer(r"\bapt-get\s+install\b([^\n]*)", text):
        packages = [
            token
            for token in match.group(1).split()
            if not token.startswith("-") and not token.startswith("$")
        ]
        if any("=" not in package for package in packages):
            fail(path, line_number(text, match.start()), "apt-get packages require exact versions")

for path in yaml_files:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if "uses: taiki-e/install-action@" not in line:
            continue
        indent = len(line) - len(line.lstrip())
        following: list[str] = []
        for candidate in lines[index + 1 :]:
            candidate_indent = len(candidate) - len(candidate.lstrip())
            if candidate.lstrip().startswith("- ") and candidate_indent <= indent:
                break
            following.append(candidate)
        step = "\n".join(following)
        if not re.search(r"^\s*tool:\s*[a-z0-9_-]+@\d+\.\d+\.\d+\s*$", step, re.MULTILINE):
            fail(path, index + 1, "install-action requires an exact tool version")
        if not re.search(r"^\s*checksum:\s*true\s*$", step, re.MULTILINE):
            fail(path, index + 1, "install-action checksum verification must remain enabled")
        if not re.search(r"^\s*fallback:\s*none\s*$", step, re.MULTILINE):
            fail(path, index + 1, "install-action mutable fallback must remain disabled")


release = RELEASE.read_text(encoding="utf-8")
if not re.search(r"^permissions:\s*\{\}\s*$", release, re.MULTILINE):
    fail(RELEASE, 1, "workflow-level permissions must be empty")
if 'releases may only be dispatched from refs/heads/main' not in release:
    fail(RELEASE, 1, "release dispatch must fail closed outside main")


def job_blocks(text: str) -> dict[str, tuple[int, str]]:
    jobs_match = re.search(r"^jobs:\s*$", text, re.MULTILINE)
    if jobs_match is None:
        return {}
    starts = list(re.finditer(r"^  ([a-z0-9_-]+):\s*$", text[jobs_match.end() :], re.MULTILINE))
    result: dict[str, tuple[int, str]] = {}
    base = jobs_match.end()
    for index, match in enumerate(starts):
        start = base + match.start()
        end = base + starts[index + 1].start() if index + 1 < len(starts) else len(text)
        result[match.group(1)] = (line_number(text, start), text[start:end])
    return result


def permissions(block: str) -> dict[str, str] | None:
    empty = re.search(r"^    permissions:\s*\{\}\s*$", block, re.MULTILINE)
    if empty:
        return {}
    header = re.search(r"^    permissions:\s*$", block, re.MULTILINE)
    if header is None:
        return None
    result: dict[str, str] = {}
    for line in block[header.end() :].splitlines():
        if not line.strip():
            continue
        match = re.match(r"^      ([a-z-]+):\s*(read|write)\s*$", line)
        if match is None:
            break
        result[match.group(1)] = match.group(2)
    return result


blocks = job_blocks(release)
expected_permissions = {
    "verify-ci": {"actions": "read"},
    "binaries": {"contents": "read"},
    "release-assets": {"contents": "read"},
    "image-build": {},
    "publish-image": {"packages": "write"},
    "sign-image": {
        "contents": "read",
        "packages": "write",
        "id-token": "write",
        "attestations": "write",
    },
    "sign-assets": {"contents": "read", "id-token": "write", "attestations": "write"},
    "publish-release": {"contents": "write"},
}
if set(blocks) != set(expected_permissions):
    missing = sorted(set(expected_permissions) - set(blocks))
    extra = sorted(set(blocks) - set(expected_permissions))
    fail(RELEASE, 1, f"release job set changed (missing={missing}, extra={extra})")
for job, expected in expected_permissions.items():
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    actual = permissions(block)
    if actual != expected:
        fail(RELEASE, job_line, f"{job} permissions are {actual!r}, expected {expected!r}")

expected_needs = {
    "publish-image": {"verify-ci", "image-build"},
    "sign-image": {"verify-ci", "image-build", "publish-image"},
    "sign-assets": {"verify-ci", "release-assets", "image-build", "publish-image"},
    "publish-release": {"verify-ci", "publish-image", "sign-image", "sign-assets"},
}
for job, expected in expected_needs.items():
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    match = re.search(r"^    needs:\s*\[([^\]]+)\]\s*$", block, re.MULTILINE)
    actual = {item.strip() for item in match.group(1).split(",")} if match else set()
    if actual != expected:
        fail(RELEASE, job_line, f"{job} needs {sorted(actual)}, expected {sorted(expected)}")

# Build/assembly jobs cannot mutate repository/package state or mint OIDC tokens.
for job in ("verify-ci", "binaries", "release-assets", "image-build"):
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    if re.search(r"^\s+(?:contents|packages|id-token|attestations):\s*write\s*$", block, re.MULTILINE):
        fail(RELEASE, job_line, f"{job} has release authority")

# Authority jobs consume verified artifacts; they never checkout/build or run an unpinned installer.
authority_forbidden = re.compile(
    r"actions/checkout@|uses:\s*\./|pip install|cargo install|apt-get|npm (?:ci|install)|"
    r"cargo (?:build|zigbuild)|docker/build-push-action@"
)
for job in ("publish-image", "sign-image", "sign-assets", "publish-release"):
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    if authority_forbidden.search(block):
        fail(RELEASE, job_line, f"{job} executes build or mutable package input")

ordered_guards = {
    "publish-image": ("sha256sum -c cairn-image.tar.sha256", '"${RUNNER_TEMP}/oras" cp'),
    "sign-image": ('resolve "${IMAGE}:latest"', '"${RUNNER_TEMP}/cosign" sign --yes'),
    "sign-assets": ("sha256sum -c RELEASE-MANIFEST.sha256", '"${RUNNER_TEMP}/cosign" sign-blob'),
    "publish-release": ("sha256sum -c PUBLISH-MANIFEST.sha256", '"$GH" release create'),
}
for job, (verification, mutation) in ordered_guards.items():
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    if verification not in block or mutation not in block or block.index(verification) > block.index(mutation):
        fail(RELEASE, job_line, f"{job} must verify its exact subject before mutation/signing")

for job in ("sign-image", "sign-assets"):
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    checksum = 'COSIGN_LINUX_AMD64_SHA256" "${RUNNER_TEMP}/cosign" | sha256sum -c -'
    signing = '"${RUNNER_TEMP}/cosign" sign'
    if checksum not in block or signing not in block or block.index(checksum) > block.index(signing):
        fail(RELEASE, job_line, f"{job} must checksum the exact Cosign binary before signing")

for job, invocation in (
    ("verify-ci", '"$GH" run list'),
    ("publish-release", '"$GH" release create'),
):
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    checksum = 'GH_LINUX_AMD64_SHA256" "$archive" | sha256sum -c -'
    if checksum not in block or invocation not in block or block.index(checksum) > block.index(invocation):
        fail(RELEASE, job_line, f"{job} must checksum the exact GitHub CLI before use")

for command in ('"$GH" release create', '"$GH" release delete'):
    for job, (job_line, block) in blocks.items():
        if command in block and job != "publish-release":
            fail(RELEASE, job_line, f"{command!r} is only allowed in publish-release")

for job in ("verify-ci", "binaries", "release-assets", "image-build", "publish-image", "publish-release"):
    if job in blocks and "uses: actions/attest@" in blocks[job][1]:
        fail(RELEASE, blocks[job][0], "attestation authority is isolated to signing jobs")

if release.count("uses: actions/attest@a1948c3f048ba23858d222213b7c278aabede763") != 3:
    fail(RELEASE, 1, "expected three full-commit-pinned SLSA attestation steps")
if release.count("predicate-type: https://slsa.dev/provenance/v1") != 3:
    fail(RELEASE, 1, "every attestation must consume an unprivileged-build SLSA v1 predicate")
for required_handoff in (
    "cairn-linux-amd64.provenance.json",
    "cairn-linux-arm64.provenance.json",
    "cairn-image.provenance.json",
    "IMAGE-METADATA.sha256",
):
    if required_handoff not in release:
        fail(RELEASE, 1, f"missing provenance handoff {required_handoff}")

if "publish-release" in blocks:
    job_line, publish_block = blocks["publish-release"]
    for manifest_subject in (
        "rel/IMAGE-DIGEST",
        "rel/RELEASE-COMMIT",
        "rel/RELEASE-VERSION",
        "rel/cairn-linux-amd64.provenance.json",
        "rel/cairn-linux-arm64.provenance.json",
        "rel/cairn-image.provenance.json",
    ):
        if manifest_subject not in publish_block:
            fail(RELEASE, job_line, f"published manifest subject is not attached: {manifest_subject}")

required_release_pins = (
    r"^\s*COSIGN_VERSION:\s*v\d+\.\d+\.\d+\s*$",
    rf"^\s*COSIGN_LINUX_AMD64_SHA256:\s*[0-9a-f]{{64}}\s*$",
    r"^\s*GH_VERSION:\s*[\"']?\d+\.\d+\.\d+[\"']?\s*$",
    rf"^\s*GH_LINUX_AMD64_SHA256:\s*[0-9a-f]{{64}}\s*$",
    r"^\s*ORAS_VERSION:\s*[\"']?\d+\.\d+\.\d+[\"']?\s*$",
    rf"^\s*ORAS_LINUX_AMD64_SHA256:\s*[0-9a-f]{{64}}\s*$",
    r"^\s*SYFT_VERSION:\s*[\"']?\d+\.\d+\.\d+[\"']?\s*$",
    rf"^\s*SYFT_LINUX_AMD64_SHA256:\s*[0-9a-f]{{64}}\s*$",
    r"^\s*version:\s*v\d+\.\d+\.\d+\s*$",
)
for pattern in required_release_pins:
    if not re.search(pattern, release, re.MULTILINE):
        fail(RELEASE, 1, f"missing immutable release input matching {pattern!r}")

if "release-assets" in blocks:
    job_line, assets_block = blocks["release-assets"]
    syft_checksum = 'SYFT_LINUX_AMD64_SHA256" "$archive" | sha256sum -c -'
    syft_run = '"${RUNNER_TEMP}/syft" scan'
    if (
        syft_checksum not in assets_block
        or syft_run not in assets_block
        or assets_block.index(syft_checksum) > assets_block.index(syft_run)
    ):
        fail(RELEASE, job_line, "release-assets must checksum the exact Syft binary before use")

if errors:
    print("release policy violations:", file=sys.stderr)
    for error in errors:
        print(f"  - {error}", file=sys.stderr)
    raise SystemExit(1)

print(
    f"release policy OK: {len(yaml_files)} YAML files, {len(blocks)} release jobs, "
    "all authority and immutable-input checks passed"
)
