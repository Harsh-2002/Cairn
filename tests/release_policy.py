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


def workflow_shell_violations(text: str) -> list[tuple[int, str]]:
    """Fail-closed lexical policy for workflow shell scripts and expression placement.

    This repository deliberately accepts only a small canonical YAML subset around `run`. That
    keeps the pre-installer, standard-library-only check auditable and prevents quoted keys, flow
    mappings, aliases, or multiline scalars from hiding a shell script from the expression scan.
    """
    lines = text.splitlines()
    scripts: list[tuple[int, str]] = []
    violations: list[tuple[int, str]] = []
    run_key = re.compile(r"^(?P<indent> *)(?P<dash>-\s+)?run:\s*(?P<value>.*)$")
    block_scalar = re.compile(
        r"[|>](?:(?:[1-9][+-]?)|(?:[+-][1-9]?))?(?:\s+#.*)?"
    )
    canonical_expression_value = re.compile(
        r"^\s*(?:-\s+)?(?P<key>[A-Za-z_][A-Za-z0-9_-]*):\s+.*\$\{\{"
    )
    canonical_mapping = re.compile(
        r"^(?P<indent> *)(?P<dash>-\s+)?[A-Za-z_][A-Za-z0-9_-]*:\s*(?P<value>.*)$"
    )

    # Every expression must remain on one canonical mapping-value line. This independently rejects
    # expressions hidden in flow mappings, multiline quoted/plain scalars, block scalars, or
    # noncanonical/encoded keys before aliasing can make such a value executable.
    for index, line in enumerate(lines):
        if "${{" not in line:
            continue
        match = canonical_expression_value.match(line)
        if match is None or match.group("key") == "run":
            violations.append(
                (
                    index + 1,
                    "GitHub expressions must be canonical non-run mapping values",
                )
            )

    # Disallow YAML features that can synthesize or conceal a `run` key/value from a lexical
    # scanner. Current workflows use none of them; keeping this subset explicit is safer than a
    # permissive parser whose edge cases silently widen the shell trust boundary.
    quoted_key = re.compile(r"^\s*(?:-\s+)?(?:'[^']*'|\"[^\"]*\")\s*:")
    spaced_mapping_key = re.compile(
        r"^\s*(?:-\s+)?[A-Za-z_][A-Za-z0-9_-]*\s+:"
    )
    flow_shell_container = re.compile(
        r"^\s*(?:-\s+)?(?:jobs|runs|steps|parallel):\s*[\[{]"
    )
    yaml_indirection = re.compile(
        r"(?:^\s*(?:-\s+)?(?:[?&*!]|<<\s*:))|(?::\s*[&*!])"
    )
    block_content_lines: set[int] = set()
    index = 0
    while index < len(lines):
        header = canonical_mapping.match(lines[index])
        if header is None or block_scalar.fullmatch(header.group("value").strip()) is None:
            index += 1
            continue
        key_indent = len(header.group("indent")) + len(header.group("dash") or "")
        index += 1
        while index < len(lines):
            candidate = lines[index]
            candidate_indent = len(candidate) - len(candidate.lstrip())
            if candidate.strip() and candidate_indent <= key_indent:
                break
            block_content_lines.add(index)
            index += 1

    for index, line in enumerate(lines):
        if index in block_content_lines:
            continue
        if re.match(r"^\s*(?:-\s*)?[\[{]", line):
            violations.append((index + 1, "flow-style collections are forbidden"))
        if quoted_key.match(line):
            violations.append((index + 1, "quoted workflow mapping keys are forbidden"))
        if spaced_mapping_key.match(line):
            violations.append((index + 1, "mapping keys may not contain space before `:`"))
        if flow_shell_container.match(line):
            violations.append(
                (
                    index + 1,
                    "jobs, runs, steps, and parallel groups must use block-style collections",
                )
            )
        if yaml_indirection.search(line):
            violations.append(
                (index + 1, "YAML explicit keys, tags, anchors, and aliases are forbidden")
            )

    index = 0
    while index < len(lines):
        match = run_key.match(lines[index])
        if match is None:
            index += 1
            continue
        value = match.group("value")
        if block_scalar.fullmatch(value.strip()) is None:
            stripped = value.strip()
            if not stripped or stripped[0] in "'\"*&!{[?":
                violations.append(
                    (
                        index + 1,
                        "inline run scripts must be non-empty plain scalars or block scalars",
                    )
                )
            key_indent = len(match.group("indent")) + len(match.group("dash") or "")
            following = index + 1
            while following < len(lines) and (
                not lines[following].strip() or lines[following].lstrip().startswith("#")
            ):
                following += 1
            if following < len(lines):
                following_indent = len(lines[following]) - len(lines[following].lstrip())
                if following_indent > key_indent:
                    violations.append(
                        (
                            index + 1,
                            "inline run scripts may not continue on later YAML lines",
                        )
                    )
            scripts.append((index + 1, value))
            index += 1
            continue

        # A compact sequence entry (`- run: |`) places the mapping key after the dash.
        # Sibling step keys align with that effective key indentation; scalar content does not.
        key_indent = len(match.group("indent")) + len(match.group("dash") or "")
        content_start = index + 1
        index = content_start
        while index < len(lines):
            candidate = lines[index]
            candidate_indent = len(candidate) - len(candidate.lstrip())
            if candidate.strip() and candidate_indent <= key_indent:
                break
            index += 1
        scripts.append((content_start + 1, "\n".join(lines[content_start:index])))
    for script_line, script in scripts:
        for match in re.finditer(r"\$\{\{", script):
            violations.append(
                (
                    script_line + line_number(script, match.start()) - 1,
                    "GitHub expressions in run scripts must be mapped through env",
                )
            )
    return violations


def self_test_workflow_shell_policy() -> None:
    accepted = """
steps:
  - env:
      REF: ${{ github.ref }}
    run: |
      printf '%s\\n' "$REF"
  - run: cargo test --workspace
"""
    if workflow_shell_violations(accepted):
        raise RuntimeError("workflow shell policy rejected its canonical fixture")

    rejected = {
        "direct block expression": "steps:\n  - run: |\n      echo '${{ github.ref }}'\n",
        "quoted run key": "steps:\n  - 'run': echo '${{ github.ref }}'\n",
        "spaced run key": "steps:\n  - run : echo '${{ github.ref }}'\n",
        "flow mapping": "steps:\n  - {run: \"echo '${{ github.ref }}'\"}\n",
        "inline steps flow": "steps: [{run: \"echo '${{ github.ref }}'\"}]\n",
        "encoded inline steps flow": (
            "steps: [{run: \"echo '$\\x7b\\x7b github.ref }}'\"}]\n"
        ),
        "parallel flow run": (
            "steps:\n"
            "  - parallel: [{run: \"echo '${{ github.ref }}'\"}]\n"
        ),
        "encoded parallel flow run": (
            "steps:\n"
            "  - parallel: [{run: \"echo '$\\x7b\\x7b github.ref }}'\"}]\n"
        ),
        "multiline quoted run": (
            "steps:\n  - run: \"echo safe\n      && echo '${{ github.ref }}'\"\n"
        ),
        "aliased run": (
            "script: &payload \"echo '${{ github.ref }}'\"\nsteps:\n  - run: *payload\n"
        ),
        "merged flow run": (
            "steps:\n  - <<: {\"\\x72un\": \"echo '$\\x7b\\x7b github.ref }}'\"}\n"
        ),
        "used flow anchor": (
            "steps:\n"
            "  - &payload {run: \"echo '$\\x7b\\x7b github.ref }}'\"}\n"
            "  - *payload\n"
        ),
        "local tag flow run": (
            "steps:\n"
            "  - !foo {run: \"echo '$\\x7b\\x7b github.ref }}'\"}\n"
        ),
        "tagged flow steps": (
            "steps: !!seq [{run: \"echo '$\\x7b\\x7b github.ref }}'\"}]\n"
        ),
        "verbatim-tagged flow steps": (
            "steps: !<tag:yaml.org,2002:seq> "
            "[{run: \"echo '$\\x7b\\x7b github.ref }}'\"}]\n"
        ),
        "split flow run": (
            "steps:\n"
            "  -\n"
            "    {run: \"echo '$\\x7b\\x7b github.ref }}'\"}\n"
        ),
        "encoded quoted key": (
            "steps:\n  - \"\\x72un\": \"echo '$\\x7b\\x7b github.ref }}'\"\n"
        ),
    }
    for name, fixture in rejected.items():
        if not workflow_shell_violations(fixture):
            raise RuntimeError(f"workflow shell policy missed {name}")


self_test_workflow_shell_policy()


# GitHub evaluates expressions before invoking the shell, so shell quoting around an expression
# does not make attacker-influenced refs, matrix values, or context fields safe. Cross that trust
# boundary through a step environment variable, then quote the ordinary shell expansion.
for path in yaml_files:
    text = path.read_text(encoding="utf-8")
    for line, message in workflow_shell_violations(text):
        fail(path, line, message)

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
if not re.search(
    r"^concurrency:\s*\n"
    r"(?:\s{2}#[^\n]*\n)*"
    r"\s{2}group:\s*cairn-production-release\s*\n"
    r"\s{2}cancel-in-progress:\s*false\s*$",
    release,
    re.MULTILINE,
):
    fail(
        RELEASE,
        1,
        "release workflow must serialize dispatches without cancelling a running mutation",
    )


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
    "stage-image": {"packages": "write"},
    "sign-image": {
        "contents": "read",
        "packages": "write",
        "id-token": "write",
        "attestations": "write",
    },
    "sign-assets": {"contents": "read", "id-token": "write", "attestations": "write"},
    "publish-release": {"contents": "write"},
    "promote-latest": {"packages": "write"},
    "retire-prior-releases": {"contents": "write"},
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

if "verify-ci" in blocks:
    job_line, verify_ci_block = blocks["verify-ci"]
    if re.search(
        r"^    if: github\.ref == 'refs/heads/main'\s*$",
        verify_ci_block,
        re.MULTILINE,
    ) is None:
        fail(RELEASE, job_line, "verify-ci must have the exact job-level main-branch gate")
    for required in (
        "RELEASE_REF: ${{ github.ref }}",
        "RELEASE_REF_NAME: ${{ github.ref_name }}",
        "RELEASE_SHA: ${{ github.sha }}",
        "REPOSITORY: ${{ github.repository }}",
        'if [ "$RELEASE_REF" != "refs/heads/main" ]',
        '--repo "$REPOSITORY"',
        '--branch "$RELEASE_REF_NAME"',
        "--json headSha,status,conclusion",
        '--arg sha "$RELEASE_SHA"',
        ".headSha == $sha",
    ):
        if required not in verify_ci_block:
            fail(
                RELEASE,
                job_line,
                f"verify-ci shell-bound context guard is missing {required!r}",
            )

expected_needs = {
    "stage-image": {"verify-ci", "image-build"},
    "sign-image": {"verify-ci", "image-build", "stage-image"},
    "sign-assets": {"verify-ci", "release-assets", "image-build", "stage-image"},
    "publish-release": {"verify-ci", "stage-image", "sign-image", "sign-assets"},
    "promote-latest": {
        "verify-ci",
        "stage-image",
        "sign-image",
        "sign-assets",
        "publish-release",
    },
    "retire-prior-releases": {"verify-ci", "promote-latest"},
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
for job in (
    "stage-image",
    "sign-image",
    "sign-assets",
    "publish-release",
    "promote-latest",
    "retire-prior-releases",
):
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    if authority_forbidden.search(block):
        fail(RELEASE, job_line, f"{job} executes build or mutable package input")

# Every consumer that relies on build provenance validates an exact workflow-local claim. A rerun
# may consume artifacts from an earlier attempt of the same run, never another run or a future
# attempt.
provenance_common_markers = (
    '--arg builder "${GITHUB_SERVER_URL}/${GITHUB_WORKFLOW_REF}"',
    '--arg invocation_prefix "${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/attempts/"',
    '--arg max_attempt "$GITHUB_RUN_ATTEMPT"',
    '--arg source "git+${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}@${GITHUB_SHA}"',
    ".buildDefinition.resolvedDependencies == [",
    "{uri: $source, digest: {gitCommit: $sha}}",
    ".runDetails.builder == {id: $builder}",
    '(.runDetails.metadata | keys == ["invocationId"])',
    ".runDetails.metadata.invocationId | startswith($invocation_prefix)",
    '| test("^[1-9][0-9]*$")',
    "<= ($max_attempt | tonumber)",
)
binary_provenance_markers = (
    '--arg build_type "${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/.github/workflows/release.yml#binaries"',
    ".buildDefinition.buildType == $build_type",
    ".buildDefinition.externalParameters == {target: $target, version: $version}",
    'rust: "1.97.1", zig: "0.16.0", cargoZigbuild: "0.23.0"',
)
image_provenance_markers = (
    '--arg build_type "${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/.github/workflows/release.yml#image-build"',
    ".buildDefinition.buildType == $build_type",
    'image: $image, platforms: ["linux/amd64", "linux/arm64"], version: $version',
)
expected_provenance_claims = {
    "release-assets": ("binary",),
    "stage-image": ("image",),
    "sign-image": ("image",),
    "sign-assets": ("binary", "image"),
    "publish-release": ("binary", "image"),
    "promote-latest": ("image",),
}
provenance_before = {
    "release-assets": "name: cairn-release-unsigned",
    "stage-image": '"${RUNNER_TEMP}/oras" cp',
    "sign-image": '"${RUNNER_TEMP}/cosign" sign --yes',
    "sign-assets": "uses: actions/attest@",
    "publish-release": "delete_resource exact-draft-tag",
    "promote-latest": '"${RUNNER_TEMP}/cosign" verify',
}
for job, claims in expected_provenance_claims.items():
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    for marker in provenance_common_markers:
        if block.count(marker) < len(claims):
            fail(
                RELEASE,
                job_line,
                f"{job} must validate {len(claims)} exact same-run provenance claim(s): {marker!r}",
            )
    for claim in claims:
        markers = (
            binary_provenance_markers
            if claim == "binary"
            else image_provenance_markers
        )
        for marker in markers:
            if marker not in block:
                fail(RELEASE, job_line, f"{job} is missing exact {claim} provenance marker {marker!r}")
    last_attempt_guard = block.rfind("<= ($max_attempt | tonumber)")
    authority_use = block.find(provenance_before[job])
    if (
        last_attempt_guard < 0
        or authority_use < 0
        or last_attempt_guard > authority_use
    ):
        fail(RELEASE, job_line, f"{job} must validate provenance before artifact authority/use")

ordered_guards = {
    "stage-image": ("sha256sum -c cairn-image.tar.sha256", '"${RUNNER_TEMP}/oras" cp'),
    "sign-image": ('resolve "${IMAGE}:${CANDIDATE}"', '"${RUNNER_TEMP}/cosign" sign --yes'),
    "sign-assets": ("sha256sum -c RELEASE-MANIFEST.sha256", '"${RUNNER_TEMP}/cosign" sign-blob'),
    "publish-release": ("sha256sum -c PUBLISH-MANIFEST.sha256", '"$GH" release create'),
    "promote-latest": ('"${RUNNER_TEMP}/cosign" verify', '"${IMAGE}:latest"'),
    "retire-prior-releases": (
        'GH_LINUX_AMD64_SHA256" "$archive" | sha256sum -c -',
        '"$GH" api --method DELETE',
    ),
}
for job, (verification, mutation) in ordered_guards.items():
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    if verification not in block or mutation not in block or block.index(verification) > block.index(mutation):
        fail(RELEASE, job_line, f"{job} must verify its exact subject before mutation/signing")

for job, invocation in (
    ("sign-image", '"${RUNNER_TEMP}/cosign" sign'),
    ("sign-assets", '"${RUNNER_TEMP}/cosign" sign'),
    ("promote-latest", '"${RUNNER_TEMP}/cosign" verify'),
):
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    checksum = 'COSIGN_LINUX_AMD64_SHA256" "${RUNNER_TEMP}/cosign" | sha256sum -c -'
    if (
        checksum not in block
        or invocation not in block
        or block.index(checksum) > block.index(invocation)
    ):
        fail(RELEASE, job_line, f"{job} must checksum the exact Cosign binary before use")

for job, invocation in (
    ("verify-ci", '"$GH" run list'),
    ("publish-release", '"$GH" api --paginate --slurp'),
    ("retire-prior-releases", '"$GH" api --method DELETE'),
):
    if job not in blocks:
        continue
    job_line, block = blocks[job]
    checksum = 'GH_LINUX_AMD64_SHA256" "$archive" | sha256sum -c -'
    if checksum not in block or invocation not in block or block.index(checksum) > block.index(invocation):
        fail(RELEASE, job_line, f"{job} must checksum the exact GitHub CLI before use")

for command, allowed_job in (
    ('"$GH" release create', "publish-release"),
    ('"$GH" release edit', "publish-release"),
):
    for job, (job_line, block) in blocks.items():
        if command in block and job != allowed_job:
            fail(RELEASE, job_line, f"{command!r} is only allowed in {allowed_job}")

for job, (job_line, block) in blocks.items():
    if '"$GH" release delete' in block:
        fail(RELEASE, job_line, "gh release delete --cleanup-tag has non-atomic partial-failure semantics")
    if '"$GH" api --method DELETE' in block and job != "retire-prior-releases":
        fail(RELEASE, job_line, "GitHub CLI API deletion is isolated to CalVer retirement")
    if "--request DELETE" in block and job != "publish-release":
        fail(RELEASE, job_line, "curl API deletion is isolated to exact-commit draft recovery")

for job in (
    "verify-ci",
    "binaries",
    "release-assets",
    "image-build",
    "stage-image",
    "publish-release",
    "promote-latest",
    "retire-prior-releases",
):
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
    publish_markers = (
        "--draft",
        '"$GH" release create',
        '"$GH" release download',
        '"$GH" release view',
        ".isDraft",
        '"$GH" release edit',
        "--draft=false",
        ".object.sha",
        '"$GITHUB_SHA"',
        ".draft == true",
        ".draft == false",
        ".target_commitish == $sha",
        '.ref == ("refs/tags/" + $tag)',
        '.object.type == "commit"',
        ".object.sha == $sha",
        "delete_resource exact-draft-tag",
        "/git/refs/tags/${TAG}",
        "delete_resource exact-commit-draft",
        "/releases/${release_id}",
        "--request DELETE",
        '"$GH" api --paginate --slurp',
        "[.[][] | select(.tag_name == $tag)] | length",
        "matching_release_count",
        "verify_exact_tag",
        "verify_published_assets",
        'mkdir "$destination"',
        'cmp -- "rel/${asset}" "${destination}/${asset}"',
        'echo "published=true" >> "$GITHUB_OUTPUT"',
        'echo "published=false" >> "$GITHUB_OUTPUT"',
        "if: steps.release-state.outputs.published != 'true'",
        'test "$(get_status tag',
        'test "$(matching_release_count',
        'if [ "$status" != 204 ]',
        'if [ "$release_count" = 0 ]',
        'if [ "$release_count" != 1 ]',
        'if [ "$tag_status" = 200 ]',
        "already exists without a release",
        "not a recoverable exact-commit draft",
        "refusing ambiguous recovery",
        "refuse_newer_calver",
        "/git/matching-refs/tags/v?per_page=100",
        '[[ "$candidate" > "$TAG" ]]',
        '[[ "$ref_path" > "$TAG" ]]',
        "newer CalVer release",
        "newer CalVer tag",
        "export LC_ALL=C",
    )
    missing_publish_markers = [
        required for required in publish_markers if required not in publish_block
    ]
    for required in missing_publish_markers:
        fail(RELEASE, job_line, f"draft/commit-safe release publication is missing {required!r}")
    if not missing_publish_markers:
        draft_check = publish_block.index(".draft == true")
        draft_tag_check = publish_block.rindex('verify_exact_tag "$tag_json"')
        tag_delete = publish_block.index("delete_resource exact-draft-tag")
        tag_verify = publish_block.index('test "$(get_status tag')
        draft_delete = publish_block.index("delete_resource exact-commit-draft")
        draft_verify = publish_block.index('test "$(matching_release_count')
        create = publish_block.index('"$GH" release create')
        if not (
            draft_check
            < draft_tag_check
            < tag_delete
            < tag_verify
            < draft_delete
            < draft_verify
            < create
        ):
            fail(
                RELEASE,
                job_line,
                "exact-commit recovery must delete/verify the tag, then delete/verify the draft, before recreation",
            )
        if not (
            create
            < publish_block.index('"$GH" release view')
            < publish_block.index('"$GH" release edit')
        ):
            fail(
                RELEASE,
                job_line,
                "release assets must be uploaded and verified as a draft before publication",
            )
        published_check = publish_block.index(".draft == false")
        published_tag_check = publish_block.index(
            'verify_exact_tag "$tag_json"',
            published_check,
        )
        published_assets = publish_block.index(
            "verify_published_assets",
            published_tag_check,
        )
        published_output = publish_block.index(
            'echo "published=true" >> "$GITHUB_OUTPUT"',
            published_assets,
        )
        if not (
            published_check
            < published_tag_check
            < published_assets
            < published_output
            < draft_check
        ):
            fail(
                RELEASE,
                job_line,
                "only an exact-tag, byte-identical published release may resume downstream jobs",
            )
        newer_guard = publish_block.index('refuse_newer_calver "$releases_json"')
        if newer_guard > published_check or newer_guard > draft_check or newer_guard > create:
            fail(
                RELEASE,
                job_line,
                "newer CalVer releases and refs must be refused before recovery or publication",
            )
    if re.search(r"\brelease\s+delete\b", publish_block):
        fail(RELEASE, job_line, "publish-release must not use partial gh release/tag cleanup")
    if "--cleanup-tag" in publish_block or "|| true" in publish_block:
        fail(RELEASE, job_line, "exact-commit draft recovery errors must remain fatal")

if "image-build" in blocks:
    job_line, image_block = blocks["image-build"]
    candidate_tag = (
        "tags: ${{ needs.verify-ci.outputs.image }}"
        ":candidate-${{ github.run_id }}-${{ github.run_attempt }}"
    )
    if candidate_tag not in image_block:
        fail(RELEASE, job_line, "the local OCI archive must use the run-unique candidate tag")
    if re.search(r"^\s*tags:.*:latest\s*$", image_block, re.MULTILINE):
        fail(RELEASE, job_line, "the unprivileged OCI build must not name its local archive latest")

if "stage-image" in blocks:
    job_line, stage_block = blocks["stage-image"]
    for required in (
        "candidate-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}",
        'echo "candidate=$candidate" >> "$GITHUB_OUTPUT"',
        '"${IMAGE}:${candidate}"',
    ):
        if required not in stage_block:
            fail(RELEASE, job_line, f"run-unique candidate staging is missing {required!r}")
    if "${IMAGE}:latest" in stage_block:
        fail(RELEASE, job_line, "stage-image must not advance the public latest tag")

if "sign-image" in blocks:
    job_line, sign_block = blocks["sign-image"]
    if '"${IMAGE}@${DIGEST}"' not in sign_block:
        fail(RELEASE, job_line, "image signing must target the immutable staged digest")
    if 'resolve "${IMAGE}:${CANDIDATE}"' not in sign_block:
        fail(RELEASE, job_line, "image signing must re-resolve the run-unique candidate")

if "sign-assets" in blocks:
    job_line, sign_assets_block = blocks["sign-assets"]
    sign_assets_markers = (
        "pattern: cairn-binary-*",
        "path: built",
        '(cd "$source_dir" && sha256sum -c "${file_name}.sha256")',
        '= "commit ${GITHUB_SHA}"',
        '= "version ${VERSION}"',
        '= "target ${target}"',
        ".buildDefinition.externalParameters == {target: $target, version: $version}",
        ".buildDefinition.resolvedDependencies == [",
        'cmp -- "${source_dir}/${file_name}" "rel/${file_name}"',
        'cmp -- "${source_dir}/${file_name}.provenance.json"',
        "amd64 x86_64-unknown-linux-musl",
        "arm64 aarch64-unknown-linux-musl",
        "sha256sum cairn-linux-amd64 cairn-linux-arm64",
        'cmp -- "${RUNNER_TEMP}/expected-SHA256SUMS" SHA256SUMS',
        "sha256sum -c SHA256SUMS",
        "(cd rel && sha256sum -c RELEASE-MANIFEST.sha256)",
        "(cd image && sha256sum -c IMAGE-METADATA.sha256)",
        'amd64_sha="$(sha256sum rel/cairn-linux-amd64',
        'arm64_sha="$(sha256sum rel/cairn-linux-arm64',
        "binarySubjects: {amd64: $amd64, arm64: $arm64}",
        ".runDetails.builder == {id: $builder}",
        ".runDetails.metadata.invocationId | startswith($invocation_prefix)",
        '| test("^[1-9][0-9]*$")',
        "<= ($max_attempt | tonumber)",
        "uses: actions/attest@",
        '"${RUNNER_TEMP}/cosign" sign-blob',
    )
    missing_sign_assets_markers = [
        marker for marker in sign_assets_markers if marker not in sign_assets_block
    ]
    for marker in missing_sign_assets_markers:
        fail(
            RELEASE,
            job_line,
            f"direct binary/image subject verification is missing {marker!r}",
        )
    if not missing_sign_assets_markers:
        direct_download = sign_assets_block.index("pattern: cairn-binary-*")
        direct_checksum = sign_assets_block.index(
            '(cd "$source_dir" && sha256sum -c "${file_name}.sha256")'
        )
        metadata_commit = sign_assets_block.index('= "commit ${GITHUB_SHA}"')
        binary_predicate = sign_assets_block.index(
            ".buildDefinition.externalParameters == {target: $target, version: $version}"
        )
        binary_compare = sign_assets_block.index(
            'cmp -- "${source_dir}/${file_name}" "rel/${file_name}"'
        )
        predicate_compare = sign_assets_block.index(
            'cmp -- "${source_dir}/${file_name}.provenance.json"'
        )
        checksum_manifest = sign_assets_block.index(
            'cmp -- "${RUNNER_TEMP}/expected-SHA256SUMS" SHA256SUMS'
        )
        checksum_verify = sign_assets_block.index("sha256sum -c SHA256SUMS")
        release_manifest = sign_assets_block.index(
            "(cd rel && sha256sum -c RELEASE-MANIFEST.sha256)"
        )
        image_manifest = sign_assets_block.index(
            "(cd image && sha256sum -c IMAGE-METADATA.sha256)"
        )
        binary_hash = sign_assets_block.index(
            'amd64_sha="$(sha256sum rel/cairn-linux-amd64'
        )
        image_binding = sign_assets_block.index(
            "binarySubjects: {amd64: $amd64, arm64: $arm64}"
        )
        image_builder = sign_assets_block.rindex(
            ".runDetails.builder == {id: $builder}"
        )
        first_attestation = sign_assets_block.index("uses: actions/attest@")
        blob_signing = sign_assets_block.index('"${RUNNER_TEMP}/cosign" sign-blob')
        if not (
            direct_download
            < direct_checksum
            < metadata_commit
            < binary_predicate
            < binary_compare
            < predicate_compare
            < checksum_manifest
            < checksum_verify
            < release_manifest
            < image_manifest
            < binary_hash
            < image_binding
            < image_builder
            < first_attestation
            < blob_signing
        ):
            fail(
                RELEASE,
                job_line,
                "original binary bytes/predicates and image subject bindings must be verified before all signing",
            )

if "promote-latest" in blocks:
    job_line, promote_block = blocks["promote-latest"]
    for required in (
        '"${RUNNER_TEMP}/cosign" verify',
        '"${RUNNER_TEMP}/oras" cp "${IMAGE}@${DIGEST}" "${IMAGE}:${VERSION}"',
        '"${RUNNER_TEMP}/oras" cp "${IMAGE}@${DIGEST}" "${IMAGE}:latest"',
        'resolve "${IMAGE}:${VERSION}"',
        'resolve "${IMAGE}:latest"',
    ):
        if required not in promote_block:
            fail(RELEASE, job_line, f"signed-digest promotion is missing {required!r}")

for job, (job_line, block) in blocks.items():
    latest_mutation = (
        '"${RUNNER_TEMP}/oras" cp "${IMAGE}@${DIGEST}" "${IMAGE}:latest"' in block
    )
    if latest_mutation and job != "promote-latest":
        fail(RELEASE, job_line, "only promote-latest may mutate the public latest tag")

if "retire-prior-releases" in blocks:
    job_line, retire_block = blocks["retire-prior-releases"]
    calver = r'^v[0-9]{4}\.[0-9]{2}\.[0-9]{2}$'
    required = (
        "/releases?per_page=100",
        "/git/matching-refs/tags/v?per_page=100",
        "--jq '.[] | [.id, .tag_name] | @tsv'",
        "--jq '.[].ref'",
        calver,
        '"$GH" api --method DELETE "/repos/${REPO}/releases/${release_id}"',
        '"/repos/${REPO}/git/refs/tags/${ref_path}"',
        '[[ "$previous" < "$TAG" ]]',
        '[[ "$ref_path" < "$TAG" ]]',
        '[[ "$remaining" < "$TAG" ]]',
        "export LC_ALL=C",
        "prior CalVer release remains after retirement",
        "prior CalVer tag remains after retirement",
    )
    missing_retirement_markers = [item for item in required if item not in retire_block]
    for item in missing_retirement_markers:
        fail(RELEASE, job_line, f"CalVer-only retirement is missing {item!r}")
    if retire_block.count("--paginate") < 4:
        fail(RELEASE, job_line, "release and tag-ref sets must both be paginated before and after deletion")
    if retire_block.count(calver) < 4:
        fail(RELEASE, job_line, "every release/ref deletion and verification pass needs an exact CalVer guard")
    if not missing_retirement_markers:
        release_delete = retire_block.index(
            '"$GH" api --method DELETE "/repos/${REPO}/releases/${release_id}"'
        )
        tag_delete = retire_block.index('"/repos/${REPO}/git/refs/tags/${ref_path}"')
        release_guard = retire_block.index(
            '[[ "$previous" =~ ^v[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}$ ]]'
        )
        ref_guard = retire_block.index(
            '[[ "$ref_path" =~ ^v[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}$ ]]'
        )
        release_older = retire_block.index('[[ "$previous" < "$TAG" ]]')
        ref_older = retire_block.index('[[ "$ref_path" < "$TAG" ]]')
        if not (
            release_guard < release_older < release_delete
            and ref_guard < ref_older < tag_delete
        ):
            fail(
                RELEASE,
                job_line,
                "exact CalVer and strictly-older guards must precede release and ref deletion",
            )
        if retire_block.rindex("/releases?per_page=100") < release_delete:
            fail(RELEASE, job_line, "deleted CalVer releases must be listed again and verified absent")
        if retire_block.rindex("/git/matching-refs/tags/v?per_page=100") < tag_delete:
            fail(RELEASE, job_line, "deleted CalVer refs must be listed again and verified absent")
    if "--cleanup-tag" in retire_block or "|| true" in retire_block:
        fail(RELEASE, job_line, "CalVer release/ref deletion errors must remain fatal")

for match in re.finditer(
    r"(?:\brelease\s+delete\b|--(?:method|request)\s+DELETE)[^\n]*\|\|\s*true",
    release,
):
    fail(
        RELEASE,
        line_number(release, match.start()),
        "release or tag deletion errors may not be suppressed",
    )

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
