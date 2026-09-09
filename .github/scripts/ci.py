#!/usr/bin/env python3
"""Select validation, enforce the required gate, and verify GitHub-bound test receipts."""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ".github/workflows/ci.yml"
VERSION = 1
SHA = re.compile(r"[0-9a-f]{40}\Z")
DOCS = {"README.md", "CONTRIBUTING.md", "GOVERNANCE.md", "SECURITY.md", "CODE_OF_CONDUCT.md"}
INSTRUCTIONS = {"CLAUDE.md", "AGENTS.md", "CONTRACT.md"}
MAX_RECEIPT = 64 * 1024
MAX_ARCHIVE = 1024 * 1024


class EvidenceError(ValueError):
    """Evidence cannot authorize reuse; the caller must select fresh full validation."""


def require(condition, message):
    if not condition:
        raise EvidenceError(message)


def sha(value):
    require(isinstance(value, str) and SHA.fullmatch(value), "invalid commit identity")
    return value


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)


def tree(revision):
    return git("rev-parse", f"{sha(revision)}^{{tree}}").decode().strip()


def policy_digest():
    return hashlib.sha256(git("ls-tree", "-r", "HEAD", "--", ".github", "tests/test_ci.py")).hexdigest()


def documentation(path):
    return Path(path).name not in INSTRUCTIONS and (
        path in DOCS or (path.startswith("docs/") and path.endswith(".md"))
    )


def classify(paths):
    return "docs" if paths and all(documentation(path) for path in paths) else "full"


def change_profile(base, revision):
    changes = git("diff", "--name-only", "--no-renames", "-z", sha(base), sha(revision)).split(b"\0")
    paths = [path.decode("utf-8", errors="surrogateescape") for path in changes if path]
    if classify(paths) != "docs":
        return "full"
    for commit in (base, revision):
        entries = git("ls-tree", "-z", commit, "--", *paths).split(b"\0")
        if any(entry and not entry.startswith(b"100644 blob ") for entry in entries):
            return "full"
    return "docs"


def expected_jobs(profile):
    require(profile in {"docs", "full"}, "unknown validation profile")
    jobs = json.loads((ROOT / ".github/scripts/validation-jobs.json").read_text())
    return {job: "success" if group in {"always", profile} else "skipped" for job, group in jobs.items()}


def check_results(profile, results):
    require(isinstance(results, dict) and results == expected_jobs(profile), "selected validation jobs did not all pass")


def check_gate(plan_result, validation_result, reused):
    require(plan_result == "success", "CI planning failed")
    require(validation_result == ("skipped" if reused else "success"), "validation failed or unexpectedly skipped")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GitHub:
    def __init__(self, repository, token):
        require(re.fullmatch(r"[\w.-]+/[\w.-]+", repository), "invalid repository")
        self.prefix = f"/repos/{repository}"
        self.token = token

    def request(self, path):
        require(path.startswith(self.prefix + "/"), "unexpected API path")
        request = urllib.request.Request("https://api.github.com" + path, headers={
            "Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "cairn-ci",
        })
        return urllib.request.build_opener(NoRedirect).open(request, timeout=20)

    def get(self, path):
        with self.request(self.prefix + path) as response:
            return json.load(response)

    def pages(self, path, key=None):
        result = []
        for page in range(1, 11):
            separator = "&" if "?" in path else "?"
            payload = self.get(f"{path}{separator}per_page=100&page={page}")
            rows = payload[key] if key else payload
            require(isinstance(rows, list), "invalid GitHub collection")
            result.extend(rows)
            if len(rows) < 100:
                return result
        raise EvidenceError("GitHub pagination limit reached")

    def artifact(self, artifact):
        require(not artifact.get("expired"), "receipt expired")
        require(0 < artifact["size_in_bytes"] <= MAX_ARCHIVE, "receipt archive too large")
        try:
            response = self.request(f"{self.prefix}/actions/artifacts/{int(artifact['id'])}/zip")
        except urllib.error.HTTPError as error:
            require(error.code == 302, "artifact download refused")
            location = error.headers.get("Location", "")
            parsed = urllib.parse.urlparse(location)
            require(parsed.scheme == "https" and parsed.hostname and (
                parsed.hostname.endswith(".blob.core.windows.net") or
                parsed.hostname.endswith(".githubusercontent.com")
            ), "unexpected artifact host")
            # Signed artifact URLs receive no GitHub authorization header.
            response = urllib.request.urlopen(location, timeout=20)
        with response:
            data = response.read(MAX_ARCHIVE + 1)
        return decode_artifact(data, artifact.get("digest"))


def decode_artifact(data, digest):
    require(len(data) <= MAX_ARCHIVE, "receipt archive too large")
    require(digest == "sha256:" + hashlib.sha256(data).hexdigest(), "receipt archive digest mismatch")
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entries = archive.infolist()
        require(len(entries) == 1 and entries[0].filename == "ci-receipt.json", "unexpected receipt files")
        require(entries[0].file_size <= MAX_RECEIPT, "receipt too large")
        return json.loads(archive.read(entries[0]))


def verify_receipt(receipt, *, run, pr, revision, current_tree, parents, tested_commit,
                   profile, digest, repository, repository_id, required_jobs):
    require(isinstance(receipt, dict), "invalid receipt")
    require(run["status"] == "completed" and run["conclusion"] == "success", "PR run is not successful")
    require(run["event"] == "pull_request" and run["path"] == WORKFLOW, "unexpected validation workflow")
    require(run["repository"]["id"] == repository_id, "run belongs to another repository")
    require(run["head_sha"] == pr["head"]["sha"], "PR head changed after validation")
    require(run["head_repository"]["id"] == pr["head"]["repo"]["id"], "unexpected PR head repository")
    require(pr["merged"] and pr["merge_commit_sha"] == revision, "PR does not identify this merge")
    require(pr["base"]["ref"] == "main" and pr["base"]["repo"]["id"] == repository_id, "unexpected PR target")
    require(len(parents) in {1, 2} and receipt.get("base_sha") == parents[0], "tested base differs from merge parent")
    require(tested_commit["sha"] == receipt.get("revision"), "tested revision mismatch")
    require([parent["sha"] for parent in tested_commit["parents"]] == [parents[0], pr["head"]["sha"]], "unexpected PR merge parents")
    require(tested_commit["tree"]["sha"] == current_tree, "tested source differs from merged source")
    expected = {
        "version": VERSION, "mode": "tested", "event": "pull_request", "workflow": WORKFLOW,
        "repository": repository, "repository_id": repository_id,
        "run_id": run["id"], "run_attempt": run["run_attempt"], "tree": current_tree,
        "pr_number": pr["number"], "head_sha": pr["head"]["sha"],
        "head_repository_id": pr["head"]["repo"]["id"], "profile": profile, "policy_digest": digest,
    }
    for key, value in expected.items():
        require(receipt.get(key) == value, f"receipt {key} mismatch")
    check_results(profile, receipt.get("results"))
    require(len(required_jobs) == 1 and required_jobs[0]["conclusion"] == "success", "required check is not successful")
    require(required_jobs[0].get("run_attempt") == run["run_attempt"], "required check attempt mismatch")


def reusable_evidence(api, revision, repository, repository_id):
    prs = api.pages(f"/commits/{revision}/pulls")
    prs = [pr for pr in prs if pr.get("merge_commit_sha") == revision and pr.get("merged_at")]
    require(len(prs) == 1, "no unique merged PR for this commit")
    pr = api.get(f"/pulls/{int(prs[0]['number'])}")
    runs = api.pages(f"/actions/workflows/ci.yml/runs?event=pull_request&head_sha={sha(pr['head']['sha'])}", "workflow_runs")
    runs = [run for run in runs if run["head_sha"] == pr["head"]["sha"]]
    require(bool(runs), "no PR validation run")
    run = max(runs, key=lambda item: item["id"])
    require(run["status"] == "completed" and run["conclusion"] == "success", "latest PR validation is not successful")
    artifacts = api.pages(f"/actions/runs/{int(run['id'])}/artifacts", "artifacts")
    name = f"ci-receipt-{run['id']}-{run['run_attempt']}"
    artifacts = [artifact for artifact in artifacts if artifact["name"] == name]
    require(len(artifacts) == 1, "missing or ambiguous validation receipt")
    receipt = api.artifact(artifacts[0])
    parents = git("show", "-s", "--format=%P", revision).decode().split()
    require(bool(parents), "merge has no base")
    profile = change_profile(parents[0], revision)
    tested = api.get(f"/git/commits/{sha(receipt['revision'])}")
    jobs = api.pages(f"/actions/runs/{int(run['id'])}/attempts/{int(run['run_attempt'])}/jobs", "jobs")
    verify_receipt(receipt, run=run, pr=pr, revision=revision, current_tree=tree(revision), parents=parents,
                   tested_commit=tested, profile=profile, digest=policy_digest(), repository=repository,
                   repository_id=repository_id, required_jobs=[job for job in jobs if job["name"] == "required"])
    if profile == "docs":
        base_runs = api.pages(f"/actions/workflows/ci.yml/runs?branch=main&head_sha={parents[0]}", "workflow_runs")
        base_runs = [item for item in base_runs if item["head_sha"] == parents[0] and item["event"] in {"push", "workflow_dispatch"}]
        require(bool(base_runs), "documentation base has no main validation")
        latest = max(base_runs, key=lambda item: item["id"])
        require(latest["status"] == "completed" and latest["conclusion"] == "success", "documentation base is not validated")
    return receipt


def output(values):
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
        for key, value in values.items():
            text = json.dumps(value, separators=(",", ":")) if not isinstance(value, str) else value
            require("\n" not in text and "\r" not in text, "multiline workflow output")
            stream.write(f"{key}={text}\n")


def plan():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    revision = sha(os.environ["GITHUB_SHA"])
    require(git("rev-parse", "HEAD").decode().strip() == revision, "checkout revision mismatch")
    values = {"revision": revision, "tree": tree(revision), "profile": "full", "reused": "false",
              "base_sha": "", "head_sha": "", "head_repository_id": 0, "pr_number": 0, "source": {}}
    kind = os.environ["GITHUB_EVENT_NAME"]
    if kind == "pull_request":
        pr = event["pull_request"]
        base, head = sha(pr["base"]["sha"]), sha(pr["head"]["sha"])
        require(pr["base"]["ref"] == "main", "unexpected PR base")
        require(git("show", "-s", "--format=%P", revision).decode().split() == [base, head], "PR checkout is not the tested merge")
        values.update(profile=change_profile(base, revision), base_sha=base, head_sha=head,
                      head_repository_id=pr["head"]["repo"]["id"], pr_number=pr["number"])
    else:
        require(kind in {"push", "workflow_dispatch"} and os.environ["GITHUB_REF"] == "refs/heads/main", "validation must target main")
        if kind == "push":
            try:
                api = GitHub(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"])
                receipt = reusable_evidence(api, revision, os.environ["GITHUB_REPOSITORY"], int(os.environ["GITHUB_REPOSITORY_ID"]))
                values.update(profile=receipt["profile"], reused="true", source=receipt)
                print(f"Reusing PR #{receipt['pr_number']} validation run {receipt['run_id']} attempt {receipt['run_attempt']}")
            except (ValueError, KeyError, TypeError, OSError, zipfile.BadZipFile, RuntimeError) as error:
                # No unavailable or malformed evidence can authorize a successful reuse.
                reason = str(error) if isinstance(error, EvidenceError) else type(error).__name__
                print(f"Fresh full validation required: {reason}")
    output(values)
    print(f"Validation profile: {values['profile']}; reused: {values['reused']}")


def complete():
    needs = json.loads(os.environ["NEEDS_JSON"])
    results = {job: value["result"] for job, value in needs.items()}
    check_results(os.environ["PROFILE"], results)
    output({"results": results})


def record():
    needs = json.loads(os.environ["NEEDS_JSON"])
    planned = needs["plan"]["outputs"]
    reused = planned["reused"] == "true"
    check_gate(needs["plan"]["result"], needs["validate"]["result"], reused)
    revision = sha(planned["revision"])
    require(git("rev-parse", "HEAD").decode().strip() == revision and tree(revision) == planned["tree"], "receipt checkout differs from plan")
    source = json.loads(planned["source"])
    results = source["results"] if reused else json.loads(needs["validate"]["outputs"]["results"])
    check_results(planned["profile"], results)
    receipt = {
        "version": VERSION, "mode": "reused" if reused else "tested", "event": os.environ["GITHUB_EVENT_NAME"],
        "workflow": WORKFLOW, "repository": os.environ["GITHUB_REPOSITORY"],
        "repository_id": int(os.environ["GITHUB_REPOSITORY_ID"]), "revision": revision, "tree": tree(revision),
        "run_id": int(os.environ["GITHUB_RUN_ID"]), "run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
        "base_sha": planned["base_sha"], "head_sha": planned["head_sha"],
        "head_repository_id": int(planned["head_repository_id"]), "pr_number": int(planned["pr_number"]),
        "profile": planned["profile"], "policy_digest": policy_digest(), "results": results,
    }
    if reused:
        receipt["source"] = {key: source[key] for key in ("run_id", "run_attempt", "revision", "pr_number")}
    Path(os.environ["RUNNER_TEMP"], "ci-receipt.json").write_text(json.dumps(receipt, sort_keys=True) + "\n")
    print(f"Required validation passed ({receipt['profile']}, {receipt['mode']})")


if __name__ == "__main__":
    try:
        {"plan": plan, "complete": complete, "record": record}[sys.argv[1]]()
    except (EvidenceError, KeyError, ValueError) as error:
        print(f"CI validation refused: {error}", file=sys.stderr)
        sys.exit(1)
