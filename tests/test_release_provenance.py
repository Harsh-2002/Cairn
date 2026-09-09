#!/usr/bin/env python3
"""Exercise the actual release JQ producers and every privileged provenance consumer."""
import copy
import json
from pathlib import Path
import re
import shlex
import string
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]
ENV = {
    'GITHUB_SERVER_URL': 'https://github.com',
    'GITHUB_REPOSITORY': 'owner/repo',
    'GITHUB_REPOSITORY_ID': '17',
    'GITHUB_REPOSITORY_OWNER_ID': '18',
    'GITHUB_WORKFLOW_REF': 'owner/repo/.github/workflows/release.yml@refs/heads/main',
    'GITHUB_SHA': 'a' * 40,
    'GITHUB_RUN_ID': '200',
    'GITHUB_RUN_ATTEMPT': '2',
    'TARGET': 'x86_64-unknown-linux-musl',
    'target': 'x86_64-unknown-linux-musl',
    'VERSION': 'v2026.09.09',
    'IMAGE': 'ghcr.io/owner/repo',
    'amd64_sha': 'b' * 64,
    'arm64_sha': 'c' * 64,
}


def commands():
    workflow = (ROOT / '.github/workflows/release.yml').read_text()
    result = []
    for match in re.finditer(r"jq -([ne]) \\\n(.*?)'(.*?)'([^\n]*)", workflow, re.S):
        if '--arg build_type ' not in match[2]:
            continue
        args = shlex.split(match[2].replace('\\\n', ' '))
        values = dict(zip(args[1::3], args[2::3]))
        kind = 'binary' if values['recipe'].endswith('#binaries') else 'image'
        result.append((match[1], kind, args, match[3]))
    return result


def run(command, predicate=None, env=None):
    mode, _, args, query = command
    context = ENV if env is None else env
    expanded = [string.Template(arg).substitute(context) for arg in args]
    return subprocess.run(['jq', '-' + mode, *expanded, query],
                          input=json.dumps(predicate), text=True, capture_output=True)


class ProvenanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.commands = commands()
        cls.generated = {}
        for command in cls.commands:
            if command[0] == 'n':
                output = run(command)
                if output.returncode:
                    raise AssertionError(output.stderr)
                cls.generated[command[1]] = json.loads(output.stdout)

    def test_generated_predicates_use_github_supported_workflow_schema(self):
        for kind, predicate in self.generated.items():
            with self.subTest(kind=kind):
                definition = predicate['buildDefinition']
                self.assertEqual(definition['buildType'], 'https://actions.github.io/buildtypes/workflow/v1')
                self.assertEqual(definition['externalParameters'], {'workflow': {
                    'repository': 'https://github.com/owner/repo',
                    'ref': 'refs/heads/main', 'path': '.github/workflows/release.yml',
                }})
                self.assertEqual(definition['internalParameters']['github'], {
                    'event_name': 'workflow_dispatch', 'repository_id': '17',
                    'repository_owner_id': '18', 'runner_environment': 'github-hosted',
                })
                self.assertEqual(definition['resolvedDependencies'], [{
                    'uri': 'git+https://github.com/owner/repo@refs/heads/main',
                    'digest': {'gitCommit': ENV['GITHUB_SHA']},
                }])

    def test_actual_producers_and_all_consumers_agree(self):
        self.assertEqual(len(self.commands), 10)
        self.assertEqual(sum(command[0] == 'n' for command in self.commands), 2)
        self.assertEqual(set(self.generated), {'binary', 'image'})
        for command in self.commands:
            if command[0] != 'e':
                continue
            with self.subTest(kind=command[1], query=command[3]):
                result = run(command, self.generated[command[1]])
                self.assertEqual(result.returncode, 0, result.stderr)
                earlier = copy.deepcopy(self.generated[command[1]])
                earlier['runDetails']['metadata']['invocationId'] = 'https://github.com/owner/repo/actions/runs/200/attempts/1'
                self.assertEqual(run(command, earlier).returncode, 0)
        binary = next(command for command in self.commands if command[:2] == ('n', 'binary'))
        arm_env = {**ENV, 'TARGET': 'aarch64-unknown-linux-musl', 'target': 'aarch64-unknown-linux-musl'}
        arm = json.loads(run(binary, env=arm_env).stdout)
        for command in self.commands:
            if command[:2] == ('e', 'binary'):
                self.assertEqual(run(command, arm, arm_env).returncode, 0)

    def test_all_consumers_refuse_changed_identity_or_configuration(self):
        for command in self.commands:
            if command[0] != 'e':
                continue
            original = self.generated[command[1]]
            mutations = [
                (('buildDefinition', 'buildType'), 'https://example.invalid/custom-build'),
                (('buildDefinition', 'externalParameters', 'workflow', 'repository'), 'https://github.com/other/repo'),
                (('buildDefinition', 'externalParameters', 'workflow', 'ref'), 'refs/heads/other'),
                (('buildDefinition', 'externalParameters', 'workflow', 'path'), '.github/workflows/other.yml'),
                (('buildDefinition', 'externalParameters', 'unexpected'), True),
                (('buildDefinition', 'internalParameters', 'github', 'event_name'), 'pull_request'),
                (('buildDefinition', 'internalParameters', 'github', 'repository_id'), '99'),
                (('buildDefinition', 'internalParameters', 'github', 'repository_owner_id'), '99'),
                (('buildDefinition', 'internalParameters', 'github', 'runner_environment'), 'self-hosted'),
                (('buildDefinition', 'internalParameters', 'unexpected'), True),
                (('buildDefinition', 'internalParameters', 'cairn', 'recipe'), 'https://example.invalid/recipe'),
                (('buildDefinition', 'internalParameters', 'cairn', 'unexpected'), True),
                (('buildDefinition', 'internalParameters', 'cairn', 'parameters', 'version'), 'v2020.01.01'),
                (('buildDefinition', 'resolvedDependencies'), [{'uri': 'git+https://github.com/owner/repo@refs/heads/main', 'digest': {'gitCommit': 'd' * 40}}]),
                (('runDetails', 'builder', 'id'), 'https://github.com/owner/repo/.github/workflows/other.yml@refs/heads/main'),
                (('runDetails', 'metadata', 'invocationId'), 'https://github.com/owner/repo/actions/runs/201/attempts/1'),
                (('runDetails', 'metadata', 'invocationId'), 'https://github.com/owner/repo/actions/runs/200/attempts/3'),
                (('runDetails', 'metadata', 'invocationId'), 'https://github.com/owner/repo/actions/runs/200/attempts/0'),
            ]
            if command[1] == 'binary':
                mutations.extend([
                    (('buildDefinition', 'internalParameters', 'cairn', 'parameters', 'target'), 'aarch64-unknown-linux-musl'),
                    (('buildDefinition', 'internalParameters', 'cairn', 'details', 'rust'), 'stable'),
                ])
            else:
                mutations.extend([
                    (('buildDefinition', 'internalParameters', 'cairn', 'parameters', 'image'), 'ghcr.io/other/repo'),
                    (('buildDefinition', 'internalParameters', 'cairn', 'parameters', 'platforms'), ['linux/amd64']),
                    (('buildDefinition', 'internalParameters', 'cairn', 'details', 'binarySubjects', 'amd64'), 'invalid-digest'),
                ])
            for path, value in mutations:
                changed = copy.deepcopy(original)
                target = changed
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                with self.subTest(kind=command[1], field=path):
                    self.assertNotEqual(run(command, changed).returncode, 0)

    def test_signing_and_publication_bind_exact_original_binary_digests(self):
        consumers = [command for command in self.commands
                     if command[:2] == ('e', 'image') and 'amd64' in command[2]]
        self.assertEqual(len(consumers), 2)
        for command in consumers:
            changed = copy.deepcopy(self.generated['image'])
            changed['buildDefinition']['internalParameters']['cairn']['details']['binarySubjects']['amd64'] = 'd' * 64
            self.assertNotEqual(run(command, changed).returncode, 0)


if __name__ == '__main__':
    unittest.main()
