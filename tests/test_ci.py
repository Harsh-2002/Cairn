#!/usr/bin/env python3
"""Regression tests for change selection, evidence reuse and required CI verdicts."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '.github/scripts'))
import ci
import docs_check


class SelectionTests(unittest.TestCase):
    def test_docs_allowlist_and_full_fallback(self):
        for paths in [['README.md'], ['CONTRIBUTING.md', 'docs/configuration.md'], ['docs/nested/example.md']]:
            with self.subTest(paths=paths):
                self.assertEqual(ci.classify(paths), 'docs')
        for paths in [[], ['web/src/app.tsx'], ['install.sh'], ['Cargo.lock'], ['.github/workflows/ci.yml'],
                      ['.github/CLAUDE.md'], ['docs/CLAUDE.md'], ['CLAUDE.md'], ['AGENTS.md'], ['CONTRACT.md'],
                      ['unknown.md'], ['docs/test.sh'], ['README.md', 'crates/cairn-blob/src/lib.rs'],
                      ['docs/readme.md', 'web/readme.md']]:
            with self.subTest(paths=paths):
                self.assertEqual(ci.classify(paths), 'full')

    def test_renames_modes_and_symlinks_use_complete_git_diff(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(ci, 'ROOT', Path(directory)):
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.DEVNULL).decode().strip()
            def commit():
                git('add', '-A'); git('commit', '-qm', 'fixture'); return git('rev-parse', 'HEAD')
            git('init', '-q'); git('config', 'user.email', 'ci@example.invalid'); git('config', 'user.name', 'CI')
            (root/'docs').mkdir(); (root/'README.md').write_text('# Start\n')
            base=commit()
            (root/'README.md').rename(root/'docs/start.md')
            renamed=commit()
            self.assertEqual(ci.change_profile(base, renamed), 'docs')
            (root/'docs/start.md').chmod(0o755)
            executable=commit()
            self.assertEqual(ci.change_profile(renamed, executable), 'full')
            (root/'docs/start.md').unlink(); (root/'docs/start.md').symlink_to('/does/not/exist')
            linked=commit()
            self.assertEqual(ci.change_profile(executable, linked), 'full')
            (root/'docs/start.md').unlink(); (root/'program.rs').write_text('fn main() {}\n')
            code=commit()
            self.assertEqual(ci.change_profile(linked, code), 'full')


class VerdictTests(unittest.TestCase):
    def test_every_selected_job_must_succeed(self):
        for profile in ['docs', 'full']:
            valid=ci.expected_jobs(profile)
            ci.check_results(profile, valid)
            for job, expected in valid.items():
                for actual in ['failure', 'cancelled', 'skipped', 'success']:
                    if actual == expected: continue
                    broken={**valid,job:actual}
                    with self.subTest(profile=profile,job=job,actual=actual), self.assertRaises(ci.EvidenceError):
                        ci.check_results(profile, broken)
            for broken in [{}, {**valid,'surprise':'success'}, {k:v for k,v in valid.items() if k!='policy'}]:
                with self.assertRaises(ci.EvidenceError): ci.check_results(profile, broken)
        with self.assertRaises(ci.EvidenceError): ci.expected_jobs('unknown')

    def test_required_gate_does_not_accept_accidental_skips(self):
        ci.check_gate('success','success',False)
        ci.check_gate('success','skipped',True)
        for reused in [True,False]:
            for planned in ['failure','cancelled','skipped']:
                with self.assertRaises(ci.EvidenceError): ci.check_gate(planned,'success',reused)
        for status in ['failure','cancelled','skipped']:
            with self.assertRaises(ci.EvidenceError): ci.check_gate('success',status,False)
        with self.assertRaises(ci.EvidenceError): ci.check_gate('success','success',True)

    def test_record_accepts_github_omitted_empty_outputs_but_requires_pr_identity(self):
        revision=ci.git('rev-parse','HEAD').decode().strip()
        planned={'revision':revision,'tree':ci.tree(revision),'profile':'full','reused':'false',
                 'source':'{}','head_repository_id':'0','pr_number':'0'}
        needs={'plan':{'result':'success','outputs':planned},
               'validate':{'result':'success','outputs':{'results':json.dumps(ci.expected_jobs('full'))}}}
        with tempfile.TemporaryDirectory() as directory:
            env={'GITHUB_REPOSITORY':'owner/repo','GITHUB_REPOSITORY_ID':'17',
                 'GITHUB_RUN_ID':'200','GITHUB_RUN_ATTEMPT':'1','RUNNER_TEMP':directory}
            for event in ['push','workflow_dispatch','pull_request']:
                with self.subTest(event=event),patch.dict(os.environ,{**env,'GITHUB_EVENT_NAME':event,
                                                                    'NEEDS_JSON':json.dumps(needs)}):
                    if event == 'pull_request':
                        with self.assertRaises(ci.EvidenceError): ci.record()
                    else:
                        ci.record()
                        receipt=json.loads((Path(directory)/'ci-receipt.json').read_text())
                        self.assertEqual(receipt['base_sha'],'')
                        self.assertEqual(receipt['head_sha'],'')
                        self.assertEqual(receipt['revision'],revision)
                        ci.check_results('full',receipt['results'])
            planned.update(base_sha='a'*40,head_sha='b'*40,head_repository_id='17',pr_number='7')
            with patch.dict(os.environ,{**env,'GITHUB_EVENT_NAME':'pull_request','NEEDS_JSON':json.dumps(needs)}):
                ci.record()
            receipt=json.loads((Path(directory)/'ci-receipt.json').read_text())
            self.assertEqual(receipt['base_sha'],'a'*40)
            self.assertEqual(receipt['head_sha'],'b'*40)

    def test_declared_jobs_match_the_reusable_workflow(self):
        text=(ROOT/'.github/workflows/validate.yml').read_text().split('\njobs:\n',1)[1]
        blocks=dict(re.findall(r'^  ([\w-]+):\n(.*?)(?=^  [\w-]+:|\Z)',text,re.M|re.S))
        expected=ci.expected_jobs('full')
        self.assertEqual(set(blocks), set(expected)|{'validation-result'})
        for job in expected:
            self.assertIn('- '+job+'\n',blocks['validation-result'])
            if job not in {'policy','docs'}:
                self.assertIn("inputs.profile == 'full'",blocks[job])
        self.assertIn('always()',blocks['validation-result'])

    def test_workflow_contract_and_release_still_require_main_ci(self):
        for name in ['ci','validate','codeql','extended']:
            text=(ROOT/f'.github/workflows/{name}.yml').read_text()
            self.assertNotRegex(text,r'run:\s*[>|]')
            self.assertNotRegex(text,r'(?m)^\s*#.*\n\s*#')
            self.assertNotIn('pull_request_target:',text)
        wrapper=(ROOT/'.github/workflows/ci.yml').read_text()
        self.assertIn('name: required',wrapper)
        self.assertIn('retention-days: ${{ github.retention_days }}',wrapper)
        self.assertIn('python3 .github/scripts/ci.py record',wrapper)
        release=(ROOT/'.github/workflows/release.yml').read_text()
        self.assertIn('.headSha == $sha',release)
        self.assertIn('--workflow ci.yml',release)
        self.assertNotIn('uses: ./.github/workflows/validate.yml',release)
        validation=(ROOT/'.github/workflows/validate.yml').read_text()
        extended=(ROOT/'.github/workflows/extended.yml').read_text()
        self.assertIn('conformance/replication_large.sh',validation)
        self.assertNotIn('conformance/bench_compare.sh',validation)
        self.assertIn('conformance/bench_compare.sh',extended)
        self.assertNotIn('conformance/replication_large.sh',extended)
        large=validation.split('\n  large-replication:\n',1)[1].split('\n  codeql:\n',1)[0]
        self.assertNotIn('upload-artifact',large)
        self.assertNotIn('LARGE_REPORT=',large)

    def test_release_preserves_the_installable_zig_wheel_filename(self):
        release=(ROOT/'.github/workflows/release.yml').read_text()
        url=re.search(r'ZIG_WHEEL_URL: (\S+)',release).group(1)
        assignment=re.search(r'^\s*wheel=(.*)$',release,re.M).group(1)
        with tempfile.TemporaryDirectory() as directory:
            actual=subprocess.check_output(['bash','-c',f'wheel={assignment}; printf "%s" "$wheel"'],
                                           env={**os.environ,'RUNNER_TEMP':directory,'ZIG_WHEEL_URL':url},text=True)
            self.assertEqual(Path(actual).name,url.rsplit('/',1)[1])
            self.assertRegex(Path(actual).name,r'^ziglang-[^-]+-py3-none-[^-]+\.whl$')


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.base='a'*40; self.head='b'*40; self.revision='c'*40; self.tested='d'*40; self.tree='e'*40
        self.run={'status':'completed','conclusion':'success','event':'pull_request','path':ci.WORKFLOW,
                  'repository':{'id':17},'head_repository':{'id':17},'head_sha':self.head,'id':200,'run_attempt':2}
        self.pr={'number':7,'merged':True,'merge_commit_sha':self.revision,
                 'head':{'sha':self.head,'repo':{'id':17}},'base':{'ref':'main','repo':{'id':17}}}
        self.receipt={'version':ci.VERSION,'mode':'tested','event':'pull_request','workflow':ci.WORKFLOW,
                      'repository':'owner/repo','repository_id':17,'run_id':200,'run_attempt':2,
                      'revision':self.tested,'tree':self.tree,'base_sha':self.base,'head_sha':self.head,
                      'head_repository_id':17,'pr_number':7,'profile':'full','policy_digest':'digest',
                      'results':ci.expected_jobs('full')}
        self.context={'run':self.run,'pr':self.pr,'revision':self.revision,'current_tree':self.tree,
                      'parents':[self.base], 'tested_commit':{'sha':self.tested,'tree':{'sha':self.tree},
                      'parents':[{'sha':self.base},{'sha':self.head}]},'profile':'full','digest':'digest',
                      'repository':'owner/repo','repository_id':17,
                      'required_jobs':[{'conclusion':'success','run_attempt':2}]}

    def test_identical_tested_source_allows_squash_commit(self):
        ci.verify_receipt(self.receipt, **self.context)
        self.context['parents']=[self.base,self.head]
        ci.verify_receipt(self.receipt, **self.context)

    def test_receipt_identity_profile_and_policy_must_match(self):
        for key in self.receipt:
            with self.subTest(field=key),self.assertRaises(ci.EvidenceError):
                changed=copy.deepcopy(self.receipt);changed[key]=None
                ci.verify_receipt(changed, **self.context)

    def test_github_metadata_cannot_be_replaced_by_receipt_claims(self):
        mutations=[('run','status','in_progress'),('run','conclusion','failure'),('run','event','push'),
                   ('run','path','.github/workflows/other.yml'),('run','head_sha','f'*40),
                   ('run','repository',{'id':18}),('run','run_attempt',3),('pr','merged',False),
                   ('pr','merge_commit_sha','f'*40),('pr','base',{'ref':'elsewhere','repo':{'id':17}}),
                   ('tested_commit','tree',{'sha':'f'*40}),('tested_commit','parents',[{'sha':'f'*40},{'sha':self.head}])]
        for obj,key,value in mutations:
            with self.subTest(obj=obj,key=key),self.assertRaises(ci.EvidenceError):
                changed=copy.deepcopy(self.context);changed[obj][key]=value
                ci.verify_receipt(self.receipt, **changed)
        for key,value in [('parents',['f'*40]),('parents',[]),('current_tree','f'*40),('required_jobs',[]),
                          ('required_jobs',[{'conclusion':'skipped','run_attempt':2}]),
                          ('required_jobs',[{'conclusion':'success','run_attempt':1}])]:
            with self.subTest(key=key),self.assertRaises(ci.EvidenceError):
                ci.verify_receipt(self.receipt,**{**self.context,key:value})

    def test_reuse_fetches_latest_run_receipt_and_exact_attempt(self):
        pr={**self.pr,'merged_at':'2026-09-09T00:00:00Z'}
        receipt=copy.deepcopy(self.receipt)
        runs=[copy.deepcopy(self.run)]
        artifact={'name':'ci-receipt-200-2','id':9,'expired':False}
        base_runs=[{'id':100,'head_sha':self.base,'event':'push','status':'completed','conclusion':'success'}]
        owner=self
        class API:
            def get(self,path):
                if path.startswith('/pulls/'): return pr
                if path.startswith('/git/commits/'): return owner.context['tested_commit']
                raise AssertionError(path)
            def pages(self,path,key=None):
                if path.startswith('/commits/'): return [pr]
                if '?branch=main' in path: return base_runs
                if '?event=pull_request' in path: return runs
                if path.endswith('/artifacts'): return [artifact]
                if path.endswith('/attempts/2/jobs'): return [{'name':'required','conclusion':'success','run_attempt':2}]
                raise AssertionError(path)
            def artifact(self,record):
                ci.require(not record['expired'],'expired')
                return receipt
        with patch.object(ci,'git',return_value=self.base.encode()),patch.object(ci,'tree',return_value=self.tree), \
             patch.object(ci,'policy_digest',return_value='digest'),patch.object(ci,'change_profile',return_value='full') as profile:
            self.assertEqual(ci.reusable_evidence(API(),self.revision,'owner/repo',17),receipt)
            runs.append({**self.run,'id':201,'conclusion':'failure'})
            with self.assertRaises(ci.EvidenceError): ci.reusable_evidence(API(),self.revision,'owner/repo',17)
            runs.pop()
            artifact['name']='ci-receipt-200-1'
            with self.assertRaises(ci.EvidenceError): ci.reusable_evidence(API(),self.revision,'owner/repo',17)
            artifact['name']='ci-receipt-200-2';artifact['expired']=True
            with self.assertRaises(ci.EvidenceError): ci.reusable_evidence(API(),self.revision,'owner/repo',17)
            artifact['expired']=False
            profile.return_value='docs';receipt['profile']='docs';receipt['results']=ci.expected_jobs('docs')
            ci.reusable_evidence(API(),self.revision,'owner/repo',17)
            base_runs[0]['conclusion']='failure'
            with self.assertRaises(ci.EvidenceError): ci.reusable_evidence(API(),self.revision,'owner/repo',17)
            base_runs.clear()
            with self.assertRaises(ci.EvidenceError): ci.reusable_evidence(API(),self.revision,'owner/repo',17)

    def test_archive_is_digest_bound_bounded_data_only(self):
        def archive(files):
            stream=io.BytesIO()
            with zipfile.ZipFile(stream,'w') as out:
                for name,data in files.items(): out.writestr(name,data)
            payload=stream.getvalue()
            return payload,'sha256:'+hashlib.sha256(payload).hexdigest()
        args=archive({'ci-receipt.json':json.dumps(self.receipt)})
        self.assertEqual(ci.decode_artifact(*args),self.receipt)
        for files in [{'../ci-receipt.json':'{}'}, {'ci-receipt.json':'{}','program.sh':'echo unexpected'},
                      {'ci-receipt.json':'x'*(ci.MAX_RECEIPT+1)}]:
            with self.assertRaises(ci.EvidenceError): ci.decode_artifact(*archive(files))
        with self.assertRaises(ci.EvidenceError): ci.decode_artifact(args[0],'sha256:'+'0'*64)
        with self.assertRaises(ci.EvidenceError): ci.decode_artifact(b'x'*(ci.MAX_ARCHIVE+1),None)

    def test_artifact_redirect_does_not_forward_github_credentials(self):
        api=ci.GitHub('owner/repo','private-token')
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as archive:
            archive.writestr('ci-receipt.json',json.dumps(self.receipt))
        payload=stream.getvalue()
        artifact={'id':9,'expired':False,'size_in_bytes':len(payload),'digest':'sha256:'+hashlib.sha256(payload).hexdigest()}
        url='https://fixture.blob.core.windows.net/receipt?signature=fixture'
        redirect=ci.urllib.error.HTTPError('https://api.github.com',302,'redirect',{'Location':url},None)
        with patch.object(api,'request',side_effect=redirect),patch('urllib.request.urlopen',return_value=io.BytesIO(payload)) as download:
            self.assertEqual(api.artifact(artifact),self.receipt)
            download.assert_called_once_with(url,timeout=20)
        redirect.headers['Location']='https://untrusted.example/receipt'
        with patch.object(api,'request',side_effect=redirect),patch('urllib.request.urlopen') as download:
            with self.assertRaises(ci.EvidenceError): api.artifact(artifact)
            download.assert_not_called()
        artifact['expired']=True
        with patch.object(api,'request') as request:
            with self.assertRaises(ci.EvidenceError): api.artifact(artifact)
            request.assert_not_called()

    def test_missing_evidence_selects_full_validation_for_direct_push(self):
        revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
        with tempfile.TemporaryDirectory() as directory:
            event=Path(directory)/'event.json';event.write_text('{}')
            out=Path(directory)/'outputs'
            env={'GITHUB_SHA':revision,'GITHUB_EVENT_NAME':'push','GITHUB_REF':'refs/heads/main',
                 'GITHUB_EVENT_PATH':str(event),'GITHUB_OUTPUT':str(out),'GITHUB_REPOSITORY':'owner/repo',
                 'GITHUB_REPOSITORY_ID':'17','GH_TOKEN':'fixture'}
            with patch.dict(os.environ,env),patch.object(ci,'reusable_evidence',side_effect=ci.EvidenceError('missing')):
                ci.plan()
            values=dict(line.split('=',1) for line in out.read_text().splitlines())
            self.assertEqual(values['profile'],'full');self.assertEqual(values['reused'],'false')

    def test_manual_validation_never_reuses_and_nonmain_dispatch_is_refused(self):
        revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
        with tempfile.TemporaryDirectory() as directory:
            event=Path(directory)/'event.json';event.write_text('{}')
            out=Path(directory)/'outputs'
            env={'GITHUB_SHA':revision,'GITHUB_EVENT_NAME':'workflow_dispatch','GITHUB_REF':'refs/heads/main',
                 'GITHUB_EVENT_PATH':str(event),'GITHUB_OUTPUT':str(out)}
            with patch.dict(os.environ,env),patch.object(ci,'reusable_evidence') as reuse:
                ci.plan();reuse.assert_not_called()
                self.assertIn('reused=false',out.read_text())
                os.environ['GITHUB_REF']='refs/heads/feature'
                with self.assertRaises(ci.EvidenceError): ci.plan()


class DocumentationTests(unittest.TestCase):
    def test_headings_duplicates_and_changed_incoming_links(self):
        self.assertEqual(docs_check.anchors('# A\n# A\n<a id="named"></a>'),{'a','a-1','named'})
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            source=root/'README.md';target=root/'guide.md'
            source.write_text('[guide](guide.md#old)\n```sh\nprintf "ok"\n```\n')
            target.write_text('# New\n')
            errors=docs_check.check(root,['guide.md'],[source,target])
            self.assertEqual(len(errors),1);self.assertIn('missing heading',errors[0])
            target.write_text('# Old\n')
            self.assertEqual(docs_check.check(root,['guide.md'],[source,target]),[])
            source.write_text('```sh\nif\n```\n')
            self.assertIn('invalid sh example',docs_check.check(root,['README.md'],[source,target])[0])


if __name__=='__main__':
    unittest.main()
