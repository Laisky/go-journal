import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from report import analyze
from verify import checked_path, manifest, sha256, verify_campaign


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class VerifyTest(unittest.TestCase):
    def test_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'data').write_bytes(b'original')
            line = hashlib.sha256(b'original').hexdigest() + '  ./data\n'
            (root / 'SHA256SUMS').write_text(line)
            self.assertEqual(len(manifest(root)), 1)
            (root / 'data').write_bytes(b'tampered')
            with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
                manifest(root)
            (root / 'data').write_bytes(b'original')
            (root / 'extra').write_text('unlisted')
            with self.assertRaisesRegex(ValueError, 'omits'):
                manifest(root)
            (root / 'extra').unlink()
            (root / 'SHA256SUMS').write_text(line + line)
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                manifest(root)
            (root / 'SHA256SUMS').write_text(line)
            (root / 'data').unlink()
            with self.assertRaisesRegex(ValueError, 'missing'):
                manifest(root)

    def test_unsafe_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'file').write_text('ok')
            (root / 'alias').symlink_to(root / 'file')
            for name in ('../file', '/etc/passwd', 'alias'):
                with self.assertRaises(ValueError):
                    checked_path(root, name)

    def test_campaign_detects_changed_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root, name = Path(temp), 'case'
            summary = {'passed': True, 'diagnostic_only': False, 'count': 32, 'duplicates': 0,
                       'lifecycle_seconds': 2., 'lifecycle_records_s': 16., 'seed_sync_p99_ms': 1.,
                       'phases': {'seed/open': {'seconds': 1., 'cpu_seconds': .2,
                           'allocated_bytes': 1024, 'peak_rss_mib': 16., 'ops': 1}}}
            options = {'count': 32, 'payload': 64, 'writers': 1, 'token': 'local'}
            hashes = {'before': 'b'*64, 'after': 'a'*64}
            report = {'expected_pairs': 1, 'case_names': [name], 'trials': [],
                      'driver_sha256': sha256(Path(__file__).with_name('run.py')),
                      'medians': {name: {}}}
            for side, binary in [('baseline', 'before'), ('candidate', 'after')]:
                target = root / 'trial' / f'{name}-0-{side}'
                write(target / 'options.json', options)
                write(target / 'environment.json', {'binary_sha256': hashes[binary],
                      'effective_cpus': 4, 'cpu_max': 'max', 'gomaxprocs': '4', 'platform': ['linux']})
                write(target / 'summary.json', summary)
                report['trials'].append({'case': name, 'pair': 0, 'side': side, 'returncode': 0, 'summary': summary})
                report['medians'][name][side] = {'lifecycle_records_s': 16., 'seed_sync_p99_ms': 1.,
                    'seed/open': {k: v for k, v in summary['phases']['seed/open'].items() if k != 'ops'}}
            report['assessment'] = analyze(report)
            write(root / 'trial/report.json', report)
            write(root / 'trial/cases.json', [{'name': name, 'count': 32, 'payload': 64, 'writers': 1}])
            with patch('verify.audit', return_value=summary):
                self.assertEqual(verify_campaign(root, 'trial', ['before', 'after'], hashes)['trials'], 2)
                target = root / 'trial' / f'{name}-0-candidate/options.json'
                for change in ({'payload': 65}, {'diagnostics': 'cpu'}, {'timeout': 9}):
                    write(target, dict(options, **change))
                    with self.assertRaises(ValueError):
                        verify_campaign(root, 'trial', ['before', 'after'], hashes)
                write(target, options)
                with self.assertRaisesRegex(ValueError, 'wrong binary'):
                    verify_campaign(root, 'trial', ['before', 'before'], hashes)
                report['medians'][name]['candidate']['lifecycle_records_s'] = 999
                write(root / 'trial/report.json', report)
                with self.assertRaisesRegex(ValueError, 'medians mismatch'):
                    verify_campaign(root, 'trial', ['before', 'after'], hashes)
