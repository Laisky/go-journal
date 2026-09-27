import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('journal_perf', Path(__file__).with_name('run.py'))
perf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(perf)


def fixture():
    options = {'count': 4, 'payload': 8, 'ack_percent': 3}
    expected, acked = {1, 2, 3, 4}, {1, 2}
    results, processes, rows = {}, {}, []
    usage = {'cpu_seconds': 1, 'total_alloc': 1, 'mallocs': 1, 'num_gc': 1, 'peak_rss_kib': 1}
    for mode in ('seed', 'transfer', 'deliver', 'verify'):
        ids = expected if mode == 'seed' else (expected - acked if mode in ('transfer', 'deliver') else set())
        records = []
        for identity in sorted(ids):
            h = perf.digest(perf.body(identity, 8).encode())
            records.append({'id': identity, 'hash': h, 'Begin': 1, 'Written': 2,
                            'Durable': 3 if mode != 'deliver' else 0,
                            'Received': 4 if mode == 'deliver' or identity in acked else 0,
                            'Acked': 5 if mode == 'deliver' or identity in acked else 0})
        results[mode] = {'mode': mode, 'count': 4, 'high': 4, 'records': records,
                         'phases': []}
        names = {'seed': ['open', 'append_sync_deliver_ack', 'seal'],
                 'transfer': ['open', 'frontier', 'replay_transfer', 'seal'],
                 'deliver': ['open', 'frontier', 'replay_deliver'],
                 'verify': ['open', 'frontier', 'replay_verify']}
        for name in names[mode]:
            results[mode]['phases'].append({'name': name, 'Begin': 1, 'End': 2,
                'ops': 4 if name == 'append_sync_deliver_ack' else len(records) if name.startswith('replay_') else 1,
                'Before': usage.copy(), 'After': usage.copy()})
        processes[mode] = {'returncode': -9 if mode in ('seed', 'transfer') else 0,
                           'held': mode in ('seed', 'transfer'), 'duration_ns': 100}
    for identity in sorted(expected):
        rows.append({'id': identity, 'hash': perf.digest(perf.body(identity, 8).encode()),
                     'stage': 'seed' if identity in acked else 'deliver'})
    return options, results, processes, rows, []


class OracleTest(unittest.TestCase):
    def test_exact_delivery(self):
        self.assertEqual(perf.audit_data(*fixture()), 0)

    def test_identical_retry_counted(self):
        data = fixture()
        data[3].append(data[3][-1].copy())
        self.assertEqual(perf.audit_data(*data), 1)

    def test_scan_resources_are_checked(self):
        r = {'mode': 'scan', 'count': 4, 'records': [], 'phases': []}
        usage = {'cpu_seconds': 1, 'total_alloc': 1, 'mallocs': 1, 'num_gc': 1, 'peak_rss_kib': 1}
        for name in ('open', 'frontier', 'scan'):
            r['phases'].append({'name': name, 'Begin': 1, 'End': 3, 'ops': 1,
                                'Before': usage.copy(), 'After': usage.copy(), 'latency_ns': [1]})
        perf.check_phases(r)
        for key, value in [('cpu_seconds', -1), ('total_alloc', float('nan')), ('peak_rss_kib', 0)]:
            changed = copy.deepcopy(r)
            changed['phases'][-1]['After'][key] = value
            with self.assertRaises(ValueError):
                perf.check_phases(changed)
        r['phases'][-1]['latency_ns'] = [100]
        with self.assertRaises(ValueError):
            perf.check_phases(r)

    def test_mutations_rejected(self):
        mutations = {
            'missing phase': lambda x: x[1]['seed']['phases'].pop(),
            'changed phase work': lambda x: x[1]['seed']['phases'][1].update(ops=1),
            'missing RSS': lambda x: x[1]['seed']['phases'][0]['Before'].update(peak_rss_kib=0),
            'fake duration': lambda x: x[2]['seed'].update(duration_ns=0),
            'missing source': lambda x: x[1]['seed']['records'].pop(),
            'missing downstream': lambda x: x[3].pop(),
            'changed payload': lambda x: x[3][0].update(hash='0' * 64),
            'joint forged payload': lambda x: (x[3][0].update(hash='0' * 64), x[1]['seed']['records'][0].update(hash='0' * 64)),
            'wrong replay set': lambda x: x[1]['transfer']['records'].pop(),
            'missing Sync': lambda x: x[1]['seed']['records'][0].update(Durable=0),
            'early ACK': lambda x: x[1]['deliver']['records'][0].update(Acked=1),
            'false initial ACK': lambda x: x[1]['seed']['records'][-1].update(Acked=5),
            'frontier regression': lambda x: x[1]['verify'].update(high=3),
            'fake crash': lambda x: x[2]['seed'].update(returncode=0),
            'wrong destination stage': lambda x: x[3][0].update(stage='deliver'),
            'hidden error': lambda x: x[4].append('failed'),
            'bad CPU': lambda x: x[1]['seed']['phases'][0].update(After=dict(x[1]['seed']['phases'][0]['After'], cpu_seconds=-1)),
            'NaN CPU': lambda x: x[1]['seed']['phases'][0].update(After=dict(x[1]['seed']['phases'][0]['After'], cpu_seconds=float('nan'))),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                data = copy.deepcopy(fixture())
                mutate(data)
                with self.assertRaises(ValueError):
                    perf.audit_data(*data)
                self.assertEqual(perf.audit_data(*fixture()), 0)


if __name__ == '__main__':
    unittest.main()
