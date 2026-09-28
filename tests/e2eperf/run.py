#!/usr/bin/env python3
"""Independent public-API worker supervisor, synchronized peer and offline oracle.

Only the Go worker links the journal. This file never reads private library state.
Failed trials remain on disk; no trial is automatically replaced or rerun.
"""
import argparse
import hashlib
import http.server
import json
import math
import os
from pathlib import Path
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time

from supervised_exec import install_signal_handlers
from monitor import METHOD, observe


def require(value, message):
    if not value:
        raise ValueError(message)


def body(identity, size):
    return f"id={identity:012d}|世界/café|" + ("0123456789abcdef" * ((size + 15) // 16))[:size]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    with open(path, 'x', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def read(path):
    return json.loads(Path(path).read_text())


def percentile(values, q=.99):
    return sorted(values)[max(0, math.ceil(len(values) * q) - 1)] if values else 0


def capacity():
    cpus = len(os.sched_getaffinity(0))
    text = Path('/sys/fs/cgroup/cpu.max').read_text().strip() if Path('/sys/fs/cgroup/cpu.max').exists() else ''
    if text and not text.startswith('max'):
        quota, period = map(int, text.split())
        cpus = min(cpus, quota / period)
    return cpus, text


class Peer:
    def __init__(self, path, options):
        self.lock = threading.Lock()
        self.rows, self.errors = [], []
        self.fp = open(path, 'xb', buffering=0)
        self.options = options
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def setup(self):
                super().setup()
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            def log_message(self, *_):
                pass

            def do_POST(self):
                try:
                    require(self.path in ('/seed', '/deliver'), 'peer route')
                    require(self.headers.get('Authorization') == 'Bearer ' + options['token'], 'peer auth')
                    require(self.headers.get('Content-Type') == 'application/json', 'peer media type')
                    length = int(self.headers.get('Content-Length', '-1'))
                    require(0 <= length <= options['payload'] * 6 + 4096, 'peer body bound')
                    raw = self.rfile.read(length)
                    require(len(raw) == length, 'peer truncated body')
                    doc = json.loads(raw)
                    identity = doc.get('id')
                    require(type(identity) is int and 1 <= identity <= options['count'], 'peer identity')
                    require(doc == {'id': identity, 'body': body(identity, options['payload'])}, 'peer payload')
                    value_hash = digest(doc['body'].encode())
                    row = {'id': identity, 'hash': value_hash, 'stage': self.path[1:],
                           'wire_sha256': digest(raw), 'received_ns': time.monotonic_ns()}
                    with owner.lock:
                        wire = (json.dumps(row, sort_keys=True) + '\n').encode()
                        view = memoryview(wire)
                        while view:
                            n = owner.fp.write(view)
                            require(n and n > 0, 'peer short ledger write')
                            view = view[n:]
                        os.fsync(owner.fp.fileno())
                        owner.rows.append(row)
                    response = json.dumps({'id': identity, 'hash': value_hash, 'durable': True}).encode()
                    self.send_response(200)
                except Exception as exc:
                    with owner.lock:
                        owner.errors.append(str(exc))
                    response = json.dumps({'error': str(exc)}).encode()
                    self.send_response(422)
                    self.close_connection = True
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(response)))
                self.end_headers()
                self.wfile.write(response)

        class Server(http.server.ThreadingHTTPServer):
            daemon_threads = True
            request_queue_size = 256
        self.server = Server(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.fp.close()


def proc_sample(pid):
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    hz = os.sysconf('SC_CLK_TCK')
    return {'ns': time.monotonic_ns(), 'cpu': (int(fields[11]) + int(fields[12])) / hz,
            'rss': int(fields[21]) * os.sysconf('SC_PAGE_SIZE')}


def launch(binary, root, mode, options, peer, *, hold=False, scan_duration=0):
    out = root / mode
    out.mkdir()
    args = [str(binary), '--mode', mode, '--dir', str(root / 'wal'), '--out', str(out),
            '--sink', peer.url, '--token', options['token'], '--count', str(options['count']),
            '--payload', str(options['payload']), '--writers', str(options['writers']),
            '--ack-percent', str(options['ack_percent']), '--scans', str(options['scans']),
            '--rotate-every', str(options.get('rotate_every', 0))]
    if options['gzip']:
        args.append('--gzip')
    if hold:
        args.append('--hold')
    if scan_duration:
        args += ['--scan-duration', str(scan_duration) + 's']
    kind = options.get('diagnostics') or ('diagnostic' if scan_duration else '')
    if kind:
        args += ['--profile', kind]
    save(out / 'command.json', args)
    samples = []
    started = time.monotonic_ns()
    guarded = [sys.executable, str(Path(__file__).with_name('supervised_exec.py')), str(os.getpid()), *args]
    observation = {}
    with open(out / 'worker.log', 'xb', buffering=0) as log:
        process = subprocess.Popen(guarded, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
        try:
            code = observe(process, log, checkpoint=(out / 'result.json') if hold else None,
                           timeout=options['timeout'], sampler=proc_sample, samples=samples,
                           metadata=observation, started_ns=started)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            save(out / 'process.json', {'returncode': process.returncode, 'held': hold,
                                      'duration_ns': time.monotonic_ns() - started,
                                      'observer': observation})
            save(out / 'resources.json', samples)
    require(code == (-9 if hold else 0), f'{mode} exited {code}; inspect worker.log')
    require((out / 'result.json').exists(), f'{mode} missing results')


def check_phases(result):
    """Validate every phase, including diagnostic scans, before computing ratios."""
    mode = result['mode']
    names = {'seed': ['open', 'append_sync_deliver_ack', 'seal'],
             'transfer': ['open', 'frontier', 'replay_transfer', 'seal'],
             'deliver': ['open', 'frontier', 'replay_deliver'],
             'verify': ['open', 'frontier', 'replay_verify'],
             'scan': ['open', 'frontier', 'scan']}
    require(mode in names, 'unknown measurement mode')
    require([p['name'] for p in result['phases']] == names[mode], 'missing/repeated phase')
    for p in result['phases']:
        require(type(p['Begin']) is int and type(p['End']) is int and p['End'] > p['Begin'], 'invalid phase interval')
        require(type(p['ops']) is int and p['ops'] >= 0, 'invalid phase work')
        expected_ops = (result['count'] if p['name'] == 'append_sync_deliver_ack' else
                        len(result['records']) if p['name'].startswith('replay_') else
                        p['ops'] if p['name'] == 'scan' else 1)
        require(p['ops'] == expected_ops, 'different phase work')
        for counter in ('cpu_seconds', 'total_alloc', 'mallocs', 'num_gc'):
            a, b = p['Before'][counter], p['After'][counter]
            require(type(a) in (int, float) and type(b) in (int, float) and
                    math.isfinite(a) and math.isfinite(b) and 0 <= a <= b, 'invalid resource counter')
        for endpoint in ('Before', 'After'):
            rss = p[endpoint]['peak_rss_kib']
            require(type(rss) is int and rss > 0, 'missing RSS observation')
        if p['name'] == 'scan':
            require(p['ops'] == len(p['latency_ns']) and p['ops'] > 0, 'scan work count')
            require(all(type(v) is int and 0 < v <= p['End'] - p['Begin'] for v in p['latency_ns']), 'invalid scan latency')


def audit_data(options, results, processes, rows, errors):
    """Reject lost/changed/invented messages, false ACK order and broken recovery."""
    require(not errors, f'downstream errors: {errors}')
    n, size = options['count'], options['payload']
    rotate = options.get('rotate_every', 0)
    require(type(rotate) is int and 0 <= rotate <= n and (not rotate or n // rotate <= 256), 'rotation workload bound')
    expected = set(range(1, n + 1))
    acknowledged = {identity for identity in expected if identity % 100 < options['ack_percent']}
    pending = expected - acknowledged
    for mode in ('seed', 'transfer', 'deliver', 'verify'):
        result = results[mode]
        require(result['mode'] == mode and result['count'] == n, 'wrong workload/stage')
        require(result.get('diagnostic', '') == options.get('diagnostics', ''), 'diagnostic metadata mismatch')
        rotations = result.get('rotations', 0)
        require(type(rotations) is int and rotations == (n // rotate if mode == 'seed' and rotate else 0), 'changed rotation workload')
        process = processes[mode]
        require(process['returncode'] == (-9 if mode in ('seed', 'transfer') else 0), 'wrong process exit')
        require(process['held'] == (mode in ('seed', 'transfer')), 'wrong checkpoint mode')
        require(type(process['duration_ns']) is int and process['duration_ns'] > 0, 'invalid process duration')
        records = result['records']
        wanted = expected if mode == 'seed' else (pending if mode in ('transfer', 'deliver') else set())
        ids = [item['id'] for item in records]
        require(len(ids) == len(set(ids)) and set(ids) == wanted, f'{mode} missing/extra records')
        if mode != 'seed':
            require(result['high'] == n, 'lost recovered frontier')
        for item in records:
            require(item['hash'] == digest(body(item['id'], size).encode()), 'changed source/replay hash')
            require(0 < item['Begin'] <= item['Written'], 'invalid operation timing')
            if mode in ('seed', 'transfer'):
                require(item['Written'] <= item['Durable'], 'missing successful Sync')
            if mode == 'deliver' or (mode == 'seed' and item['id'] in acknowledged):
                lower = item['Durable'] if mode == 'seed' else item['Written']
                require(lower <= item['Received'] <= item['Acked'] and item['Acked'] > 0, 'ACK precedes downstream receipt')
            elif mode == 'seed':
                require(item['Received'] == item['Acked'] == 0, 'invented initial ACK')
        check_phases(result)
    ledger_ids = [row['id'] for row in rows]
    require(set(ledger_ids) == expected, 'downstream missing/extra identity')
    for row in rows:
        require(row['hash'] == digest(body(row['id'], size).encode()), 'downstream hash mismatch')
        require(row['stage'] == ('seed' if row['id'] in acknowledged else 'deliver'), 'wrong replay/ACK selection')
    return len(ledger_ids) - n


def audit(root):
    root = Path(root)
    options = read(root / 'options.json')
    modes = ('seed', 'transfer', 'deliver', 'verify')
    results = {mode: read(root / mode / 'result.json') for mode in modes}
    processes = {mode: read(root / mode / 'process.json') for mode in modes}
    rows = [json.loads(line) for line in (root / 'downstream.jsonl').read_text().splitlines()]
    duplicates = audit_data(options, results, processes, rows, read(root / 'peer-errors.json'))
    if options['scans']:
        s = read(root / 'scan/result.json')
        check_phases(s)
        kind = options.get('diagnostics') or ('diagnostic' if options.get('profile_seconds') else '')
        require(s.get('diagnostic', '') == kind, 'scan diagnostic metadata mismatch')
        require(s['mode'] == 'scan' and s['count'] == options['count'], 'changed scan identity')
        require(s['high'] == options['count'] and not s['records'] and s.get('rotations', 0) == 0, 'scan mutated records/frontier')
        require(read(root / 'scan/process.json')['returncode'] == 0, 'scan failed')
        p = next(p for p in s['phases'] if p['name'] == 'scan')
        require(p['ops'] == len(p['latency_ns']) and p['ops'] > 0, 'scan work count')
        if not options.get('profile_seconds'):
            require(p['ops'] == options['scans'] * options['writers'], 'changed scan workload')
        results['scan'] = s
    n = options['count']
    duration = sum(p['duration_ns'] for p in processes.values()) / 1e9
    summary = {'diagnostic_only': bool(options.get('diagnostics') or options.get('profile_seconds')), 'count': n,
               'duplicates': duplicates, 'lifecycle_seconds': duration,
               'lifecycle_records_s': n / duration, 'phases': {}, 'passed': True}
    for mode, result in results.items():
        for p in result['phases']:
            elapsed = (p['End'] - p['Begin']) / 1e9
            cpu = p['After']['cpu_seconds'] - p['Before']['cpu_seconds']
            allocated = p['After']['total_alloc'] - p['Before']['total_alloc']
            entry = {'seconds': elapsed, 'cpu_seconds': cpu, 'allocated_bytes': allocated,
                     'peak_rss_mib': p['After']['peak_rss_kib'] / 1024, 'ops': p['ops'],
                     'cpu_cores': cpu / elapsed,
                     'gc_cycles': p['After']['num_gc'] - p['Before']['num_gc'],
                     'mallocs': p['After']['mallocs'] - p['Before']['mallocs']}
            if p['name'] == 'scan':
                work = n * p['ops']
                entry.update(records_s=work / elapsed, allocated_bytes_per_record=allocated / work,
                             cpu_us_per_record=cpu * 1e6 / work, p99_ms=percentile(p['latency_ns']) / 1e6)
            summary['phases'][mode + '/' + p['name']] = entry
    if 'measurement_method' in options:
        require(options['measurement_method'] == METHOD, 'unknown measurement method')
        summary['measurement_method'] = METHOD
        phase_ns = sum(p['End'] - p['Begin'] for mode in modes for p in results[mode]['phases'])
        process_ns = sum(p['duration_ns'] for p in processes.values())
        require(0 < phase_ns <= process_ns, 'phase time exceeds observed lifecycle')
        summary['worker_phase_seconds'] = phase_ns / 1e9
        summary['outside_phase_seconds'] = (process_ns - phase_ns) / 1e9
        summary['worker_phase_fraction'] = phase_ns / process_ns
        backends = set()
        for mode in results:
            proc = read(root / mode / 'process.json')
            require(type(proc.get('duration_ns')) is int and proc['duration_ns'] > 0,
                    'invalid observed process duration')
            require(proc.get('held') is (mode in ('seed', 'transfer')), 'wrong observed checkpoint mode')
            require(type(proc.get('returncode')) is int and
                    proc['returncode'] == (-9 if mode in ('seed', 'transfer') else 0), 'wrong observed exit')
            info = proc.get('observer', {})
            require(info.get('method') == METHOD, 'missing/mixed observer method')
            require(info.get('backend') in ('pidfd', 'pipe-poll'), 'missing observer backend')
            backends.add(info['backend'])
            require(sum(p['End'] - p['Begin'] for p in results[mode]['phases']) <= proc['duration_ns'],
                    'phase time exceeds worker observation')
            require(type(info.get('started_ns')) is int and type(info.get('exit_observed_ns')) is int
                    and 0 < info['started_ns'] <= info['exit_observed_ns'], 'invalid exit observation')
            require(info['exit_observed_ns'] - info['started_ns'] <= proc['duration_ns'], 'observation outside lifecycle')
            require(info.get('killed_at_checkpoint') is (mode in ('seed', 'transfer')), 'unsupervised checkpoint')
            if mode in ('seed', 'transfer'):
                require(type(info.get('checkpoint_ns')) is int and type(info.get('kill_sent_ns')) is int
                        and info['started_ns'] <= info['checkpoint_ns'] <= info['kill_sent_ns'] <= info['exit_observed_ns'],
                        'invalid checkpoint/kill/exit ordering')
        require(len(backends) == 1, 'mixed observer backends within trial')
        summary['observer_backends'] = sorted(backends)
    summary['seed_sync_p99_ms'] = percentile([r['Durable'] - r['Begin'] for r in results['seed']['records']]) / 1e6
    summary['latency_ms'] = {}
    for label, mode, start, end in (
            ('append', 'seed', 'Begin', 'Written'),
            ('append_sync', 'seed', 'Begin', 'Durable'),
            ('initial_delivery_ack', 'seed', 'Begin', 'Acked'),
            ('replay_transfer_sync', 'transfer', 'Begin', 'Durable'),
            ('replay_delivery_ack', 'deliver', 'Begin', 'Acked')):
        values = [r[end] - r[start] for r in results[mode]['records'] if r[end] > 0]
        summary['latency_ms'][label] = {
            'samples': len(values),
            **{f'p{int(q*100)}': percentile(values, q) / 1e6 for q in (.5, .95, .99)}}
    return summary


def run_trial(binary, root, options):
    root.mkdir(parents=True, exist_ok=False)
    (root / 'wal').mkdir()
    options = dict(options, measurement_method=METHOD)
    save(root / 'options.json', options)
    cpus, quota = capacity()
    save(root / 'environment.json', {'effective_cpus': cpus, 'cpu_max': quota,
         'platform': os.uname()._asdict() if hasattr(os.uname(), '_asdict') else list(os.uname()),
         'binary_sha256': file_digest(binary),
         'gomaxprocs': os.environ.get('GOMAXPROCS'),
         'mounts': Path('/proc/mounts').read_text(), 'cpuinfo': Path('/proc/cpuinfo').read_text()})
    peer = Peer(root / 'downstream.jsonl', options)
    try:
        launch(binary, root, 'seed', options, peer, hold=True)
        if options['scans']:
            launch(binary, root, 'scan', options, peer, scan_duration=options.get('profile_seconds', 0))
        launch(binary, root, 'transfer', options, peer, hold=True)
        launch(binary, root, 'deliver', options, peer)
        launch(binary, root, 'verify', options, peer)
    finally:
        peer.close()
        save(root / 'peer-errors.json', peer.errors)
    summary = audit(root)
    save(root / 'summary.json', summary)
    return summary


def main():
    install_signal_handlers()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit-only', type=Path)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--count', type=int, default=2048)
    parser.add_argument('--payload', type=int, default=16384)
    parser.add_argument('--writers', type=int, default=4)
    parser.add_argument('--ack-percent', type=int, default=50)
    parser.add_argument('--scans', type=int, default=4)
    parser.add_argument('--rotate-every', type=int, default=0)
    parser.add_argument('--gzip', action='store_true')
    parser.add_argument('--timeout', type=float, default=180)
    parser.add_argument('--profile-seconds', type=int, default=0)
    parser.add_argument('--diagnostics', choices=('', 'cpu', 'trace', 'contention'), default='',
                        help='diagnostic-only profiles for every lifecycle stage; never compare timings')
    args = parser.parse_args()
    if args.audit_only:
        value = audit(args.audit_only)
        old = read(args.audit_only / 'summary.json')
        require(value == old, 'stored summary differs from recomputed observations')
        print(json.dumps(value, allow_nan=False))
        return
    require(args.binary and args.out, '--binary and --out required')
    require(1 <= args.count <= 1000000 and 0 <= args.payload <= 4 << 20, 'workload bounds')
    require(args.count * (args.payload + 256) <= 2 << 30, 'synthetic disk-work bound (2 GiB)')
    require(1 <= args.writers <= 128 and 0 <= args.ack_percent <= 100, 'concurrency/ACK bounds')
    require(0 <= args.scans <= 10000 and 0 <= args.profile_seconds <= 600 and 0 < args.timeout <= 3600, 'time/scan bounds')
    require(0 <= args.rotate_every <= args.count and (not args.rotate_every or args.count // args.rotate_every <= 256), 'rotation workload bound')
    require(not args.profile_seconds or args.scans, 'profiles require scanning')
    options = {k: v for k, v in vars(args).items() if k not in ('binary', 'out', 'audit_only')}
    options['token'] = os.urandom(16).hex()
    summary = run_trial(args.binary.resolve(), args.out.resolve(), options)
    print(json.dumps(summary, allow_nan=False))


if __name__ == '__main__':
    main()
