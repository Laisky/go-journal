"""Validate observation provenance before attributing controller time to a worker."""
import json
from pathlib import Path

from monitor import MAX_LOG_BYTES, METHOD


def require(value, message):
    if not value:
        raise ValueError(message)


def observation_summary(root, options, results, lifecycle_processes):
    """Keep legacy reports readable, but never silently downgrade event evidence."""
    processes = dict(lifecycle_processes)
    for mode in results:
        if mode not in processes:
            processes[mode] = json.loads((Path(root)/mode/'process.json').read_text())
    method = options.get('measurement_method')
    if method is None:
        require('measurement_method' not in options and
                all('observer' not in p for p in processes.values()), 'event evidence cannot downgrade to legacy')
        return {}
    require(method == METHOD, 'unknown measurement method')
    backends = set()
    for mode, result in results.items():
        proc = processes[mode]
        held = mode in ('seed', 'transfer')
        require(type(proc.get('duration_ns')) is int and proc['duration_ns'] > 0,
                'invalid observed process duration')
        require(proc.get('held') is held, 'wrong observed checkpoint mode')
        require(type(proc.get('returncode')) is int and proc['returncode'] == (-9 if held else 0),
                'wrong observed exit')
        info = proc.get('observer')
        require(isinstance(info, dict) and info.get('method') == METHOD, 'missing/mixed observer method')
        require(info.get('completed') is True, 'observer did not complete successfully')
        require(info.get('backend') in ('pidfd', 'pipe-poll'), 'missing observer backend')
        backends.add(info['backend'])
        reason = info.get('fallback_reason')
        require(reason is None if info['backend'] == 'pidfd' else isinstance(reason, str) and bool(reason),
                'invalid observer fallback reason')
        require(type(info.get('sample_interval_ns')) is int and info['sample_interval_ns'] == 20_000_000,
                'changed resource sampling cadence')
        for key in ('samples', 'stdout_bytes'):
            require(type(info.get(key)) is int and info[key] >= 0, 'invalid observer counter')
        require(info['stdout_bytes'] <= MAX_LOG_BYTES, 'invalid observer log size')
        require(type(info.get('stdout_eof')) is bool, 'invalid observer EOF state')
        require(sum(p['End'] - p['Begin'] for p in result['phases']) <= proc['duration_ns'],
                'phase time exceeds worker observation')
        start, end = info.get('started_ns'), info.get('exit_observed_ns')
        require(type(start) is int and type(end) is int and 0 < start <= end, 'invalid exit observation')
        require(end-start <= proc['duration_ns'], 'observation outside lifecycle')
        first = info.get('first_output_ns')
        require(first is None or type(first) is int and start <= first <= end, 'invalid first output observation')
        require(info.get('killed_at_checkpoint') is held, 'unsupervised checkpoint')
        checkpoint, killed = info.get('checkpoint_ns'), info.get('kill_sent_ns')
        if held:
            require(type(first) is int and type(checkpoint) is int and type(killed) is int and
                    start <= first <= checkpoint <= killed <= end, 'invalid checkpoint/kill/exit ordering')
        else:
            require(checkpoint is None and killed is None, 'unexpected kill in non-held process')
    require(len(backends) == 1, 'mixed observer backends within trial')
    phase_ns = sum(p['End']-p['Begin'] for mode in lifecycle_processes for p in results[mode]['phases'])
    process_ns = sum(p['duration_ns'] for p in lifecycle_processes.values())
    require(0 < phase_ns <= process_ns, 'phase time exceeds observed lifecycle')
    return {'measurement_method': METHOD, 'observer_backends': sorted(backends),
            'worker_phase_seconds': phase_ns/1e9, 'outside_phase_seconds': (process_ns-phase_ns)/1e9,
            'worker_phase_fraction': phase_ns/process_ns}
