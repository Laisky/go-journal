#!/usr/bin/env python3
"""Isolate ACK-only buffering without altering data/compressor/Sync behavior."""
import argparse
import json
from pathlib import Path

from recovery_negative import run_case
from supervised_exec import install_signal_handlers


def candidate(text):
    start = text.index('func NewIdsEncoder(')
    end = text.index('// NewIdsDecoder', start)
    part = text[start:end]
    if part.count('bufio.NewWriterSize(') != 2 or part.count(', BufSize)') != 2:
        raise ValueError('ACK-only constructor anchors changed')
    part = part.replace(', BufSize)', ', len(enc.word))')
    # The compressor's own buffer setting is intentionally unchanged.
    return text[:start] + part + text[end:]


def missing_flush(text):
    start = text.index('func (enc *IdsEncoder) Write(')
    end = text.index('// Flush flush buf to fp', start)
    old = '\tif err = enc.writer.Flush(); err != nil {\n\t\treturn errors.Wrap(err, "flush journal record")\n\t}\n'
    part = text[start:end]
    if part.count(old) != 1:
        raise ValueError('ACK Flush mutation anchor changed')
    return text[:start] + part.replace(old, '', 1) + text[end:]


def main():
    install_signal_handlers()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--negative', type=Path)
    a = p.parse_args()
    root = a.source.resolve()
    path = root/'serialize.go'
    text = path.read_text()
    if a.negative:
        a.negative.mkdir(parents=True, exist_ok=False)
        test = 'TestACKWriterBudgetScalarCallShape'
        assertion = 'ACK write did not reach sink exactly once before return'
        outcomes = [run_case(root, a.negative, 'positive', test, assertion),
                    run_case(root, a.negative, 'no-ack-flush', test, assertion, (path, missing_flush(text)))]
        print(json.dumps(outcomes, indent=2))
    else:
        path.write_text(candidate(text))


if __name__ == '__main__':
    main()
