#!/usr/bin/env python3
"""Buffered-word hypothesis; only mutate a detached candidate source."""
import argparse
import json
from pathlib import Path

from recovery_negative import run_case
from supervised_exec import install_signal_handlers

OLD = '''func (dec *IdsDecoder) readOffset() (int64, error) {
\tif _, err := io.ReadFull(dec.reader, dec.word[:]); err != nil {'''
NEW = '''func (dec *IdsDecoder) readOffset() (int64, error) {
\tif dec.reader.Buffered() >= len(dec.word) {
\t\t// Both operations stay within Buffered: no I/O or pending-error change.
\t\t// Consume before returning to a potentially reentrant set callback.
\t\tp, _ := dec.reader.Peek(len(dec.word))
\t\tid := int64(bitOrder.Uint64(p))
\t\t_, _ = dec.reader.Discard(len(dec.word))
\t\treturn id, nil
\t}
\tif _, err := io.ReadFull(dec.reader, dec.word[:]); err != nil {'''


def transform(text):
    if text.count(OLD) != 1:
        raise ValueError('ACK readOffset anchor changed')
    return text.replace(OLD, NEW, 1)


def main():
    install_signal_handlers()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', required=True, type=Path)
    p.add_argument('--negative', type=Path)
    a = p.parse_args()
    root = a.source.resolve()
    path = root/'serialize.go'
    if a.negative:
        a.negative.mkdir(parents=True, exist_ok=False)
        text = path.read_text()
        old = '\t\t_, _ = dec.reader.Discard(len(dec.word))'
        if text.count(old) != 1:
            raise ValueError('cursor mutation anchor changed')
        test = 'TestACKOffsetBufferedWordAdvances'
        assertion = 'buffered ACK cursor did not advance'
        results = [run_case(root, a.negative, 'positive', test, assertion),
                   run_case(root, a.negative, 'cursor-not-advanced', test, assertion,
                            (path, text.replace(old, '\t\t// faulty no-op discard', 1)))]
        print(json.dumps(results, indent=2))
    else:
        path.write_text(transform(path.read_text()))


if __name__ == '__main__':
    main()
