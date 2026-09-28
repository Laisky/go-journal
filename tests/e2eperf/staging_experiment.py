#!/usr/bin/env python3
"""Reassign the existing data writer budget to transactional record staging.

Only apply in a fresh detached worktree, retaining the exact diff. Production
adoption requires matched E2E/resource evidence, not a construction microbenchmark.
"""
import argparse
from pathlib import Path


def transform(source):
    pairs = [
        ('\t"bytes"\n', ''),
        ('\t// Bound idle scratch retention, not the maximum accepted record size.\n\tmaxRetainedRecordBuffer = 128 << 10',
         '\t// The 4 MiB data-writer budget now belongs to private record staging.\n\t// Every validated record is still appended/flushed before Write returns.\n\tdataWriteBufferSize = 4 << 10'),
        ('record   bytes.Buffer // scratch owned by the encoder mutex', 'record   recordStage  // complete private record, protected by encoder mutex'),
        ('\tenc = &DataEncoder{\n\t\tBaseSerializer:', '\tenc = &DataEncoder{\n\t\trecord: newRecordStage(),\n\t\tBaseSerializer:'),
        ('msgp.NewWriterSize(enc.gzWriter, BufSize)', 'msgp.NewWriterSize(enc.gzWriter, dataWriteBufferSize)'),
        ('msgp.NewWriterSize(fp, BufSize)', 'msgp.NewWriterSize(fp, dataWriteBufferSize)'),
        ('\tdefer func() {\n\t\tif enc.record.Cap() > maxRetainedRecordBuffer {\n\t\t\tenc.record = bytes.Buffer{}\n\t\t} else {\n\t\t\tenc.record.Reset()\n\t\t}\n\t}()',
         '\t// Always discard oversized spill storage, including after encoding errors.\n\tdefer enc.record.Reset()'),
        ('enc.record = bytes.Buffer{}', 'enc.record = recordStage{}'),
        ('enc.writer.Write(enc.record.Bytes())', 'enc.record.appendTo(enc.writer)'),
    ]
    for old, new in pairs:
        if source.count(old) != 1:
            raise ValueError(f'staging anchor changed: {old!r}')
        source = source.replace(old, new, 1)
    return source


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    a = p.parse_args()
    path = a.source/'serialize.go'
    path.write_text(transform(path.read_text()))


if __name__ == '__main__':
    main()
