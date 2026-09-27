#!/usr/bin/env python3
"""Integrate scoped read-buffer reuse only in an explicitly selected worktree."""
import argparse
from pathlib import Path


def integrate(text):
    edits = (
        ('\tnewest := l.newestDataName()\n\tfor _, fname := range l.dataFNames {\n\t\tid, dataErr := maxDataID(fname, fname == newest)',
         '\tnewest := l.newestDataName()\n\tvar buffers scanReaderBuffers\n\tfor _, fname := range l.dataFNames {\n\t\tid, dataErr := maxDataIDWithBuffers(fname, fname == newest, &buffers)'),
        ('func maxDataID(name string, newest bool) (int64, error) {\n',
         'func maxDataID(name string, newest bool) (int64, error) {\n\treturn maxDataIDWithBuffers(name, newest, nil)\n}\n\nfunc maxDataIDWithBuffers(name string, newest bool, buffers *scanReaderBuffers) (int64, error) {\n'),
        ('\tdecoder, err := NewDataDecoder(fp, isFileGZ(name))\n\tif err != nil {\n\t\treturn 0, errors.Wrap(err, "decode recovery data header")\n\t}\n',
         '\tdecoder, err := buffers.decoder(fp, stat, isFileGZ(name))\n\tif err != nil {\n\t\treturn 0, errors.Wrap(err, "decode recovery data header")\n\t}\n\tdefer buffers.release(decoder)\n'),
    )
    for old, new in edits:
        if text.count(old) != 1:
            raise ValueError('scan integration changed; review exact patch instead of silently skipping')
        text = text.replace(old, new, 1)
    return text


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    args = p.parse_args()
    path = args.source/'legacy.go'
    path.write_text(integrate(path.read_text()))


if __name__ == '__main__':
    main()
