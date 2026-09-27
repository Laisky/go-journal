#!/usr/bin/env python3
"""Apply only the four live encoder writer-buffer sizes in a detached worktree.

No reader, compressor, staging, Flush, Sync or ACK operation is modified.
Keep the resulting patch and exact source with each experimental executable.
"""
import argparse
from pathlib import Path
import re


def resize(source, size):
    if type(size) is not int or size not in (256, 4096, 65536, 4194304):
        raise ValueError('explicit experiment size required')
    patterns = (
        r'(enc\.writer = msgp\.NewWriterSize\(enc\.gzWriter, )(?:BufSize|writeBufferSize|256|4096|65536|4194304)(\))',
        r'(enc\.writer = msgp\.NewWriterSize\(fp, )(?:BufSize|writeBufferSize|256|4096|65536|4194304)(\))',
        r'(enc\.writer = bufio\.NewWriterSize\(enc\.gzWriter, )(?:BufSize|writeBufferSize|256|4096|65536|4194304)(\))',
        r'(enc\.writer = bufio\.NewWriterSize\(fp, )(?:BufSize|writeBufferSize|256|4096|65536|4194304)(\))',
    )
    for pattern in patterns:
        if len(re.findall(pattern, source)) != 1:
            raise ValueError('writer constructor changed; review experiment explicitly')
        source = re.sub(pattern, lambda m: m[1] + str(size) + m[2], source)
    return source


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--size', type=int, required=True)
    args = p.parse_args()
    path = args.source / 'serialize.go'
    path.write_text(resize(path.read_text(), args.size))


if __name__ == '__main__':
    main()
