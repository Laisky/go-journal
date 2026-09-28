#!/usr/bin/env python3
"""Apply scoped ACK-reader reuse to an explicitly selected experiment worktree."""
import argparse
from pathlib import Path


def integrate(text):
    sections = (
        ('func (l *LegacyLoader) LoadMaxId()', '// LoadAllids'),
        ('func (l *LegacyLoader) loadAllIDs(', '// Scope each descriptor'),
        ('func (l *LegacyLoader) Clean()', '// maxDataID'),
    )
    for start, end in sections:
        before, tail = text.split(start, 1)
        section, after = tail.split(end, 1)
        loop = 'for _, name := range l.idsFNames {'
        anchor = 'readIDsFile(name, '
        if section.count(loop) != 1 or section.count(anchor) != 1 or section.count('}); err != nil {') != 1:
            raise ValueError('ACK integration anchors changed; inspect before applying')
        indent = '\t\t' if start.endswith('Clean()') else '\t'
        section = section.replace(loop, 'var ackBuffers scanIDBuffers\n' + indent + loop, 1)
        section = section.replace(anchor, 'readIDsFileWithBuffers(name, ', 1)
        section = section.replace('}); err != nil {', '}, &ackBuffers); err != nil {', 1)
        text = before + start + section + end + after
    start = 'func readIDsFile(name string, consume func(*IdsDecoder) error) (err error) {'
    before, tail = text.split(start, 1)
    _, after = tail.split('// Clean remove old legacy files', 1)
    return (before + start + '\n\treturn readIDsFileWithBuffers(name, consume, nil)\n}\n\n'
            + '// Clean remove old legacy files' + after)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    a = p.parse_args()
    path = a.source/'legacy.go'
    path.write_text(integrate(path.read_text()))


if __name__ == '__main__':
    main()
