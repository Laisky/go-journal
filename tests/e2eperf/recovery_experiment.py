#!/usr/bin/env python3
"""Apply separately measurable recovery hypotheses in a detached source tree."""
import argparse
from pathlib import Path


def apply(root, variant):
    if variant in ('batch', 'combined'):
        path = root/'serialize.go'
        text = path.read_text()
        start = text.index('func (dec *IdsDecoder) LoadMaxId()')
        end = text.index('// ReadAllToBmap', start)
        function = text[start:end]
        old = '\t\tif id > maxId {\n\t\t\tmaxId = id\n\t\t}\n'
        addition = old + '''		// Fold complete buffered deltas without per-word ReadFull copies.
		// The next scalar read preserves partial words and pending I/O errors.
		if dec.reader.Buffered() >= 8 {
			buffered, err := bufferedACKMaximum(dec.reader, dec.baseID)
			if err != nil {
				return 0, errors.Wrap(err, "read ids")
			}
			if buffered > maxId {
				maxId = buffered
			}
		}
'''
        if function.count(old) != 1 or 'bufferedACKMaximum' in function:
            raise ValueError('ACK experiment anchor changed/already applied')
        path.write_text(text[:start]+function.replace(old, addition, 1)+text[end:])
    if variant in ('directory', 'combined'):
        path = root/'fs.go'
        text = path.read_text()
        if text.count('ioutil.ReadDir(dirPath)') != 1 or text.count('[]os.FileInfo') != 1:
            raise ValueError('directory experiment anchor changed/already applied')
        text = text.replace('\t"io/ioutil"\n', '', 1).replace('[]os.FileInfo', '[]os.DirEntry', 1).replace('ioutil.ReadDir(dirPath)', 'os.ReadDir(dirPath)', 1)
        text = text.replace('// scan existing buf files.', '// Enumerate sorted names without eager FileInfo/Lstat work. The explicit\n\t// os.Stat below still validates EVERY entry, including unknown symlinks.\n\t// scan existing buf files.', 1)
        path.write_text(text)


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,required=True)
    p.add_argument('--variant',choices=('batch','directory','combined'),required=True)
    a=p.parse_args();apply(a.source,a.variant)
