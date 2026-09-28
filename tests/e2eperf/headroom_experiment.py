#!/usr/bin/env python3
"""Isolate framing slack from the measured two-part staging implementation.

This candidate reserves 4 KiB for a common power-of-two payload's envelope.
It does not bound arbitrary metadata or remove support for larger records.
Both source and adjusted capacity-boundary tests are retained in candidate.patch.
"""
import argparse
from pathlib import Path


def transform(repo):
    path = repo/'record_stage.go'
    source = path.read_text()
    old = 'arena := make([]byte, BufSize)'
    if source.count(old) != 1 or 'recordStageHeadroom' in source:
        raise ValueError('framing-slack hypothesis no longer applies')
    source = source.replace(old, 'arena := make([]byte, BufSize+recordStageHeadroom)', 1)
    source += '\n// Framing slack avoids an extra append for a 4 MiB payload plus a small\n// MessagePack envelope. Larger metadata/payloads still use overflow storage.\nconst recordStageHeadroom = 4 << 10\n'
    path.write_text(source)
    tests = repo/'record_stage_test.go'
    original = tests.read_text()
    if 'recordStageTestCapacity' in original or original.count('BufSize') < 5:
        raise ValueError('capacity-boundary tests no longer match the hypothesis')
    # These are buffer-boundary tests, so move them WITH the buffer capacity.
    # The public E2E payloads remain frozen and identical between the binaries.
    original = original.replace('BufSize', 'recordStageTestCapacity')
    original += '''
const recordStageTestCapacity = BufSize + recordStageHeadroom

func TestRecordStageFramedPayloadUsesOneAppend(t *testing.T) {
    s := newRecordStage()
    s.WriteString(strings.Repeat("x", BufSize+256))
    w := &stageFailWriter{failCall: 10}
    if n, err := s.appendTo(w); err != nil || n != BufSize+256 || w.call != 1 {
        t.Fatal("framed payload still needs a second append", n, err, w.call)
    }
    if len(s.arena) != BufSize+(4<<10) || s.overflow.Len() != 0 {
        t.Fatal("framing slack is not bounded", len(s.arena), s.overflow.Len())
    }
}
'''
    tests.write_text(original)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    args = parser.parse_args()
    transform(args.source)


if __name__ == '__main__':
    main()
