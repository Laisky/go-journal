#!/usr/bin/env python3
"""Defer clock acquisition until TTL lookup actually examines the old generation."""
import argparse
import json
from pathlib import Path
from recovery_negative import run_case
from supervised_exec import install_signal_handlers


def candidate(text):
    old = '\tvar (\n\t\tt  = time.Now().UnixNano()\n\t\tvi interface{}\n\t)'
    if text.count(old) != 1 or text.count('\tif s.og != nil {') != 1:
        raise ValueError('TTL lookup clock anchors changed')
    return text.replace(old, '\tvar vi interface{}', 1).replace(
        '\tif s.og != nil {',
        '\tif s.og != nil {\n\t\t// Current-generation hits and misses without an old generation need no clock.\n\t\tt := time.Now().UnixNano()', 1)


def main():
    install_signal_handlers()
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--negative', type=Path)
    a=p.parse_args();root=a.source.resolve();path=root/'set.go';text=path.read_text()
    if a.negative:
        a.negative.mkdir(parents=True,exist_ok=False)
        old='if vi.(int64) > t {'
        if text.count(old)!=1: raise ValueError('expiry mutation anchor changed')
        test='TestTTLLookupOldGenerationDeadline';assertion='expired old ACK accepted'
        changed=text.replace(old,'if vi.(int64) > t || true {',1)
        print(json.dumps([run_case(root,a.negative,'positive',test,assertion),
                         run_case(root,a.negative,'skip-expiry',test,assertion,(path,changed))],indent=2))
    else:
        path.write_text(candidate(text))


if __name__=='__main__':main()
