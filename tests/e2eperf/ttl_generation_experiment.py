#!/usr/bin/env python3
"""Isolate TTL-generation storage; never patch a measured baseline."""
import argparse
import json
from pathlib import Path

from recovery_negative import run_case
from supervised_exec import install_signal_handlers


REPLACEMENTS = (
    ('\tog, ng   *sync.Map', '\tog, ng   *ttlGeneration'),
    ('\t\tng:       &sync.Map{},', '\t\tng:       newTTLGeneration(),'),
    ('\tvar vi interface{}', '\tvar vi int64'),
    ('if vi.(int64) > t {', 'if vi > t {'),
    ('\t\t\ts.ng = &sync.Map{}', '\t\t\ts.ng = newTTLGeneration()'),
)


def transform(text):
    for old, new in REPLACEMENTS:
        if text.count(old) != 1:
            raise ValueError('generation candidate anchor changed: '+old)
        text = text.replace(old, new, 1)
    return text


def apply(root, template):
    path = root/'set.go'
    changed = transform(path.read_text())
    target = root/'ttl_generation.go'
    if target.exists():
        raise ValueError('generation helper already exists')
    # These old tests inject concrete generation fixtures. Only their factory
    # expressions change; every assertion, deadline and concurrency loop stays.
    adapters = []
    for name, expected in (('ttl_lookup_test.go', 3), ('reliability_test.go', 1)):
        p = root/name
        text = p.read_text()
        old = 'ng: &sync.Map{}, og: &sync.Map{}'
        if text.count(old) != expected:
            raise ValueError('private test fixture changed: '+name)
        adapters.append((p, text.replace(old, 'ng: newTTLGeneration(), og: newTTLGeneration()')))
    path.write_text(changed)
    target.write_bytes(template.read_bytes())
    for p, text in adapters:
        p.write_text(text)


def main():
    install_signal_handlers()
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--negative', type=Path)
    a=p.parse_args();root=a.source.resolve()
    if a.negative:
        a.negative.mkdir(parents=True,exist_ok=False)
        path=root/'set.go';text=path.read_text()
        matches=[s for s in ('if vi > t {','if vi.(int64) > t {') if s in text]
        if len(matches)!=1 or text.count(matches[0])!=1: raise ValueError('expiry anchor changed')
        anchor=matches[0];test='TestTTLGenerationPublicExpiry';assertion='expired generation accepted'
        mutated=text.replace(anchor,anchor.replace(' > t',' > t || true'),1)
        print(json.dumps([run_case(root,a.negative,'positive',test,assertion),
              run_case(root,a.negative,'expiry-bypass',test,assertion,(path,mutated))],indent=2))
    else:
        apply(root,Path(__file__).with_name('ttl_generation_candidate.go.txt'))


if __name__=='__main__':main()
