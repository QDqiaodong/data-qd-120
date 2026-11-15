"""Subprocess helper for build-corpus crash, restart and concurrency tests.

The helper is test-only.  It imports the real module and injects faults that
terminate the process with ``os._exit`` so no cleanup handler runs, exactly as
an external crash would leave the release root behind.
"""
import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from label_agreement import corpus  # noqa: E402


def die(code=17):
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--corpus', required=True)
    parser.add_argument('--seed', required=True)
    parser.add_argument('--root', required=True)
    parser.add_argument('--mode', default='run')
    parser.add_argument('--delay', type=float, default=0.0)
    args = parser.parse_args()

    if args.mode == 'crash-before-files':
        # Die before the first data file has been fully written.
        def stage(root, manifest, data):
            from tempfile import mkdtemp
            mkdtemp(prefix='stage-', dir=root / 'staging')
            die()
        corpus._stage_version = stage
    elif args.mode == 'crash-mid-file':
        original_write = corpus._write_bytes
        calls = {'n': 0}

        def stage(root, manifest, data):
            staging = Path(tempfile.mkdtemp(prefix='stage-', dir=root / 'staging'))
            payload = data[corpus.SPLITS[0]]
            with open(staging / corpus.DATA_FILENAMES[corpus.SPLITS[0]], 'wb') as stream:
                stream.write(payload[:max(1, len(payload) // 2)])
            die()
        corpus._stage_version = stage
        corpus._write_bytes = original_write
    elif args.mode == 'crash-before-manifest':
        original_write = corpus._write_bytes

        def write(path, payload):
            if Path(path).name == corpus.MANIFEST_FILENAME:
                die()
            original_write(path, payload)
        corpus._write_bytes = write
    elif args.mode == 'crash-before-switch':
        corpus._switch_current = lambda root, version: die()
    elif args.mode == 'slow':
        original = corpus._write_bytes

        def write(path, payload):
            time.sleep(args.delay)
            original(path, payload)
        corpus._write_bytes = write
    elif args.mode != 'run':
        raise SystemExit('unknown mode: ' + args.mode)

    manifest = corpus.build_corpus(args.source, args.corpus, int(args.seed), args.root)
    print(json.dumps({'version': manifest['version'],
                      'line_count': {split: manifest['files'][split]['line_count']
                                     for split in corpus.SPLITS}}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
