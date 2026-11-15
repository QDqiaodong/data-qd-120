#!/usr/bin/env python3
"""Generate a bounded-memory, near-2 GiB v1 source for resource tests.

The generator streams JSON in fixed chunks (the full document is never held
in memory) and produces either:

* ``--mode exact-2gib``  : a document of exactly 2**31 bytes (legal, accepted),
* ``--mode over-2gib``   : a sparse document beyond 2**31 bytes (rejected),
* ``--mode sized``       : a small document of ``--bytes`` bytes.

Layout stays a valid v1 document at every size: up to one million items,
exactly 50 annotators and 50 labels, and at least the requested ticket count
(default two million).  Whitespace padding inside the annotations array
reaches the byte target without adding tickets, so the SQLite and report
costs stay tied to the ticket count, not the byte count.
"""
import argparse
import json
import os

CHUNK = 1 << 20
TARGET_BYTES = 2 * 1024 ** 3


def write_padding(target, written, stream):
    remaining = target - written
    while remaining > 0:
        block = min(CHUNK, remaining)
        stream.write(b' ' * block)
        written += block
        remaining -= block
    return written


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--tickets', type=int, default=2_000_000)
    parser.add_argument('--items', type=int, default=200_000)
    parser.add_argument('--annotators', type=int, default=50)
    parser.add_argument('--labels', type=int, default=50)
    parser.add_argument('--mode', choices=('exact-2gib', 'over-2gib', 'sized'),
                        default='exact-2gib')
    parser.add_argument('--bytes', type=int, default=0)
    parser.add_argument('--seed', type=int, default=20261115)
    args = parser.parse_args()

    if args.items > 1_000_000:
        parser.error('items must be <= 1,000,000')
    if args.annotators > 50 or args.labels > 50:
        parser.error('annotators and labels must be <= 50')
    if args.tickets > 20_000_000:
        parser.error('tickets must be <= 20,000,000')

    target = {'exact-2gib': TARGET_BYTES, 'over-2gib': TARGET_BYTES + CHUNK,
              'sized': args.bytes}[args.mode]
    if not target:
        parser.error('--bytes is required for --mode sized')

    state = {'seed': args.seed}

    def rng():
        # Deterministic LCG; no third-party dependency.
        state['seed'] = (1_103_515_245 * state['seed'] + 12_345) % (2 ** 31)
        return state['seed']

    with open(args.output, 'wb') as out:
        written = 0

        def emit(text):
            nonlocal written
            data = text.encode('utf-8') if isinstance(text, str) else text
            out.write(data)
            written += len(data)

        emit('{\n  "items": [')
        for k in range(args.items):
            emit(('' if k == 0 else ', ') + '"I%07d"' % k)
        emit('],\n  "annotators": [')
        for k in range(args.annotators):
            emit(('' if k == 0 else ', ') + '"A%02d"' % k)
        emit('],\n  "labels": [')
        for k in range(args.labels):
            emit(('' if k == 0 else ', ') + '"L%02d"' % k)
        emit('],\n  "annotations": [')

        # Two million unique (item, annotator) decisions: item cycles through
        # the roster while the annotator follows a distinct cycle, so a pair
        # only repeats after lcm(items, annotators) tickets (far beyond the
        # run).  Labels are deterministic pseudo-random roster entries.
        for ordinal in range(args.tickets):
            item = ordinal % args.items
            annotator = (ordinal * 7 + ordinal // args.items) % args.annotators
            label = 'L%02d' % (rng() % args.labels)
            row = {'item': 'I%07d' % item, 'annotator': 'A%02d' % annotator,
                   'label': label}
            emit(('' if ordinal == 0 else ', ') + json.dumps(row,
                 separators=(',', ':')))
        # Pad to the exact byte target with in-array whitespace (legal JSON).
        # No trailing whitespace: the strict parser rejects any byte after the
        # root closing token, so the document ends exactly on '}'.
        suffix = b']\n}'
        if target > written + len(suffix):
            written = write_padding(target - len(suffix), written, out)
        elif target <= written:
            parser.error('target size %d is smaller than payload %d'
                         % (target, written))
        emit(suffix)
        if written != target:
            parser.error('size mismatch: wrote %d, wanted %d'
                         % (written, target))
    # The document is exactly the target bytes with no trailing whitespace:
    # the strict parser rejects any byte after the root closing token.
    actual = os.path.getsize(args.output)
    if actual != target:
        parser.error('final size mismatch: %d != %d' % (actual, target))
    actual = os.path.getsize(args.output)
    print(json.dumps({'output': args.output, 'bytes': actual,
                      'tickets': args.tickets, 'items': args.items,
                      'annotators': args.annotators, 'labels': args.labels,
                      'mode': args.mode}, sort_keys=True))


if __name__ == '__main__':
    raise SystemExit(main())
