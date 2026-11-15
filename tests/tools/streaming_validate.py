#!/usr/bin/env python3
"""Independent streaming validator for v1 agreement reports.

The validator recomputes every published statistic from the original source
using its own SQLite aggregation and its own raw-byte SHA-256, then walks the
report JSON as a token stream (the report is never loaded wholesale) and
cross-checks counts, coverage, pairwise fractions, per-item votes, the source
hash and the strict report shape.  A non-zero exit means a concrete mismatch;
the output ends with a machine-readable summary line.

Usage:
    python3 streaming_validate.py --source SRC.json --report REPORT.json \
        [--expect-annotations N] [--expect-items N] [--expect-annotators N] \
        [--expect-labels N]
"""
import argparse
import hashlib
import sqlite3
import sys
import tempfile
from fractions import Fraction
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from label_agreement.tokens import StreamingJSONError, Tokenizer  # noqa: E402

ITEM_LIMIT = 1_000_000
ANNOTATION_LIMIT = 20_000_000
ROSTER_LIMIT = 50


class ValidationFailure(Exception):
    pass


def fail(message):
    raise ValidationFailure(message)


def check(condition, message):
    if not condition:
        fail(message)


# --------------------------------------------------------------------------- #
# Independent source ingest (own hash, own SQLite aggregation)
# --------------------------------------------------------------------------- #

def expect(tk, kind):
    found, value = tk.next_token()
    check(found == kind, 'source: expected %s got %s' % (kind, found))
    return value


def ident(value):
    check(isinstance(value, str) and value and value == value.strip()
          and len(value) <= 128 and all(ord(c) >= 32 for c in value),
          'source: bad identifier %r' % (value,))
    return value


def parse_source(path, con, max_bytes=2 * 1024 ** 3):
    hasher = hashlib.sha256()
    with open(path, 'rb') as stream:
        tk = Tokenizer(stream, max_bytes, hasher=hasher)
        expect(tk, '{')
        top_seen = set()
        ann_count = 0
        batch = []
        while True:
            kind, key = tk.next_token()
            if kind == '}':
                break
            check(kind == 'str', 'source: non-string top-level key')
            check(key not in top_seen, 'source: duplicate top-level key ' + key)
            top_seen.add(key)
            check(key in ('items', 'annotators', 'labels', 'annotations'),
                  'source: unknown top-level field ' + key)
            expect(tk, ':')
            if key in ('items', 'annotators', 'labels'):
                table = {'items': 'roster_items', 'annotators': 'roster_annotators',
                         'labels': 'roster_labels'}[key]
                expect(tk, '[')
                n = 0
                while True:
                    k2, name = tk.next_token()
                    if k2 == ']':
                        break
                    check(k2 == 'str', 'source: roster entry not a string')
                    name = ident(name)
                    n += 1
                    limit = ITEM_LIMIT if key == 'items' else ROSTER_LIMIT
                    check(n <= limit, 'source: %s exceeds limit' % key)
                    try:
                        con.execute('INSERT INTO %s(name) VALUES (?)' % table, (name,))
                    except sqlite3.IntegrityError:
                        fail('source: duplicate %s identifier %s' % (key, name))
                    k2, _ = tk.next_token()
                    if k2 == ']':
                        break
                    check(k2 == ',', 'source: bad roster separator')
                check(n > 0, 'source: empty roster ' + key)
            else:
                expect(tk, '[')
                while True:
                    k2, _ = tk.peek()
                    if k2 == ']':
                        tk.next_token()
                        break
                    expect(tk, '{')
                    fields = {}
                    while True:
                        k3, fkey = tk.next_token()
                        check(k3 == 'str', 'source: annotation key not a string')
                        check(fkey not in fields, 'source: duplicate annotation key ' + fkey)
                        check(fkey in ('item', 'annotator', 'label'),
                              'source: unknown annotation field ' + fkey)
                        expect(tk, ':')
                        k3, fval = tk.next_token()
                        check(k3 == 'str', 'source: annotation value not a string')
                        fields[fkey] = ident(fval)
                        k3, _ = tk.next_token()
                        if k3 == '}':
                            break
                        check(k3 == ',', 'source: bad annotation separator')
                    check(set(fields) == {'item', 'annotator', 'label'},
                          'source: annotation missing a required field')
                    ann_count += 1
                    check(ann_count <= ANNOTATION_LIMIT, 'source: annotation limit exceeded')
                    batch.append((fields['item'], fields['annotator'], fields['label']))
                    if len(batch) == 1000:
                        insert_tickets(con, batch)
                        batch.clear()
                    k2, _ = tk.next_token()
                    if k2 == ']':
                        break
                    check(k2 == ',', 'source: bad annotations separator')
                if batch:
                    insert_tickets(con, batch)
            kind, _ = tk.next_token()
            if kind == '}':
                break
            check(kind == ',', 'source: bad top-level separator')
        tk.expect_end()
    check(top_seen == {'items', 'annotators', 'labels', 'annotations'},
          'source: missing top-level fields')
    con.commit()
    return hasher.hexdigest(), ann_count


def insert_tickets(con, rows):
    for item, annotator, label in rows:
        check(con.execute('SELECT 1 FROM roster_items WHERE name=?', (item,)).fetchone(),
              'source: unknown item ' + item)
        check(con.execute('SELECT 1 FROM roster_annotators WHERE name=?',
                          (annotator,)).fetchone(),
              'source: unknown annotator ' + annotator)
        check(con.execute('SELECT 1 FROM roster_labels WHERE name=?', (label,)).fetchone(),
              'source: unknown label ' + label)
    try:
        con.executemany('INSERT INTO tickets(item,annotator,label) VALUES (?,?,?)', rows)
    except sqlite3.IntegrityError:
        fail('source: duplicate item/annotator decision')


def expected_basics(con):
    """Bounded aggregates: annotators/labels are <=50 each; the item roster
    is exposed only via a streaming cursor, never as a resident list."""
    item_count = con.execute('SELECT COUNT(*) FROM roster_items').fetchone()[0]
    annotators = [r[0] for r in con.execute(
        'SELECT name FROM roster_annotators ORDER BY name')]
    labels = [r[0] for r in con.execute(
        'SELECT name FROM roster_labels ORDER BY name')]
    counts = dict(con.execute('SELECT annotator, COUNT(*) FROM tickets GROUP BY annotator'))
    coverage = {}
    for a in annotators:
        labeled = counts.get(a, 0)
        coverage[a] = (labeled, item_count - labeled,
                       str(Fraction(labeled, item_count)) if item_count else None)
    total = con.execute('SELECT COUNT(*) FROM tickets').fetchone()[0]
    return {
        'item_count': item_count, 'annotators': annotators, 'labels': labels,
        'coverage': coverage, 'annotation_count': total,
    }


def iter_item_rows(con, annotators, labels):
    """Yield one expected item view at a time; the full item table is never
    resident (each view is bounded by the 50-annotator roster)."""
    items = con.execute('SELECT name FROM roster_items ORDER BY name')
    ticket_cursor = con.execute(
        'SELECT item,annotator,label FROM tickets ORDER BY item,annotator')
    row = ticket_cursor.fetchone()
    for (item_name,) in items:
        votes = {}
        while row is not None and row[0] == item_name:
            votes[row[1]] = row[2]
            row = ticket_cursor.fetchone()
        n = len(votes)
        vc = {label: 0 for label in labels}
        for label in votes.values():
            vc[label] += 1
        winners = [label for label, c in vc.items() if c * 2 > n]
        status = 'unlabeled' if not n else ('majority' if winners else 'no_strict_majority')
        yield item_name, {
            'annotated_by': sorted(votes),
            'missing': sorted(set(annotators) - set(votes)),
            'votes': vc, 'status': status,
            'consensus': winners[0] if winners else None}
    check(row is None, 'source: ticket outside roster during item merge')


def iter_pair_stats(con, annotators, labels):
    """Yield each pair's exact statistics on demand; only one 50x50
    confusion table is resident at a time."""
    for i, left in enumerate(annotators):
        for right in annotators[i + 1:]:
            rows = con.execute(
                'SELECT a.label,b.label,COUNT(*) FROM tickets a JOIN tickets b '
                'ON a.item=b.item WHERE a.annotator=? AND b.annotator=? '
                'GROUP BY a.label,b.label', (left, right)).fetchall()
            n = sum(c for _, _, c in rows)
            table = {(x, y): 0 for x in labels for y in labels}
            rmargin = {x: 0 for x in labels}
            cmargin = {x: 0 for x in labels}
            for rl, cl, c in rows:
                table[(rl, cl)] = c
                rmargin[rl] += c
                cmargin[cl] += c
            if not n:
                yield left, right, {'paired_items': 0, 'po': None, 'pe': None,
                                    'kappa': None, 'reason': 'no_paired_items',
                                    'table': table}
                continue
            po = Fraction(sum(table[(x, x)] for x in labels), n)
            pe = sum((Fraction(rmargin[x] * cmargin[x], n * n) for x in labels), Fraction())
            kappa = (po - pe) / (1 - pe) if pe != 1 else None
            yield left, right, {
                'paired_items': n, 'po': str(po), 'pe': str(pe),
                'kappa': str(kappa) if kappa is not None else None,
                'reason': 'constant_identical_marginals' if pe == 1 else None,
                'table': table}



# --------------------------------------------------------------------------- #
# Token-stream report walker: checks are driven directly off the tokenizer,
# so even a multi-hundred-MiB report never becomes a resident Python object.
# --------------------------------------------------------------------------- #

class ReportStream:
    def __init__(self, path):
        self._file = open(path, 'rb')
        self.tk = Tokenizer(self._file, 2 * 1024 ** 3)

    def close(self):
        self._file.close()

    def take(self, kind):
        found, value = self.tk.next_token()
        check(found == kind, 'report: expected %s got %s' % (kind, found))
        return value

    def scalar(self):
        kind, value = self.tk.next_token()
        check(kind in ('str', 'num', 'lit'), 'report: expected scalar got ' + kind)
        if kind == 'lit':
            check(value in ('true', 'false', 'null'),
                  'report: disallowed literal ' + value)
            return {'true': True, 'false': False, 'null': None}[value]
        return value

    def expect_key(self, allowed):
        kind, key = self.tk.next_token()
        check(kind == 'str', 'report: expected object key')
        check(key in allowed, 'report: unexpected key ' + key)
        self.take(':')
        return key

    def skip_value(self):
        kind, _ = self.tk.next_token()
        depth = 0
        while True:
            if kind in ('{', '['):
                depth += 1
            elif kind in ('}', ']'):
                depth -= 1
                if depth == 0:
                    return
            kind, _ = self.tk.next_token()


def _read_confusion(stream, expected_labels, want_table):
    stream.take('{')
    kind, _ = stream.tk.peek()
    seen = []
    if kind != '}':
        while True:
            rlabel = stream.take('str')
            check(rlabel in expected_labels, 'report: unknown confusion row label')
            seen.append(rlabel)
            stream.take(':')
            stream.take('{')
            k2, _ = stream.tk.peek()
            if k2 != '}':
                while True:
                    clabel = stream.take('str')
                    check(clabel in expected_labels, 'report: unknown confusion column label')
                    stream.take(':')
                    cell = stream.scalar()
                    check(cell == want_table[(rlabel, clabel)],
                          'report: confusion cell mismatch %s/%s' % (rlabel, clabel))
                    k2, _ = stream.tk.next_token()
                    if k2 == '}':
                        break
                    check(k2 == ',', 'report: bad confusion separator')
            else:
                stream.tk.next_token()
            kind, _ = stream.tk.next_token()
            if kind == '}':
                break
            check(kind == ',', 'report: bad confusion row separator')
    else:
        stream.tk.next_token()
    check(seen == expected_labels, 'report: confusion rows missing/extra labels')


def validate_report(path, con, expected, source_hash, expectations):
    stream = ReportStream(path)
    try:
        stream.take('{')
        top_seen = set()
        while True:
            kind, _ = stream.tk.peek()
            if kind == '}':
                stream.tk.next_token()
                break
            key = stream.expect_key({'schema_version', 'source_sha256', 'result'})
            check(key not in top_seen, 'report: duplicate top-level key ' + key)
            top_seen.add(key)
            if key == 'schema_version':
                check(stream.scalar() == 1, 'report: schema_version is not 1')
            elif key == 'source_sha256':
                check(stream.scalar() == source_hash, 'report: source_sha256 mismatch')
            else:
                _validate_result(stream, con, expected, expectations)
            kind, _ = stream.tk.next_token()
            if kind == '}':
                break
            check(kind == ',', 'report: bad top-level separator')
        check(top_seen == {'schema_version', 'source_sha256', 'result'},
              'report: top-level fields differ')
        stream.tk.expect_end(allow_trailing_lf=True)
    finally:
        stream.close()


def _validate_result(stream, con, expected, expectations):
    labels = expected['labels']
    annotators = expected['annotators']
    item_count = expected['item_count']
    stream.take('{')
    seen = set()
    while True:
        kind, _ = stream.tk.peek()
        if kind == '}':
            stream.tk.next_token()
            break
        key = stream.expect_key({'annotation_count', 'annotator_count', 'coverage',
                                 'interpretation', 'item_count', 'items', 'pairs'})
        check(key not in seen, 'report: duplicate result key ' + key)
        seen.add(key)
        if key == 'annotation_count':
            value = stream.scalar()
            check(value == expected['annotation_count'], 'report: annotation_count mismatch')
            check(value == expectations.get('annotations', value),
                  'report: expected annotation count argument mismatch')
        elif key == 'annotator_count':
            value = stream.scalar()
            check(value == len(annotators), 'report: annotator_count mismatch')
            check(value == expectations.get('annotators', value),
                  'report: expected annotator count argument mismatch')
        elif key == 'item_count':
            value = stream.scalar()
            check(value == item_count, 'report: item_count mismatch')
            check(value == expectations.get('items', value),
                  'report: expected item count argument mismatch')
        elif key == 'interpretation':
            check(isinstance(stream.scalar(), str), 'report: interpretation shape')
        elif key == 'coverage':
            _validate_coverage(stream, expected)
        elif key == 'items':
            _validate_items(stream, con, expected)
        elif key == 'pairs':
            _validate_pairs(stream, con, expected)
        kind, _ = stream.tk.next_token()
        if kind == '}':
            break
        check(kind == ',', 'report: bad result separator')
    check(seen == {'annotation_count', 'annotator_count', 'coverage', 'interpretation',
                   'item_count', 'items', 'pairs'}, 'report: result field set differs')
    check(len(labels) == expectations.get('labels', len(labels)),
          'report: expected label count argument mismatch')


def _validate_coverage(stream, expected):
    stream.take('{')
    kind, _ = stream.tk.peek()
    seen = []
    if kind == '}':
        stream.tk.next_token()
    else:
        while True:
            annotator = stream.take('str')
            seen.append(annotator)
            stream.take(':')
            stream.take('{')
            fields = set()
            row = {}
            while True:
                k2, _ = stream.tk.peek()
                if k2 == '}':
                    stream.tk.next_token()
                    break
                fkey = stream.expect_key({'fraction', 'labeled', 'missing'})
                check(fkey not in fields, 'report: duplicate coverage key')
                fields.add(fkey)
                row[fkey] = stream.scalar()
                k2, _ = stream.tk.next_token()
                if k2 == '}':
                    break
                check(k2 == ',', 'report: bad coverage separator')
            check(fields == {'fraction', 'labeled', 'missing'}, 'report: coverage shape')
            labeled, missing, fraction = expected['coverage'][annotator]
            check(row['labeled'] == labeled and row['missing'] == missing
                  and row['fraction'] == fraction,
                  'report: coverage mismatch for ' + annotator)
            kind, _ = stream.tk.next_token()
            if kind == '}':
                break
            check(kind == ',', 'report: bad coverage row separator')
    check(seen == expected['annotators'], 'report: coverage annotator set/order mismatch')


def _validate_items(stream, con, expected):
    stream.take('[')
    expected_rows = iter_item_rows(con, expected['annotators'], expected['labels'])
    count = 0
    kind, _ = stream.tk.peek()
    if kind == ']':
        stream.tk.next_token()
    else:
        while True:
            try:
                item, want = next(expected_rows)
            except StopIteration:
                fail('report: more item rows than the roster contains')
            stream.take('{')
            fields = set()
            row = {}
            while True:
                k2, _ = stream.tk.peek()
                if k2 == '}':
                    stream.tk.next_token()
                    break
                fkey = stream.expect_key({'annotated_by', 'consensus', 'item',
                                          'missing_annotators', 'status', 'votes'})
                check(fkey not in fields, 'report: duplicate item key ' + fkey)
                fields.add(fkey)
                if fkey in ('annotated_by', 'missing_annotators'):
                    row[fkey] = _read_string_array(stream)
                elif fkey == 'votes':
                    row[fkey] = _read_votes(stream, expected['labels'])
                else:
                    row[fkey] = stream.scalar()
                k2, _ = stream.tk.next_token()
                if k2 == '}':
                    break
                check(k2 == ',', 'report: bad item separator')
            check(fields == {'annotated_by', 'consensus', 'item', 'missing_annotators',
                             'status', 'votes'}, 'report: item row shape mismatch')
            check(row['item'] == item, 'report: item ordering/roster mismatch')
            check(row['annotated_by'] == want['annotated_by'],
                  'report: annotated_by mismatch for ' + item)
            check(row['missing_annotators'] == want['missing'],
                  'report: missing_annotators mismatch for ' + item)
            check(row['status'] == want['status'] and row['consensus'] == want['consensus'],
                  'report: status/consensus mismatch for ' + item)
            check(row['votes'] == want['votes'], 'report: votes mismatch for ' + item)
            count += 1
            kind, _ = stream.tk.next_token()
            if kind == ']':
                break
            check(kind == ',', 'report: bad item array separator')
    # The generator raises if a ticket falls outside the roster; exhaust it to
    # confirm every roster item was represented exactly once.
    leftover = sum(1 for _ in expected_rows)
    check(count + leftover == expected['item_count'],
          'report: item array length mismatch')


def _read_string_array(stream):
    stream.take('[')
    result = []
    kind, _ = stream.tk.peek()
    if kind == ']':
        stream.tk.next_token()
        return result
    while True:
        result.append(stream.take('str'))
        kind, _ = stream.tk.next_token()
        if kind == ']':
            return result
        check(kind == ',', 'report: bad string-array separator')


def _read_votes(stream, labels):
    stream.take('{')
    result = {}
    kind, _ = stream.tk.peek()
    if kind == '}':
        stream.tk.next_token()
        return result
    while True:
        label = stream.take('str')
        check(label in labels, 'report: unknown vote label')
        stream.take(':')
        result[label] = stream.scalar()
        kind, _ = stream.tk.next_token()
        if kind == '}':
            return result
        check(kind == ',', 'report: bad votes separator')


def _validate_pairs(stream, con, expected):
    stream.take('[')
    pair_stats = iter_pair_stats(con, expected['annotators'], expected['labels'])
    count = 0
    kind, _ = stream.tk.peek()
    if kind == ']':
        stream.tk.next_token()
    else:
        while True:
            try:
                left, right, want = next(pair_stats)
            except StopIteration:
                fail('report: more pair rows than annotator combinations')
            stream.take('{')
            fields = set()
            scalars = {}
            while True:
                k2, _ = stream.tk.peek()
                if k2 == '}':
                    stream.tk.next_token()
                    break
                fkey = stream.expect_key({'chance_agreement', 'cohen_kappa', 'confusion',
                                          'left', 'observed_agreement', 'paired_items',
                                          'right', 'undefined_reason'})
                check(fkey not in fields, 'report: duplicate pair key ' + fkey)
                fields.add(fkey)
                if fkey == 'confusion':
                    _read_confusion(stream, expected['labels'], want['table'])
                else:
                    scalars[fkey] = stream.scalar()
                k2, _ = stream.tk.next_token()
                if k2 == '}':
                    break
                check(k2 == ',', 'report: bad pair separator')
            check(fields == {'chance_agreement', 'cohen_kappa', 'confusion', 'left',
                             'observed_agreement', 'paired_items', 'right',
                             'undefined_reason'}, 'report: pair shape mismatch')
            check(scalars['left'] == left and scalars['right'] == right,
                  'report: pair identity mismatch')
            check(scalars['paired_items'] == want['paired_items'], 'report: paired_items mismatch')
            check(scalars['observed_agreement'] == want['po'], 'report: Po mismatch')
            check(scalars['chance_agreement'] == want['pe'], 'report: Pe mismatch')
            check(scalars['cohen_kappa'] == want['kappa'], 'report: kappa mismatch')
            check(scalars['undefined_reason'] == want['reason'],
                  'report: undefined_reason mismatch')
            count += 1
            kind, _ = stream.tk.next_token()
            if kind == ']':
                break
            check(kind == ',', 'report: bad pair array separator')
    # Exhaust the generator to confirm every annotator combination appeared.
    leftover = sum(1 for _ in pair_stats)
    expected_pairs = len(expected['annotators']) * (len(expected['annotators']) - 1) // 2
    check(count + leftover == expected_pairs, 'report: pair array length mismatch')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--report', required=True)
    parser.add_argument('--expect-annotations', type=int, default=None)
    parser.add_argument('--expect-items', type=int, default=None)
    parser.add_argument('--expect-annotators', type=int, default=None)
    parser.add_argument('--expect-labels', type=int, default=None)
    args = parser.parse_args()

    # A file-backed temp DB keeps this independent validator's own RSS bounded
    # on the multi-million-ticket scale run; it is deleted on exit.
    import os
    fd, db_name = tempfile.mkstemp(prefix='.validate-', suffix='.sqlite')
    os.close(fd)
    db_path = Path(db_name)
    con = sqlite3.connect(str(db_path))
    con.execute('PRAGMA temp_store=FILE')
    con.execute('PRAGMA cache_size=-2048')
    con.executescript(
        'CREATE TABLE roster_items(name TEXT PRIMARY KEY) WITHOUT ROWID;'
        'CREATE TABLE roster_annotators(name TEXT PRIMARY KEY) WITHOUT ROWID;'
        'CREATE TABLE roster_labels(name TEXT PRIMARY KEY) WITHOUT ROWID;'
        'CREATE TABLE tickets(item TEXT NOT NULL,annotator TEXT NOT NULL,'
        'label TEXT NOT NULL,PRIMARY KEY(item,annotator)) WITHOUT ROWID;'
        'CREATE INDEX ta ON tickets(annotator,item);')
    expected = None
    try:
        source_hash, ann_count = parse_source(args.source, con)
        expected = expected_basics(con)
        expectations = {
            'annotations': args.expect_annotations,
            'items': args.expect_items,
            'annotators': args.expect_annotators,
            'labels': args.expect_labels,
        }
        expectations = {k: v for k, v in expectations.items() if v is not None}
        validate_report(args.report, con, expected, source_hash, expectations)
    except (ValidationFailure, StreamingJSONError, sqlite3.Error, OSError) as error:
        print('VALIDATION FAILED: %s' % error)
        return 2
    finally:
        con.close()
        try:
            db_path.unlink()
        except OSError:
            pass
    print('VALIDATION OK items=%d annotators=%d labels=%d annotations=%d source_sha256=%s'
          % (expected['item_count'], len(expected['annotators']),
             len(expected['labels']), ann_count, source_hash))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
