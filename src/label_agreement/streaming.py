"""Streaming v1 analysis pipeline: parse -> temp SQLite -> stream the report.

The default v1 command line accepts sources up to 2 GiB (up to one million
roster items and twenty million annotations) while the process stays bounded:
the source file, the rosters, the full ticket table and the complete report
never become resident Python objects.  A temporary SQLite database holds the
rosters and tickets, every aggregate is streamed or computed on bounded
inputs (at most 50 annotators and 50 labels), and the report itself is
serialized through a structural writer straight into the staging file.

The output is byte-identical to the report produced by
:func:`label_agreement.model.analyze` plus :func:`label_agreement.common.save`
(sorted keys, ``indent=2``, trailing LF); the field-by-field equivalence is
covered by the test suite.
"""
import json
import os
import sqlite3
import tempfile
from fractions import Fraction
from pathlib import Path

from .common import ident
from .tokens import StreamingJSONError, Tokenizer

open_builtin = open

SCHEMA_VERSION = 1
ITEM_LIMIT = 1_000_000
ANNOTATOR_LIMIT = 50
LABEL_LIMIT = 50
ANNOTATION_LIMIT = 20_000_000
SOURCE_MAX_BYTES = 2 * 1024 ** 3

_TOP_FIELDS = ('annotations', 'annotators', 'items', 'labels')
_ROSTER_FIELDS = ('annotators', 'items', 'labels')

_SCHEMA = """
CREATE TABLE roster_items(name TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE roster_annotators(name TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE roster_labels(name TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE ticket_stage(rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                          item TEXT NOT NULL, annotator TEXT NOT NULL,
                          label TEXT NOT NULL);
CREATE TABLE tickets(item TEXT NOT NULL, annotator TEXT NOT NULL,
                     label TEXT NOT NULL, PRIMARY KEY (item, annotator))
WITHOUT ROWID;
CREATE INDEX tickets_annotator ON tickets(annotator, item);
"""


class StreamError(ValueError):
    """Raised when a streamed v1 source violates the protocol."""


# --------------------------------------------------------------------------- #
# Structural parser over the streaming tokenizer
# --------------------------------------------------------------------------- #

def _expect(tokenizer, kind):
    found, _ = tokenizer.next_token()
    if found != kind:
        raise StreamError('expected %s token, got %s' % (kind, found))


def _read_identifier(tokenizer, field):
    kind, value = tokenizer.next_token()
    if kind != 'str':
        raise StreamError(field + ' must be a JSON string')
    try:
        return ident(value)
    except ValueError as error:
        raise StreamError(str(error))


def _duplicate_key(key, seen, where):
    if key in seen:
        raise StreamError('duplicate JSON key %r in %s' % (key, where))
    seen.add(key)


def _parse_roster(tokenizer, key, limit, connection):
    """Stream one identifier roster array into its SQLite table.

    Uniqueness is enforced by the table's primary key, so even a million-item
    roster never needs a Python set resident in memory.
    """
    _expect(tokenizer, '[')
    count = 0
    kind, _ = tokenizer.peek()
    if kind == ']':
        tokenizer.next_token()
        raise StreamError(key + ' roster must be nonempty')
    table = 'roster_' + key
    while True:
        name = _read_identifier(tokenizer, key + ' entry')
        count += 1
        if count > limit:
            raise StreamError(key + ' exceeds the limit of %d entries' % limit)
        try:
            connection.execute('INSERT INTO %s(name) VALUES (?)' % table, (name,))
        except sqlite3.IntegrityError:
            raise StreamError('duplicate %s identifier: %s' % (key, name))
        kind, _ = tokenizer.next_token()
        if kind == ']':
            break
        if kind != ',':
            raise StreamError('expected , or ] in ' + key + ' roster')
    return count


def _parse_annotations(tokenizer, connection):
    """Stream annotation rows straight into SQLite with reference checks."""
    _expect(tokenizer, '[')
    count = 0
    rows = []
    kind, _ = tokenizer.peek()
    if kind == ']':
        tokenizer.next_token()
        return 0
    while True:
        _expect(tokenizer, '{')
        entry_keys = set()
        item = annotator = label = None
        while True:
            kkind, key = tokenizer.next_token()
            if kkind != 'str':
                raise StreamError('annotation keys must be JSON strings')
            _duplicate_key(key, entry_keys, 'annotation')
            _expect(tokenizer, ':')
            if key == 'item':
                item = _read_identifier(tokenizer, 'annotation item')
            elif key == 'annotator':
                annotator = _read_identifier(tokenizer, 'annotation annotator')
            elif key == 'label':
                label = _read_identifier(tokenizer, 'annotation label')
            else:
                raise StreamError('unknown annotation field: ' + key)
            kind, _ = tokenizer.next_token()
            if kind == '}':
                break
            if kind != ',':
                raise StreamError('expected , or } in annotation')
        if item is None or annotator is None or label is None:
            raise StreamError('each annotation requires item, annotator and label')
        count += 1
        if count > ANNOTATION_LIMIT:
            raise StreamError('annotations exceed the limit of %d rows' % ANNOTATION_LIMIT)
        rows.append((item, annotator, label))
        if len(rows) == 1000:
            _insert_ticket_batch(connection, rows)
            rows.clear()
        kind, _ = tokenizer.next_token()
        if kind == ']':
            break
        if kind != ',':
            raise StreamError('expected , or ] in annotations')
    if rows:
        _insert_ticket_batch(connection, rows)
    return count


def _insert_ticket_batch(connection, rows):
    """Buffer parsed rows; references are validated only after the full
    document is parsed, because annotations may legally precede the rosters."""
    connection.executemany(
        'INSERT INTO ticket_stage(item,annotator,label) VALUES (?,?,?)', rows)


def _validate_and_promote_tickets(connection):
    """Enforce references and unique decisions once every roster is known."""
    problems = connection.execute(
        'SELECT item FROM ticket_stage WHERE item NOT IN '
        '(SELECT name FROM roster_items) LIMIT 1').fetchone()
    if problems:
        raise StreamError('unknown annotation item reference: ' + problems[0])
    problems = connection.execute(
        'SELECT annotator FROM ticket_stage WHERE annotator NOT IN '
        '(SELECT name FROM roster_annotators) LIMIT 1').fetchone()
    if problems:
        raise StreamError('unknown annotation annotator reference: ' + problems[0])
    problems = connection.execute(
        'SELECT label FROM ticket_stage WHERE label NOT IN '
        '(SELECT name FROM roster_labels) LIMIT 1').fetchone()
    if problems:
        raise StreamError('unknown annotation label: ' + problems[0])
    try:
        connection.execute(
            'INSERT INTO tickets(item,annotator,label) '
            'SELECT item,annotator,label FROM ticket_stage ORDER BY rowid')
    except sqlite3.IntegrityError:
        raise StreamError('duplicate item/annotator decision')
    connection.execute('DROP TABLE ticket_stage')


def parse_source(path, connection, max_bytes=SOURCE_MAX_BYTES):
    """Strictly parse a v1 source file into the SQLite connection.

    Top-level fields and their arrays may appear in any order, including
    annotations before the rosters; duplicate JSON keys are rejected at every
    object level.  Returns the source byte SHA-256 hex digest.
    """
    dest = Path(path)
    with dest.open('rb') as stream:
        tokenizer = Tokenizer(stream, max_bytes)
        tokenizer.reject_bom()
        _expect(tokenizer, '{')
        seen = set()
        while True:
            kkind, key = tokenizer.next_token()
            if kkind == 'eof':
                raise StreamError('unterminated top-level object')
            if kkind == '}':
                break
            if kkind != 'str':
                raise StreamError('top-level keys must be JSON strings')
            _duplicate_key(key, seen, 'top-level object')
            if key not in _TOP_FIELDS:
                raise StreamError('unknown top-level field: ' + key)
            _expect(tokenizer, ':')
            if key in _ROSTER_FIELDS:
                limit = ITEM_LIMIT if key == 'items' else (
                    ANNOTATOR_LIMIT if key == 'annotators' else LABEL_LIMIT)
                _parse_roster(tokenizer, key, limit, connection)
            elif key == 'annotations':
                _parse_annotations(tokenizer, connection)
            kind, _ = tokenizer.next_token()
            if kind == 'eof':
                raise StreamError('unterminated top-level object')
            if kind == '}':
                break
            if kind != ',':
                raise StreamError('expected , or } at top level')
        tokenizer.expect_end()
    for key in _TOP_FIELDS:
        if key not in seen:
            raise StreamError('missing top-level field: ' + key)
    # Annotations may precede the rosters, so referential integrity and the
    # unique item/annotator decision rule are enforced only now.
    _validate_and_promote_tickets(connection)
    connection.commit()
    return tokenizer.hexdigest()


# --------------------------------------------------------------------------- #
# Exact statistics computed out of SQLite with bounded Python state
# --------------------------------------------------------------------------- #

def _fraction_text(numerator, denominator):
    return str(Fraction(numerator, denominator)) if denominator else None


def _pairwise(connection, left, right, labels):
    """Exact pairwise agreement for one annotator pair via a SQL self-join."""
    rows = connection.execute(
        'SELECT a.label, b.label, COUNT(*) FROM tickets a '
        'JOIN tickets b ON a.item=b.item '
        'WHERE a.annotator=? AND b.annotator=? GROUP BY a.label, b.label',
        (left, right)).fetchall()
    n = sum(count for _, _, count in rows)
    table = {x: {y: 0 for y in labels} for x in labels}
    row_marginal = {x: 0 for x in labels}
    col_marginal = {x: 0 for x in labels}
    for row_label, col_label, count in rows:
        table[row_label][col_label] = count
        row_marginal[row_label] += count
        col_marginal[col_label] += count
    if not n:
        return {'paired_items': 0, 'observed_agreement': None,
                'chance_agreement': None, 'cohen_kappa': None,
                'undefined_reason': 'no_paired_items', 'confusion': table}
    agreed = sum(table[x][x] for x in labels)
    po = Fraction(agreed, n)
    pe = sum((Fraction(row_marginal[x] * col_marginal[x], n * n)
              for x in labels), Fraction())
    kappa = (po - pe) / (1 - pe) if pe != 1 else None
    return {'paired_items': n, 'observed_agreement': str(po),
            'chance_agreement': str(pe),
            'cohen_kappa': str(kappa) if kappa is not None else None,
            'undefined_reason': 'constant_identical_marginals' if pe == 1 else None,
            'confusion': table}


def _coverage(connection, item_count, annotators):
    counts = dict(connection.execute(
        'SELECT annotator, COUNT(*) FROM tickets GROUP BY annotator'))
    result = {}
    for annotator in annotators:
        labeled = counts.get(annotator, 0)
        result[annotator] = {
            'fraction': _fraction_text(labeled, item_count),
            'labeled': labeled,
            'missing': item_count - labeled,
        }
    return result


def _iter_item_rows(items, ticket_cursor):
    """Merge the ordered item roster with ordered tickets, one item at a time.

    Both iterables are ordered by item name, and every ticket references a
    roster item (enforced during parsing), so a single forward pass suffices.
    At most one item's vote dict (bounded by the 50-annotator roster) is ever
    resident.
    """
    row = ticket_cursor.fetchone()
    for item in items:
        votes = {}
        while row is not None and row[0] == item:
            votes[row[1]] = row[2]
            row = ticket_cursor.fetchone()
        yield item, votes
    if row is not None:  # pragma: no cover - reference validation prevents this
        raise StreamError('ticket references item outside the roster: ' + row[0])


# --------------------------------------------------------------------------- #
# Structural JSON writer (byte-identical to json.dump(..., indent=2,
# sort_keys=True, ensure_ascii=False) plus a trailing LF)
# --------------------------------------------------------------------------- #

class JsonWriter:
    def __init__(self, stream):
        self._stream = stream
        self._stack = []  # True = object, False = array
        self._first = []
        self._after_key = []
        self._key_cache = {}
        self._indents = [b'']
        self._buffer = bytearray()
        self._flush_at = 1 << 16

    def _write(self, data):
        self._buffer += data
        if len(self._buffer) >= self._flush_at:
            self._stream.write(bytes(self._buffer))
            self._buffer.clear()

    def _encoded_key(self, name):
        # Roster identifiers and the fixed report keys repeat millions of
        # times; cache their encoded form to avoid re-running json.dumps.
        encoded = self._key_cache.get(name)
        if encoded is None:
            encoded = json.dumps(name, ensure_ascii=False).encode('utf-8')
            if len(self._key_cache) < 4096:
                self._key_cache[name] = encoded
        return encoded

    def _indent_bytes(self, depth):
        # Indentation only ever depends on stack depth; cache the built bytes.
        while len(self._indents) <= depth:
            self._indents.append(b'\n' + b'  ' * len(self._indents))
        return self._indents[depth]

    def _indent(self):
        if self._stack:
            self._write(self._indent_bytes(len(self._stack)))

    def _before_value(self):
        if not self._stack:
            return
        if self._after_key[-1]:
            # The value follows an object key; the key already wrote ": ".
            self._after_key[-1] = False
            return
        if not self._first[-1]:
            self._write(b',')
        self._first[-1] = False
        self._indent()

    def _after_container_open(self, is_object):
        self._stack.append(is_object)
        self._first.append(True)
        self._after_key.append(False)

    def begin_object(self):
        self._before_value()
        self._write(b'{')
        self._after_container_open(True)

    def begin_array(self):
        self._before_value()
        self._write(b'[')
        self._after_container_open(False)

    def key(self, name):
        if not self._first[-1]:
            self._write(b',')
        self._first[-1] = False
        self._indent()
        self._write(self._encoded_key(name))
        self._write(b': ')
        self._after_key[-1] = True

    def value(self, value):
        after_key = self._stack and self._after_key[-1]
        if after_key:
            # The value follows an object key; the key already wrote ": ".
            self._after_key[-1] = False
        else:
            self._before_value()
        if isinstance(value, bool) or value is None:
            self._write(b'true' if value is True else
                        b'false' if value is False else b'null')
        elif isinstance(value, int):
            self._write(str(value).encode('ascii'))
        elif isinstance(value, str):
            self._write(json.dumps(value, ensure_ascii=False).encode('utf-8'))
        elif isinstance(value, (dict, list)):
            # Match the stdlib indented encoder: nested containers are always
            # expanded, even inside an array.  These containers are bounded
            # (annotator/label rosters of at most 50 entries) by construction.
            self._encode_expanded(value)
        else:
            self._write(json.dumps(value, ensure_ascii=False, allow_nan=False,
                                   separators=(',', ':')).encode('utf-8'))

    def _encode_expanded(self, value):
        if isinstance(value, dict):
            self._write(b'{')
            self._after_container_open(True)
            for key in sorted(value):
                self.key(key)
                self.value(value[key])
            self.end()
        else:
            self._write(b'[')
            self._after_container_open(False)
            for element in value:
                self.value(element)
            self.end()

    def end(self):
        is_object = self._stack.pop()
        was_empty = self._first.pop()
        self._after_key.pop()
        if not was_empty:
            self._write(b'\n' + b'  ' * len(self._stack))
        self._write(b'}' if is_object else b']')

    def finish(self):
        if self._stack:
            raise StreamError('internal report writer state error')
        self._write(b'\n')
        self._stream.write(bytes(self._buffer))
        self._buffer.clear()


# --------------------------------------------------------------------------- #
# Report streaming and atomic publication
# --------------------------------------------------------------------------- #

def _confusion_template(depth, labels, encoded_labels):
    """Pre-render an all-zero confusion block split on value sentinels.

    The block is cut on the single-byte sentinel ``\\x00`` into fixed text
    segments; a concrete table is reassembled with one C-level ``join`` of
    the segments and the rendered cell values, avoiding the quadratic cost
    of splicing a bytearray once per cell (up to 2,500 cells per pair).

    ``depth`` is the writer stack depth when the confusion value begins
    (the pair object is innermost).  Rendering the value opens one more
    object (the confusion table itself), so its rows sit at depth+2,
    cells at depth+3 and the closing brace back at depth+1.
    """
    row_indent = b'  ' * (depth + 2)
    cell_indent = b'  ' * (depth + 3)
    close_indent = b'  ' * (depth + 1)
    sentinel = b'\x00'
    row_blocks = []
    for row_index in range(len(labels)):
        cells = []
        for col_index in range(len(labels)):
            cell_prefix = b',' if col_index else b''
            cells.append(b''.join((cell_prefix, b'\n', cell_indent,
                                   encoded_labels[col_index], b': ', sentinel)))
        row_prefix = b',' if row_index else b''
        row_blocks.append(b''.join((row_prefix, b'\n', row_indent,
                                    encoded_labels[row_index], b': {',
                                    b''.join(cells), b'\n', row_indent, b'}')))
    skeleton = b'{' + b''.join(row_blocks) + b'\n' + close_indent + b'}'
    return skeleton.split(sentinel)


def _write_confusion(writer, table, labels, int_bytes, segments):
    # Reassemble the shared template with this pair's rendered cell values.
    rendered_values = []
    for row_label in labels:
        columns = table[row_label]
        for col_label in labels:
            value = columns[col_label]
            if not value:
                rendered_values.append(b'0')
                continue
            rendered = int_bytes.get(value)
            if rendered is None:
                rendered = str(value).encode('ascii')
                if len(int_bytes) < 8192:
                    int_bytes[value] = rendered
            rendered_values.append(rendered)
    parts = [segments[0]]
    for index, rendered in enumerate(rendered_values):
        parts.append(rendered)
        parts.append(segments[index + 1])
    writer._before_value()
    writer._write(b''.join(parts))


def _write_pairs(writer, connection, annotators, labels):
    writer.key('pairs')
    writer.begin_array()
    encoded_labels = [writer._encoded_key(label) for label in labels]
    int_bytes = {}
    # When the confusion value is emitted the open stack is root > result >
    # pairs array > pair object (depth 4).  The confusion object is rendered
    # at exactly that depth, so every pair reuses this one split template.
    confusion_depth = len(writer._stack)
    segments = _confusion_template(confusion_depth, labels, encoded_labels)
    for index, left in enumerate(annotators):
        for right in annotators[index + 1:]:
            pair = _pairwise(connection, left, right, labels)
            writer.begin_object()
            writer.key('chance_agreement')
            writer.value(pair['chance_agreement'])
            writer.key('cohen_kappa')
            writer.value(pair['cohen_kappa'])
            writer.key('confusion')
            _write_confusion(writer, pair['confusion'], labels,
                             int_bytes, segments)
            writer.key('left')
            writer.value(left)
            writer.key('observed_agreement')
            writer.value(pair['observed_agreement'])
            writer.key('paired_items')
            writer.value(pair['paired_items'])
            writer.key('right')
            writer.value(right)
            writer.key('undefined_reason')
            writer.value(pair['undefined_reason'])
            writer.end()
    writer.end()


def _write_items(writer, connection, items, annotators, labels):
    writer.key('items')
    writer.begin_array()
    cursor = connection.execute(
        'SELECT item, annotator, label FROM tickets ORDER BY item, annotator')
    depth = len(writer._stack)  # depth of each item object's container
    ind_k = writer._indent_bytes(depth + 1)       # item fields
    ind_n = writer._indent_bytes(depth + 2)       # entries of nested containers
    bare_k = ind_k[1:]                            # same spacing, no newline
    bare_n = ind_n[1:]
    encoded_labels = [writer._encoded_key(label) for label in labels]
    annotator_set = set(annotators)

    def expanded_array(values):
        # Stdlib: first element on its own line, every element at the same
        # indentation, closing bracket aligned with the containing field.
        if not values:
            return b'[]'
        parts = [b'[\n', bare_n]
        for index, value in enumerate(values):
            if index:
                parts.append(b',\n' + bare_n)
            parts.append(json.dumps(value, ensure_ascii=False).encode('utf-8'))
        parts.append(b'\n' + bare_k + b']')
        return b''.join(parts)

    for item, votes in _iter_item_rows(items, cursor):
        n = len(votes)
        counts = {label: 0 for label in labels}
        for label in votes.values():
            counts[label] += 1
        winners = [label for label, count in counts.items() if count * 2 > n]
        status = ('unlabeled' if not n else
                  'majority' if winners else 'no_strict_majority')
        consensus = (b'null' if not winners
                     else json.dumps(winners[0], ensure_ascii=False).encode('utf-8'))
        annotated_block = expanded_array(sorted(votes))
        missing_block = expanded_array(sorted(annotator_set - set(votes)))

        # votes expanded object in label order
        vp = [b'{\n', bare_n]
        for index, label in enumerate(labels):
            if index:
                vp.append(b',\n' + bare_n)
            vp.append(encoded_labels[index])
            vp.append(b': ')
            vp.append(str(counts[label]).encode('ascii'))
        vp.append(b'\n' + bare_k + b'}')
        votes_block = b''.join(vp)

        block = b''.join((
            b'{',
            ind_k, b'"annotated_by": ', annotated_block,
            b',', ind_k, b'"consensus": ', consensus,
            b',', ind_k, b'"item": ',
            json.dumps(item, ensure_ascii=False).encode('utf-8'),
            b',', ind_k, b'"missing_annotators": ', missing_block,
            b',', ind_k, b'"status": ',
            json.dumps(status, ensure_ascii=False).encode('utf-8'),
            b',', ind_k, b'"votes": ', votes_block,
            b'\n', writer._indent_bytes(depth)[1:], b'}',
        ))
        writer._before_value()
        writer._write(block)
    writer.end()


def write_report(path, connection, digest, item_count, annotator_count):
    """Stream the full v1 report into ``path`` (a staging file).

    Roster cursors are streaming (``arraysize=1``); the largest resident
    collections are the 50-element annotator/label rosters.
    """
    item_cursor = connection.execute('SELECT name FROM roster_items ORDER BY name')
    annotator_cursor = connection.execute(
        'SELECT name FROM roster_annotators ORDER BY name')
    label_cursor = connection.execute('SELECT name FROM roster_labels ORDER BY name')
    annotators = [row[0] for row in annotator_cursor]
    labels = [row[0] for row in label_cursor]
    items = (row[0] for row in item_cursor)
    annotation_count = connection.execute('SELECT COUNT(*) FROM tickets').fetchone()[0]
    coverage = _coverage(connection, item_count, annotators)
    with open_builtin(path, 'wb') as stream:
        writer = JsonWriter(stream)
        writer.begin_object()
        writer.key('result')
        writer.begin_object()
        writer.key('annotation_count')
        writer.value(annotation_count)
        writer.key('annotator_count')
        writer.value(annotator_count)
        writer.key('coverage')
        writer.begin_object()
        for annotator in annotators:
            writer.key(annotator)
            writer.begin_object()
            writer.key('fraction')
            writer.value(coverage[annotator]['fraction'])
            writer.key('labeled')
            writer.value(coverage[annotator]['labeled'])
            writer.key('missing')
            writer.value(coverage[annotator]['missing'])
            writer.end()
        writer.end()
        writer.key('interpretation')
        writer.value('Agreement is consistency of recorded labels, not ground-truth accuracy.')
        writer.key('item_count')
        writer.value(item_count)
        _write_items(writer, connection, items, annotators, labels)
        _write_pairs(writer, connection, annotators, labels)
        writer.end()
        writer.key('schema_version')
        writer.value(SCHEMA_VERSION)
        writer.key('source_sha256')
        writer.value(digest)
        writer.end()
        writer.finish()


def _open_database(path):
    connection = sqlite3.connect(path)
    # Keep SQLite's own footprint small: cap the page cache, keep rollback
    # journals in memory (the staging DB is disposable) and spill temp sorts
    # to files rather than building large in-process structures.
    connection.execute('PRAGMA page_size=4096')
    connection.execute('PRAGMA cache_size=-2048')  # ~2 MiB page cache cap
    connection.execute('PRAGMA temp_store=FILE')
    connection.execute('PRAGMA journal_mode=MEMORY')
    connection.execute('PRAGMA synchronous=OFF')
    connection.execute('PRAGMA locking_mode=EXCLUSIVE')
    connection.executescript(_SCHEMA)
    return connection


def analyze_file(source, output, max_bytes=SOURCE_MAX_BYTES):
    """Parse, analyze and atomically publish one v1 source.

    A private temporary SQLite database and a private staging report are used;
    any failure removes both and leaves a previously published report in
    place.  Symlink and same-path protections match :func:`common.save`.
    """
    dest = Path(output)
    if dest.is_symlink():
        raise StreamError('output cannot be a symlink')
    if dest.resolve() == Path(source).resolve():
        raise StreamError('input and output must differ')
    dest.parent.mkdir(parents=True, exist_ok=True)
    handle, db_path = tempfile.mkstemp(prefix='.label-agreement-', suffix='.sqlite',
                                       dir=str(dest.parent))
    os.close(handle)
    staging = None
    connection = None
    try:
        connection = _open_database(db_path)
        try:
            digest = parse_source(source, connection, max_bytes=max_bytes)
        except StreamingJSONError as error:
            raise StreamError(str(error)) from error
        item_count = connection.execute('SELECT COUNT(*) FROM roster_items').fetchone()[0]
        annotator_count = connection.execute(
            'SELECT COUNT(*) FROM roster_annotators').fetchone()[0]
        label_count = connection.execute('SELECT COUNT(*) FROM roster_labels').fetchone()[0]
        if not item_count or not annotator_count or not label_count:
            raise StreamError('rosters must be nonempty')
        handle2, staging = tempfile.mkstemp(prefix='.label-agreement-report-',
                                            suffix='.tmp', dir=str(dest.parent))
        os.close(handle2)
        write_report(staging, connection, digest, item_count, annotator_count)
        connection.close()
        connection = None
        os.replace(staging, dest)
        staging = None
    finally:
        if connection is not None:
            connection.close()
        for path in (staging, db_path):
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
    return dest
