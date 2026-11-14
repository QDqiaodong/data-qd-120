"""SQLite append-only annotation revision ledger.

The ledger stores a fixed roster (items/annotators/labels) and an ordered
stream of revision events grouped into committed batches.  Every batch is a
single transaction: either all of its events become visible together or the
database is left untouched.  Historical exports replay only the events whose
revision was committed up to a cutoff batch, so later corrections never leak
into an earlier report.

Only the Python standard library is used.
"""
import hashlib
import json
import sqlite3
from pathlib import Path

from .common import ident, read, save
from .model import analyze

SCHEMA_VERSION = 1
EVENT_LIMIT = 100_000
LEDGER_ANNOTATION_LIMIT = 500_000

_SCHEMA = """
CREATE TABLE meta(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE roster_items(name TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE roster_annotators(name TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE roster_labels(name TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE batches(
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL UNIQUE,
    content_hash TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    committed_at TEXT NOT NULL
);
CREATE TABLE key_versions(
    item TEXT NOT NULL,
    annotator TEXT NOT NULL,
    max_revision INTEGER NOT NULL,
    PRIMARY KEY (item, annotator)
) WITHOUT ROWID;
CREATE TABLE events(
    batch_seq INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    item TEXT NOT NULL,
    annotator TEXT NOT NULL,
    revision INTEGER NOT NULL,
    op TEXT NOT NULL CHECK (op IN ('set', 'delete')),
    label TEXT,
    PRIMARY KEY (batch_seq, ordinal)
);
CREATE INDEX events_replay ON events(batch_seq, ordinal);
"""


class LedgerError(ValueError):
    """Raised when a ledger command cannot be completed."""


def _canonical_events(events):
    """Deterministic encoding of the ordered event list for hashing."""
    return json.dumps(events, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode('utf-8')


def _validate_roster(data):
    if not isinstance(data, dict) or set(data) != {'items', 'annotators', 'labels'}:
        raise LedgerError('roster file must contain exactly items, annotators and labels')
    rosters = {}
    for key, limit in (('items', 10000), ('annotators', 50), ('labels', 50)):
        values = data[key]
        if not isinstance(values, list) or not values or len(values) > limit:
            raise LedgerError(key + ' must be a nonempty bounded array')
        cleaned = [ident(v) for v in values]
        if len(cleaned) != len(set(cleaned)):
            raise LedgerError(key + ' must be unique')
        rosters[key] = sorted(cleaned)
    return rosters


def init_ledger(db_path, roster_path):
    """Create a ledger database from a roster file. The ledger starts empty."""
    dest = Path(db_path)
    if dest.is_symlink():
        raise LedgerError('database path cannot be a symlink')
    if dest.exists():
        raise LedgerError('database already exists: ' + str(dest))
    data, _ = read(roster_path)
    rosters = _validate_roster(data)
    dest.parent.mkdir(parents=True, exist_ok=True)
    connection = None
    try:
        connection = sqlite3.connect(dest)
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('PRAGMA journal_mode=WAL')
        connection.executescript(_SCHEMA)
        connection.executemany('INSERT INTO roster_items(name) VALUES (?)',
                               [(v,) for v in rosters['items']])
        connection.executemany('INSERT INTO roster_annotators(name) VALUES (?)',
                               [(v,) for v in rosters['annotators']])
        connection.executemany('INSERT INTO roster_labels(name) VALUES (?)',
                               [(v,) for v in rosters['labels']])
        connection.executemany('INSERT INTO meta(key,value) VALUES (?,?)', [
            ('schema_version', str(SCHEMA_VERSION)),
            ('item_count', str(len(rosters['items']))),
            ('annotator_count', str(len(rosters['annotators']))),
            ('label_count', str(len(rosters['labels']))),
        ])
        connection.commit()
    except Exception:
        if connection is not None:
            connection.close()
        for suffix in ('', '-wal', '-shm'):
            stale = Path(str(dest) + suffix)
            if stale.exists():
                stale.unlink()
        raise
    else:
        connection.close()


def _validate_batch(payload):
    if not isinstance(payload, dict) or set(payload) != {'batch_id', 'events'}:
        raise LedgerError('batch file must contain exactly batch_id and events')
    batch_id = ident(payload['batch_id'])
    raw_events = payload['events']
    if not isinstance(raw_events, list) or not raw_events or len(raw_events) > EVENT_LIMIT:
        raise LedgerError('events must be a nonempty array of at most ' + str(EVENT_LIMIT))
    events = []
    for ordinal, event in enumerate(raw_events):
        if not isinstance(event, dict):
            raise LedgerError('event %d must be an object' % ordinal)
        base = {'item', 'annotator', 'revision', 'op'}
        if not base <= set(event):
            raise LedgerError('event %d must contain item, annotator, revision, op' % ordinal)
        op = event['op']
        if op not in ('set', 'delete'):
            raise LedgerError('event %d op must be set or delete' % ordinal)
        if op == 'set':
            if set(event) != base | {'label'} or not isinstance(event['label'], str):
                raise LedgerError('event %d set requires a valid label' % ordinal)
            label = ident(event['label'])
        else:
            if set(event) != base:
                raise LedgerError('event %d delete must not carry a label' % ordinal)
            label = None
        item = ident(event['item'])
        annotator = ident(event['annotator'])
        revision = event['revision']
        if type(revision) is not int or not 1 <= revision <= 10 ** 9:
            raise LedgerError('event %d revision must be a positive integer' % ordinal)
        events.append({'item': item, 'annotator': annotator, 'revision': revision,
                       'op': op, 'label': label})
    content_hash = hashlib.sha256(_canonical_events(events)).hexdigest()
    return batch_id, events, content_hash


def import_batch(db_path, batch_path, committed_at):
    """Atomically import one ordered batch of revision events."""
    if not Path(db_path).exists():
        raise LedgerError('database does not exist: ' + str(db_path))
    data, source_hash = read(batch_path)
    batch_id, events, content_hash = _validate_batch(data)
    connection = sqlite3.connect(db_path)
    try:
        try:
            connection.execute('BEGIN IMMEDIATE')
            items = {row[0] for row in connection.execute('SELECT name FROM roster_items')}
            annotators = {row[0] for row in connection.execute('SELECT name FROM roster_annotators')}
            labels = {row[0] for row in connection.execute('SELECT name FROM roster_labels')}
            row = connection.execute('SELECT seq,content_hash FROM batches WHERE batch_id=?',
                                     (batch_id,)).fetchone()
            if row is not None:
                if row[1] != content_hash:
                    raise LedgerError('batch_id %s already committed with different content' % batch_id)
                connection.execute('ROLLBACK')
                return {'batch_id': batch_id, 'seq': row[0], 'idempotent': True}
            pending = {}
            for ordinal, event in enumerate(events):
                if event['item'] not in items:
                    raise LedgerError('event %d references unknown item' % ordinal)
                if event['annotator'] not in annotators:
                    raise LedgerError('event %d references unknown annotator' % ordinal)
                if event['op'] == 'set' and event['label'] not in labels:
                    raise LedgerError('event %d references unknown label' % ordinal)
                key = (event['item'], event['annotator'])
                ceiling = pending.get(key)
                if ceiling is None:
                    stored = connection.execute(
                        'SELECT max_revision FROM key_versions WHERE item=? AND annotator=?', key).fetchone()
                    ceiling = stored[0] if stored is not None else 0
                if event['revision'] <= ceiling:
                    raise LedgerError(
                        'event %d revision %d must exceed the highest received revision %d'
                        % (ordinal, event['revision'], ceiling))
                pending[key] = event['revision']
            cursor = connection.execute(
                'INSERT INTO batches(batch_id,content_hash,source_hash,committed_at) VALUES (?,?,?,?)',
                (batch_id, content_hash, source_hash, committed_at))
            batch_seq = cursor.lastrowid
            connection.executemany(
                'INSERT INTO events(batch_seq,ordinal,item,annotator,revision,op,label)'
                ' VALUES (?,?,?,?,?,?,?)',
                [(batch_seq, ordinal, e['item'], e['annotator'], e['revision'], e['op'], e['label'])
                 for ordinal, e in enumerate(events)])
            connection.executemany(
                'INSERT INTO key_versions(item,annotator,max_revision) VALUES (?,?,?)'
                ' ON CONFLICT(item,annotator) DO UPDATE SET max_revision=excluded.max_revision'
                ' WHERE excluded.max_revision>key_versions.max_revision',
                [(item, annotator, revision) for (item, annotator), revision in pending.items()])
            connection.commit()
        except Exception:
            try:
                connection.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        connection.close()
    return {'batch_id': batch_id, 'seq': batch_seq, 'idempotent': False}


def _readonly_connection(db_path):
    path = Path(db_path).resolve()
    uri = 'file:' + path.as_posix().replace('?', '%3f').replace('#', '%23') + '?mode=ro'
    try:
        return sqlite3.connect(uri, uri=True)
    except sqlite3.OperationalError as error:
        raise LedgerError('cannot open database: ' + str(error))


def export_report(db_path, output_path, cutoff_batch_id=None):
    """Replay committed events up to a cutoff batch and run the original analysis."""
    connection = _readonly_connection(db_path)
    try:
        meta = dict(connection.execute('SELECT key,value FROM meta').fetchall())
        if meta.get('schema_version') != str(SCHEMA_VERSION):
            raise LedgerError('unsupported ledger schema version')
        if cutoff_batch_id is None:
            row = connection.execute('SELECT seq,batch_id,source_hash FROM batches ORDER BY seq DESC LIMIT 1').fetchone()
            if row is None:
                raise LedgerError('no committed batches to export')
            cutoff_seq, cutoff_id, _ = row
        else:
            row = connection.execute(
                'SELECT seq,batch_id,source_hash FROM batches WHERE batch_id=?',
                (cutoff_batch_id,)).fetchone()
            if row is None:
                raise LedgerError('unknown cutoff batch: ' + cutoff_batch_id)
            cutoff_seq, cutoff_id, _ = row
        batches = connection.execute(
            'SELECT seq,batch_id,source_hash,committed_at FROM batches WHERE seq<=? ORDER BY seq',
            (cutoff_seq,)).fetchall()
        event_rows = connection.execute(
            'SELECT item,annotator,op,label FROM events WHERE batch_seq<=? ORDER BY batch_seq,ordinal',
            (cutoff_seq,)).fetchall()
        items = [r[0] for r in connection.execute('SELECT name FROM roster_items ORDER BY name')]
        annotators = [r[0] for r in connection.execute('SELECT name FROM roster_annotators ORDER BY name')]
        labels = [r[0] for r in connection.execute('SELECT name FROM roster_labels ORDER BY name')]
    finally:
        connection.close()

    tickets = {}
    for item, annotator, op, label in event_rows:
        key = (item, annotator)
        if op == 'set':
            tickets[key] = label
        else:
            tickets.pop(key, None)
    annotations = [{'item': item, 'annotator': annotator, 'label': label}
                   for (item, annotator), label in sorted(tickets.items())]
    data = {'items': items, 'annotators': annotators, 'labels': labels, 'annotations': annotations}
    result = analyze(data, limit=LEDGER_ANNOTATION_LIMIT)
    report = {
        'schema_version': 2,
        'ledger': {
            'ledger_schema_version': SCHEMA_VERSION,
            'cutoff_batch_id': cutoff_id,
            'cutoff_batch_seq': cutoff_seq,
            'commit_order': [
                {'seq': seq, 'batch_id': batch_id, 'source_sha256': source_hash, 'committed_at': committed_at}
                for seq, batch_id, source_hash, committed_at in batches
            ],
        },
        'result': result,
    }
    save(output_path, report)
    return report
