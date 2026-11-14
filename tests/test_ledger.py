import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from label_agreement.ledger import LedgerError, export_report, import_batch, init_ledger
from label_agreement.model import analyze

ROSTER = {'items': ['I1', 'I2', 'I3', 'I4'],
          'annotators': ['A', 'B', 'C'],
          'labels': ['positive', 'negative']}


def batch(batch_id, events):
    return {'batch_id': batch_id, 'events': events}


def ev(item, annotator, revision, op, label=None):
    event = {'item': item, 'annotator': annotator, 'revision': revision, 'op': op}
    if op == 'set':
        event['label'] = label
    return event


class LedgerFlowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)
        self.db = self.dir / 'ledger.db'
        self.roster = self.dir / 'roster.json'
        self.roster.write_text(json.dumps(ROSTER))
        init_ledger(self.db, self.roster)

    def tearDown(self):
        self.temp.cleanup()

    def write_batch(self, name, payload):
        path = self.dir / name
        path.write_text(json.dumps(payload))
        return path

    def seqs(self):
        with sqlite3.connect(self.db) as con:
            return con.execute('SELECT seq,batch_id FROM batches ORDER BY seq').fetchall()

    def event_count(self):
        with sqlite3.connect(self.db) as con:
            return con.execute('SELECT COUNT(*) FROM events').fetchone()[0]

    def max_revision(self, item, annotator):
        with sqlite3.connect(self.db) as con:
            row = con.execute('SELECT max_revision FROM key_versions WHERE item=? AND annotator=?',
                              (item, annotator)).fetchone()
        return None if row is None else row[0]

    def export(self, cutoff=None):
        out = self.dir / ('report-%s.json' % (cutoff or 'head'))
        return export_report(self.db, out, cutoff)

    def test_cross_batch_correction_replays_each_horizon(self):
        b1 = self.write_batch('b1.json', batch('b1', [
            ev('I1', 'A', 1, 'set', 'positive'),
            ev('I1', 'B', 1, 'set', 'positive'),
            ev('I2', 'A', 1, 'set', 'negative'),
        ]))
        first = import_batch(self.db, b1, '2026-01-01T00:00:00+00:00')
        self.assertEqual((first['seq'], first['idempotent']), (1, False))
        b2 = self.write_batch('b2.json', batch('b2', [
            ev('I1', 'A', 2, 'set', 'negative'),
        ]))
        second = import_batch(self.db, b2, '2026-01-02T00:00:00+00:00')
        self.assertEqual((second['seq'], second['idempotent']), (2, False))
        historical = self.export('b1')
        current = self.export()
        self.assertEqual(historical['result']['items'][0]['votes']['positive'], 2)
        self.assertEqual(historical['ledger']['cutoff_batch_id'], 'b1')
        self.assertEqual(historical['ledger']['cutoff_batch_seq'], 1)
        self.assertEqual(current['result']['items'][0]['votes']['positive'], 1)
        self.assertEqual(current['result']['items'][0]['votes']['negative'], 1)
        self.assertEqual(current['ledger']['cutoff_batch_id'], 'b2')
        self.assertEqual([c['batch_id'] for c in current['ledger']['commit_order']], ['b1', 'b2'])
        self.assertEqual([c['seq'] for c in current['ledger']['commit_order']], [1, 2])
        self.assertEqual(len(current['ledger']['commit_order'][0]['source_sha256']), 64)

    def test_delete_of_missing_ticket_keeps_version_ceiling_and_old_horizon(self):
        b1 = self.write_batch('b1.json', batch('b1', [
            ev('I1', 'A', 1, 'set', 'positive'),
            ev('I1', 'B', 1, 'set', 'positive'),
        ]))
        import_batch(self.db, b1, 't1')
        # A withdraws at revision 2; then revision 3 re-labels the same key.
        b2 = self.write_batch('b2.json', batch('b2', [
            ev('I1', 'A', 2, 'delete'),
            ev('I1', 'A', 3, 'set', 'negative'),
        ]))
        import_batch(self.db, b2, 't2')
        # Delete a ticket that never existed: still booked, ceiling retained.
        b3 = self.write_batch('b3.json', batch('b3', [ev('I4', 'C', 1, 'delete')]))
        import_batch(self.db, b3, 't3')
        self.assertEqual(self.max_revision('I4', 'C'), 1)
        historical = self.export('b1')
        self.assertEqual(historical['result']['items'][0]['votes']['positive'], 2)
        current = self.export()
        head = current['result']['items'][0]
        self.assertEqual(head['votes'], {'positive': 1, 'negative': 1})
        self.assertEqual(self.max_revision('I1', 'A'), 3)
        # The retained ceiling rejects a stale revision even after withdrawal.
        stale = self.write_batch('stale.json', batch('stale', [ev('I4', 'C', 1, 'set', 'positive')]))
        with self.assertRaises(LedgerError):
            import_batch(self.db, stale, 't4')

    def test_last_event_failure_rolls_back_everything_and_frees_sequence(self):
        good = self.write_batch('good.json', batch('good', [ev('I1', 'A', 1, 'set', 'positive')]))
        import_batch(self.db, good, 't1')
        bad = self.write_batch('bad.json', batch('bad', [
            ev('I2', 'A', 1, 'set', 'positive'),
            ev('I2', 'B', 1, 'set', 'positive'),
            ev('I9', 'A', 1, 'set', 'positive'),
        ]))
        with self.assertRaises(LedgerError):
            import_batch(self.db, bad, 't2')
        self.assertEqual(self.seqs(), [(1, 'good')])
        self.assertEqual(self.event_count(), 1)
        replacement = self.write_batch('replacement.json', batch('replacement', [
            ev('I2', 'A', 1, 'set', 'negative')]))
        outcome = import_batch(self.db, replacement, 't3')
        # Rolled-back AUTOINCREMENT allocation is reused: no gap in batch ordering.
        self.assertEqual(outcome['seq'], 2)
        self.assertEqual(self.seqs(), [(1, 'good'), (2, 'replacement')])

    def test_idempotent_same_batch_conflict_and_event_order_matter(self):
        events = [ev('I1', 'A', 1, 'set', 'positive'), ev('I1', 'A', 2, 'set', 'negative')]
        first = self.write_batch('b.json', batch('b', events))
        outcome = import_batch(self.db, first, 't1')
        again = import_batch(self.db, first, 't1-other')
        self.assertTrue(again['idempotent'])
        self.assertEqual(again['seq'], outcome['seq'])
        self.assertEqual(self.seqs(), [(1, 'b')])
        reordered = self.write_batch('reordered.json', batch('b', list(reversed(events))))
        with self.assertRaises(LedgerError):
            import_batch(self.db, reordered, 't2')
        changed = self.write_batch('changed.json',
                                   batch('b', [ev('I1', 'A', 1, 'set', 'negative')]))
        with self.assertRaises(LedgerError):
            import_batch(self.db, changed, 't3')
        self.assertEqual(self.seqs(), [(1, 'b')])

    def test_history_diverges_from_current_after_corrections(self):
        b1 = self.write_batch('b1.json', batch('b1', [
            ev('I3', 'A', 1, 'set', 'positive'),
            ev('I3', 'B', 1, 'set', 'positive'),
            ev('I3', 'C', 1, 'set', 'positive'),
        ]))
        import_batch(self.db, b1, 't1')
        b2 = self.write_batch('b2.json', batch('b2', [
            ev('I3', 'A', 2, 'set', 'negative'),
            ev('I3', 'B', 2, 'delete'),
        ]))
        import_batch(self.db, b2, 't2')
        historical = self.export('b1')['result']
        current = self.export()['result']
        self.assertEqual(historical['items'][2]['status'], 'majority')
        self.assertEqual(historical['items'][2]['consensus'], 'positive')
        head = current['items'][2]
        self.assertEqual(head['status'], 'no_strict_majority')
        self.assertIsNone(head['consensus'])
        self.assertEqual(head['votes'], {'positive': 1, 'negative': 1})
        self.assertEqual(current['annotation_count'], 2)
        # Replay feeds the original analysis logic with only effective tickets.
        tickets = {'items': ROSTER['items'], 'annotators': ROSTER['annotators'],
                   'labels': ROSTER['labels'],
                   'annotations': [
                       {'item': 'I3', 'annotator': 'A', 'label': 'negative'},
                       {'item': 'I3', 'annotator': 'C', 'label': 'positive'}]}
        self.assertEqual(current, analyze(tickets, limit=500_000))

    def test_validation_rules_and_empty_ledger(self):
        with self.assertRaises(LedgerError):
            export_report(self.db, self.dir / 'empty.json')
        non_positive = self.write_batch('np.json', batch('np', [ev('I1', 'A', 0, 'set', 'positive')]))
        with self.assertRaises(LedgerError):
            import_batch(self.db, non_positive, 't')
        delete_with_label = self.write_batch('dl.json', batch('dl', [{
            'item': 'I1', 'annotator': 'A', 'revision': 1, 'op': 'delete', 'label': 'positive'}]))
        with self.assertRaises(LedgerError):
            import_batch(self.db, delete_with_label, 't')
        set_without_label = self.write_batch('nl.json', batch('nl', [ev('I1', 'A', 1, 'set')]))
        with self.assertRaises(LedgerError):
            import_batch(self.db, set_without_label, 't')
        unknown_label = self.write_batch('ul.json',
                                         batch('ul', [ev('I1', 'A', 1, 'set', 'neutral')]))
        with self.assertRaises(LedgerError):
            import_batch(self.db, unknown_label, 't')
        self.assertEqual(self.event_count(), 0)
        self.assertEqual(self.seqs(), [])

    def test_missing_database_is_not_created(self):
        missing = self.dir / 'no-ledger.db'
        batch_path = self.write_batch('x.json', batch('x', [ev('I1', 'A', 1, 'set', 'positive')]))
        with self.assertRaises(LedgerError):
            import_batch(missing, batch_path, 't')
        self.assertFalse(missing.exists())
        with self.assertRaises(LedgerError):
            export_report(missing, self.dir / 'r.json')
        self.assertFalse(missing.exists())


class LedgerCLITests(unittest.TestCase):
    def run_cli(self, *arguments):
        return subprocess.run([sys.executable, '-m', 'label_agreement.cli', *map(str, arguments)],
                              capture_output=True, text=True)

    def test_reopen_database_in_new_process_and_legacy_cli_unchanged(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db = base / 'ledger.db'
            roster = base / 'roster.json'
            roster.write_text(json.dumps(ROSTER))
            init_result = self.run_cli('ledger-init', '--db', db, '--roster', roster)
            self.assertEqual(init_result.returncode, 0, init_result.stderr)
            batch_path = base / 'b1.json'
            batch_path.write_text(json.dumps(batch('b1', [
                ev('I1', 'A', 1, 'set', 'positive'),
                ev('I1', 'B', 1, 'set', 'positive'),
            ])))
            imported = self.run_cli('ledger-import', '--db', db, '--batch', batch_path)
            self.assertEqual(imported.returncode, 0, imported.stderr)
            # Reopen the same database from a fresh process and export twice.
            report = base / 'report.json'
            first = self.run_cli('ledger-export', '--db', db, '--output', report)
            self.assertEqual(first.returncode, 0, first.stderr)
            payload = json.loads(report.read_text())
            self.assertEqual(payload['schema_version'], 2)
            self.assertEqual(payload['result']['items'][0]['votes']['positive'], 2)
            second = self.run_cli('ledger-export', '--db', db, '--output', report)
            self.assertEqual(second.returncode, 0, second.stderr)
            self.assertEqual(report.read_text(), json.dumps(payload, sort_keys=True, indent=2) + '\n')
            # Unknown cutoff batch fails without touching the prior report.
            failed = self.run_cli('ledger-export', '--db', db, '--output', report,
                                  '--cutoff-batch-id', 'missing')
            self.assertEqual(failed.returncode, 2)
            self.assertEqual(json.loads(report.read_text()), payload)
            # Symlink output is rejected, old report retained.
            link = base / 'link.json'
            link.symlink_to(report)
            rejected = self.run_cli('ledger-export', '--db', db, '--output', link)
            self.assertEqual(rejected.returncode, 2)
            self.assertEqual(json.loads(report.read_text()), payload)
            # Re-init on an existing database is refused.
            duplicate = self.run_cli('ledger-init', '--db', db, '--roster', roster)
            self.assertEqual(duplicate.returncode, 2)
            # Legacy default CLI still behaves exactly as before on the old sample.
            legacy_out = base / 'legacy.json'
            source = Path(__file__).resolve().parents[1] / 'fixtures' / 'input.json'
            legacy = self.run_cli('--source', source, '--output', legacy_out)
            self.assertEqual(legacy.returncode, 0, legacy.stderr)
            legacy_report = json.loads(legacy_out.read_text())
            self.assertEqual(legacy_report['schema_version'], 1)
            self.assertEqual(legacy_report['result'], analyze(json.loads(source.read_text())))

    def test_failed_import_leaves_no_events_in_cli(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db = base / 'ledger.db'
            roster = base / 'roster.json'
            roster.write_text(json.dumps(ROSTER))
            self.assertEqual(self.run_cli('ledger-init', '--db', db, '--roster', roster).returncode, 0)
            good = base / 'good.json'
            good.write_text(json.dumps(batch('good', [ev('I1', 'A', 1, 'set', 'positive')])))
            self.assertEqual(self.run_cli('ledger-import', '--db', db, '--batch', good).returncode, 0)
            bad = base / 'bad.json'
            bad.write_text(json.dumps(batch('bad', [
                ev('I2', 'A', 1, 'set', 'positive'),
                ev('I2', 'A', 1, 'set', 'negative'),
            ])))
            failed = self.run_cli('ledger-import', '--db', db, '--batch', bad)
            self.assertEqual(failed.returncode, 2)
            with sqlite3.connect(db) as con:
                self.assertEqual(con.execute('SELECT COUNT(*) FROM events').fetchone()[0], 1)
                self.assertEqual(con.execute('SELECT COUNT(*) FROM batches').fetchone()[0], 1)


if __name__ == '__main__':
    unittest.main()
