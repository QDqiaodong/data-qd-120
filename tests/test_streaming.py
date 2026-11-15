"""Tests for the streaming v1 pipeline (tokens.py + streaming.py).

Covers byte-for-byte equivalence with the original in-memory algorithm,
field-by-field agreement, permutation/order freedom (annotations before the
rosters, arbitrary top-level and array ordering), strict parsing (duplicate
JSON keys across chunk boundaries, trailing garbage, non-finite numbers,
unknown references, duplicate decisions, missing fields), exact fractions and
zero-cell preservation, atomic publication, staging cleanup, injected write
failures and symlink/path protections.
"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from label_agreement import streaming
from label_agreement.model import analyze
from label_agreement.tokens import StreamingJSONError, Tokenizer
from label_agreement.streaming import JsonWriter, StreamError

ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / 'tests' / 'tools' / 'streaming_validate.py'


def canonical_report(source_bytes, payload):
    return (json.dumps(
        {'schema_version': 1,
         'source_sha256': hashlib.sha256(source_bytes).hexdigest(),
         'result': analyze(payload)},
        sort_keys=True, ensure_ascii=False, allow_nan=False, indent=2) + '\n')


def write(path, payload):
    if isinstance(payload, (dict, list)):
        payload = json.dumps(payload)
    Path(path).write_text(payload)
    return path


def run_pipeline(source, output, max_bytes=streaming.SOURCE_MAX_BYTES):
    return streaming.analyze_file(source, output, max_bytes=max_bytes)


class TokenizerTests(unittest.TestCase):
    def tokenize(self, raw):
        tk = Tokenizer(_Bytes(raw), 1 << 30)
        out = []
        while True:
            kind, value = tk.next_token()
            if kind == 'eof':
                return out, tk.hexdigest()
            out.append((kind, value))

    def test_matches_stdlib_decoding_and_hashes_raw_bytes(self):
        raw = json.dumps({'a': ['x', 1, -2.5, True, False, None,
                                'é\t\n\\\u0001']}, ensure_ascii=False).encode()
        tokens, digest = self.tokenize(raw)
        self.assertEqual(json.loads(raw), json.loads(json.dumps(
            _reconstruct(tokens))))
        self.assertEqual(digest, hashlib.sha256(raw).hexdigest())

    def test_rejects_nonfinite_literals_bom_and_trailing_bytes(self):
        for bad in (b'NaN', b'Infinity', b'-Infinity', b'{"a":1}garbage',
                    b'\xef\xbb\xbf{}', b'{"a":01}'):
            with self.assertRaises(StreamingJSONError):
                self.tokenize(bad)
        # Structural grammar (dangling commas, missing values) is enforced by
        # the layered v1 parser rather than the generic tokenizer.
        for bad in (b'[1,]', b'{"a":}'):
            tk = Tokenizer(_Bytes(bad), 1 << 30)
            tokens = []
            while True:
                kind, _ = tk.next_token()
                if kind == 'eof':
                    break
                tokens.append(kind)
            self.assertEqual(tokens[0], '[' if bad.startswith(b'[') else '{')
        # Trailing whitespace is trailing data for a single-document stream.
        tk = Tokenizer(_Bytes(b'{"a":1}  '), 1 << 30)
        with self.assertRaises(StreamingJSONError):
            tk.expect_end()

    def test_chunk_straddling_string_and_number(self):
        raw = b'{"long": "abcdefghijklmnopqrstuvwxyz0123456789", "n": 123456789.25}'
        for cut in range(1, len(raw)):
            tk = Tokenizer(_Bytes(raw, cut), 1 << 30)
            tokens, _ = self.tokenize_via(tk)
            self.assertEqual(_reconstruct(tokens), json.loads(raw))

    def tokenize_via(self, tk):
        out = []
        while True:
            kind, value = tk.next_token()
            if kind == 'eof':
                return out, tk.hexdigest()
            out.append((kind, value))

    def test_duplicate_key_detection_is_layered_on_the_parser(self):
        # The tokenizer itself is key-agnostic; the streaming parser rejects.
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / 'dup.json'
            src.write_text('{"items":["I1"],"items":["I2"],"annotators":["A"],'
                           '"labels":["l"],"annotations":[]}')
            with self.assertRaises(StreamError):
                _parse_only(src)

    def test_size_limit_enforced_during_read(self):
        raw = b'{"a": "' + b'x' * 100 + b'"}'
        tk = Tokenizer(_Bytes(raw), 10)
        with self.assertRaises(StreamingJSONError):
            while tk.next_token()[0] != 'eof':
                pass


class _Bytes:
    """File-like object serving fixed-size chunks to force straddling."""

    def __init__(self, data, chunk=7):
        self._data = data
        self._chunk = chunk
        self._at = 0

    def read(self, size=-1):
        size = min(size, self._chunk)
        part = self._data[self._at:self._at + size]
        self._at += len(part)
        return part


def _reconstruct(tokens):
    # Convert the flat token stream back to nested Python values.
    index = 0

    def parse():
        nonlocal index
        kind, value = tokens[index]
        index += 1
        if kind in ('str', 'num'):
            return value
        if kind == 'lit':
            return {'true': True, 'false': False, 'null': None}[value]
        if kind == '[':
            result = []
            if tokens[index][0] == ']':
                index += 1
                return result
            while True:
                result.append(parse())
                sep = tokens[index][0]
                index += 1
                if sep == ']':
                    return result
        if kind == '{':
            result = {}
            if tokens[index][0] == '}':
                index += 1
                return result
            while True:
                key = tokens[index][1]
                index += 1
                assert tokens[index][0] == ':'
                index += 1
                result[key] = parse()
                sep = tokens[index][0]
                index += 1
                if sep == '}':
                    return result
        raise AssertionError(kind)

    return parse()


def _parse_only(source):
    fd, name = tempfile.mkstemp(suffix='.sqlite')
    os.close(fd)
    try:
        con = streaming._open_database(name)
        streaming.parse_source(source, con)
        con.close()
    finally:
        Path(name).unlink(missing_ok=True)


class WriterTests(unittest.TestCase):
    def test_matches_stdlib_indented_encoder_including_nested_containers(self):
        payload = {'z': [1, 2], 'a': {'m': ['x', 'y'], 'n': [], 'o': {}},
                   's': 'é"\\', 'f': None, 'b': True}
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'w.json'
            with open(path, 'wb') as stream:
                writer = JsonWriter(stream)
                writer.begin_object()
                writer.key('a')
                writer.value(payload['a'])
                writer.key('b')
                writer.value(True)
                writer.key('f')
                writer.value(None)
                writer.key('s')
                writer.value('é"\\')
                writer.key('z')
                writer.value([1, 2])
                writer.end()
                writer.finish()
            expected = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                                  allow_nan=False, indent=2) + '\n'
            self.assertEqual(path.read_text(), expected)


def base_payload():
    return {
        'items': ['I1', 'I2', 'I3', 'I4'],
        'annotators': ['A', 'B', 'C'],
        'labels': ['positive', 'negative'],
        'annotations': [
            {'item': 'I1', 'annotator': 'A', 'label': 'positive'},
            {'item': 'I1', 'annotator': 'B', 'label': 'positive'},
            {'item': 'I1', 'annotator': 'C', 'label': 'positive'},
            {'item': 'I2', 'annotator': 'A', 'label': 'positive'},
            {'item': 'I2', 'annotator': 'B', 'label': 'negative'},
            {'item': 'I3', 'annotator': 'A', 'label': 'negative'},
            {'item': 'I3', 'annotator': 'B', 'label': 'negative'},
            {'item': 'I3', 'annotator': 'C', 'label': 'negative'},
        ],
    }


class EquivalenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def assertPipelineMatches(self, payload):
        source = self.dir / 'source.json'
        raw = json.dumps(payload, ensure_ascii=False).encode()
        source.write_bytes(raw)
        out = self.dir / 'report.json'
        run_pipeline(source, out)
        self.assertEqual(out.read_text(), canonical_report(raw, payload))
        self.assertEqual(sorted(p.name for p in self.dir.iterdir()),
                         ['report.json', 'source.json'])
        return out

    def test_fixture_is_byte_identical_to_original_algorithm(self):
        source = ROOT / 'fixtures' / 'input.json'
        raw = source.read_bytes()
        out = self.dir / 'report.json'
        run_pipeline(source, out)
        self.assertEqual(out.read_bytes().decode(), canonical_report(raw, json.loads(raw)))

    def test_field_by_field_equivalence_on_rich_case(self):
        payload = base_payload()
        out = self.assertPipelineMatches(payload)
        report = json.loads(out.read_text())
        original = analyze(payload)
        self.assertEqual(report['result'], original)
        # Exact fractions, zero cells and undefined reasons survive.
        pair = report['result']['pairs'][0]
        self.assertEqual((pair['observed_agreement'], pair['chance_agreement'],
                          pair['cohen_kappa']), ('2/3', '4/9', '2/5'))
        self.assertEqual(pair['confusion']['positive']['negative'], 1)
        self.assertEqual(pair['confusion']['negative']['positive'], 0)
        self.assertIsNone(report['result']['items'][3]['consensus'])

    def test_empty_annotations_unlabeled_and_null_pair_metrics(self):
        payload = base_payload()
        payload['annotations'] = []
        out = self.assertPipelineMatches(payload)
        report = json.loads(out.read_text())
        self.assertTrue(all(row['status'] == 'unlabeled'
                            for row in report['result']['items']))
        for pair in report['result']['pairs']:
            self.assertIsNone(pair['cohen_kappa'])
            self.assertEqual(pair['undefined_reason'], 'no_paired_items')
            self.assertEqual(pair['paired_items'], 0)

    def test_constant_identical_marginals_keep_kappa_undefined(self):
        payload = {'items': ['I1', 'I2'], 'annotators': ['A', 'B'],
                   'labels': ['p', 'n'],
                   'annotations': [
                       {'item': 'I1', 'annotator': 'A', 'label': 'p'},
                       {'item': 'I1', 'annotator': 'B', 'label': 'p'},
                       {'item': 'I2', 'annotator': 'A', 'label': 'p'},
                       {'item': 'I2', 'annotator': 'B', 'label': 'p'}]}
        out = self.assertPipelineMatches(payload)
        pair = json.loads(out.read_text())['result']['pairs'][0]
        self.assertIsNone(pair['cohen_kappa'])
        self.assertEqual(pair['undefined_reason'], 'constant_identical_marginals')

    def test_annotations_may_precede_rosters_and_fields_can_be_reordered(self):
        payload = base_payload()
        reordered = {'annotations': payload['annotations'],
                     'labels': list(reversed(payload['labels'])),
                     'annotators': payload['annotators'],
                     'items': payload['items']}
        raw = json.dumps(reordered, ensure_ascii=False).encode()
        source = self.dir / 'reordered.json'
        source.write_bytes(raw)
        out = self.dir / 'r.json'
        run_pipeline(source, out)
        # Same content, different raw bytes: result equals the original result.
        self.assertEqual(json.loads(out.read_text())['result'], analyze(payload))

    def test_annotation_array_order_does_not_change_report(self):
        first = base_payload()
        second = json.loads(json.dumps(first))
        second['annotations'] = list(reversed(second['annotations']))
        second['items'] = list(reversed(second['items']))
        self.assertPipelineMatches(first)
        out2 = self.assertPipelineMatches(second)
        self.assertEqual(json.loads(out2.read_text())['result'], analyze(first))

    def test_unicode_identifiers_round_trip_through_sqlite(self):
        payload = {'items': ['项目-1', '项目-2'], 'annotators': ['标注者甲', '乙'],
                   'labels': ['标签α', '标签β'],
                   'annotations': [
                       {'item': '项目-1', 'annotator': '标注者甲', 'label': '标签α'},
                       {'item': '项目-1', 'annotator': '乙', 'label': '标签α'},
                       {'item': '项目-2', 'annotator': '标注者甲', 'label': '标签β'}]}
        self.assertPipelineMatches(payload)


class StrictParsingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def assertRejected(self, text):
        source = self.dir / 'bad.json'
        source.write_text(text)
        with self.assertRaises((StreamError, StreamingJSONError)):
            run_pipeline(source, self.dir / 'out.json')
        self.assertFalse((self.dir / 'out.json').exists())
        leftovers = [p.name for p in self.dir.iterdir()
                     if p.name.startswith('.label-agreement')]
        self.assertEqual(leftovers, [])

    def test_duplicate_json_keys_at_every_level(self):
        good = base_payload()
        self.assertRejected('{"items":["I1"],"items":["I2"],'
                            '"annotators":["A"],"labels":["l"],"annotations":[]}')
        bad = json.dumps(good)[:-1]
        self.assertRejected(json.dumps(good).replace(
            '"item": "I1"', '"item": "I1", "item": "I9"', 1))
        self.assertRejected('{"items":["I1"],"annotators":["A","A"],'
                            '"labels":["l"],"annotations":[]}')

    def test_duplicate_key_split_across_read_chunks(self):
        # A long first value pushes the repeated key past chunk boundaries.
        text = ('{"items":["' + 'x' * 4096 + '","I2"],'
                '"items":["I3"],"annotators":["A"],"labels":["l"],'
                '"annotations":[]}')
        self.assertRejected(text)

    def test_trailing_garbage_and_nonfinite_numbers_rejected(self):
        good = base_payload()
        raw = json.dumps(good)
        self.assertRejected(raw + ' ')
        self.assertRejected(raw + 'GARBAGE')
        self.assertRejected('{"items":["I1"],"annotators":["A"],"labels":[NaN],'
                            '"annotations":[]}')
        self.assertRejected('{"items":["I1"],"annotators":["A"],"labels":[Infinity],'
                            '"annotations":[]}')
        self.assertRejected('{"items":["I1",],"annotators":["A"],"labels":["l"],'
                            '"annotations":[]}')
    def test_missing_top_level_field_and_unknown_fields(self):
        good = base_payload()
        for drop in ('items', 'annotators', 'labels', 'annotations'):
            partial = dict(good)
            del partial[drop]
            self.assertRejected(json.dumps(partial))
        extended = dict(good)
        extended['extra'] = 1
        self.assertRejected(json.dumps(extended))

    def test_unknown_references_and_duplicate_decisions_rejected(self):
        good = base_payload()
        bad = json.loads(json.dumps(good))
        bad['annotations'][0]['item'] = 'NOPE'
        self.assertRejected(json.dumps(bad))
        bad = json.loads(json.dumps(good))
        bad['annotations'][0]['annotator'] = 'Z'
        self.assertRejected(json.dumps(bad))
        bad = json.loads(json.dumps(good))
        bad['annotations'][0]['label'] = 'ghost'
        self.assertRejected(json.dumps(bad))
        bad = json.loads(json.dumps(good))
        bad['annotations'].append(dict(bad['annotations'][0]))
        self.assertRejected(json.dumps(bad))

    def test_last_row_conflict_rolls_back_everything(self):
        # The duplicate is the final annotation, after many valid rows.
        rows = [{'item': 'I%d' % k, 'annotator': 'A', 'label': 'positive'}
                for k in range(1, 5)]
        rows.append(dict(rows[-1]))  # duplicate final decision
        payload = {'items': ['I%d' % k for k in range(1, 5)],
                   'annotators': ['A', 'B'], 'labels': ['positive', 'negative'],
                   'annotations': rows}
        self.assertRejected(json.dumps(payload))

    def test_identifier_rules_and_roster_limits(self):
        self.assertRejected('{"items":[""],"annotators":["A"],"labels":["l"],'
                            '"annotations":[]}')
        self.assertRejected('{"items":[" leading"],"annotators":["A"],'
                            '"labels":["l"],"annotations":[]}')
        self.assertRejected('{"items":["tab\\t"],"annotators":["A"],'
                            '"labels":["l"],"annotations":[]}')
        long_id = 'x' * 129
        self.assertRejected(json.dumps(
            {'items': [long_id], 'annotators': ['A'], 'labels': ['l'],
             'annotations': []}))
        self.assertRejected('{"items":[],"annotators":["A"],"labels":["l"],'
                            '"annotations":[]}')

    def test_limit_boundaries(self):
        payload = {'items': ['I1'], 'annotators': ['A'],
                   'labels': ['l'], 'annotations': []}
        source = self.dir / 'small.json'
        write(source, payload)
        with self.assertRaises(StreamError):
            run_pipeline(source, self.dir / 'o.json', max_bytes=10)
        self.assertFalse((self.dir / 'o.json').exists())


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)
        self.source = self.dir / 'source.json'
        write(self.source, base_payload())
        self.out = self.dir / 'report.json'

    def tearDown(self):
        self.temp.cleanup()

    def test_successful_rerun_is_deterministic_and_atomically_replaces(self):
        run_pipeline(self.source, self.out)
        first = self.out.read_bytes()
        run_pipeline(self.source, self.out)
        self.assertEqual(self.out.read_bytes(), first)
        self.assertEqual(sorted(p.name for p in self.dir.iterdir()),
                         ['report.json', 'source.json'])

    def test_failure_retains_prior_report_and_clears_staging(self):
        run_pipeline(self.source, self.out)
        prior = self.out.read_bytes()
        bad = self.dir / 'bad.json'
        bad.write_text('{"items":["I1"],"annotators":["A"],"labels":[NaN],'
                       '"annotations":[]}')
        with self.assertRaises(StreamError):
            run_pipeline(bad, self.out)
        self.assertEqual(self.out.read_bytes(), prior)
        self.assertEqual([p.name for p in self.dir.iterdir()
                          if p.name.startswith('.label-agreement')], [])

    def test_injected_write_failures_keep_old_report_and_remove_staging(self):
        run_pipeline(self.source, self.out)
        prior = self.out.read_bytes()
        original_open = streaming.open_builtin

        def fail_open(path, *args, **kwargs):
            if str(path).startswith(str(self.dir)) and 'report' in str(path):
                raise OSError('injected staging write failure')
            return original_open(path, *args, **kwargs)

        streaming.open_builtin = fail_open
        try:
            with self.assertRaises(OSError):
                run_pipeline(self.source, self.out)
        finally:
            streaming.open_builtin = original_open
        self.assertEqual(self.out.read_bytes(), prior)
        self.assertEqual([p.name for p in self.dir.iterdir()
                          if p.name.startswith('.label-agreement')], [])

        # Failure during the final atomic rename leaves the old report intact.
        original_replace = streaming.os.replace

        def fail_replace(src, dst):
            if Path(dst) == self.out:
                raise OSError('injected rename failure')
            return original_replace(src, dst)

        streaming.os.replace = fail_replace
        try:
            with self.assertRaises(OSError):
                run_pipeline(self.source, self.out)
        finally:
            streaming.os.replace = original_replace
        self.assertEqual(self.out.read_bytes(), prior)
        self.assertEqual([p.name for p in self.dir.iterdir()
                          if p.name.startswith('.label-agreement')], [])

    def test_symlink_output_and_same_path_are_rejected(self):
        link = self.dir / 'link.json'
        link.symlink_to(self.out)
        with self.assertRaises(StreamError):
            run_pipeline(self.source, link)
        with self.assertRaises(StreamError):
            run_pipeline(self.source, self.source)

    def test_cli_process_exit_codes_and_stdout_budget(self):
        env = {**os.environ, 'PYTHONPATH': str(ROOT / 'src')}
        ok = subprocess.run([sys.executable, '-m', 'label_agreement.cli',
                             '--source', str(self.source), '--output',
                             str(self.out)], capture_output=True, text=True, env=env)
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertLess(len(ok.stdout.encode()), 4096)
        bad = self.dir / 'bad.json'
        bad.write_text('not json at all')
        failed = subprocess.run([sys.executable, '-m', 'label_agreement.cli',
                                 '--source', str(bad), '--output',
                                 str(self.out)], capture_output=True, text=True, env=env)
        self.assertEqual(failed.returncode, 2)
        self.assertTrue(failed.stderr.strip())


class IndependentValidatorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)
        self.source = self.dir / 'source.json'
        write(self.source, base_payload())
        self.report = self.dir / 'report.json'
        run_pipeline(self.source, self.report)

    def tearDown(self):
        self.temp.cleanup()

    def test_validator_accepts_good_report_and_flags_tampering(self):
        env = {**os.environ, 'PYTHONPATH': str(ROOT / 'src')}
        good = subprocess.run([sys.executable, str(VALIDATOR),
                               '--source', str(self.source), '--report',
                               str(self.report), '--expect-annotations', '8',
                               '--expect-items', '4', '--expect-annotators', '3',
                               '--expect-labels', '2'], capture_output=True, text=True, env=env)
        self.assertEqual(good.returncode, 0, good.stderr)
        self.assertIn('VALIDATION OK', good.stdout)
        # Tamper with a statistic; the independent recomputation must catch it.
        tampered = self.dir / 'tampered.json'
        text = self.report.read_text().replace('"paired_items": 3',
                                               '"paired_items": 9', 1)
        tampered.write_text(text)
        bad = subprocess.run([sys.executable, str(VALIDATOR),
                              '--source', str(self.source), '--report',
                              str(tampered)], capture_output=True, text=True, env=env)
        self.assertEqual(bad.returncode, 2)
        self.assertIn('VALIDATION FAILED', bad.stdout)
        # A wrong source hash is detected as well.
        other = self.dir / 'other.json'
        payload = json.loads(self.source.read_text())
        payload['annotations'][0] = dict(payload['annotations'][0], label='negative')
        other.write_text(json.dumps(payload))
        bad2 = subprocess.run([sys.executable, str(VALIDATOR),
                               '--source', str(other), '--report',
                               str(self.report)], capture_output=True, text=True, env=env)
        self.assertEqual(bad2.returncode, 2)
        self.assertIn('VALIDATION FAILED', bad2.stdout)


if __name__ == '__main__':
    unittest.main()
