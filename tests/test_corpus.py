"""Tests for the build-corpus release pipeline.

The verifier in this file is deliberately independent of the implementation:
it recomputes the documented version id, split bucket, every file hash and the
JSONL contents from first principles, so a self-consistent but wrong release
cannot pass.
"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
import unittest
from pathlib import Path

from label_agreement import corpus
from label_agreement.corpus import CorpusError, build_corpus, read_current, verify_version

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / 'tests' / 'corpus_runner.py'
SPLITS = ('train', 'validation', 'test')


# --------------------------------------------------------------------------- #
# Fixture builders
# --------------------------------------------------------------------------- #

def source(annotations, items=None, annotators=('A', 'B', 'C'), labels=('p', 'n', 'm')):
    if items is None:
        items = sorted({row['item'] for row in annotations})
    return {'items': list(items), 'annotators': list(annotators), 'labels': list(labels),
            'annotations': list(annotations)}


def vote(item, annotator, label):
    return {'item': item, 'annotator': annotator, 'label': label}


def corpus_rows(pairs):
    return [{'item': item, 'text': text, 'group': group} for item, text, group in pairs]


def write_json(path, payload):
    Path(path).write_text(json.dumps(payload), encoding='utf-8')
    return Path(path)


def tree_digest(directory):
    """Hash every regular file under a directory (paths included)."""
    digest = hashlib.sha256()
    for path in sorted(Path(directory).rglob('*')):
        if path.is_file() and not path.is_symlink():
            digest.update(str(path.relative_to(directory)).encode('utf-8'))
            digest.update(b'\0')
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


# --------------------------------------------------------------------------- #
# Independent verification helpers
# --------------------------------------------------------------------------- #

def independent_split(seed, minimum_item):
    material = (b'build-corpus/1\x00' + str(seed).encode('ascii') + b'\x00'
                + minimum_item.encode('utf-8'))
    value = int.from_bytes(hashlib.sha256(material).digest()[:8], 'big')
    if value * 100 < 80 * (1 << 64):
        return 'train'
    if value * 100 < 90 * (1 << 64):
        return 'validation'
    return 'test'


def independent_version(manifest):
    payload = {'policy': manifest['policy'],
               'source_sha256': manifest['source_sha256'],
               'corpus_sha256': manifest['corpus_sha256']}
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                         allow_nan=False, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def read_jsonl(path):
    records = []
    for line in Path(path).read_bytes().split(b'\n'):
        if line == b'':
            continue
        records.append(json.loads(line.decode('utf-8')))
    return records


def verify_release(testcase, root, expected_source=None, expected_corpus=None):
    """Fully verify the release named by CURRENT, returning the manifest."""
    root = Path(root)
    current_path = root / 'CURRENT'
    testcase.assertTrue(current_path.is_file())
    testcase.assertFalse(current_path.is_symlink())
    version = current_path.read_text(encoding='ascii').strip()
    testcase.assertRegex(version, r'^[0-9a-f]{64}$')
    testcase.assertEqual(version, read_current(root))
    directory = root / 'versions' / version
    testcase.assertTrue(directory.is_dir())
    testcase.assertEqual(sorted(p.name for p in directory.iterdir()),
                         ['manifest.json', 'test.jsonl', 'train.jsonl', 'validation.jsonl'])
    manifest = json.loads((directory / 'manifest.json').read_text(encoding='utf-8'))

    # Hashes of both inputs, recomputed from the files themselves.
    if expected_source is not None:
        testcase.assertEqual(manifest['source_sha256'],
                             hashlib.sha256(Path(expected_source).read_bytes()).hexdigest())
    if expected_corpus is not None:
        testcase.assertEqual(manifest['corpus_sha256'],
                             hashlib.sha256(Path(expected_corpus).read_bytes()).hexdigest())
    # Content address: independent recomputation of the documented rule.
    testcase.assertEqual(manifest['version'], version)
    testcase.assertEqual(independent_version(manifest), version)
    testcase.assertEqual(manifest['schema_version'], 4)
    testcase.assertEqual(manifest['command'], 'build-corpus')
    testcase.assertEqual(manifest['seed'], manifest['policy']['seed'])

    # All roster items accounted exactly once, included/excluded disjoint.
    items = {row['item']: row for row in manifest['items']}
    testcase.assertEqual(len(items), len(manifest['items']))
    testcase.assertEqual(len(items), manifest['counts']['roster_items'])
    included = {item for item, row in items.items() if row['included']}
    excluded = {item for item, row in items.items() if not row['included']}
    testcase.assertFalse(included & excluded)
    testcase.assertEqual(len(included) + len(excluded), len(items))
    reasons = {row['item']: row['reason'] for row in manifest['exclusions']}
    testcase.assertEqual(set(reasons), excluded)
    for item, row in items.items():
        testcase.assertEqual(row['exclusion_reason'], reasons.get(item))
        if row['included']:
            testcase.assertIsNotNone(row['label'])
        else:
            testcase.assertIsNone(row['label'])
            testcase.assertIn(row['exclusion_reason'],
                              {'no_votes', 'fewer_than_two_votes', 'no_strict_majority'})
    testcase.assertEqual(manifest['counts']['included_items'], len(included))
    testcase.assertEqual(manifest['counts']['excluded_items'], len(excluded))

    # Per-file hash/line count, JSONL shape, ordering and item coverage.
    split_items, split_records = {}, {}
    for split in SPLITS:
        record = manifest['files'][split]
        testcase.assertEqual(record['path'], split + '.jsonl')
        raw = (directory / record['path']).read_bytes()
        testcase.assertEqual(hashlib.sha256(raw).hexdigest(), record['sha256'])
        testcase.assertEqual(raw.count(b'\n'), record['line_count'])
        records = read_jsonl(directory / record['path'])
        testcase.assertEqual(len(records), record['line_count'])
        testcase.assertEqual(manifest['counts']['splits'][split]['lines'], len(records))
        entries = [(row['item'], row['label'], row['text']) for row in records]
        testcase.assertEqual(entries, sorted(entries, key=lambda entry: entry[0]))
        for row in records:
            testcase.assertEqual(set(row), {'item', 'text', 'label'})
            testcase.assertTrue(row['text'])
        split_items[split] = {row['item'] for row in records}
        split_records[split] = {row['item']: row for row in records}
    testcase.assertEqual(set().union(*split_items.values()), included)
    testcase.assertEqual(sum(len(s) for s in split_items.values()), len(included))
    for left in SPLITS:
        for right in SPLITS:
            if left != right:
                testcase.assertEqual(split_items[left] & split_items[right], set())

    # Components: every roster item in exactly one; same split as its component;
    # bucket keyed by the lexicographically smallest item.
    seen, component_of = set(), {}
    for component in manifest['components']:
        members = component['items']
        testcase.assertEqual(component['item_count'], len(members))
        testcase.assertEqual(component['minimum_item'], min(members))
        testcase.assertEqual(component['split'], independent_split(manifest['seed'],
                                                                   min(members)))
        testcase.assertEqual(component['included_items'],
                             [item for item in members if item in included])
        testcase.assertEqual(component['excluded_items'],
                             [item for item in members if item in excluded])
        for item in members:
            testcase.assertEqual(items[item]['split'], component['split'])
            testcase.assertEqual(items[item]['component'], component['index'])
            testcase.assertEqual(items[item]['component_minimum_item'],
                                 component['minimum_item'])
        testcase.assertFalse(seen & set(members))
        seen |= set(members)
        for item in members:
            component_of[item] = component['index']
    testcase.assertEqual(seen, set(items))
    testcase.assertEqual(len(manifest['components']), manifest['counts']['components'])
    exclusion_rows = {row['item']: row for row in manifest['exclusions']}
    for item, reason in reasons.items():
        testcase.assertEqual(exclusion_rows[item]['component'], component_of[item])
        testcase.assertEqual(exclusion_rows[item]['split'], items[item]['split'])

    # Included records live exactly in their component's split and carry the
    # item's own text and majority label; excluded items never leak into data.
    for item in included:
        split = items[item]['split']
        row = split_records[split][item]
        testcase.assertEqual(row['label'], items[item]['label'])
    for split in SPLITS:
        for item in split_items[split]:
            testcase.assertEqual(items[item]['split'], split)
    return manifest


# --------------------------------------------------------------------------- #
# The main fixture: exclusion bridge across a transitive component
# --------------------------------------------------------------------------- #

BRIDGE_SOURCE = source(
    [
        vote('A1', 'A', 'p'), vote('A1', 'B', 'p'),          # majority -> included
        vote('B1', 'A', 'p'), vote('B1', 'B', 'n'),          # tie -> excluded
        vote('C1', 'A', 'p'), vote('C1', 'B', 'p'),          # majority -> included
        vote('D1', 'A', 'p'),                                # one vote -> excluded
        vote('E1', 'A', 'p'), vote('E1', 'B', 'p'), vote('E1', 'C', 'p'),
        vote('F1', 'A', 'p'), vote('F1', 'B', 'n'), vote('F1', 'C', 'm'),  # 1-1-1
        vote('G1', 'A', 'n'), vote('G1', 'B', 'n'),
    ],
    items=['A1', 'B1', 'C1', 'D1', 'E1', 'F1', 'G1', 'H1'],   # H1 has no votes
)

# A1 and B1 share a group.  B1 and C1 share normalized text (width and
# whitespace variations only; NFKC does not fold case).  A1--B1--C1 is one
# transitive component, so two included items and one excluded item must all
# land in the same split.
BRIDGE_CORPUS = corpus_rows([
    ('A1', 'shared sample ', 'g-bridge'),               # group bridge
    ('B1', 'shared\t\t sample', 'g-other'),             # text bridge (NFKC+ws)
    ('C1', 'ｓｈａｒｅｄ　ｓａｍｐｌｅ', 'g-gamma'),      # fullwidth == same text
    ('D1', 'solo text', 'g-delta'),                     # excluded, alone
    ('E1', 'E sample', 'g-epsilon'),
    ('F1', 'F sample', 'g-zeta'),
    ('G1', 'G sample', 'g-eta'),
    ('H1', 'H sample', 'g-theta'),
])


class CorpusContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)
        self.src = write_json(self.dir / 'source.json', BRIDGE_SOURCE)
        self.corpus = write_json(self.dir / 'corpus.json', BRIDGE_CORPUS)
        self.root = self.dir / 'release'

    def tearDown(self):
        self.temp.cleanup()

    def build(self, seed=0, root=None):
        return build_corpus(self.src, self.corpus, seed, root or self.root)

    def test_bridge_component_is_excluded_together_and_never_leaks(self):
        reference = None
        for seed in range(40):
            manifest = self.build(seed)
            if manifest['components'][0]['split'] == 'validation' and any(
                    component['split'] == 'test' for component in manifest['components']):
                reference = (seed, manifest)
                break
        self.assertIsNotNone(reference, 'no seed produced validation and test components')
        seed, manifest = reference
        verify_release(self, self.root, self.src, self.corpus)
        component = next(c for c in manifest['components'] if c['minimum_item'] == 'A1')
        self.assertEqual(component['items'], ['A1', 'B1', 'C1'])
        self.assertEqual(component['included_items'], ['A1', 'C1'])
        self.assertEqual(component['excluded_items'], ['B1'])
        self.assertEqual(component['split'],
                         independent_split(seed, 'A1'))
        exclusions = {row['item']: row for row in manifest['exclusions']}
        self.assertEqual(exclusions['B1']['reason'], 'no_strict_majority')
        self.assertEqual(exclusions['B1']['split'], component['split'])
        # No included record from the bridge component appears outside its split.
        for split in SPLITS:
            items = {row['item'] for row in read_jsonl(
                self.root / 'versions' / manifest['version'] / (split + '.jsonl'))}
            if split == component['split']:
                self.assertEqual(items & {'A1', 'C1'}, {'A1', 'C1'})
            else:
                self.assertEqual(items & {'A1', 'B1', 'C1'}, set())

    def test_inclusion_requires_two_votes_and_strict_majority(self):
        manifest = self.build(seed=3)
        included = {row['item'] for row in manifest['items'] if row['included']}
        self.assertEqual(included, {'A1', 'C1', 'E1', 'G1'})
        reasons = {row['item']: row['reason'] for row in manifest['exclusions']}
        self.assertEqual(reasons['B1'], 'no_strict_majority')
        self.assertEqual(reasons['D1'], 'fewer_than_two_votes')
        self.assertEqual(reasons['F1'], 'no_strict_majority')
        self.assertEqual(reasons['H1'], 'no_votes')
        labels = {row['item']: row['label'] for row in manifest['items'] if row['included']}
        self.assertEqual(labels, {'A1': 'p', 'C1': 'p', 'E1': 'p', 'G1': 'n'})

    def test_transitive_closure_through_group_and_normalized_text(self):
        # A1<-group->B1<-text->C1 plus an extra pair chained by both rules.
        rows = corpus_rows([
            ('A1', 'zero width', 'g1'),
            ('B1', 'zero width', 'g2'),
            ('C1', 'ｚｅｒｏ　ｗｉｄｔｈ', 'g3'),
            ('D1', 'x', 'g4'), ('E1', 'x', 'g4'), ('F1', 'y', 'g4'),
            ('G1', 'z', 'g5'), ('H1', 'z', 'g6'),
        ])
        src = write_json(self.dir / 'chain-source.json', source(
            [vote('A1', 'A', 'p'), vote('A1', 'B', 'p'),
             vote('B1', 'A', 'p'), vote('B1', 'B', 'p'),
             vote('C1', 'A', 'p'), vote('C1', 'B', 'p'),
             vote('D1', 'A', 'p'), vote('D1', 'B', 'n'),
             vote('E1', 'A', 'p'), vote('E1', 'B', 'p'),
             vote('F1', 'A', 'p'), vote('F1', 'B', 'p'),
             vote('G1', 'A', 'p'), vote('G1', 'B', 'p'),
             vote('H1', 'A', 'p'), vote('H1', 'B', 'n')],
            items=['A1', 'B1', 'C1', 'D1', 'E1', 'F1', 'G1', 'H1']))
        corpus_path = write_json(self.dir / 'chain-corpus.json', rows)
        manifest = build_corpus(src, corpus_path, 5, self.dir / 'chain')
        component = next(c for c in manifest['components'] if c['minimum_item'] == 'A1')
        # A1~B1 by text; B1~C1 by normalized text; D1~E1~F1 all share g4.
        self.assertEqual(component['items'], ['A1', 'B1', 'C1'])
        group_component = next(c for c in manifest['components'] if c['minimum_item'] == 'D1')
        self.assertEqual(group_component['items'], ['D1', 'E1', 'F1'])
        # G1 and H1 share text z despite different groups.
        text_component = next(c for c in manifest['components'] if c['minimum_item'] == 'G1')
        self.assertEqual(text_component['items'], ['G1', 'H1'])
        verify_release(self, self.dir / 'chain', src, corpus_path)


class SplitAndDeterminismTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def _big_fixture(self, names, shuffle_seed=None):
        import random
        rng = random.Random(shuffle_seed if shuffle_seed is not None else 0)
        annotations, rows = [], []
        for name in names:
            annotations += [vote(name, 'A', 'p'), vote(name, 'B', 'p')]
            rows.append({'item': name, 'text': ' text  %s ' % name,
                         'group': 'group-' + name})
        if shuffle_seed is not None:
            rng.shuffle(annotations)
            rng.shuffle(rows)
        return source(annotations, items=list(names)), rows

    def test_same_bytes_are_byte_identical_and_reused(self):
        names = ['I%03d' % k for k in range(60)]
        src, rows = self._big_fixture(names)
        src_path = write_json(self.dir / 'source.json', src)
        corpus_path = write_json(self.dir / 'corpus.json', rows)
        root = self.dir / 'root'
        first = build_corpus(src_path, corpus_path, 11, root)
        first_tree = tree_digest(root / 'versions' / first['version'])
        staging_after_first = sorted((root / 'staging').iterdir())
        self.assertEqual(staging_after_first, [])
        # Same bytes, fresh process semantics (plain call): reuse, no staging.
        second = build_corpus(src_path, corpus_path, 11, root)
        self.assertEqual(first, second)
        self.assertEqual(first_tree, tree_digest(root / 'versions' / first['version']))
        self.assertEqual(sorted((root / 'staging').iterdir()), [])
        verify_release(self, root, src_path, corpus_path)

    def test_permutation_keeps_assignment_but_new_bytes_change_version(self):
        names = ['I%03d' % k for k in range(60)]
        src_a, rows_a = self._big_fixture(names)
        src_b, rows_b = self._big_fixture(names, shuffle_seed=77)
        # Reordering within one file only: same logical content, new bytes.
        self.assertNotEqual(json.dumps(src_a, sort_keys=True),
                            json.dumps(src_b, sort_keys=True))
        a = build_corpus(write_json(self.dir / 'a-src.json', src_a),
                         write_json(self.dir / 'a-corpus.json', rows_a), 9, self.dir / 'a')
        b = build_corpus(write_json(self.dir / 'b-src.json', src_b),
                         write_json(self.dir / 'b-corpus.json', rows_b), 9, self.dir / 'b')
        self.assertNotEqual(a['version'], b['version'])
        mapping_a = {row['item']: row['split'] for row in a['items']}
        mapping_b = {row['item']: row['split'] for row in b['items']}
        self.assertEqual(mapping_a, mapping_b)
        components_a = [(c['minimum_item'], c['items'], c['split']) for c in a['components']]
        components_b = [(c['minimum_item'], c['items'], c['split']) for c in b['components']]
        self.assertEqual(components_a, components_b)
        for split in SPLITS:
            self.assertEqual(
                (self.dir / 'a' / 'versions' / a['version'] / (split + '.jsonl')).read_bytes(),
                (self.dir / 'b' / 'versions' / b['version'] / (split + '.jsonl')).read_bytes())
        verify_release(self, self.dir / 'a')
        verify_release(self, self.dir / 'b')

    def test_seed_and_policy_change_content_address(self):
        names = ['I%03d' % k for k in range(20)]
        src, rows = self._big_fixture(names)
        src_path = write_json(self.dir / 'source.json', src)
        corpus_path = write_json(self.dir / 'corpus.json', rows)
        root = self.dir / 'root'
        manifests = [build_corpus(src_path, corpus_path, seed, root) for seed in (0, 1)]
        self.assertNotEqual(manifests[0]['version'], manifests[1]['version'])
        self.assertEqual(len(list((root / 'versions').iterdir())), 2)
        self.assertEqual(read_current(root), manifests[1]['version'])
        # Changing one corpus byte creates yet another content address.
        rows_alt = [dict(row) for row in rows]
        rows_alt[0]['text'] = rows_alt[0]['text'] + '!'
        alt = write_json(self.dir / 'alt-corpus.json', rows_alt)
        third = build_corpus(src_path, alt, 1, root)
        self.assertNotEqual(third['version'], manifests[1]['version'])
        verify_release(self, root, src_path, alt)

    def test_two_fresh_roots_produce_identical_bytes(self):
        names = ['I%03d' % k for k in range(40)]
        src, rows = self._big_fixture(names)
        src_path = write_json(self.dir / 'source.json', src)
        corpus_path = write_json(self.dir / 'corpus.json', rows)
        a = build_corpus(src_path, corpus_path, 6, self.dir / 'root-a')
        b = build_corpus(src_path, corpus_path, 6, self.dir / 'root-b')
        self.assertEqual(a, b)
        self.assertEqual(read_current(self.dir / 'root-a'), read_current(self.dir / 'root-b'))
        for name in ('manifest.json', 'train.jsonl', 'validation.jsonl', 'test.jsonl'):
            self.assertEqual(
                (self.dir / 'root-a' / 'versions' / a['version'] / name).read_bytes(),
                (self.dir / 'root-b' / 'versions' / b['version'] / name).read_bytes(), name)

    def test_bucket_coverage_of_all_three_splits(self):
        names = ['I%03d' % k for k in range(300)]
        annotations, rows = [], []
        for name in names:
            annotations += [vote(name, 'A', 'p'), vote(name, 'B', 'p')]
            rows.append({'item': name, 'text': name, 'group': 'g-' + name})
        src = write_json(self.dir / 'source.json', source(annotations, items=names))
        corpus_path = write_json(self.dir / 'corpus.json', rows)
        # Find a seed that lands items in all three buckets, exercising the 10%
        # validation and 10% test bands with a documented hash, not luck at one seed.
        seed = next(candidate for candidate in range(50)
                    if {independent_split(candidate, 'I%03d' % k) for k in range(300)}
                    == set(SPLITS))
        manifest = build_corpus(src, corpus_path, seed, self.dir / 'root')
        counts = {split: manifest['counts']['splits'][split]['included_items']
                  for split in SPLITS}
        self.assertEqual(sum(counts.values()), 300)
        self.assertGreater(counts['train'], counts['validation'])
        self.assertGreater(counts['train'], counts['test'])
        verify_release(self, self.dir / 'root', src, corpus_path)

    def test_split_bucket_boundaries_and_encoding(self):
        # Encoding and boundaries are part of the contract; recompute them by
        # hand for a few fixed inputs so a refactor cannot silently drift.
        for seed in (-5, 0, 1, 12345678901234567890):
            for item in ('I1', 'I9', 'Zz', 'a', 'ünïcode'):
                self.assertEqual(corpus.split_for(seed, item),
                                 independent_split(seed, item))
        # The material is length-delimited, so seed/item pairs cannot collide
        # by shifting digits between the two fields.
        material = lambda seed, item: (b'build-corpus/1\x00' + str(seed).encode() +
                                       b'\x00' + item.encode())
        self.assertNotEqual(material(12, '3'), material(1, '23'))
        # Boundary arithmetic: exactly 80% is train, exactly 90% is validation.
        def smallest(boundary_percent):
            return -(-boundary_percent * (1 << 64) // 100)   # ceil
        self.assertEqual(corpus._bucket(smallest(80) - 1), 'train')
        self.assertEqual(corpus._bucket(smallest(80)), 'validation')
        self.assertEqual(corpus._bucket(smallest(90) - 1), 'validation')
        self.assertEqual(corpus._bucket(smallest(90)), 'test')
        self.assertEqual(corpus._bucket((1 << 64) - 1), 'test')


class StrictParsingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)
        self.src = write_json(self.dir / 'source.json', BRIDGE_SOURCE)
        self.corpus = write_json(self.dir / 'corpus.json', BRIDGE_CORPUS)
        self.root = self.dir / 'root'
        build_corpus(self.src, self.corpus, 1, self.root)
        self.version = read_current(self.root)
        self.before = tree_digest(self.root)

    def tearDown(self):
        self.temp.cleanup()

    def assert_rejected(self, corpus_payload=None, source_payload=None, seed=1,
                        corpus_raw=None, source_raw=None):
        path = write_json(self.dir / 'bad-corpus.json',
                          corpus_payload if corpus_payload is not None else BRIDGE_CORPUS)
        if corpus_raw is not None:
            path.write_text(corpus_raw, encoding='utf-8')
        src = self.src
        if source_payload is not None or source_raw is not None:
            src = self.dir / 'bad-source.json'
            if source_raw is not None:
                src.write_text(source_raw, encoding='utf-8')
            else:
                src.write_text(json.dumps(source_payload), encoding='utf-8')
        with self.assertRaises((CorpusError, ValueError)):
            build_corpus(src, path, seed, self.root)
        # Failure before the root is touched: prior release is byte-identical.
        self.assertEqual(tree_digest(self.root), self.before)
        self.assertEqual(read_current(self.root), self.version)

    def test_corpus_shape_and_roster_coverage_are_strict(self):
        cases = [
            {'not': 'an array'},
            BRIDGE_CORPUS + [dict(BRIDGE_CORPUS[0])],             # duplicate item
            [dict(row) for row in BRIDGE_CORPUS[:-1]],             # missing item
            [dict(BRIDGE_CORPUS[0], extra='x')] + BRIDGE_CORPUS[1:],
            [dict(BRIDGE_CORPUS[0], item='Z9')] + BRIDGE_CORPUS[1:],
            [dict(BRIDGE_CORPUS[0], text='')] + BRIDGE_CORPUS[1:],
            [dict(BRIDGE_CORPUS[0], group='')] + BRIDGE_CORPUS[1:],
            [dict(BRIDGE_CORPUS[0], group=7)] + BRIDGE_CORPUS[1:],
            [dict(BRIDGE_CORPUS[0], text=None)] + BRIDGE_CORPUS[1:],
            [dict(BRIDGE_CORPUS[0], item=' A1')] + BRIDGE_CORPUS[1:],
        ]
        for payload in cases:
            with self.subTest(payload=payload):
                self.assert_rejected(corpus_payload=payload)

    def test_duplicate_keys_and_nonfinite_numbers_are_rejected(self):
        duplicated = json.dumps(BRIDGE_CORPUS)
        duplicated = duplicated[:-1] + ',{"item":"A1","item":"A1","text":"x","group":"g"}]'
        self.assert_rejected(corpus_raw=duplicated)
        self.assert_rejected(corpus_raw=json.dumps(BRIDGE_CORPUS).replace('[', '[NaN,', 1))
        self.assert_rejected(corpus_raw='{"a":1,"a":2}')
        self.assert_rejected(corpus_raw='[{"item":"A1","text":"x","group":"g","group":"h"}]')
        self.assert_rejected(corpus_raw='[{"item":"A1","text":"x","group":Infinity}]')

    def test_source_protocol_errors_are_rejected(self):
        bad = json.loads(json.dumps(BRIDGE_SOURCE))
        bad['annotations'].append(dict(bad['annotations'][0]))
        self.assert_rejected(source_payload=bad)
        bad = json.loads(json.dumps(BRIDGE_SOURCE))
        bad['annotations'][0]['label'] = 'unknown'
        self.assert_rejected(source_payload=bad)
        self.assert_rejected(source_raw='{"items":[],"items":[],"annotators":[],"labels":[]}')

    def test_non_integer_seed_and_symlinked_root_rejected(self):
        for seed in (1.5, '1', True, None):
            with self.subTest(seed=seed):
                with self.assertRaises((CorpusError, ValueError)):
                    build_corpus(self.src, self.corpus, seed, self.root)
        link = self.dir / 'link-root'
        link.symlink_to(self.root)
        with self.assertRaises(CorpusError):
            build_corpus(self.src, self.corpus, 1, link)
        self.assertEqual(read_current(self.root), self.version)


class VersionReuseAndTamperTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)
        self.src = write_json(self.dir / 'source.json', BRIDGE_SOURCE)
        self.corpus = write_json(self.dir / 'corpus.json', BRIDGE_CORPUS)
        self.root = self.dir / 'root'
        self.manifest = build_corpus(self.src, self.corpus, 2, self.root)
        self.version = self.manifest['version']

    def tearDown(self):
        self.temp.cleanup()

    def test_reuse_does_not_rewrite_version_bytes(self):
        directory = self.root / 'versions' / self.version
        stat_before = (directory / 'train.jsonl').stat().st_mtime_ns
        self.assertEqual(build_corpus(self.src, self.corpus, 2, self.root), self.manifest)
        self.assertEqual((directory / 'train.jsonl').stat().st_mtime_ns, stat_before)
        self.assertEqual(read_current(self.root), self.version)

    def test_tampered_version_is_refused_not_reused(self):
        directory = self.root / 'versions' / self.version
        target = directory / 'train.jsonl'
        original = target.read_bytes()
        target.write_bytes(original + b'{"item":"EVIL","text":"x","label":"p"}\n')
        current_before = (self.root / 'CURRENT').read_bytes()
        with self.assertRaises(CorpusError):
            build_corpus(self.src, self.corpus, 2, self.root)
        # Nothing was healed or replaced; the caller can still see what is there.
        self.assertEqual((self.root / 'CURRENT').read_bytes(), current_before)
        self.assertEqual(target.read_bytes(), original + b'{"item":"EVIL","text":"x","label":"p"}\n')
        with self.assertRaises(CorpusError):
            verify_version(directory)

    def test_manifest_tamper_is_detected(self):
        directory = self.root / 'versions' / self.version
        manifest_path = directory / 'manifest.json'
        payload = json.loads(manifest_path.read_text())
        payload['counts']['roster_items'] = 999
        manifest_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + '\n')
        with self.assertRaises(CorpusError):
            verify_version(directory)
        with self.assertRaises(CorpusError):
            build_corpus(self.src, self.corpus, 2, self.root)

    def test_extra_file_in_version_directory_is_detected(self):
        directory = self.root / 'versions' / self.version
        (directory / 'extra.txt').write_text('surprise')
        with self.assertRaises(CorpusError):
            verify_version(directory)

    def test_corrupt_current_pointer_does_not_break_reader_contract(self):
        (self.root / 'CURRENT').write_text('not-a-version\n')
        with self.assertRaises(CorpusError):
            read_current(self.root)
        # A successful build restores a valid pointer without reusing old bytes.
        manifest = build_corpus(self.src, self.corpus, 2, self.root)
        self.assertEqual(read_current(self.root), manifest['version'])
        verify_release(self, self.root, self.src, self.corpus)


# --------------------------------------------------------------------------- #
# Crash/restart and concurrency
# --------------------------------------------------------------------------- #

class CrashRestartTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)
        self.src = write_json(self.dir / 'source.json', BRIDGE_SOURCE)
        self.corpus = write_json(self.dir / 'corpus.json', BRIDGE_CORPUS)
        self.root = self.dir / 'root'

    def tearDown(self):
        self.temp.cleanup()

    def run_helper(self, seed, mode, root=None, delay=0.0):
        return subprocess.run(
            [sys.executable, str(RUNNER), '--source', str(self.src), '--corpus',
             str(self.corpus), '--seed', str(seed), '--root', str(root or self.root),
             '--mode', mode, '--delay', str(delay)],
            capture_output=True, text=True)

    def baseline(self, root=None):
        manifest = build_corpus(self.src, self.corpus, 1, root or self.root)
        verify_release(self, root or self.root, self.src, self.corpus)
        return manifest

    def test_crash_points_then_restart_recovers_and_cleans_staging(self):
        # Each fault gets a fresh root with one completed release, so a crash
        # never happens in a run that would have reused an existing version.
        for mode in ('crash-before-files', 'crash-mid-file', 'crash-before-manifest',
                     'crash-before-switch'):
            with self.subTest(mode=mode):
                root = self.dir / ('root-' + mode)
                old = self.baseline(root)
                old_current = (root / 'CURRENT').read_bytes()
                crashed = self.run_helper(8, mode, root)
                self.assertNotEqual(crashed.returncode, 0, mode)
                # The previous complete release stays current and verifiable.
                self.assertEqual((root / 'CURRENT').read_bytes(), old_current)
                verify_release(self, root, self.src, self.corpus)
                if mode == 'crash-before-switch':
                    # The release landed in versions/ but was never exposed.
                    landed = [path for path in (root / 'versions').iterdir()
                              if path.name != old['version']]
                    self.assertEqual(len(landed), 1)
                    verify_version(landed[0])
                    self.assertEqual(sorted((root / 'staging').iterdir()), [])
                else:
                    self.assertTrue(any((root / 'staging').iterdir()),
                                    'helper did not leave staging residue for ' + mode)
                restarted = self.run_helper(8, 'run', root)
                self.assertEqual(restarted.returncode, 0, restarted.stderr)
                new_version = json.loads(restarted.stdout)['version']
                self.assertEqual(read_current(root), new_version)
                self.assertNotEqual(new_version, old['version'])
                verify_release(self, root, self.src, self.corpus)
                self.assertEqual(sorted((root / 'staging').iterdir()), [])
                # Restarting the same build reuses it and stays clean.
                again = self.run_helper(8, 'run', root)
                self.assertEqual(again.returncode, 0, again.stderr)
                self.assertEqual(json.loads(again.stdout)['version'], new_version)
                self.assertEqual(sorted((root / 'staging').iterdir()), [])

    def test_crashed_pointer_temp_files_are_cleaned_on_restart(self):
        manifest = build_corpus(self.src, self.corpus, 4, self.root)
        residue = self.root / '.CURRENT-crashed'
        residue.write_text('deadbeef\n')
        verify_release(self, self.root, self.src, self.corpus)
        again = build_corpus(self.src, self.corpus, 4, self.root)
        self.assertEqual(again, manifest)
        self.assertFalse(residue.exists())

    def test_crash_before_first_release_leaves_no_current_and_restart_works(self):
        crashed = self.run_helper(3, 'crash-before-switch')
        self.assertNotEqual(crashed.returncode, 0)
        self.assertFalse((self.root / 'CURRENT').exists())
        # The verified release may already sit in versions/, but nothing was
        # exposed and no staging residue remains.
        self.assertEqual(sorted((self.root / 'staging').iterdir()), [])
        for directory in (self.root / 'versions').iterdir():
            verify_version(directory)
        restarted = self.run_helper(3, 'run')
        self.assertEqual(restarted.returncode, 0, restarted.stderr)
        verify_release(self, self.root, self.src, self.corpus)
        self.assertEqual(sorted((self.root / 'staging').iterdir()), [])


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)
        self.src = write_json(self.dir / 'source.json', BRIDGE_SOURCE)
        self.corpus = write_json(self.dir / 'corpus.json', BRIDGE_CORPUS)
        self.root = self.dir / 'root'

    def tearDown(self):
        self.temp.cleanup()

    def launch(self, seed, mode='run', delay=0.0):
        return subprocess.Popen(
            [sys.executable, str(RUNNER), '--source', str(self.src), '--corpus',
             str(self.corpus), '--seed', str(seed), '--root', str(self.root),
             '--mode', mode, '--delay', str(delay)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def test_interleaved_publishers_never_expose_partial_release(self):
        # The slow publisher holds the lock across staging (one delay per file).
        first = self.launch(1, mode='slow', delay=0.15)
        staging_dir = self.root / 'staging'
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if staging_dir.exists() and any(
                    entry.name.startswith('stage-') for entry in staging_dir.iterdir()):
                break
            time.sleep(0.01)
        else:
            first.kill()
            self.fail('slow publisher never started staging')

        observed_versions, errors = [], []

        def watch():
            current = self.root / 'CURRENT'
            limit = time.monotonic() + 20
            while time.monotonic() < limit:
                try:
                    text = current.read_text(encoding='ascii').strip()
                except FileNotFoundError:
                    time.sleep(0.001)
                    continue
                if text and text not in observed_versions:
                    observed_versions.append(text)
                    try:
                        verify_version(self.root / 'versions' / text)
                    except Exception as error:  # noqa: BLE001 - report any problem
                        errors.append((text, repr(error)))
                time.sleep(0.001)
                if first.poll() is not None and second.poll() is not None:
                    return

        # While the slow publisher stages, a second publisher must block on the
        # lock instead of deleting its staging or publishing half a release.
        second = self.launch(2, mode='run')
        watcher = threading.Thread(target=watch)
        watcher.start()
        first_out, first_err = first.communicate(timeout=30)
        second_out, second_err = second.communicate(timeout=30)
        watcher.join(timeout=30)
        self.assertEqual(first.returncode, 0, first_err)
        self.assertEqual(second.returncode, 0, second_err)
        self.assertEqual(errors, [])
        self.assertGreaterEqual(len(observed_versions), 1)
        first_version = json.loads(first_out)['version']
        second_version = json.loads(second_out)['version']
        # The first publisher's staging work survived and became a release.
        verify_version(self.root / 'versions' / first_version)
        verify_version(self.root / 'versions' / second_version)
        # Publication order is serialized: the second, blocked publisher ends
        # up with the pointer (it did not roll back to a version of its own
        # choosing from stale state).
        self.assertEqual(read_current(self.root), second_version)
        self.assertEqual(sorted((self.root / 'staging').iterdir()), [])
        verify_release(self, self.root, self.src, self.corpus)

    def test_concurrent_same_content_publishers_reuse_one_version(self):
        first = self.launch(1, mode='slow', delay=0.1)
        second = None
        staging_dir = self.root / 'staging'
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if staging_dir.exists() and any(
                    entry.name.startswith('stage-') for entry in staging_dir.iterdir()):
                break
            time.sleep(0.01)
        else:
            first.kill()
            self.fail('slow publisher never started staging')
        second = self.launch(1, mode='run')
        first_out, first_err = first.communicate(timeout=30)
        second_out, second_err = second.communicate(timeout=30)
        self.assertEqual(first.returncode, 0, first_err)
        self.assertEqual(second.returncode, 0, second_err)
        a = json.loads(first_out)['version']
        b = json.loads(second_out)['version']
        self.assertEqual(a, b)
        self.assertEqual([entry.name for entry in (self.root / 'versions').iterdir()], [a])
        self.assertEqual(read_current(self.root), a)
        self.assertEqual(sorted(staging_dir.iterdir()), [])
        verify_release(self, self.root, self.src, self.corpus)

    def test_crashing_publisher_does_not_undo_successful_publication(self):
        good = self.launch(1, mode='slow', delay=0.1)
        staging_dir = self.root / 'staging'
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if staging_dir.exists() and any(
                    entry.name.startswith('stage-') for entry in staging_dir.iterdir()):
                break
            time.sleep(0.01)
        # A second publisher of different content crashes after files/manifest
        # handling; it must not disturb the first publication once it lands.
        bad = self.launch(2, mode='crash-before-switch')
        bad.communicate(timeout=30)
        out, err = good.communicate(timeout=30)
        self.assertEqual(good.returncode, 0, err)
        version = json.loads(out)['version']
        self.assertNotEqual(bad.returncode, 0)
        # Either pointer state is legitimate, but the successful version must
        # exist intact and the pointer must never dangle.
        self.assertEqual(read_current(self.root), version)
        verify_version(self.root / 'versions' / version)
        verify_release(self, self.root, self.src, self.corpus)
        restarted = subprocess.run(
            [sys.executable, str(RUNNER), '--source', str(self.src), '--corpus',
             str(self.corpus), '--seed', '2', '--root', str(self.root)],
            capture_output=True, text=True)
        self.assertEqual(restarted.returncode, 0, restarted.stderr)


class ScaleTests(unittest.TestCase):
    def test_thousand_item_release_stays_bounded(self):
        import random
        rng = random.Random(21)
        items = ['I%04d' % k for k in range(1000)]
        annotations = []
        for item in items:
            for annotator in ('A', 'B', 'C'):
                if rng.random() < 0.6:
                    annotations.append(vote(item, annotator, rng.choice(['p', 'n', 'm'])))
        rows = [{'item': item, 'text': 'entry  %s' % item, 'group': 'grp-%d' % (k % 97)}
                for k, item in enumerate(items)]
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            src = write_json(base / 'source.json', source(annotations, items=items))
            corpus_path = write_json(base / 'corpus.json', rows)
            root = base / 'root'
            manifest = build_corpus(src, corpus_path, 3, root)
            verify_release(self, root, src, corpus_path)
            self.assertGreater(manifest['counts']['included_items'], 100)
            self.assertLess(
                (root / 'versions' / manifest['version'] / 'manifest.json').stat().st_size,
                8_000_000)


class CorpusCLITests(unittest.TestCase):
    def run_cli(self, *arguments):
        return subprocess.run(
            [sys.executable, '-m', 'label_agreement.cli', *map(str, arguments)],
            capture_output=True, text=True, cwd=ROOT,
            env={**os.environ, 'PYTHONPATH': str(ROOT / 'src')})

    def test_cli_builds_and_reports_protocol_failures(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            src = write_json(base / 'source.json', BRIDGE_SOURCE)
            corpus_path = write_json(base / 'corpus.json', BRIDGE_CORPUS)
            root = base / 'root'
            ok = self.run_cli('build-corpus', '--source', src, '--corpus', corpus_path,
                              '--seed', '5', '--output-root', root)
            self.assertEqual(ok.returncode, 0, ok.stderr)
            summary = json.loads(ok.stdout)
            self.assertEqual(summary['command'], 'build-corpus')
            self.assertEqual(summary['version'], read_current(root))
            verify_release(self, root, src, corpus_path)

            prior = (root / 'CURRENT').read_bytes()
            bad_corpus = write_json(base / 'bad.json', BRIDGE_CORPUS[:-1])
            failed = self.run_cli('build-corpus', '--source', src, '--corpus', bad_corpus,
                                  '--seed', '5', '--output-root', root)
            self.assertEqual(failed.returncode, 2)
            self.assertTrue(failed.stderr.strip())
            self.assertEqual((root / 'CURRENT').read_bytes(), prior)

            for seed in ('x', '1.5', ''):
                rejected = self.run_cli('build-corpus', '--source', src,
                                        '--corpus', corpus_path, '--seed', seed,
                                        '--output-root', root)
                self.assertEqual(rejected.returncode, 2, seed)
            collide = self.run_cli('build-corpus', '--source', src, '--corpus', corpus_path,
                                   '--seed', '5', '--output-root', src)
            self.assertEqual(collide.returncode, 2)

    def test_legacy_cli_and_other_subcommands_are_unchanged(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            out = base / 'legacy.json'
            legacy = self.run_cli('--source', ROOT / 'fixtures' / 'input.json',
                                  '--output', out)
            self.assertEqual(legacy.returncode, 0, legacy.stderr)
            self.assertEqual(json.loads(out.read_text())['schema_version'], 1)
            planned = self.run_cli('plan-tasks', '--source',
                                   ROOT / 'fixtures' / 'input.json', '--strategy',
                                   ROOT / 'fixtures' / 'strategy.sample.json',
                                   '--output', base / 'plan.json')
            self.assertEqual(planned.returncode, 0, planned.stderr)
            self.assertEqual(json.loads((base / 'plan.json').read_text())['schema_version'], 3)


if __name__ == '__main__':
    unittest.main()
