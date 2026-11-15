"""Content-addressed, atomically published corpus releases (build-corpus).

The command joins a v1 annotation source with a ``{item, text, group}`` corpus
file and emits deterministic train/validation/test JSON Lines files.  Only
items with at least two recorded votes and a strict majority label are written;
every other roster item is still accounted for in the manifest with an explicit
exclusion reason.

Split assignment happens per connected component of the relation "same group
(exact) or same whitespace-folded NFKC text", transitively closed.  A whole
component is mapped by a SHA-256 bucket over the seed and the component's
smallest item, so input permutations cannot move an item between splits.

Releases are content addressed by both source byte hashes and the full policy
payload.  A release is written into a private staging directory, verified, moved
into ``versions/<id>`` atomically and only then made visible through ``CURRENT``.
An exclusive lock covers staging creation through the pointer switch, so two
publishers cannot expose partial files, delete each other's staging work or
replace a successful publication with a stale pointer.  Reused versions are
re-verified byte-for-byte; corrupt ones are refused instead of overwritten.

Only the Python standard library is used.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
import re
import shutil
import tempfile
import unicodedata
from pathlib import Path

from .common import ident, read
from .model import analyze

SCHEMA_VERSION = 4
POLICY_VERSION = 1
SPLITS = ('train', 'validation', 'test')
CORPUS_LIMIT = 10_000
DATA_FILENAMES = {split: split + '.jsonl' for split in SPLITS}
MANIFEST_FILENAME = 'manifest.json'
CORPUS_MAX_BYTES = 64_000_000
MANIFEST_LIMIT = 64_000_000
DATA_MAX_BYTES = 128_000_000
SEED_MAX_DIGITS = 4000
VERSION_PATTERN = re.compile(r'\A[0-9a-f]{64}\Z')
TWO_64 = 1 << 64


class CorpusError(ValueError):
    """Raised when an input or an existing release violates the contract."""


# --------------------------------------------------------------------------- #
# Strict input validation
# --------------------------------------------------------------------------- #

def _text(value, field, item):
    if not isinstance(value, str) or not value:
        raise CorpusError('%s for item %s must be a nonempty string' % (field, item))
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        raise CorpusError('%s for item %s is not valid Unicode text' % (field, item))
    return value


def _load_corpus(payload, roster):
    if not isinstance(payload, list):
        raise CorpusError('corpus file must contain a JSON array')
    if len(payload) > CORPUS_LIMIT:
        raise CorpusError('corpus contains more than %d rows' % CORPUS_LIMIT)
    corpus = {}
    for ordinal, row in enumerate(payload):
        if not isinstance(row, dict) or set(row) != {'item', 'text', 'group'}:
            raise CorpusError('corpus row %d must contain exactly item, text, group' % ordinal)
        item = ident(row['item'])
        text = _text(row['text'], 'text', item)
        group = _text(row['group'], 'group', item)
        if item in corpus:
            raise CorpusError('duplicate corpus item: ' + item)
        if item not in roster:
            raise CorpusError('corpus references unknown item: ' + item)
        corpus[item] = {'text': text, 'group': group}
    missing = sorted(roster - set(corpus))
    if missing:
        raise CorpusError('corpus does not cover roster items: ' + ', '.join(missing[:5]))
    if len(corpus) != len(roster):  # defensive; duplicates are rejected above
        raise CorpusError('corpus must cover every roster item exactly once')
    return corpus


def _read_limited(path, limit):
    """Strict JSON read for artifacts larger than the v1 8 MB input cap.

    Corpus text is naturally larger than the v1 source cap, so this reader
    follows the same strict rules (bounded size, no duplicate keys, no
    non-finite numbers) with an explicit limit; the v1 source keeps
    ``common.read`` unchanged.
    """
    with Path(path).open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise CorpusError('JSON exceeds %d bytes: %s' % (limit, path))

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise CorpusError('duplicate JSON key: ' + key)
            result[key] = value
        return result

    def invalid(value):
        raise CorpusError('nonfinite JSON number: ' + value)

    try:
        data = json.loads(raw, object_pairs_hook=unique, parse_constant=invalid)
    except CorpusError:
        raise
    except (ValueError, TypeError) as error:
        raise CorpusError('invalid JSON in %s: %s' % (path, error))
    return data, hashlib.sha256(raw).hexdigest()


def normalize_text(text):
    """NFKC, then fold every Unicode whitespace run into one ASCII space."""
    return ' '.join(unicodedata.normalize('NFKC', text).split())


def _validate_seed(seed):
    if type(seed) is not int:
        raise CorpusError('seed must be an integer')
    return seed


def parse_seed(text):
    """Parse a base-10 seed from the CLI into the canonical integer.

    The textual form is part of the invocation contract: an optional leading
    minus followed by plain decimal digits, no leading zeros (except "0"), and
    a bounded digit count so the encoded material stays bounded.
    """
    if not isinstance(text, str) or not re.fullmatch(r'-?(0|[1-9][0-9]*)', text):
        raise CorpusError('seed must be a base-10 integer without leading zeros')
    if len(text.lstrip('-')) > SEED_MAX_DIGITS:
        raise CorpusError('seed is too large to encode deterministically')
    return int(text)


# --------------------------------------------------------------------------- #
# Components and deterministic split assignment
# --------------------------------------------------------------------------- #

class _UnionFind:
    def __init__(self, members):
        self.parent = {member: member for member in members}

    def find(self, member):
        root = member
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[member] != member:
            following = self.parent[member]
            self.parent[member] = root
            member = following
        return root

    def union(self, left, right):
        left, right = self.find(left), self.find(right)
        if left != right:
            self.parent[right] = left


def _components(items, corpus):
    union = _UnionFind(items)
    by_group, by_text = {}, {}
    for item in items:
        group = corpus[item]['group']
        text = normalize_text(corpus[item]['text'])
        if group in by_group:
            union.union(item, by_group[group])
        else:
            by_group[group] = item
        if text in by_text:
            union.union(item, by_text[text])
        else:
            by_text[text] = item
    grouped = {}
    for item in items:
        grouped.setdefault(union.find(item), []).append(item)
    return sorted((sorted(members) for members in grouped.values()), key=lambda members: members[0])


def _bucket(value):
    """Map an unsigned 64-bit value to a split at the documented boundaries."""
    if value * 100 < 80 * TWO_64:
        return 'train'
    if value * 100 < 90 * TWO_64:
        return 'validation'
    return 'test'


def split_for(seed, minimum_item):
    """Map a component to a split by SHA-256(seed, smallest item).

    Encoding: UTF-8 bytes ``b"build-corpus/1" + NUL + ASCII decimal seed +
    NUL + UTF-8 smallest item``.  The first 8 digest bytes are read as one
    unsigned big-endian integer ``v`` in ``[0, 2**64)``.  Buckets are
    ``train: v*100 < 80*2**64``, ``validation: 80*2**64 <= v*100 < 90*2**64``
    and ``test: v*100 >= 90*2**64``.
    """
    material = (b'build-corpus/1\x00' + str(seed).encode('ascii') + b'\x00'
                + minimum_item.encode('utf-8'))
    return _bucket(int.from_bytes(hashlib.sha256(material).digest()[:8], 'big'))


def _policy(seed):
    return {
        'policy_version': POLICY_VERSION,
        'seed': seed,
        'inclusion_rule': 'vote_count>=2 and exactly one label satisfies count*2>vote_count',
        'exclusion_reasons': {
            'no_votes': 'the item has no recorded annotation',
            'fewer_than_two_votes': 'the item has exactly one recorded annotation',
            'no_strict_majority': 'at least two votes but no label exceeds half of them',
        },
        'component_rule': 'equal exact group OR equal normalized text, transitively closed',
        'text_normalization': 'Unicode NFKC, then split on Unicode whitespace and join with one ASCII space',
        'split_rule': {
            'unit': 'whole transitive component keyed by its lexicographically smallest item',
            'encoding': "UTF-8 b'build-corpus/1' NUL ASCII-decimal seed NUL UTF-8 minimum item; SHA-256; first 8 bytes big-endian",
            'boundaries': {
                'train': 'v*100 < 80*2**64',
                'validation': '80*2**64 <= v*100 < 90*2**64',
                'test': 'v*100 >= 90*2**64',
            },
        },
        'output_encoding': ('UTF-8 JSON Lines, sorted keys, compact separators, '
                            'one LF per record, records sorted by item code point'),
        'version_rule': "SHA-256 of canonical JSON {'policy', 'source_sha256', 'corpus_sha256'}",
        'data_file_order': list(SPLITS),
    }


def _version_id(policy, source_sha256, corpus_sha256):
    material = json.dumps(
        {'policy': policy, 'source_sha256': source_sha256, 'corpus_sha256': corpus_sha256},
        sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(material).hexdigest()


# --------------------------------------------------------------------------- #
# Manifest and data bytes
# --------------------------------------------------------------------------- #

def _jsonl_bytes(records):
    chunks = []
    for record in records:
        payload = {'item': record['item'], 'text': record['text'], 'label': record['label']}
        chunks.append(json.dumps(payload, sort_keys=True, ensure_ascii=False,
                                 allow_nan=False, separators=(',', ':')))
        chunks.append('\n')
    return ''.join(chunks).encode('utf-8')


def _manifest_bytes(manifest):
    text = json.dumps(manifest, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, indent=2) + '\n'
    return text.encode('utf-8')


def _build(source_hash, corpus_hash, seed, roster_items, analysis, corpus):
    by_item = {row['item']: row for row in analysis['items']}
    labels, reasons, votes = {}, {}, {}
    for item in roster_items:
        row = by_item[item]
        count = len(row['annotated_by'])
        votes[item] = count
        if count == 0:
            reasons[item] = 'no_votes'
        elif count == 1:
            reasons[item] = 'fewer_than_two_votes'
        elif row['status'] != 'majority':
            reasons[item] = 'no_strict_majority'
        else:
            labels[item] = row['consensus']

    components = _components(roster_items, corpus)
    component_rows, membership = [], {}
    for index, members in enumerate(components):
        split = split_for(seed, members[0])
        included = [item for item in members if item in labels]
        excluded = [item for item in members if item not in labels]
        for item in members:
            membership[item] = (index, split)
        component_rows.append({
            'index': index,
            'split': split,
            'minimum_item': members[0],
            'item_count': len(members),
            'items': members,
            'included_items': included,
            'excluded_items': excluded,
        })

    records = {split: [] for split in SPLITS}
    for item in roster_items:
        if item in labels:
            split = membership[item][1]
            records[split].append({'item': item, 'text': corpus[item]['text'],
                                   'label': labels[item]})
    for split in SPLITS:
        records[split].sort(key=lambda record: record['item'])

    data = {split: _jsonl_bytes(records[split]) for split in SPLITS}
    files = {
        split: {'path': DATA_FILENAMES[split], 'sha256': hashlib.sha256(data[split]).hexdigest(),
                'line_count': len(records[split])}
        for split in SPLITS
    }
    policy = _policy(seed)
    version = _version_id(policy, source_hash, corpus_hash)
    item_rows = [{
        'item': item,
        'vote_count': votes[item],
        'label': labels.get(item),
        'included': item in labels,
        'exclusion_reason': reasons.get(item),
        'component': membership[item][0],
        'component_minimum_item': components[membership[item][0]][0],
        'split': membership[item][1],
    } for item in roster_items]
    exclusions = [{
        'item': item,
        'vote_count': votes[item],
        'reason': reasons[item],
        'component': membership[item][0],
        'split': membership[item][1],
    } for item in roster_items if item in reasons]
    counts = {
        'roster_items': len(roster_items),
        'included_items': len(labels),
        'excluded_items': len(exclusions),
        'components': len(components),
        'splits': {split: {'included_items': len(records[split]),
                           'lines': len(records[split])} for split in SPLITS},
    }
    manifest = {
        'schema_version': SCHEMA_VERSION,
        'command': 'build-corpus',
        'version': version,
        'seed': seed,
        'source_sha256': source_hash,
        'corpus_sha256': corpus_hash,
        'policy': policy,
        'counts': counts,
        'files': files,
        'components': component_rows,
        'items': item_rows,
        'exclusions': exclusions,
    }
    return manifest, data


# --------------------------------------------------------------------------- #
# Release verification
# --------------------------------------------------------------------------- #

def _strict_line(line, number):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise CorpusError('duplicate JSON key on data line %d: %s' % (number, key))
            result[key] = value
        return result

    def constant(value):
        raise CorpusError('nonfinite JSON number on data line %d: %s' % (number, value))

    try:
        return json.loads(line, object_pairs_hook=unique, parse_constant=constant)
    except CorpusError:
        raise
    except (ValueError, TypeError) as error:
        raise CorpusError('invalid JSON on data line %d: %s' % (number, error))


def _verify_data_file(path, record):
    if type(record['line_count']) is not int or record['line_count'] < 0:
        raise CorpusError('line count must be an integer: ' + str(path))
    if not isinstance(record['sha256'], str) or not VERSION_PATTERN.fullmatch(record['sha256']):
        raise CorpusError('recorded file hash is not a SHA-256 digest: ' + str(path))
    try:
        size = path.stat().st_size
    except OSError as error:
        raise CorpusError('cannot stat %s: %s' % (path, error))
    if size > DATA_MAX_BYTES:
        raise CorpusError('data file exceeds the size bound: ' + str(path))
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise CorpusError('cannot read %s: %s' % (path, error))
    digest = hashlib.sha256(raw).hexdigest()
    if digest != record['sha256']:
        raise CorpusError('hash mismatch for ' + str(path))
    lines = raw.splitlines()
    if len(lines) != record['line_count']:
        raise CorpusError('line count mismatch for ' + str(path))
    # The writer emits exactly one trailing LF per record and nothing else.
    if lines:
        canonical = b''.join(line + b'\n' for line in lines)
        if canonical != raw:
            raise CorpusError('data file line endings are not canonical: ' + str(path))
    seen = []
    for number, line in enumerate(lines, 1):
        if not line:
            raise CorpusError('empty data line %d in %s' % (number, path))
        try:
            text = line.decode('utf-8')
        except UnicodeDecodeError:
            raise CorpusError('data line %d is not UTF-8 in %s' % (number, path))
        value = _strict_line(text, number)
        if not isinstance(value, dict) or set(value) != {'item', 'text', 'label'}:
            raise CorpusError('data line %d must contain exactly item, text, label' % number)
        ident(value['item'])
        _text(value['text'], 'text', value['item'])
        ident(value['label'])
        seen.append((value['item'], value['label']))
    if seen != sorted(seen):
        raise CorpusError('data file is not sorted by item: ' + str(path))
    if len({item for item, _ in seen}) != len(seen):
        raise CorpusError('duplicate item in data file: ' + str(path))
    return seen


def verify_version(directory, expected_manifest=None, check_name=True):
    """Verify a complete release directory and return its parsed manifest.

    When ``expected_manifest`` is supplied (as it always is during a build),
    the stored manifest must match it exactly.  Independently of that, the
    directory name (when ``check_name``) and manifest ``version`` must agree
    with the content hash, every data file must match its recorded
    SHA-256/line count, records must be well formed, sorted and agree with the
    manifest's included item lists.
    """
    directory = Path(directory)
    if directory.is_symlink() or not directory.is_dir():
        raise CorpusError('version directory is missing or is a symlink: ' + str(directory))
    names = sorted(entry.name for entry in directory.iterdir())
    expected_names = sorted([MANIFEST_FILENAME] + [DATA_FILENAMES[s] for s in SPLITS])
    if names != expected_names:
        raise CorpusError('version directory has unexpected entries: ' + str(directory))
    manifest_path = directory / MANIFEST_FILENAME
    manifest, _ = _read_limited(manifest_path, MANIFEST_LIMIT)
    if not isinstance(manifest, dict):
        raise CorpusError('manifest must be a JSON object')
    if expected_manifest is not None and manifest != expected_manifest:
        raise CorpusError('existing version manifest does not match this input and policy')
    if manifest.get('schema_version') != SCHEMA_VERSION or manifest.get('command') != 'build-corpus':
        raise CorpusError('unsupported manifest schema')
    version = manifest.get('version')
    if not isinstance(version, str) or not VERSION_PATTERN.fullmatch(version):
        raise CorpusError('manifest version is not a SHA-256 digest')
    if check_name and directory.name != version:
        raise CorpusError('version directory name does not match manifest version')
    try:
        recomputed = _version_id(manifest['policy'], manifest['source_sha256'],
                                 manifest['corpus_sha256'])
    except (KeyError, TypeError) as error:
        raise CorpusError('manifest is missing version inputs: %s' % error)
    if recomputed != version:
        raise CorpusError('manifest version is not content addressed by its inputs and policy')
    files = manifest.get('files')
    if not isinstance(files, dict) or set(files) != set(SPLITS):
        raise CorpusError('manifest files must describe exactly train/validation/test')
    items = manifest.get('items')
    exclusions = manifest.get('exclusions')
    components = manifest.get('components')
    if not isinstance(items, list) or not isinstance(exclusions, list) \
            or not isinstance(components, list):
        raise CorpusError('manifest items, exclusions and components must be arrays')
    counts = manifest.get('counts')
    if not isinstance(counts, dict):
        raise CorpusError('manifest counts must be an object')
    included = [row for row in items if row.get('included')]
    excluded = [row for row in items if not row.get('included')]
    if counts.get('roster_items') != len(items) \
            or counts.get('included_items') != len(included) \
            or counts.get('excluded_items') != len(excluded) \
            or counts.get('components') != len(components):
        raise CorpusError('manifest counts disagree with its item and component tables')
    if len(exclusions) != len(excluded) \
            or {row.get('item') for row in exclusions} != {row.get('item') for row in excluded}:
        raise CorpusError('manifest exclusions do not match the excluded items')
    if len({row.get('item') for row in items}) != len(items):
        raise CorpusError('manifest item table is not unique')
    expected_included = {split: [] for split in SPLITS}
    for row in items:
        if row.get('included'):
            expected_included[row.get('split')].append((row.get('item'), row.get('label')))
        elif row.get('label') is not None or not row.get('exclusion_reason'):
            raise CorpusError('excluded item carries a label or lacks a reason')
        elif row.get('split') not in SPLITS:
            raise CorpusError('item split is not train/validation/test')
    split_counts = counts.get('splits')
    if not isinstance(split_counts, dict) or set(split_counts) != set(SPLITS):
        raise CorpusError('manifest split counts are malformed')
    for split in SPLITS:
        record = files[split]
        if not isinstance(record, dict) or set(record) != {'path', 'sha256', 'line_count'}:
            raise CorpusError('manifest file record is malformed for ' + split)
        if record['path'] != DATA_FILENAMES[split]:
            raise CorpusError('manifest file path is not canonical for ' + split)
        if split_counts[split] != {'included_items': len(expected_included[split]),
                                   'lines': record['line_count']}:
            raise CorpusError('manifest split counts disagree with items for ' + split)
        seen = _verify_data_file(directory / record['path'], record)
        if seen != sorted(expected_included[split], key=lambda pair: pair[0]):
            raise CorpusError('file contents disagree with manifest items for ' + split)
    return manifest


# --------------------------------------------------------------------------- #
# Atomic publication
# --------------------------------------------------------------------------- #

@contextmanager
def _publish_lock(root):
    """Hold the cross-process publication lock for the whole build window.

    The lock is a regular file opened with ``O_NOFOLLOW`` so a symlink planted
    at the path cannot redirect writes.  ``flock`` is released by the kernel
    when the process dies, which is why a crashed publisher can be cleaned up
    by the next one.
    """
    lock_path = root / '.build-corpus.lock'
    flags = os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0)
    try:
        descriptor = os.open(str(lock_path), flags, 0o644)
    except OSError as error:
        raise CorpusError('cannot open publication lock: %s' % error)
    handle = os.fdopen(descriptor, 'a+b')
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _layout(root):
    """Create and validate the release layout before locking it."""
    root = Path(root)
    if root.is_symlink():
        raise CorpusError('output root cannot be a symlink')
    if root.exists() and not root.is_dir():
        raise CorpusError('output root must be a directory')
    root.mkdir(parents=True, exist_ok=True)
    for name in ('versions', 'staging'):
        path = root / name
        if path.is_symlink():
            raise CorpusError(name + ' path cannot be a symlink')
        if path.exists() and not path.is_dir():
            raise CorpusError(name + ' path must be a directory')
        path.mkdir(exist_ok=True)
    current = root / 'CURRENT'
    if current.is_symlink():
        raise CorpusError('CURRENT path cannot be a symlink')
    if current.exists() and not current.is_file():
        raise CorpusError('CURRENT path must be a regular file')
    return root


def _cleanup_staging(root):
    """Reap abandoned staging and pointer temp files; caller holds the lock.

    Every live publisher holds the publication lock for its whole staging
    window, so leftover ``stage-*`` directories and ``.CURRENT-*`` temp files
    seen while we hold that lock belong to processes that died.  Unrelated
    entries are left untouched.
    """
    staging = root / 'staging'
    for entry in sorted(staging.iterdir(), key=lambda path: path.name):
        if not entry.name.startswith('stage-'):
            continue
        if entry.is_symlink() or entry.is_file():
            entry.unlink()
        else:
            shutil.rmtree(entry, ignore_errors=True)
    for entry in sorted(root.iterdir(), key=lambda path: path.name):
        if entry.name.startswith('.CURRENT-') and (entry.is_file() or entry.is_symlink()):
            entry.unlink()


def _fsync_directory(path):
    descriptor = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_bytes(path, payload):
    with open(path, 'wb') as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _stage_version(root, manifest, data):
    """Write the complete release into a private staging directory and verify."""
    staging = Path(tempfile.mkdtemp(prefix='stage-', dir=root / 'staging'))
    try:
        for split in SPLITS:
            _write_bytes(staging / DATA_FILENAMES[split], data[split])
        _write_bytes(staging / MANIFEST_FILENAME, _manifest_bytes(manifest))
        _fsync_directory(staging)
        verify_version(staging, manifest, check_name=False)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return staging


def _switch_current(root, version):
    """Atomically replace CURRENT with a version id (temp file + os.replace).

    The caller holds the publication lock (except for the recovery path, which
    also runs under it), so the pointer is only ever moved to a version that
    was fully verified in the same lock window.  A process never re-applies a
    CURRENT value it observed before another process published, which is what
    prevents interleaved publishers from reverting each other's release.
    """
    current = root / 'CURRENT'
    temp = None
    try:
        with tempfile.NamedTemporaryFile(mode='wb', dir=root, prefix='.CURRENT-',
                                         delete=False) as stream:
            temp = Path(stream.name)
            stream.write(version.encode('ascii') + b'\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, current)
        temp = None
        _fsync_directory(root)
    finally:
        if temp is not None:
            try:
                temp.unlink()
            except FileNotFoundError:
                pass


def read_current(root):
    """Return the version id named by CURRENT after validating its shape."""
    try:
        text = (Path(root) / 'CURRENT').read_text(encoding='ascii')
    except OSError as error:
        raise CorpusError('cannot read CURRENT: %s' % error)
    version = text.strip()
    if not VERSION_PATTERN.fullmatch(version):
        raise CorpusError('CURRENT does not contain a SHA-256 version id')
    return version


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def build_corpus(source_path, corpus_path, seed, output_root):
    """Build, verify and atomically publish one corpus release.

    Inputs are parsed and every deterministic byte is computed before the
    output root is touched, so a rejected input leaves an existing release
    untouched.  Under the publication lock the release is either reused (after
    a full re-verification) or staged, verified, moved into ``versions`` and
    finally exposed through ``CURRENT``.  A failure at any point before the
    pointer switch leaves the previous CURRENT and its version intact.
    """
    seed = _validate_seed(seed)
    source_data, source_hash = read(source_path)
    corpus_data, corpus_hash = _read_limited(corpus_path, CORPUS_MAX_BYTES)

    analysis = analyze(source_data)
    roster_items = [row['item'] for row in analysis['items']]
    roster = set(roster_items)
    corpus = _load_corpus(corpus_data, roster)

    manifest, data = _build(source_hash, corpus_hash, seed, roster_items, analysis, corpus)
    version = manifest['version']
    root = _layout(Path(output_root))

    with _publish_lock(root):
        _cleanup_staging(root)
        versions = root / 'versions'
        target = versions / version
        if target.exists() or target.is_symlink():
            # Byte-for-byte re-verification; a corrupt or foreign version is
            # refused rather than rewritten, so a tampered release can never
            # be "healed" into a misleading CURRENT.
            verify_version(target, manifest)
            _switch_current(root, version)
            return manifest

        staging = _stage_version(root, manifest, data)
        try:
            # Same-filesystem rename: either the complete verified directory
            # appears in versions/ or nothing does.  Staging work is never
            # deleted by another publisher because the lock is held.
            os.rename(staging, target)
            _fsync_directory(versions)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        _switch_current(root, version)
    return manifest
