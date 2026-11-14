import hashlib
import json
import random
import subprocess
import sys
import tempfile
import unittest
from itertools import product
from pathlib import Path

from label_agreement.scheduling import plan, verify_mincut_certificate

ROOT = Path(__file__).resolve().parents[1]


def source(items, annotators, annotations=None, labels=('p', 'n')):
    return {'items': list(items), 'annotators': list(annotators), 'labels': list(labels),
            'annotations': list(annotations or [])}


def ticket(item, annotator, label='p'):
    return {'item': item, 'annotator': annotator, 'label': label}


def strategy(targets, bounds, forbiddens=None):
    return {
        'strategy_version': 1,
        'targets': [{'item': item, 'target': target} for item, target in targets],
        'annotators': [
            {'annotator': a, 'min_new_tasks': lo, 'max_new_tasks': hi}
            for a, lo, hi in bounds
        ],
        'forbiddens': [{'item': i, 'annotator': a} for i, a in (forbiddens or [])],
    }


class PlanningHarness:
    def __init__(self, tempdir):
        self.dir = Path(tempdir)
        self._seq = 0

    def write(self, name, payload):
        path = self.dir / name
        path.write_text(json.dumps(payload))
        return path

    def run(self, src, strat, output='report.json'):
        self._seq += 1
        sp = self.write('source-%d.json' % self._seq, src)
        tp = self.write('strategy-%d.json' % self._seq, strat)
        op = self.dir / output
        return plan(sp, tp, op), sp, tp, op


def eligible_edges(items, annotators, tickets, forbiddens):
    holders = {}
    for item, annotator in tickets:
        holders.setdefault(item, set()).add(annotator)
    edges = []
    for item in items:
        for annotator in annotators:
            if annotator not in holders.get(item, ()) and (item, annotator) not in forbiddens:
                edges.append((item, annotator))
    return edges


def brute_force_peak(items, annotators, demand, bounds, tickets, forbiddens):
    """Independent exhaustive reference: returns the true minimum feasible peak.

    Enumerates 0/1 choices over every eligible pair without enumerating full
    schedules; returns None when no assignment meets all demands and bounds.
    """
    edges = eligible_edges(items, annotators, tickets, forbiddens)
    lower = {a: lo for a, lo, _ in bounds}
    upper = {a: hi for a, _, hi in bounds}
    best = None
    for choices in product((0, 1), repeat=len(edges)):
        per_item = {item: 0 for item in items}
        per_annotator = {a: 0 for a in annotators}
        for chosen, (item, annotator) in zip(choices, edges):
            if chosen:
                per_item[item] += 1
                per_annotator[annotator] += 1
        if any(per_item[item] != demand[item] for item in items):
            continue
        if any(not lower[a] <= per_annotator[a] <= upper[a] for a in annotators):
            continue
        peak = max(per_annotator.values(), default=0)
        best = peak if best is None else min(best, peak)
    return best


def check_report_invariants(testcase, report, src, strat, source_path, strategy_path,
                            tickets, forbiddens):
    testcase.assertEqual(report['source_sha256'], hashlib.sha256(source_path.read_bytes()).hexdigest())
    testcase.assertEqual(report['strategy_sha256'],
                         hashlib.sha256(strategy_path.read_bytes()).hexdigest())
    if report['status'] != 'planned':
        testcase.assertNotIn('tasks', report)
        return
    items = sorted(src['items'])
    annotators = sorted(src['annotators'])
    tasks = [(t['item'], t['annotator']) for t in report['tasks']]
    # Ordered task sheet: deterministic (item, annotator) order, unique pairs.
    testcase.assertEqual(tasks, sorted(tasks))
    testcase.assertEqual(len(tasks), len(set(tasks)))
    ticket_set = set(tickets)
    forbidden_set = set(forbiddens)
    for pair in tasks:
        testcase.assertNotIn(pair, ticket_set)
        testcase.assertNotIn(pair, forbidden_set)
    loads = {row['annotator']: row['new_tasks'] for row in report['annotator_loads']}
    testcase.assertEqual(sorted(loads), annotators)
    # Every bound respected; quotas count only new tasks.
    for row in report['annotator_loads']:
        testcase.assertGreaterEqual(row['new_tasks'], row['min_new_tasks'])
        testcase.assertLessEqual(row['new_tasks'], row['max_new_tasks'])
        testcase.assertEqual(row['prior_tickets'],
                             sum(1 for i, a in ticket_set if a == row['annotator']))
    # Every gap filled exactly; over-target items are never rolled back.
    holders = {}
    for item, annotator in ticket_set:
        holders.setdefault(item, set()).add(annotator)
    targets = {row['item']: row['target'] for row in strat['targets']}
    planned = {item: 0 for item in items}
    for item, annotator in tasks:
        planned[item] += 1
        testcase.assertNotIn(annotator, holders.get(item, ()))
    for row in report['item_coverage']:
        item = row['item']
        existing = len(holders.get(item, ()))
        gap = max(0, targets[item] - existing)
        testcase.assertEqual(row['existing_tickets'], existing)
        testcase.assertEqual(row['demand'], gap)
        testcase.assertEqual(row['planned'], gap)
        if existing <= targets[item]:
            testcase.assertEqual(existing + gap, targets[item])
        else:
            # Over-target tickets are retained, never revoked.
            testcase.assertEqual(existing + gap, existing)
    testcase.assertEqual(report['optimal_peak'], max(loads.values(), default=0))
    return tasks


class SchedulingOptimalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.harness = PlanningHarness(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_small_cases_match_exhaustive_brute_force(self):
        items = ['I%d' % k for k in range(1, 6)]
        annotators = ['A', 'B', 'C', 'D']
        rng = random.Random(20261114)
        for case in range(24):
            tickets = []
            seen = set()
            for item in items:
                for a in annotators:
                    if rng.random() < 0.18 and (item, a) not in seen:
                        tickets.append(ticket(item, a))
                        seen.add((item, a))
            forbiddens = []
            for item in items:
                for a in annotators:
                    if rng.random() < 0.12 and (item, a) not in seen:
                        forbiddens.append((item, a))
            targets = [(item, rng.randint(0, len(annotators))) for item in items]
            bounds = [(a, rng.randint(0, 2), rng.randint(2, 4)) for a in annotators]
            src = source(items, annotators, tickets)
            strat = strategy(targets, bounds, forbiddens)
            report, sp, tp, _ = self.harness.run(src, strat, 'case-%d.json' % case)

            holders = {}
            for row in tickets:
                holders.setdefault(row['item'], set()).add(row['annotator'])
            target_map = dict(targets)
            demand = {item: max(0, target_map[item] - len(holders.get(item, ()))) for item in items}
            expected_peak = brute_force_peak(items, annotators, demand, bounds,
                                             {(r['item'], r['annotator']) for r in tickets},
                                             set(forbiddens))
            if expected_peak is None:
                self.assertEqual(report['status'], 'infeasible')
                self._check_mincut(report, src, strat)
            else:
                self.assertEqual(report['status'], 'planned')
                self.assertEqual(report['optimal_peak'], expected_peak)
                check_report_invariants(self, report, src, strat, sp, tp,
                                        {(r['item'], r['annotator']) for r in tickets}, forbiddens)

    def test_greedy_trap_still_solved_optimally(self):
        # Item-first greedy assigns I1 to the only eligible annotator C as its
        # first step, then cannot staff I4, whose only eligible annotator is C
        # as well. A global view reserves C for I4 and covers I1 with B. The
        # flow solver must not confuse that local failure with infeasibility.
        items = ['I1', 'I2', 'I3', 'I4']
        annotators = ['A', 'B', 'C']
        forbiddens = [
            ('I1', 'A'),
            ('I4', 'A'), ('I4', 'B'),
        ]
        targets = [('I1', 1), ('I2', 1), ('I3', 1), ('I4', 1)]
        bounds = [('A', 0, 2), ('B', 0, 2), ('C', 0, 2)]
        src = source(items, annotators, [])
        strat = strategy(targets, bounds, forbiddens)
        report, sp, tp, _ = self.harness.run(src, strat)
        self.assertEqual(report['status'], 'planned')
        self.assertEqual(report['optimal_peak'], 2)
        tasks = check_report_invariants(self, report, src, strat, sp, tp, set(), forbiddens)
        i4 = [a for item, a in tasks if item == 'I4']
        self.assertEqual(i4, ['C'])  # C is the sole route into I4
        i1 = [a for item, a in tasks if item == 'I1']
        self.assertEqual(i1, ['B'])  # B keeps I1 reachable while C is reserved

    def test_enough_total_capacity_but_locally_infeasible(self):
        items = ['I1', 'I2']
        annotators = ['A', 'B', 'C']
        # Total capacity is 3 >= 3 demand, but I1 can only be staffed by A and
        # B while I2 requires two of the same three people; forbidding A/B on
        # I2 leaves only C for an item that needs two.
        forbiddens = [('I2', 'A'), ('I2', 'B')]
        targets = [('I1', 1), ('I2', 2)]
        bounds = [('A', 0, 3), ('B', 0, 3), ('C', 0, 3)]
        src = source(items, annotators, [])
        strat = strategy(targets, bounds, forbiddens)
        report, _, _, _ = self.harness.run(src, strat)
        self.assertEqual(report['status'], 'infeasible')
        self.assertNotIn('tasks', report)
        gaps = {row['item']: row for row in report['demand_gaps']}
        self.assertEqual(gaps['I2']['shortfall'], 1)
        self.assertEqual(gaps['I2']['eligible_annotators'], ['C'])
        self._check_mincut(report, src, strat)

    def test_lower_bound_conflict_is_infeasible(self):
        items = ['I1']
        annotators = ['A', 'B']
        # Two annotators each forced into >=2 new tasks, but the item needs
        # exactly one new ticket and each pair can be used at most once.
        targets = [('I1', 1)]
        bounds = [('A', 2, 3), ('B', 2, 3)]
        src = source(items, annotators, [])
        strat = strategy(targets, bounds)
        report, _, _, _ = self.harness.run(src, strat)
        self.assertEqual(report['status'], 'infeasible')
        self.assertNotIn('tasks', report)
        self._check_mincut(report, src, strat)

    def test_overfulfilled_item_is_never_rolled_back(self):
        items = ['I1', 'I2']
        annotators = ['A', 'B', 'C']
        tickets = [ticket('I1', 'A'), ticket('I1', 'B'), ticket('I1', 'C'),
                   ticket('I2', 'A')]
        targets = [('I1', 2), ('I2', 2)]  # I1 already exceeds target
        bounds = [('A', 0, 2), ('B', 0, 2), ('C', 0, 2)]
        src = source(items, annotators, tickets)
        strat = strategy(targets, bounds)
        report, sp, tp, _ = self.harness.run(src, strat)
        self.assertEqual(report['status'], 'planned')
        row_i1 = next(row for row in report['item_coverage'] if row['item'] == 'I1')
        self.assertEqual(row_i1['existing_tickets'], 3)
        self.assertEqual(row_i1['demand'], 0)
        self.assertEqual(row_i1['planned'], 0)
        check_report_invariants(self, report, src, strat, sp, tp,
                                {(r['item'], r['annotator']) for r in tickets}, [])

    def _check_mincut(self, report, src=None, strat=None):
        cert = report['mincut_certificate']
        self.assertLess(cert['achieved_flow'], cert['required_flow'])
        reachable = set(cert['reachable_nodes'])
        self.assertIn('SS', reachable)
        self.assertNotIn('TT', reachable)
        for edge in cert['cut_edges'] + cert['auxiliary_cut_edges']:
            self.assertIn(edge['from'], reachable)
            self.assertNotIn(edge['to'], reachable)
            self.assertGreaterEqual(edge['capacity'], edge['lower_bound'])
            self.assertGreaterEqual(edge['flow'], edge['lower_bound'])
            self.assertLessEqual(edge['flow'], edge['capacity'])
        if src is not None:
            rebuilt = verify_mincut_certificate(src, strat, cert)
            self.assertTrue(rebuilt['flow_consistent'])
            self.assertTrue(rebuilt['cut_edges_exist_and_cross'])
            self.assertTrue(rebuilt['valid'])
            self.assertEqual(rebuilt['required_flow'], cert['required_flow'])
            self.assertEqual(rebuilt['achieved_flow'], cert['achieved_flow'])
            # Max flows are not unique: the published cut is one valid min cut,
            # while the rebuild may surface a different one. Both must witness
            # the same required-vs-achieved flow deficit.
            self.assertEqual(rebuilt['required_flow'] - rebuilt['achieved_flow'],
                             cert['required_flow'] - cert['achieved_flow'])

    def test_fabricated_certificate_is_rejected_on_rebuild(self):
        items = ['I1', 'I2']
        annotators = ['A', 'B', 'C']
        forbiddens = [('I2', 'A'), ('I2', 'B')]
        src = source(items, annotators, [])
        strat = strategy([('I1', 1), ('I2', 2)],
                         [('A', 0, 3), ('B', 0, 3), ('C', 0, 3)], forbiddens)
        report, _, _, _ = self.harness.run(src, strat)
        self.assertEqual(report['status'], 'infeasible')
        good_cert = report['mincut_certificate']
        rebuilt = verify_mincut_certificate(src, strat, good_cert)
        self.assertTrue(rebuilt['valid'])
        forged = json.loads(json.dumps(good_cert))
        forged['required_flow'] = forged['required_flow'] + 100
        self.assertFalse(verify_mincut_certificate(src, strat, forged)['valid'])
        forged = json.loads(json.dumps(good_cert))
        forged['cut_edges'].append({'kind': 'item_to_sink', 'from': 'item:I9', 'to': 'T',
                                    'lower_bound': 0, 'capacity': 1, 'flow': 0})
        with self.assertRaises(ValueError):
            verify_mincut_certificate(src, strat, forged)


class PermutationAndScaleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.harness = PlanningHarness(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_permutation_invariance_of_assignment(self):
        rng = random.Random(7)
        items = ['I%d' % k for k in range(1, 9)]
        annotators = ['A', 'B', 'C', 'D', 'E']
        tickets = [ticket('I1', 'A'), ticket('I2', 'B'), ticket('I3', 'C')]
        forbiddens = [('I4', 'A'), ('I5', 'B')]
        targets = [(item, 3) for item in items]
        bounds = [(a, 0, 6) for a in annotators]
        reference = None
        for trial in range(8):
            shuffled_items = items[:]
            shuffled_annotators = annotators[:]
            shuffled_tickets = tickets[:]
            shuffled_forbiddens = forbiddens[:]
            rng.shuffle(shuffled_items)
            rng.shuffle(shuffled_annotators)
            rng.shuffle(shuffled_tickets)
            rng.shuffle(shuffled_forbiddens)
            shuffled_targets = targets[:]
            shuffled_bounds = bounds[:]
            rng.shuffle(shuffled_targets)
            rng.shuffle(shuffled_bounds)
            src = source(shuffled_items, shuffled_annotators, shuffled_tickets)
            strat = strategy(shuffled_targets, shuffled_bounds, shuffled_forbiddens)
            report, sp, tp, _ = self.harness.run(src, strat, 'perm-%d.json' % trial)
            self.assertEqual(report['status'], 'planned')
            fingerprint = {
                'tasks': report['tasks'],
                'loads': sorted((row['annotator'], row['new_tasks'])
                                for row in report['annotator_loads']),
                'peak': report['optimal_peak'],
            }
            if reference is None:
                reference = fingerprint
            else:
                self.assertEqual(fingerprint, reference)
            self.assertEqual(report['source_sha256'],
                             hashlib.sha256(sp.read_bytes()).hexdigest())
            self.assertEqual(report['strategy_sha256'],
                             hashlib.sha256(tp.read_bytes()).hexdigest())

    def test_thousand_item_scale_runs_in_bounded_time(self):
        rng = random.Random(11)
        n_items, n_annotators = 1000, 50
        items = ['I%04d' % k for k in range(n_items)]
        annotators = ['A%02d' % k for k in range(n_annotators)]
        tickets = []
        for item in items:
            for a in annotators:
                if rng.random() < 0.05:
                    tickets.append(ticket(item, a))
        targets = [(item, 10) for item in items]
        bounds = [(a, 0, 1000) for a in annotators]
        src = source(items, annotators, tickets)
        strat = strategy(targets, bounds)
        report, sp, tp, out_path = self.harness.run(src, strat, 'large.json')
        self.assertEqual(report['status'], 'planned')
        check_report_invariants(self, report, src, strat, sp, tp,
                                {(r['item'], r['annotator']) for r in tickets}, [])
        # Every item finishes with exactly ten tickets.
        holders = {}
        for row in tickets:
            holders.setdefault(row['item'], set()).add(row['annotator'])
        planned = {item: 0 for item in items}
        for row in report['tasks']:
            planned[row['item']] += 1
        for item in items:
            self.assertEqual(max(len(holders.get(item, ())), 10),
                             len(holders.get(item, ())) + planned[item])
        self.assertLess(out_path.stat().st_size, 8_000_000)


class SchedulingProtocolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.harness = PlanningHarness(self.temp.name)
        self.src = source(['I1', 'I2'], ['A', 'B'], [ticket('I1', 'A')])
        self.strat = strategy([('I1', 2), ('I2', 1)], [('A', 0, 2), ('B', 0, 2)])

    def tearDown(self):
        self.temp.cleanup()

    def test_protocol_violations_exit_like_validation_errors(self):
        base_dir = self.harness.dir
        sp = self.harness.write('ok-source.json', self.src)
        out = base_dir / 'report.json'
        valid = self.harness.write('ok-strategy.json', self.strat)
        good, _, _, _ = self.harness.run(self.src, self.strat)
        self.assertEqual(good['status'], 'planned')
        before = out.read_bytes()

        bad_strategies = {
            'bad-version.json': {**self.strat, 'strategy_version': 2},
            'unknown-item.json': strategy([('I9', 1), ('I2', 1)],
                                          [('A', 0, 2), ('B', 0, 2)]),
            'unknown-annotator.json': strategy(
                [('I1', 2), ('I2', 1)], [('A', 0, 2), ('Z', 0, 2)]),
            'duplicate-target.json': {
                **self.strat,
                'targets': self.strat['targets'] + [{'item': 'I1', 'target': 1}]},
            'duplicate-annotator.json': {
                **self.strat,
                'annotators': self.strat['annotators'] + [
                    {'annotator': 'A', 'min_new_tasks': 0, 'max_new_tasks': 1}]},
            'duplicate-forbidden.json': {
                **self.strat,
                'forbiddens': [{'item': 'I2', 'annotator': 'A'},
                               {'item': 'I2', 'annotator': 'A'}]},
            'contradiction.json': strategy([('I1', 2), ('I2', 1)],
                                           [('A', 2, 1), ('B', 0, 2)]),
            'missing-coverage.json': strategy([('I1', 2)], [('A', 0, 2), ('B', 0, 2)]),
        }
        for name, payload in bad_strategies.items():
            bad = self.harness.write(name, payload)
            with self.assertRaises(ValueError):
                plan(sp, bad, out)
            self.assertEqual(out.read_bytes(), before, name + ' overwrote the old report')

        bad_sources = {
            'dup-ticket.json': source(['I1'], ['A'], [ticket('I1', 'A'), ticket('I1', 'A', 'n')]),
            'bad-ref.json': source(['I1'], ['A'], [ticket('I9', 'A')]),
        }
        for name, payload in bad_sources.items():
            bad = self.harness.write(name, payload)
            with self.assertRaises(ValueError):
                plan(bad, valid, out)
            self.assertEqual(out.read_bytes(), before)

    def test_non_integer_bounds_rejected(self):
        bad = {**self.strat,
               'annotators': [{'annotator': 'A', 'min_new_tasks': 0.5,
                               'max_new_tasks': 2},
                              {'annotator': 'B', 'min_new_tasks': 0,
                               'max_new_tasks': 2}]}
        bad_path = self.harness.write('noninteger.json', bad)
        sp = self.harness.dir / 'ok-source.json'
        sp.write_text(json.dumps(self.src))
        with self.assertRaises(ValueError):
            plan(sp, bad_path, self.harness.dir / 'x.json')


class SchedulingCLITests(unittest.TestCase):
    def run_cli(self, *arguments):
        return subprocess.run([sys.executable, '-m', 'label_agreement.cli', *map(str, arguments)],
                              capture_output=True, text=True)

    def test_cli_reports_and_failure_modes(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            src = base / 'source.json'
            strat = base / 'strategy.json'
            out = base / 'report.json'
            src.write_text(json.dumps(source(['I1', 'I2'], ['A', 'B'], [ticket('I1', 'A')])))
            strat.write_text(json.dumps(
                strategy([('I1', 2), ('I2', 1)], [('A', 0, 2), ('B', 0, 2)])))
            ok = self.run_cli('plan-tasks', '--source', src, '--strategy', strat, '--output', out)
            self.assertEqual(ok.returncode, 0, ok.stderr)
            payload = json.loads(out.read_text())
            self.assertEqual(payload['schema_version'], 3)
            self.assertEqual(payload['status'], 'planned')

            # Infeasibility is a published state report, not an error exit.
            inf = base / 'infeasible.json'
            strat.write_text(json.dumps(
                strategy([('I1', 2), ('I2', 1)], [('A', 0, 0), ('B', 0, 0)])))
            failed_plan = self.run_cli('plan-tasks', '--source', src,
                                       '--strategy', strat, '--output', inf)
            self.assertEqual(failed_plan.returncode, 0, failed_plan.stderr)
            infeasible_report = json.loads(inf.read_text())
            self.assertEqual(infeasible_report['status'], 'infeasible')
            self.assertIn('mincut_certificate', infeasible_report)
            self.assertNotIn('tasks', infeasible_report)

            # Protocol errors exit 2 and retain the prior report byte-for-byte.
            prior = out.read_bytes()
            strat.write_text('{"strategy_version": 1}')
            broken = self.run_cli('plan-tasks', '--source', src,
                                  '--strategy', strat, '--output', out)
            self.assertEqual(broken.returncode, 2)
            self.assertTrue(broken.stderr.strip())
            self.assertEqual(out.read_bytes(), prior)

            # Outputs colliding with either input are rejected.
            collide = self.run_cli('plan-tasks', '--source', src,
                                   '--strategy', strat, '--output', src)
            self.assertEqual(collide.returncode, 2)

            # Legacy entry point is unchanged.
            legacy = base / 'legacy.json'
            fixture = ROOT / 'fixtures' / 'input.json'
            legacy_run = self.run_cli('--source', fixture, '--output', legacy)
            self.assertEqual(legacy_run.returncode, 0, legacy_run.stderr)
            self.assertEqual(json.loads(legacy.read_text())['schema_version'], 1)


if __name__ == '__main__':
    unittest.main()
