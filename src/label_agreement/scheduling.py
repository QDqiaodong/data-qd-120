"""Offline annotation task orchestration.

Given the existing v1 source JSON (roster plus current tickets) and a strategy
JSON, this module schedules *new* annotation tasks so that every item reaches
its target headcount while each annotator's new-task count stays inside the
strategy bounds.  Existing tickets count toward the target; overfulfilled
items are never rolled back.

Feasibility is decided exactly with a lower/upper-bound bipartite flow, never
with a greedy heuristic.  The objective is the minimum possible maximum number
of new tasks handed to any one annotator; binary search on that peak reuses the
same feasibility network.  When no schedule exists, a min-cut witness rebuilt
from the residual graph is published together with the demand gaps.

Only the Python standard library is used.
"""
from collections import deque
import json

from .common import ident, number, read, require, rows, save
from .model import unique_ids

SCHEMA_VERSION = 3
STRATEGY_VERSION = 1
ITEM_LIMIT = 10_000
ANNOTATOR_LIMIT = 50
LABEL_LIMIT = 50
ANNOTATION_LIMIT = 20_000
FORBIDDEN_LIMIT = 100_000
BOUND_MAX = 10 ** 9


class PlanningError(ValueError):
    """Raised when an input violates the scheduling protocol."""


# --------------------------------------------------------------------------- #
# Strict input validation
# --------------------------------------------------------------------------- #

def _load_source(data):
    require(data, 'items annotators labels annotations')
    items = unique_ids(data['items'], ITEM_LIMIT)
    annotators = unique_ids(data['annotators'], ANNOTATOR_LIMIT)
    labels = unique_ids(data['labels'], LABEL_LIMIT)
    item_set, annotator_set, label_set = set(items), set(annotators), set(labels)
    tickets = set()
    for ordinal, row in enumerate(rows(data['annotations'], ANNOTATION_LIMIT)):
        require(row, 'item annotator label')
        item = ident(row['item'])
        annotator = ident(row['annotator'])
        label = ident(row['label'])
        if item not in item_set or annotator not in annotator_set or label not in label_set:
            raise PlanningError('annotation %d references an unknown roster entry' % ordinal)
        if (item, annotator) in tickets:
            raise PlanningError('duplicate item/annotator decision')
        tickets.add((item, annotator))
    return items, annotators, tickets


def _load_strategy(data, items, annotators):
    require(data, 'strategy_version targets annotators forbiddens')
    if type(data['strategy_version']) is not int or data['strategy_version'] != STRATEGY_VERSION:
        raise PlanningError('unsupported strategy_version: expected %d' % STRATEGY_VERSION)
    item_set, annotator_set = set(items), set(annotators)

    targets = {}
    for row in rows(data['targets'], ITEM_LIMIT):
        require(row, 'item target')
        item = ident(row['item'])
        target = number(row['target'], 0, BOUND_MAX)
        if item not in item_set:
            raise PlanningError('target row references unknown item: ' + item)
        if item in targets:
            raise PlanningError('duplicate strategy target row: ' + item)
        targets[item] = target
    if set(targets) != item_set:
        raise PlanningError('strategy targets must cover every roster item exactly once')

    bounds = {}
    for row in rows(data['annotators'], ANNOTATOR_LIMIT):
        require(row, 'annotator min_new_tasks max_new_tasks')
        annotator = ident(row['annotator'])
        lower = number(row['min_new_tasks'], 0, BOUND_MAX)
        upper = number(row['max_new_tasks'], 0, BOUND_MAX)
        if annotator not in annotator_set:
            raise PlanningError('annotator row references unknown annotator: ' + annotator)
        if lower > upper:
            raise PlanningError('contradictory bounds for annotator: ' + annotator)
        if annotator in bounds:
            raise PlanningError('duplicate strategy annotator row: ' + annotator)
        bounds[annotator] = (lower, upper)
    if set(bounds) != annotator_set:
        raise PlanningError('strategy annotator rows must cover every roster annotator exactly once')

    forbidden = set()
    for row in rows(data['forbiddens'], FORBIDDEN_LIMIT):
        require(row, 'item annotator')
        item = ident(row['item'])
        annotator = ident(row['annotator'])
        if item not in item_set or annotator not in annotator_set:
            raise PlanningError('forbidden row references an unknown item/annotator pair')
        if (item, annotator) in forbidden:
            raise PlanningError('duplicate forbidden item/annotator pair')
        forbidden.add((item, annotator))
    return targets, bounds, forbidden


# --------------------------------------------------------------------------- #
# Dinic maximum flow on the lower/upper-bound feasibility network
# --------------------------------------------------------------------------- #

class _Edge:
    __slots__ = ('to', 'rev', 'cap')

    def __init__(self, to, rev, cap):
        self.to, self.rev, self.cap = to, rev, cap


class _Dinic:
    def __init__(self, size):
        self.graph = [[] for _ in range(size)]

    def add(self, source, target, capacity):
        forward = _Edge(target, len(self.graph[target]), capacity)
        reverse = _Edge(source, len(self.graph[source]), 0)
        self.graph[source].append(forward)
        self.graph[target].append(reverse)
        return forward

    def max_flow(self, source, sink):
        graph = self.graph
        total = 0
        while True:
            level = [-1] * len(graph)
            level[source] = 0
            queue = deque([source])
            while queue:
                node = queue.popleft()
                for edge in graph[node]:
                    if edge.cap > 0 and level[edge.to] < 0:
                        level[edge.to] = level[node] + 1
                        queue.append(edge.to)
            if level[sink] < 0:
                return total
            iterator = [0] * len(graph)

            def discharge(node, pushed):
                if node == sink:
                    return pushed
                while iterator[node] < len(graph[node]):
                    edge = graph[node][iterator[node]]
                    if edge.cap > 0 and level[edge.to] == level[node] + 1:
                        moved = discharge(edge.to, min(pushed, edge.cap))
                        if moved:
                            edge.cap -= moved
                            graph[edge.to][edge.rev].cap += moved
                            return moved
                    iterator[node] += 1
                return 0

            while True:
                moved = discharge(source, BOUND_MAX)
                if not moved:
                    break
                total += moved

    def reachable(self, source):
        seen = {source}
        queue = deque([source])
        while queue:
            node = queue.popleft()
            for edge in self.graph[node]:
                if edge.cap > 0 and edge.to not in seen:
                    seen.add(edge.to)
                    queue.append(edge.to)
        return seen


def _build_network(items, annotators, eligible_per_annotator, demand, bounds, peak):
    """Construct the lower-bound feasibility network for one candidate peak."""
    item_index = {item: 2 + len(annotators) + k for k, item in enumerate(items)}
    annotator_index = {annotator: 2 + k for k, annotator in enumerate(annotators)}
    source, sink = 0, 1
    super_source, super_sink = 2 + len(annotators) + len(items), 3 + len(annotators) + len(items)
    size = super_sink + 1
    dinic = _Dinic(size)
    balance = [0] * size
    logical = []   # protocol-level edges carrying lower/upper bounds
    auxiliary = []  # super-source/super-sink and sink->source edges

    infinite = sum(demand.values()) + sum(lo for lo, _ in bounds.values()) + 1

    for annotator in annotators:
        lower, upper = bounds[annotator]
        capped = min(upper, peak)
        handle = dinic.add(source, annotator_index[annotator], capped - lower)
        balance[annotator_index[annotator]] += lower
        balance[source] -= lower
        logical.append({'kind': 'source_to_annotator', 'from': 'S',
                        'to': 'annotator:' + annotator,
                        'lower_bound': lower, 'capacity': capped, 'handle': handle})

    for annotator in annotators:
        for item in sorted(eligible_per_annotator[annotator]):
            handle = dinic.add(annotator_index[annotator], item_index[item], 1)
            logical.append({'kind': 'annotator_to_item', 'from': 'annotator:' + annotator,
                            'to': 'item:' + item,
                            'lower_bound': 0, 'capacity': 1, 'handle': handle})

    for item in items:
        needed = demand[item]
        # Lower bound equals upper bound: the residual edge itself has capacity 0.
        handle = dinic.add(item_index[item], sink, 0)
        balance[sink] += needed
        balance[item_index[item]] -= needed
        logical.append({'kind': 'item_to_sink', 'from': 'item:' + item, 'to': 'T',
                        'lower_bound': needed, 'capacity': needed, 'handle': handle})

    handle = dinic.add(sink, source, infinite)
    auxiliary.append({'kind': 'sink_to_source', 'from': 'T', 'to': 'S',
                      'lower_bound': 0, 'capacity': infinite, 'handle': handle})

    required_flow = 0
    for node in range(size):
        if balance[node] > 0:
            handle = dinic.add(super_source, node, balance[node])
            auxiliary.append({'kind': 'super_source_to_node', 'from': 'SS',
                              'to': _node_name(node, item_index, annotator_index),
                              'lower_bound': 0, 'capacity': balance[node], 'handle': handle})
            required_flow += balance[node]
        elif balance[node] < 0:
            handle = dinic.add(node, super_sink, -balance[node])
            auxiliary.append({'kind': 'node_to_super_sink',
                              'from': _node_name(node, item_index, annotator_index), 'to': 'TT',
                              'lower_bound': 0, 'capacity': -balance[node], 'handle': handle})

    achieved_flow = dinic.max_flow(super_source, super_sink)
    context = {
        'dinic': dinic, 'logical': logical, 'auxiliary': auxiliary,
        'item_index': item_index, 'annotator_index': annotator_index,
        'required_flow': required_flow, 'achieved_flow': achieved_flow,
        'super_source': super_source, 'super_sink': super_sink,
    }
    return achieved_flow == required_flow, context


def _node_name(node, item_index, annotator_index):
    if node == 0:
        return 'S'
    if node == 1:
        return 'T'
    for annotator, index in annotator_index.items():
        if index == node:
            return 'annotator:' + annotator
    for item, index in item_index.items():
        if index == node:
            return 'item:' + item
    return str(node)


def _edge_flow(record):
    residual = record['handle'].cap
    return record['lower_bound'] + (record['capacity'] - record['lower_bound'] - residual)


def _cut_certificate(context):
    dinic = context['dinic']
    reachable_indices = dinic.reachable(context['super_source'])
    item_index, annotator_index = context['item_index'], context['annotator_index']

    def name(node):
        if node == context['super_source']:
            return 'SS'
        if node == context['super_sink']:
            return 'TT'
        return _node_name(node, item_index, annotator_index)

    index_to_name = {node: name(node) for node in reachable_indices}
    reachable_nodes = sorted(index_to_name.values())

    cut_edges, auxiliary_cut_edges = [], []
    # Resolve endpoints through the stored edge handles: every handle knows its
    # head; the tail is the reverse edge's head.
    for record in context['logical']:
        handle = record['handle']
        tail = dinic.graph[handle.to][handle.rev].to
        head = handle.to
        if tail in reachable_indices and head not in reachable_indices:
            cut_edges.append({'kind': record['kind'], 'from': record['from'], 'to': record['to'],
                               'lower_bound': record['lower_bound'], 'capacity': record['capacity'],
                               'flow': _edge_flow(record)})
    for record in context['auxiliary']:
        handle = record['handle']
        tail = dinic.graph[handle.to][handle.rev].to
        head = handle.to
        if tail in reachable_indices and head not in reachable_indices:
            auxiliary_cut_edges.append({'kind': record['kind'], 'from': record['from'],
                                        'to': record['to'], 'lower_bound': record['lower_bound'],
                                        'capacity': record['capacity'],
                                        'flow': record['capacity'] - handle.cap})
    return {
        'construction': 'lower/upper-bound bipartite flow; see docs/contract.md section v3',
        'required_flow': context['required_flow'],
        'achieved_flow': context['achieved_flow'],
        'reachable_nodes': reachable_nodes,
        'cut_edges': cut_edges,
        'auxiliary_cut_edges': auxiliary_cut_edges,
    }


def verify_mincut_certificate(source_data, strategy_data, certificate, peak=None):
    """Rebuild the feasibility network from scratch and validate a cut report.

    The rebuild follows the documented protocol in docs/contract.md and never
    touches the solver's internal graph objects.  Returns a dict describing the
    rebuilt network and whether every certificate claim holds; raises
    PlanningError when a certificate entry cannot be matched to a real edge.
    """
    items, annotators, tickets = _load_source(source_data)
    targets, bounds, forbidden = _load_strategy(strategy_data, items, annotators)
    holders = {}
    for item, annotator in tickets:
        holders.setdefault(item, set()).add(annotator)
    demand = {item: max(0, targets[item] - len(holders.get(item, ()))) for item in items}
    if peak is None:
        peak = max(hi for _, hi in bounds.values())

    # Independent node/edge tables keyed by canonical endpoint names.
    node_names = ['S', 'T']
    node_names += ['annotator:' + a for a in annotators]
    node_names += ['item:' + i for i in items]
    node_names += ['SS', 'TT']
    index = {name: k for k, name in enumerate(node_names)}
    balance = [0] * len(node_names)
    documented = []

    def register(kind, tail, head, lower, capacity):
        if tail not in index or head not in index:
            raise PlanningError('certificate references unknown node: %s -> %s' % (tail, head))
        balance[index[head]] += lower
        balance[index[tail]] -= lower
        documented.append({'kind': kind, 'from': tail, 'to': head,
                           'lower_bound': lower, 'capacity': capacity})

    for annotator in annotators:
        lower, upper = bounds[annotator]
        register('source_to_annotator', 'S', 'annotator:' + annotator,
                 lower, min(upper, peak))
    for annotator in annotators:
        for item in items:
            if annotator not in holders.get(item, ()) and (item, annotator) not in forbidden:
                register('annotator_to_item', 'annotator:' + annotator, 'item:' + item, 0, 1)
    for item in items:
        register('item_to_sink', 'item:' + item, 'T', demand[item], demand[item])
    infinite = sum(demand.values()) + sum(lo for lo, _ in bounds.values()) + 1
    register('sink_to_source', 'T', 'S', 0, infinite)

    rebuilt_auxiliary = []
    required_flow = 0
    for name in node_names:
        node = index[name]
        if balance[node] > 0:
            rebuilt_auxiliary.append({'kind': 'super_source_to_node', 'from': 'SS',
                                      'to': name, 'lower_bound': 0, 'capacity': balance[node]})
            required_flow += balance[node]
        elif balance[node] < 0:
            rebuilt_auxiliary.append({'kind': 'node_to_super_sink', 'from': name, 'to': 'TT',
                                      'lower_bound': 0, 'capacity': -balance[node]})

    dinic = _Dinic(len(node_names))
    for edge in documented:
        dinic.add(index[edge['from']], index[edge['to']],
                  edge['capacity'] - edge['lower_bound'])
    for edge in rebuilt_auxiliary:
        dinic.add(index[edge['from']], index[edge['to']], edge['capacity'])
    achieved = dinic.max_flow(index['SS'], index['TT'])
    reachable_indices = dinic.reachable(index['SS'])
    rebuilt_reachable = sorted(node_names[node] for node in reachable_indices)

    def match(edges, claimed):
        keyed = {(e['kind'], e['from'], e['to']): e for e in edges}
        for edge in claimed:
            real = keyed.get((edge['kind'], edge['from'], edge['to']))
            if real is None:
                raise PlanningError('certificate cut edge is not part of the rebuilt network: '
                                    + json.dumps(edge, sort_keys=True))
            if edge['lower_bound'] != real['lower_bound'] or edge['capacity'] != real['capacity']:
                yield False
            else:
                yield True

    cut_ok = all(match(documented, certificate.get('cut_edges', [])))
    aux_ok = all(match(rebuilt_auxiliary, certificate.get('auxiliary_cut_edges', [])))
    reachable = set(rebuilt_reachable)
    crossing_ok = all(e['from'] in reachable and e['to'] not in reachable
                      for e in certificate.get('cut_edges', [])
                      + certificate.get('auxiliary_cut_edges', []))
    flow_ok = (certificate.get('required_flow') == required_flow
               and certificate.get('achieved_flow') == achieved)
    return {
        'required_flow': required_flow,
        'achieved_flow': achieved,
        'reachable_nodes': rebuilt_reachable,
        'flow_consistent': flow_ok,
        'cut_edges_exist_and_cross': cut_ok and aux_ok and crossing_ok,
        'valid': flow_ok and cut_ok and aux_ok and crossing_ok and achieved < required_flow,
    }


# --------------------------------------------------------------------------- #
# Orchestration entry point
# --------------------------------------------------------------------------- #

def plan(source_path, strategy_path, output_path):
    source_data, source_hash = read(source_path)
    strategy_data, strategy_hash = read(strategy_path)
    items, annotators, tickets = _load_source(source_data)
    targets, bounds, forbidden = _load_strategy(strategy_data, items, annotators)

    holders = {item: set() for item in items}
    prior_per_annotator = {annotator: 0 for annotator in annotators}
    for item, annotator in tickets:
        holders[item].add(annotator)
        prior_per_annotator[annotator] += 1

    existing = {item: len(holders[item]) for item in items}
    demand = {item: max(0, targets[item] - existing[item]) for item in items}
    eligible_per_annotator = {annotator: set() for annotator in annotators}
    eligible_per_item = {}
    for item in items:
        members = set()
        if demand[item] > 0:
            for annotator in annotators:
                if annotator not in holders[item] and (item, annotator) not in forbidden:
                    eligible_per_annotator[annotator].add(item)
                    members.add(annotator)
        eligible_per_item[item] = members

    total_demand = sum(demand.values())
    peak_low = max((lo for lo, _ in bounds.values()), default=0)
    peak_high = max((hi for _, hi in bounds.values()), default=0)

    feasible, context = _build_network(items, annotators, eligible_per_annotator,
                                       demand, bounds, peak_high)
    if not feasible:
        report = {
            'schema_version': SCHEMA_VERSION,
            'status': 'infeasible',
            'source_sha256': source_hash,
            'strategy_sha256': strategy_hash,
            'demand_summary': {
                'total_demand': total_demand,
                'sum_lower_bounds': sum(lo for lo, _ in bounds.values()),
                'sum_upper_bounds': sum(hi for _, hi in bounds.values()),
                'peak_attempted': peak_high,
            },
            'demand_gaps': [
                {
                    'item': item,
                    'target': targets[item],
                    'existing_tickets': existing[item],
                    'demand': demand[item],
                    'eligible_annotators': sorted(eligible_per_item[item]),
                    'eligible_count': len(eligible_per_item[item]),
                    'shortfall': max(0, demand[item] - len(eligible_per_item[item])),
                }
                for item in items if demand[item] > len(eligible_per_item[item])
            ],
            'mincut_certificate': _cut_certificate(context),
        }
        save(output_path, report)
        return report

    # Exact minimum peak via binary search; no assignment enumeration.
    low, high = peak_low, peak_high
    while low < high:
        middle = (low + high) // 2
        ok, _ = _build_network(items, annotators, eligible_per_annotator, demand, bounds, middle)
        if ok:
            high = middle
        else:
            low = middle + 1
    peak = low
    feasible, context = _build_network(items, annotators, eligible_per_annotator,
                                       demand, bounds, peak)
    if not feasible:  # pragma: no cover - binary search invariant
        raise PlanningError('internal feasibility inconsistency')

    tasks = []
    for record in context['logical']:
        if record['kind'] == 'annotator_to_item' and _edge_flow(record) == 1:
            tasks.append((record['to'][len('item:'):], record['from'][len('annotator:'):]))
    tasks.sort()

    new_per_annotator = {annotator: 0 for annotator in annotators}
    planned_per_item = {item: 0 for item in items}
    for item, annotator in tasks:
        new_per_annotator[annotator] += 1
        planned_per_item[item] += 1

    # Defensive invariants: the flow certificate must satisfy every constraint.
    for annotator in annotators:
        lower, upper = bounds[annotator]
        load = new_per_annotator[annotator]
        if not lower <= load <= upper:
            raise PlanningError('internal bounds inconsistency for ' + annotator)
    for item in items:
        if planned_per_item[item] != demand[item]:
            raise PlanningError('internal demand inconsistency for ' + item)
    observed_peak = max(new_per_annotator.values(), default=0)
    if observed_peak != peak:
        raise PlanningError('internal peak inconsistency')

    report = {
        'schema_version': SCHEMA_VERSION,
        'status': 'planned',
        'source_sha256': source_hash,
        'strategy_sha256': strategy_hash,
        'optimal_peak': peak,
        'totals': {'demand': total_demand, 'new_tasks': len(tasks)},
        'tasks': [{'item': item, 'annotator': annotator} for item, annotator in tasks],
        'annotator_loads': [
            {
                'annotator': annotator,
                'prior_tickets': prior_per_annotator[annotator],
                'new_tasks': new_per_annotator[annotator],
                'min_new_tasks': bounds[annotator][0],
                'max_new_tasks': bounds[annotator][1],
            }
            for annotator in annotators
        ],
        'item_coverage': [
            {
                'item': item,
                'target': targets[item],
                'existing_tickets': existing[item],
                'demand': demand[item],
                'planned': planned_per_item[item],
            }
            for item in items
        ],
    }
    save(output_path, report)
    return report
