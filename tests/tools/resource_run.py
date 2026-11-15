#!/usr/bin/env python3
"""Resource measurement harness for the streaming v1 CLI.

Runs the real installed CLI in a fully independent process (``subprocess``
with a clean module set), records its wall time and peak resident set size,
then runs the independent streaming validator in a second independent
process and records its peak RSS as well.

Measurement method (Linux):
* the child is launched via ``subprocess.Popen`` and waited on;
* peak RSS is read from ``resource.getrusage(RUSAGE_CHILDREN).ru_maxrss``
  immediately after the wait, which on Linux is the largest resident set
  size in kibibytes across all reaped children of this harness; each
  measurement uses a fresh harness process (one child per invocation) so the
  reported value is exactly the child's peak, never a cumulative max;
* the CLI's own stdout is captured: only a short JSON summary is printed, so
  the pipe cannot inflate the measured resident set materially;
* exit code alone is never treated as success: the validator independently
  re-hashes the raw source bytes, recomputes every count/statistic in its
  own SQLite database and streams the full report to cross-check all fields.

Usage:
    python3 resource_run.py --source SRC.json --report OUT.json \
        --expect-annotations N --expect-items N --expect-annotators N \
        --expect-labels N [--rss-budget-mib 128]
"""
import argparse
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CLI = [sys.executable, '-m', 'label_agreement.cli']
TOOLS = Path(__file__).resolve().parent
VALIDATOR = TOOLS / 'streaming_validate.py'
SELF_MEASURE = TOOLS / 'self_measure.py'
BUDGET_MIB = 128


def child_peak_rss_mib():
    # ru_maxrss is KiB on Linux; a single child has been reaped by callers.
    peak_kib = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return peak_kib / 1024


def self_peak_rss_mib():
    # RUSAGE_SELF reports this process's own high-water mark; used by the
    # --self-measure wrapper so each measured process gets an exact,
    # non-cumulative peak rather than a max across siblings.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--report', required=True)
    parser.add_argument('--expect-annotations', type=int, required=True)
    parser.add_argument('--expect-items', type=int, required=True)
    parser.add_argument('--expect-annotators', type=int, required=True)
    parser.add_argument('--expect-labels', type=int, required=True)
    parser.add_argument('--rss-budget-mib', type=int, default=BUDGET_MIB)
    parser.add_argument('--expect-reject', action='store_true',
                        help='expect the CLI to reject the source with exit 2')
    args = parser.parse_args()

    env = {**os.environ, 'PYTHONPATH': str(ROOT / 'src')}
    started = time.monotonic()
    cli = subprocess.run(CLI + ['--source', args.source, '--output',
                                args.report],
                         capture_output=True, text=True, env=env)
    elapsed = time.monotonic() - started
    cli_rss = child_peak_rss_mib()

    if args.expect_reject:
        if cli.returncode != 2:
            print(json.dumps({'ok': False, 'reason': 'expected exit 2',
                              'returncode': cli.returncode,
                              'stderr': cli.stderr[-500:]}))
            return 2
        if Path(args.report).exists():
            print(json.dumps({'ok': False,
                              'reason': 'rejected run left an output file'}))
            return 2
        print(json.dumps({'ok': True, 'stage': 'rejection',
                          'cli_peak_rss_mib': round(cli_rss, 2),
                          'elapsed_s': round(elapsed, 2),
                          'budget_mib': args.rss_budget_mib,
                          'within_budget': cli_rss <= args.rss_budget_mib},
                         sort_keys=True))
        return 0 if cli_rss <= args.rss_budget_mib else 3

    if cli.returncode != 0:
        print(json.dumps({'ok': False, 'reason': 'cli failed',
                          'returncode': cli.returncode,
                          'stderr': cli.stderr[-500:]}))
        return 2

    started = time.monotonic()
    # Run the validator through the self-measure wrapper so RUSAGE_CHILDREN
    # attributes one exact peak to this single workload.
    validation = subprocess.run(
        [sys.executable, str(SELF_MEASURE), str(VALIDATOR),
         '--source', args.source, '--report', args.report,
         '--expect-annotations', str(args.expect_annotations),
         '--expect-items', str(args.expect_items),
         '--expect-annotators', str(args.expect_annotators),
         '--expect-labels', str(args.expect_labels)],
        capture_output=True, text=True, env=env)
    validator_elapsed = time.monotonic() - started
    combined_rss = child_peak_rss_mib()
    validator_peak = None
    marker = 'SELF_PEAK_RSS_MIB='
    for line in validation.stderr.splitlines():
        if line.startswith(marker):
            try:
                validator_peak = float(line[len(marker):])
            except ValueError:
                validator_peak = None
    report_bytes = Path(args.report).stat().st_size

    ok = (validation.returncode == 0
          and 'VALIDATION OK' in validation.stdout
          and cli_rss <= args.rss_budget_mib)
    summary = {
        'ok': ok,
        'cli_returncode': cli.returncode,
        'cli_stdout': cli.stdout.strip(),
        'cli_peak_rss_mib': round(cli_rss, 2),
        'validator_returncode': validation.returncode,
        'validator_stdout': validation.stdout.strip(),
        'validator_stderr_tail': validation.stderr[-300:],
        'validator_peak_rss_mib': (round(validator_peak, 2)
                                   if validator_peak is not None else None),
        'max_child_rss_mib': round(combined_rss, 2),
        'cli_elapsed_s': round(elapsed, 2),
        'validator_elapsed_s': round(validator_elapsed, 2),
        'report_bytes': report_bytes,
        'budget_mib': args.rss_budget_mib,
        'within_budget': cli_rss <= args.rss_budget_mib,
    }
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0 if ok else 3


if __name__ == '__main__':
    raise SystemExit(main())
