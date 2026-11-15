#!/usr/bin/env python3
"""Run a target module/script in this process and report its own peak RSS.

The resource harness needs an exact, non-cumulative peak for the independent
validator.  Running it inside this wrapper means ``RUSAGE_SELF`` reports this
process's high-water mark (which includes the validator's work), and the
parent's ``RUSAGE_CHILDREN`` captures the wrapper as a single child whose peak
is attributable to that one workload.

Usage:
    python3 self_measure.py <script.py> [script args ...]

Prints a final line ``SELF_PEAK_RSS_MIB=<value>`` on stderr and propagates the
target's exit code.
"""
import resource
import runpy
import sys


def main():
    if len(sys.argv) < 2:
        print('usage: self_measure.py <script> [args ...]', file=sys.stderr)
        return 64
    script = sys.argv[1]
    sys.argv = sys.argv[1:]
    try:
        runpy.run_path(script, run_name='__main__')
        code = 0
    except SystemExit as exit_event:
        code = exit_event.code if isinstance(exit_event.code, int) else 1
    peak_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print('SELF_PEAK_RSS_MIB=%.2f' % peak_mib, file=sys.stderr)
    return code


if __name__ == '__main__':
    raise SystemExit(main())
