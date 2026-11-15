"""Resource-scale tests for the streaming v1 pipeline.

These tests build and process a 2 GiB source with at least two million
tickets and assert the independent CLI process stays within the 128 MiB RSS
budget.  They are deliberately gated behind ``RUN_SCALE_TESTS=1`` because
they need roughly 2 GiB of scratch space and tens of seconds; the regular
unit suite stays fast.  The same gating applies to the 2 GiB acceptance and
over-limit rejection checks.

The measurement never trusts an exit code alone: the independent streaming
validator (tests/tools/streaming_validate.py) recomputes the source hash and
every statistic and streams the full report to cross-check all fields.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / 'tests' / 'tools'
RUN_SCALE = os.environ.get('RUN_SCALE_TESTS') == '1'
RSS_BUDGET_MIB = 128
TICKETS = 2_000_000
ITEMS = 200_000
ANNOTATORS = 50
LABELS = 50
TWO_GIB = 2 * 1024 ** 3


@unittest.skipUnless(RUN_SCALE, 'set RUN_SCALE_TESTS=1 to run the 2 GiB resource test')
class ScaleResourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='scale-')
        cls.dir = Path(cls.temp.name)
        cls.source = cls.dir / 'source-2gib.json'
        cls.report = cls.dir / 'report.json'
        cls.env = {**os.environ, 'PYTHONPATH': str(ROOT / 'src')}
        if shutil.disk_usage(cls.dir).free < TWO_GIB + 512 * 1024 ** 2:
            raise unittest.SkipTest('not enough free disk space for a 2 GiB source')

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def run_tool(self, script, *arguments):
        return subprocess.run([sys.executable, str(TOOLS / script), *arguments],
                              capture_output=True, text=True, env=self.env)

    def test_01_exact_two_gib_with_two_million_tickets(self):
        generated = self.run_tool(
            'make_scale_source.py', '--output', self.source,
            '--tickets', str(TICKETS), '--items', str(ITEMS),
            '--annotators', str(ANNOTATORS), '--labels', str(LABELS),
            '--mode', 'exact-2gib')
        self.assertEqual(generated.returncode, 0, generated.stderr)
        meta = json.loads(generated.stdout)
        self.assertEqual(meta['bytes'], TWO_GIB)
        self.assertEqual(meta['tickets'], TICKETS)

        runner = self.run_tool(
            'resource_run.py', '--source', self.source, '--report',
            self.report, '--expect-annotations', str(TICKETS),
            '--expect-items', str(ITEMS),
            '--expect-annotators', str(ANNOTATORS),
            '--expect-labels', str(LABELS),
            '--rss-budget-mib', str(RSS_BUDGET_MIB))
        self.assertEqual(runner.returncode, 0,
                         runner.stdout + runner.stderr)
        summary = json.loads(runner.stdout)
        self.assertTrue(summary['ok'], summary)
        self.assertIn('VALIDATION OK', summary['validator_stdout'])
        self.assertLessEqual(summary['cli_peak_rss_mib'], RSS_BUDGET_MIB,
                             summary)
        # The independent validator confirms counts, not just the exit code.
        self.assertIn('annotations=%d' % TICKETS, summary['validator_stdout'])
        self.assertIn('items=%d' % ITEMS, summary['validator_stdout'])

    def test_02_over_limit_source_is_rejected_within_budget(self):
        oversized = self.dir / 'source-over.json'
        generated = self.run_tool(
            'make_scale_source.py', '--output', oversized,
            '--tickets', str(TICKETS), '--items', str(ITEMS),
            '--annotators', str(ANNOTATORS), '--labels', str(LABELS),
            '--mode', 'over-2gib')
        self.assertEqual(generated.returncode, 0, generated.stderr)
        self.assertGreater(oversized.stat().st_size, TWO_GIB)
        runner = self.run_tool(
            'resource_run.py', '--source', oversized,
            '--report', self.dir / 'never.json',
            '--expect-annotations', str(TICKETS),
            '--expect-items', str(ITEMS),
            '--expect-annotators', str(ANNOTATORS),
            '--expect-labels', str(LABELS),
            '--rss-budget-mib', str(RSS_BUDGET_MIB), '--expect-reject')
        self.assertEqual(runner.returncode, 0, runner.stdout + runner.stderr)
        summary = json.loads(runner.stdout)
        self.assertTrue(summary['ok'], summary)
        self.assertLessEqual(summary['cli_peak_rss_mib'], RSS_BUDGET_MIB,
                             summary)
        self.assertFalse((self.dir / 'never.json').exists())

    def test_03_rerun_is_byte_deterministic_and_validates_again(self):
        first = self.report.read_bytes()
        again = subprocess.run(
            [sys.executable, '-m', 'label_agreement.cli', '--source',
             str(self.source), '--output', str(self.report)],
            capture_output=True, text=True, env=self.env)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(self.report.read_bytes(), first)
        validation = self.run_tool(
            'streaming_validate.py', '--source', self.source,
            '--report', self.report,
            '--expect-annotations', str(TICKETS),
            '--expect-items', str(ITEMS),
            '--expect-annotators', str(ANNOTATORS),
            '--expect-labels', str(LABELS))
        self.assertEqual(validation.returncode, 0, validation.stderr)
        self.assertIn('VALIDATION OK', validation.stdout)


if __name__ == '__main__':
    unittest.main()
