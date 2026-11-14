import unittest, tempfile, subprocess, sys, json, hashlib
from pathlib import Path
from label_agreement.model import analyze
ROOT=Path(__file__).resolve().parents[1]
class CLITests(unittest.TestCase):
    def test_installed_cli_and_exact_source_hash(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'report.json';source=ROOT/'fixtures/input.json'
            command=[sys.executable,'-m','label_agreement.cli','--source',str(source),'--output',str(out)]
            result=subprocess.run(command,capture_output=True)
            self.assertEqual(result.returncode,0,result.stderr)
            report=json.loads(out.read_text());self.assertEqual(report['result'],analyze(json.loads(source.read_text())))
            self.assertEqual(report['source_sha256'],hashlib.sha256(source.read_bytes()).hexdigest())
            before=out.read_bytes();self.assertEqual(subprocess.run(command,capture_output=True).returncode,0)
            self.assertEqual(before,out.read_bytes())
    def test_failed_update_retains_prior_output_and_cannot_overwrite_source(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td);source=p/'bad.json';out=p/'report.json';out.write_text('prior')
            for invalid in ['{}','{"a":1,"a":2}','NaN']:
                source.write_text(invalid)
                result=subprocess.run([sys.executable,'-m','label_agreement.cli','--source',str(source),'--output',str(out)],capture_output=True)
                self.assertEqual(result.returncode,2);self.assertEqual(out.read_text(),'prior')
            before=source.read_bytes()
            result=subprocess.run([sys.executable,'-m','label_agreement.cli','--source',str(source),'--output',str(source)],capture_output=True)
            self.assertEqual(result.returncode,2);self.assertEqual(source.read_bytes(),before)
if __name__=='__main__':unittest.main()
