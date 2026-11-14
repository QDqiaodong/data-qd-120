import json,unittest
from pathlib import Path
from label_agreement.model import analyze
from label_agreement.agreement import pairwise
ROOT=Path(__file__).resolve().parents[1]
class AgreementTests(unittest.TestCase):
    def setUp(self):self.d=json.loads((ROOT/'fixtures/input.json').read_text())
    def test_known_pairwise_kappa_and_confusion(self):
        r=analyze(self.d);ab=r['pairs'][0]
        self.assertEqual((ab['paired_items'],ab['observed_agreement'],ab['chance_agreement'],ab['cohen_kappa']),(3,'2/3','4/9','2/5'))
        self.assertEqual(ab['confusion']['positive']['negative'],1)
    def test_missing_and_tied_items_are_distinct(self):
        r=analyze(self.d);self.assertEqual(r['items'][1]['status'],'no_strict_majority')
        self.assertEqual(r['items'][3]['status'],'unlabeled');self.assertEqual(r['coverage']['C']['fraction'],'1/2')
    def test_no_overlap_and_constant_labels_have_explicit_unknown(self):
        r=pairwise({'X':'p'},{'Y':'p'},['p','n']);self.assertEqual(r['undefined_reason'],'no_paired_items')
        r=pairwise({'X':'p'},{'X':'p'},['p','n']);self.assertIsNone(r['cohen_kappa']);self.assertEqual(r['observed_agreement'],'1')
    def test_pair_marginals_only_use_common_items(self):
        left={'X':'p','Y':'n','extra':'p'};right={'X':'p','Y':'n'}
        self.assertEqual(pairwise(left,right,['p','n'])['cohen_kappa'],'1')
    def test_disagreement_can_have_negative_kappa(self):
        r=pairwise({'X':'p','Y':'n'},{'X':'n','Y':'p'},['p','n']);self.assertEqual(r['cohen_kappa'],'-1')
    def test_duplicate_and_unknown_reference_rejected(self):
        self.d['annotations'].append(self.d['annotations'][0])
        with self.assertRaises(ValueError):analyze(self.d)
        self.setUp();self.d['annotations'][0]['label']='unknown'
        with self.assertRaises(ValueError):analyze(self.d)
    def test_permutation_invariance_and_empty_annotations(self):
        r=analyze(self.d);self.d['annotations'].reverse();self.d['items'].reverse();self.assertEqual(r,analyze(self.d))
        self.d['annotations']=[];r=analyze(self.d);self.assertTrue(all(x['status']=='unlabeled' for x in r['items']))
if __name__=='__main__':unittest.main()
