from collections import Counter
from itertools import combinations
from .common import require,ident,rows
from .agreement import pairwise,exact
from fractions import Fraction

def unique_ids(values,limit):
    result=[ident(v) for v in rows(values,limit)]
    if not result or len(result)!=len(set(result)):raise ValueError('identifier lists must be nonempty and unique')
    return sorted(result)

def analyze(data,limit=20000):
    require(data,'items annotators labels annotations');items=unique_ids(data['items'],10000)
    annotators=unique_ids(data['annotators'],50);labels=unique_ids(data['labels'],50)
    decisions={a:{} for a in annotators};per_item={i:{} for i in items}
    for row in rows(data['annotations'],limit):
        require(row,'item annotator label');i=ident(row['item']);a=ident(row['annotator']);label=ident(row['label'])
        if i not in per_item or a not in decisions or label not in labels:raise ValueError('unknown annotation reference or label')
        if i in decisions[a]:raise ValueError('duplicate item/annotator decision')
        decisions[a][i]=label;per_item[i][a]=label
    pairs=[dict(left=a,right=b,**pairwise(decisions[a],decisions[b],labels)) for a,b in combinations(annotators,2)]
    consensus=[]
    for i,votes in per_item.items():
        counts=Counter(votes.values());n=len(votes)
        winners=[label for label,count in counts.items() if count*2>n]
        status='unlabeled' if not n else ('majority' if winners else 'no_strict_majority')
        consensus.append({'item':i,'annotated_by':sorted(votes),'missing_annotators':sorted(set(annotators)-set(votes)),
                          'votes':{label:counts[label] for label in labels},'status':status,'consensus':winners[0] if winners else None})
    return {'item_count':len(items),'annotator_count':len(annotators),'annotation_count':sum(map(len,decisions.values())),
            'coverage':{a:{'labeled':len(v),'missing':len(items)-len(v),'fraction':exact(Fraction(len(v),len(items)))} for a,v in decisions.items()},
            'pairs':pairs,'items':consensus,
            'interpretation':'Agreement is consistency of recorded labels, not ground-truth accuracy.'}
