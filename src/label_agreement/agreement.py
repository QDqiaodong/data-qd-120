from collections import Counter
from fractions import Fraction

def exact(value):return str(value) if value is not None else None

def pairwise(left,right,labels):
    common=sorted(set(left)&set(right));n=len(common)
    table={a:{b:0 for b in labels} for a in labels}
    for item in common:table[left[item]][right[item]]+=1
    if not n:return {'paired_items':0,'observed_agreement':None,'chance_agreement':None,'cohen_kappa':None,'undefined_reason':'no_paired_items','confusion':table}
    row=Counter(left[i] for i in common);col=Counter(right[i] for i in common)
    po=Fraction(sum(table[x][x] for x in labels),n)
    pe=sum((Fraction(row[x]*col[x],n*n) for x in labels),Fraction())
    kappa=(po-pe)/(1-pe) if pe!=1 else None
    return {'paired_items':n,'observed_agreement':exact(po),'chance_agreement':exact(pe),'cohen_kappa':exact(kappa),
            'undefined_reason':'constant_identical_marginals' if pe==1 else None,'confusion':table}
