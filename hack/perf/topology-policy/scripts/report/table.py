import json,sys
files=sys.argv[1].split(','); pols=sys.argv[2].split(',')
rows=[]
for f in files:
    d=json.load(open(f))
    for r in d['runs']: rows+=[dict(c,src=f) for c in r['cells']]
print(f"{'cell':22}"+''.join(f"{p.replace('avoidable','av'):>16}" for p in pols))
for w in ['single','hot','adjacent','random','scan']:
    for L in (16,128):
        for db in (1,4):
            line=f"{w+' L'+str(L)+' db'+str(db):22}"
            for p in pols:
                label,_,src=p.partition('@')
                cs=[c for c in rows if c['workload']==w and c['leafMaxEntries']==L and c['databases']==db and c['policy']==label and (not src or src in c['src'])]
                c=cs[0] if cs else None
                line+=f"{c['txPerSec']:7.1f}({c['adaptSplits']:>3}/{c['adaptMerges']:<3})" if c else ' '*16
            print(line)
