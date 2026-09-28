import json,sys,math,collections
groups=collections.defaultdict(list)
for f in sys.argv[1:]:
    d=json.load(open(f))
    tag=('gcs' if '-gcs-' in f else 's3')
    cells=[c for r in d['runs'] for c in r['cells']]
    base={(c['workload'],c['leafMaxEntries'],c['databases']):c['txPerSec'] for c in cells if c['policy']=='fixed'}
    for c in cells:
        k=(c['workload'],c['leafMaxEntries'],c['databases'])
        if c['policy']=='fixed' or k not in base: continue
        groups[(tag,c['policy'])].append((c['txPerSec']/base[k],k))
for (tag,p),v in sorted(groups.items()):
    g=math.exp(sum(math.log(r) for r,_ in v)/len(v))
    worst=min(v); best=max(v)
    print(f"{tag:4} {p:22} n={len(v):2} geomean={g:5.3f} worst={worst[0]:.2f} {worst[1]} best={best[0]:.2f} {best[1]}")
