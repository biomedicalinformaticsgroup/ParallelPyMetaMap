from collections import defaultdict

def concept2dict(concepts):
    d = defaultdict(list)
    for c in concepts:
        d[c.index].append(c._asdict())
    return d