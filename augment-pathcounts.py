#!/usr/bin/env python3

import sys
import pandas as pd
import itertools

def exact_path_equals(ps):
    ps = ['-'.join(p) for p in ps]
    ps.sort()
    for k, g in itertools.groupby(ps):
        g = len(list(g))
        yield k, g

def summarize(ps, path_grouper):
    keep_fields = lambda p: (p['aspair'], p['timeslot'])
    ps.sort(key=keep_fields)
    for keep, xs in itertools.groupby(ps, key=keep_fields):
        for p, lp in path_grouper(x['path'] for x in xs):
            yield {'aspair': keep[0], 'timeslot': keep[1], 'path': p, 'pathcount': lp}

# slice for ASpair overall, and ASpair-timeslot
def grp(ps, key):
    ps.sort(key=key)
    for _, asptsps in itertools.groupby(ps, key=key):
        paths = list(asptsps)
        tot_len = sum(x['pathcount'] for x in paths)
        fracs = [(float(x['pathcount']) / tot_len, x) for x in paths]
        most_prevalent_frac = max(fracs, key=lambda x:x[0])[1]['pathcount'] / tot_len
        for i, p in zip(range(0, len(paths)), paths):
            p['tot_pathcount'] = tot_len
            p['path_idx'] = i
            p['most_prevalent_path_frac'] = most_prevalent_frac
            yield p

if len(sys.argv) < 4:
    print("./augment-pathcounts.py <hourly_df> <overall_df> <dfs...>")
    sys.exit(1)

hourly_df_fln = sys.argv[1]
overall_df_fln = sys.argv[2]
sys.argv = sys.argv[3:]

df = pd.read_csv(sys.argv[0], sep=" ")
if len(sys.argv) > 1:
    for fln in sys.argv[1:]:
        d = pd.read_csv(fln, sep=" ")
        df = df.append(d)

ps = df.to_dict(orient='records')
ps = list(summarize(ps, exact_path_equals))
for p in ps:
    p['aspair_ts'] = f"{p['aspair']}-{p['timeslot']}"

hourly_ps = list(grp(ps, lambda x: x['aspair_ts']))
hourly_df = pd.DataFrame.from_records(hourly_ps)
print(hourly_df)
hourly_df.to_csv(hourly_df_fln)

overall_ps = list(grp(ps, lambda x: x['aspair']))
overall_df = pd.DataFrame.from_records(overall_ps)
print(overall_df)
overall_df.to_csv(overall_df_fln)
