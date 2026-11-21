#!/usr/bin/env python3
"""Differential fuzz: an implementation vs pipeline/pipeline.py on small adversarial inputs.

Usage: python scripts/fuzz_reference.py [CASES] -- CMD...
Inputs respect the documented guarantees: ingest_ts is valid F1 and unique per
event copy, and duplicate click lines are identical after unquoting.
"""
import json, os, random, subprocess, sys, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable
SEP = sys.argv.index('--')
CASES = int(sys.argv[1]) if SEP > 1 else 30
CMD = sys.argv[SEP + 1:]

LO = 1790812800000  # 2026-10-01


def iso(t, rnd):
    import datetime
    k = rnd.random()
    if k < 0.15:
        return str(t)
    if k < 0.2:
        return str(t).zfill(rnd.choice([20, 25]))
    dt = datetime.datetime.fromtimestamp(t / 1000, datetime.timezone.utc)
    if k < 0.3:
        oh = rnd.randint(0, 23); om = rnd.choice([0, 30, 59])
        sign = rnd.choice('+-')
        off = (oh * 60 + om) * (1 if sign == '+' else -1)
        loc = dt + datetime.timedelta(minutes=off)
        return loc.strftime('%Y-%m-%dT%H:%M:%S') + f'{sign}{oh:02d}:{om:02d}'
    if k < 0.33:
        return rnd.choice(['2026-10-03 10:00:00', '2026-02-30T00:00:00Z', '', 'x', '2026-10-03T10:00:00+24:00'])
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def weird(v, rnd):
    k = rnd.random()
    if k < 0.05:
        return f'"{v}"'
    if k < 0.07:
        return v + '\r'
    if k < 0.08:
        return ' ' + v
    if k < 0.09:
        return v + '"'
    if k < 0.095:
        return '"' + v
    return v


def write(path, header, lines, rnd):
    body = header + '\n' + ''.join(l + '\n' for l in lines)
    if rnd.random() < 0.3 and body.endswith('\n') and lines:
        body = body[:-1]  # no trailing LF
    if rnd.random() < 0.1:
        body += '\n'  # trailing empty line
    path.write_bytes(body.encode())


def make(d, rnd):
    users = [f'u{i}' for i in range(rnd.randint(1, 40))]
    for sub in ('events', 'clicks', 'links'):
        (d / sub).mkdir(parents=True)
    (d / 'manifest.json').write_text(json.dumps(
        {'window_start': '2026-10-01T00:00:00Z', 'window_end': rnd.choice(['2026-10-15T00:00:00Z', '2026-10-03T00:00:00Z', '2026-12-31T00:00:00Z'])}))
    write(d / 'products.csv', 'product_id,category',
          [f'{weird("p%d" % i, rnd)},{rnd.choice(["shoes", "books", "toys", "a", "B"])}' for i in range(8)], rnd)
    write(d / 'campaigns.csv', 'campaign_id,channel',
          [f'{weird("c%d" % i, rnd)},{rnd.choice(["search", "social"])}' for i in range(6)], rnd)
    for part in range(rnd.randint(1, 3)):
        lines = []
        for _ in range(rnd.randint(0, 25)):
            a, b = rnd.choice(users + ['', 'd1', 'd2', 'U1']), rnd.choice(users + ['', 'd3'])
            lines.append(rnd.choice([f'{weird(a, rnd)},{weird(b, rnd)}', f'{a},{a}', f'{a}', f'{a},{b},x', 'user_id_a,user_id_b']))
        write(d / 'links' / f'part-{part:05d}.csv', 'user_id_a,user_id_b', lines, rnd)
    eid = 0
    for part in range(rnd.randint(1, 3)):
        lines = []
        for _ in range(rnd.randint(0, 300)):
            eid += 1
            e = f'e{rnd.randint(1, eid)}' if rnd.random() < 0.15 else f'e{eid}'
            t = LO - 86400000 + rnd.randint(0, 20 * 86400000)
            ing = t + rnd.choice([0, 1000, 86400000, 86400001, 3 * 86400000, -1000]) + 1000 * eid  # unique per copy
            kind = rnd.choice(['view'] * 6 + ['cart'] * 2 + ['purchase'] * 3 + ['Purchase', ''])
            price = rnd.choice(['', '0', '100', '0250', '19.99', '-5', '10000000', '10000001', str(rnd.randint(1, 99999))]) if kind == 'purchase' else rnd.choice(['', '', '5'])
            u = rnd.choice(users + ['', 'd1', 'd3'])
            fields = [e, u, iso(t, rnd), __import__('datetime').datetime.fromtimestamp(ing / 1000, __import__('datetime').timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'), kind, rnd.choice([f'p{i}' for i in range(10)] + ['']), price]
            fields = [f if i == 3 else (weird(f, rnd) if rnd.random() < 0.5 else f) for i, f in enumerate(fields)]
            if rnd.random() < 0.03:
                fields = fields[:-1]
            lines.append(','.join(fields))
        write(d / 'events' / f'part-{part:05d}.csv', 'event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents', lines, rnd)
    pool = []
    for part in range(rnd.randint(1, 2)):
        lines = []
        for k in range(rnd.randint(0, 120)):
            if pool and rnd.random() < 0.15:
                lines.append(rnd.choice(pool))  # duplicates are identical
                continue
            t = LO - 8 * 86400000 + rnd.randint(0, 25 * 86400000)
            fields = [f'k{len(pool)}', rnd.choice(users + ['d1', '']), f'c{rnd.randint(0, 7)}', iso(t, rnd)]
            line = ','.join(weird(f, rnd) if rnd.random() < 0.3 else f for f in fields)
            pool.append(line)
            lines.append(line)
        write(d / 'clicks' / f'part-{part:05d}.csv', 'click_id,user_id,campaign_id,ts', lines, rnd)


def run(cmd, data, out, threads):
    env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out), SCRATCH_DIR=str(out) + '-scratch', NCPU=str(threads))
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if p.returncode:
        raise SystemExit(f'{cmd} failed: {p.stderr[-2000:]}')


failures = 0
for seed in range(CASES):
    rnd = random.Random(seed)
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        make(tmp / 'data', rnd)
        run([PY, str(ROOT / 'pipeline/pipeline.py')], tmp / 'data', tmp / 'ref', 1)
        for threads in (1, 3):
            run(CMD, tmp / 'data', tmp / f'nat{threads}', threads)
            p = subprocess.run([PY, str(ROOT / 'scripts/compare.py'), str(tmp / 'ref'), str(tmp / f'nat{threads}')], capture_output=True, text=True)
            if 'MATCH' not in p.stdout.splitlines()[-1]:
                failures += 1
                print(f'seed {seed} threads {threads} DIFFER\n{p.stdout}')
print(f'{CASES} cases, {failures} failures')
sys.exit(1 if failures else 0)
