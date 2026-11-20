#!/usr/bin/env python3
"""Correctness-first native baseline. Python parsing/aggregation, DuckDB Parquet.
Only input files are consulted; no fixture or reference-answer dependencies.
"""
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

DAY = 86_400_000
REACH = 7 * DAY
DIGITS = re.compile(r'[0-9]+')
ISO = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})')
EPOCH = datetime(1970, 1, 1)


def timestamp(s):
    if DIGITS.fullmatch(s):
        # Bound conversion size, allowing arbitrarily many leading zeroes.
        n = s.lstrip('0') or '0'
        return int(n) if len(n) <= 18 else None
    if not ISO.fullmatch(s):
        return None
    try:
        local = datetime(int(s[:4]), int(s[5:7]), int(s[8:10]),
                         int(s[11:13]), int(s[14:16]), int(s[17:19]))
        delta = local - EPOCH
        value = (delta.days * 86400 + delta.seconds) * 1000
        if s[-1] != 'Z':
            hours, minutes = int(s[20:22]), int(s[23:25])
            if hours > 23 or minutes > 59:
                return None
            value -= (1 if s[19] == '+' else -1) * (hours * 60 + minutes) * 60000
        return value
    except ValueError:
        return None


def rows(paths, width):
    """Exactly one header per file; LF alone terminates records; CR is data."""
    for path in sorted(paths):
        with open(path, encoding='utf-8', newline='\n') as f:
            next(f, None)
            for line in f:
                fields = line.removesuffix('\n').split(',')
                if len(fields) != width:
                    continue
                valid = True
                for i, field in enumerate(fields):
                    if '"' in field:
                        if len(field) < 2 or field[0] != '"' or field[-1] != '"' or '"' in field[1:-1]:
                            valid = False
                            break
                        fields[i] = field[1:-1]
                if valid:
                    yield fields


class People:
    def __init__(self):
        self.parent = {}
        self.size = {}
        self.name = {}

    def find(self, x):
        p = self.parent
        if x not in p:
            p[x] = x
            self.size[x] = 1
            self.name[x] = x
        while p[x] != x:
            p[x] = p[p[x]]
            x = p[x]
        return x

    def join(self, a, b):
        a, b = self.find(a), self.find(b)
        if a == b:
            return
        if self.size[a] < self.size[b]:
            a, b = b, a
        self.parent[b] = a
        self.size[a] += self.size[b]
        # Unicode scalar ordering agrees with UTF-8 byte ordering.
        self.name[a] = min(self.name[a], self.name[b])

    def person(self, x):
        return self.name[self.find(x)]


def run():
    started = time.perf_counter()
    data = Path(os.environ.get('DATA_DIR', '/data'))
    out = Path(os.environ.get('OUTPUT_DIR', '/output'))
    scratch = Path(os.environ.get('SCRATCH_DIR', '/scratch'))
    scratch.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((data / 'manifest.json').read_text())
    lo, hi = timestamp(manifest['window_start']), timestamp(manifest['window_end'])
    people = People()
    for a, b in rows((data / 'links').glob('*.csv'), 2):
        if a and b:
            people.join(a, b)
    products = dict(rows([data / 'products.csv'], 2))
    campaigns = dict(rows([data / 'campaigns.csv'], 2))
    unique = {}
    for row in rows((data / 'events').glob('*.csv'), 7):
        eid = row[0]
        if eid and (eid not in unique or row[3] < unique[eid][3]):
            unique[eid] = row
    events = defaultdict(list)
    for _, user, ts, ingest, kind, product, price in unique.values():
        if not user or not product or kind not in ('view', 'cart', 'purchase'):
            continue
        ts = timestamp(ts)
        if ts is None or not lo <= ts < hi or timestamp(ingest) - ts > DAY:
            continue
        cents = 0
        if kind == 'purchase':
            if not DIGITS.fullmatch(price):
                continue
            price = price.lstrip('0') or '0'
            if len(price) > 8 or not 1 <= (cents := int(price)) <= 10_000_000:
                continue
        events[people.person(user)].append((ts, cents, products.get(product, 'unknown')))
    del unique
    clicks = defaultdict(list)
    seen = set()
    for cid, user, campaign, ts in rows((data / 'clicks').glob('*.csv'), 4):
        if not cid or cid in seen:
            continue
        seen.add(cid)
        ts = timestamp(ts)
        if user and campaign in campaigns and ts is not None and lo - REACH <= ts < hi:
            clicks[people.person(user)].append((ts, cid, campaign))
    del seen
    user_rows = []
    campaign_daily = defaultdict(lambda: [0, 0, 0, 0])
    for user, ev in events.items():
        ev.sort(key=lambda x: x[0])
        # At a timestamp, descending ID means the smallest ID is added last.
        cl = clicks.get(user, [])
        cl.sort(key=lambda x: x[1], reverse=True)
        cl.sort(key=lambda x: x[0])
        left = right = 0
        active = Counter()
        daily = {}
        i = 0
        while i < len(ev):
            start = ev[i][0]
            day = date(1970, 1, 1) + timedelta(days=start // DAY)
            j = i + 1
            while j < len(ev) and ev[j][0] - ev[j - 1][0] <= 1_800_000:
                j += 1
            stat = daily.setdefault(day, [0, 0, 0, Counter()])
            revenue = 0
            for _, cents, category in ev[i:j]:
                revenue += cents
                if cents:
                    stat[3][category] += cents
            stat[0] += 1
            stat[1] += j - i
            stat[2] += revenue
            while right < len(cl) and cl[right][0] <= start:
                active[cl[right][2]] += 1
                right += 1
            while left < right and cl[left][0] < start - REACH:
                campaign = cl[left][2]
                active[campaign] -= 1
                if not active[campaign]:
                    del active[campaign]
                left += 1
            credit = cl[right - 1][2] if right > left else 'organic'
            agg = campaign_daily[credit, day]
            agg[0] += 1
            agg[1] += int(revenue > 0)
            agg[2] += revenue
            for campaign in active:
                campaign_daily[campaign, day][3] += 1
            i = j
        for day, (sessions, count, revenue, categories) in daily.items():
            top = min(categories, key=lambda c: (-categories[c], c)) if categories else None
            user_rows.append((user, day, sessions, count, revenue, top))
    campaign_rows = [(cid, 'organic' if cid == 'organic' else campaigns[cid], day, *v)
                     for (cid, day), v in campaign_daily.items()]
    con = duckdb.connect()
    con.execute(f"SET threads={int(os.environ.get('NCPU', '2'))}")
    con.execute(f"SET memory_limit='{max(1, int(int(os.environ.get('MEM_GB', '4')) * .8))}GB'")
    con.execute('SET temp_directory = ?', [str(scratch / 'duckdb_tmp')])
    schemas = {
        'user_daily': [('user_id','VARCHAR'),('day','DATE'),('sessions','BIGINT'),('events','BIGINT'),('revenue_cents','BIGINT'),('top_category','VARCHAR')],
        'campaign_daily': [('campaign_id','VARCHAR'),('channel','VARCHAR'),('day','DATE'),('sessions','BIGINT'),('purchasing_sessions','BIGINT'),('revenue_cents','BIGINT'),('assisted_sessions','BIGINT')],
    }
    for table, records in [('user_daily', user_rows), ('campaign_daily', campaign_rows)]:
        folder = out / table
        folder.mkdir(parents=True, exist_ok=True)
        schema = schemas[table]
        columns = list(zip(*records)) if records else [[] for _ in schema]
        query = 'SELECT ' + ', '.join(f'unnest(?::{typ}[]) AS {name}' for name, typ in schema)
        con.execute('CREATE TABLE result AS ' + query, columns)
        path = str(folder / 'part-0.parquet').replace("'", "''")
        con.execute(f"COPY result TO '{path}' (FORMAT PARQUET)")
        con.execute('DROP TABLE result')
    print(json.dumps({'user_rows':len(user_rows),'campaign_rows':len(campaign_rows),
                      'sessions':sum(r[2] for r in user_rows),
                      'events':sum(r[3] for r in user_rows),
                      'revenue_cents':sum(r[4] for r in user_rows),
                      'elapsed_seconds':time.perf_counter()-started}), file=sys.stderr)


if __name__ == '__main__':
    run()
