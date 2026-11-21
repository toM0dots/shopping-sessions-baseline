#!/usr/bin/env python3
"""Check the generator fast path and adversarial mutations against exact references.

Example: python scripts/check_generator_fastpath.py /path/to/generate \
  --candidate build/fastpath-ci/bin/shopping-sessions-dispatch \
  --baseline build/fastpath-ci/bin/shopping-sessions --report logs/fastpath.json

Commands are shell-quoted argv strings, executed without a shell. They receive
DATA_DIR, OUTPUT_DIR, SCRATCH_DIR and NCPU. Normal and shuffled holdouts must use
the fast engine; opaque IDs, unsupported catalogs/windows must visibly fall back.
The partial-output case seeds stale Parquet files before a fallback invocation:
it tests cleanup, not the timing of a failure after this invocation writes files.
Use --mode-check off only to validate this harness with an untraced reference.
"""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import random
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
TABLES = ("user_daily", "campaign_daily")
WIDTHS = {"events": 7, "clicks": 4, "links": 2}
MUTATIONS = ("shuffled", "opaque_ids", "permuted_event_ids", "cross_person_links",
             "earliest_invalid", "unusual_timestamps", "unusual_catalog",
             "wide_window", "partial_output_fallback")


def parse_row(line, width):
    """The challenge's LF-only dialect; malformed rows are left untouched."""
    fields = line.split(b",")
    if len(fields) != width:
        return None
    quoted = []
    for i, field in enumerate(fields):
        q = b'"' in field
        if q:
            if len(field) < 2 or field[:1] != b'"' or field[-1:] != b'"' or b'"' in field[1:-1]:
                return None
            fields[i] = field[1:-1]
        quoted.append(q)
    return fields, quoted


def documents(data, kind):
    for path in sorted((data / kind).glob("*.csv")):
        raw = path.read_bytes()
        if b"\n" not in raw:
            continue
        header, body = raw.split(b"\n", 1)
        lines = body.split(b"\n")
        if lines[-1] == b"":
            lines.pop()
        yield path, header, lines


def rows(data, kind):
    for _, _, lines in documents(data, kind):
        for line in lines:
            parsed = parse_row(line, WIDTHS[kind])
            if parsed is not None:
                yield parsed[0]


def rewrite(data, kind, change):
    changed = 0
    for path, header, lines in documents(data, kind):
        for i, line in enumerate(lines):
            parsed = parse_row(line, WIDTHS[kind])
            if parsed is None:
                continue
            fields, quoted = parsed
            before = fields.copy()
            change(fields)
            if fields != before:
                changed += 1
                lines[i] = b",".join(b'"' + f + b'"' if q else f for f, q in zip(fields, quoted))
        path.write_bytes(header + b"\n" + b"\n".join(lines) + b"\n")
    return changed


def f1(value):
    return datetime.strptime(value.decode("ascii"), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def stamp(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ").encode("ascii")


def mutate(data, name, seed):
    rng = random.Random(seed ^ 0xF45A9)
    if name == "shuffled":
        count = 0
        for kind in WIDTHS:
            for index, (path, header, lines) in enumerate(documents(data, kind)):
                rng.shuffle(lines)
                # Exercise both trailing-LF and final-unterminated-row paths.
                path.write_bytes(header + b"\n" + b"\n".join(lines) + (b"\n" if index % 2 else b""))
                count += len(lines)
        return {"rows_shuffled": count}
    if name == "opaque_ids":
        columns = {"events": ((0, b"event"), (1, b"user")),
                   "clicks": ((0, b"click"), (1, b"user")),
                   "links": ((0, b"user"), (1, b"user"))}
        def change(fields, cols):
            for i, kind in cols:
                if fields[i]:
                    fields[i] = b"opaque-" + kind + b"-" + fields[i].hex().encode()
        return {kind: rewrite(data, kind, lambda f, cols=cols: change(f, cols)) for kind, cols in columns.items()}
    if name == "permuted_event_ids":
        ids = sorted({f[0] for f in rows(data, "events") if f[0]})
        shuffled = ids.copy()
        rng.shuffle(shuffled)
        mapping = dict(zip(ids, shuffled))
        return {"rows_changed": rewrite(data, "events", lambda f: f.__setitem__(0, mapping.get(f[0], f[0]))),
                "distinct_ids_permuted": len(ids)}
    if name == "cross_person_links":
        parent = {}
        def find(x):
            parent.setdefault(x, x)
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x
        for a, b in rows(data, "links"):
            if a and b:
                parent[find(a)] = find(b)
        chosen, components = [], set()
        for user, _ in Counter(f[1] for f in rows(data, "events") if f[1]).most_common():
            component = find(user)
            if component not in components:
                chosen.append(user)
                components.add(component)
            if len(chosen) == 24:
                break
        if len(chosen) < 2:
            raise ValueError("need two separate input people for cross-person links")
        links = [b'"' + a + b'","' + b + b'"' for a, b in zip(chosen, chosen[1:])]
        (data / "links/mutation.csv").write_bytes(b"user_id_a,user_id_b\n" + b"\n".join(links) + b"\n")
        return {"previously_separate_components_joined": len(chosen)}
    if name == "earliest_invalid":
        winners = {}
        for fields in rows(data, "events"):
            eid = fields[0]
            if eid and (eid not in winners or fields[3] < winners[eid][3]):
                winners[eid] = fields
        manifest = json.loads((data / "manifest.json").read_text())
        lo, hi = (f1(manifest[k].encode()) for k in ("window_start", "window_end"))
        added = []
        for fields in winners.values():
            if not fields[1] or not fields[5] or fields[4] not in (b"view", b"cart"):
                continue
            try:
                ts, ingest = f1(fields[2]), f1(fields[3])
            except (ValueError, UnicodeDecodeError):
                continue
            if lo <= ts < hi and ingest - ts <= timedelta(days=1):
                new = fields.copy()
                new[2] = b"2026-02-30T00:00:00Z"
                new[3] = stamp(ingest - timedelta(seconds=1))
                added.append(b",".join(b'"' + f + b'"' for f in new))
            if len(added) == 64:
                break
        if not added:
            raise ValueError("no valid winner available for earliest-invalid mutation")
        (data / "events/mutation.csv").write_bytes(
            b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n" + b"\n".join(added) + b"\n")
        return {"valid_events_poisoned_by_earlier_invalid_copy": len(added)}
    if name == "unusual_timestamps":
        counts = {}
        for kind, col in (("events", 2), ("clicks", 3)):
            replacements = {}
            def change(fields):
                key = (fields[0], fields[col])
                if key not in replacements and len(replacements) < 300:
                    try:
                        ts = f1(fields[col])
                    except (ValueError, UnicodeDecodeError):
                        return
                    ms = str(int(ts.timestamp()) * 1000).encode()
                    options = (ms, ms.rjust(80, b"0"),
                               (ts + timedelta(hours=23, minutes=59)).strftime("%Y-%m-%dT%H:%M:%S+23:59").encode(),
                               b"2026-02-30T00:00:00Z", b"2026-10-01T00:00:00+24:00", "١٢٣٤٥٦".encode())
                    replacements[key] = options[len(replacements) % len(options)]
                fields[col] = replacements.get(key, fields[col])
            counts[kind] = rewrite(data, kind, change)
        return counts
    if name == "unusual_catalog":
        lines = (data / "products.csv").read_bytes().split(b"\n")
        changed = 0
        for i in range(1, len(lines)):
            parsed = parse_row(lines[i], 2)
            if parsed is not None and changed < 48:
                fields, _ = parsed
                fields[1] = f"mutation-category-{changed:03d}".encode()
                lines[i] = b",".join(b'"' + f + b'"' for f in fields)
                changed += 1
        if changed < 32:
            raise ValueError("need at least 32 products to exceed fast category representation")
        (data / "products.csv").write_bytes(b"\n".join(lines))
        return {"distinct_new_categories": changed}
    if name in ("wide_window", "partial_output_fallback"):
        path = data / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest["window_end"] = stamp(f1(manifest["window_end"].encode()) + timedelta(days=40)).decode()
        path.write_text(json.dumps(manifest))
        return {"window_extended_days": 40}
    raise ValueError(name)


def engine_mode(stderr):
    if re.search(r"\[engine\] fallback:", stderr):
        return "fallback"
    if re.search(r"\[engine\] fast(?:\s|$)", stderr):
        return "fast"
    if re.search(r"\[engine\] baseline(?:\s|$)", stderr):
        return "baseline"
    return "unknown"


def run(command, data, output, threads, timeout):
    output.mkdir(parents=True, exist_ok=True)
    scratch = output.parent / (output.name + "-scratch")
    scratch.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith("XL_") and k not in
           ("SS_ENGINE", "SS_ENGINE_TRACE", "SS_BASELINE_BIN")}
    env.update(DATA_DIR=str(data), OUTPUT_DIR=str(output), SCRATCH_DIR=str(scratch),
               NCPU=str(threads), SS_ENGINE_TRACE="1", XL_TIMING="1")
    started = time.monotonic()
    result = subprocess.run(command, env=env, capture_output=True, text=True, errors="replace", timeout=timeout)
    elapsed = time.monotonic() - started
    output.with_suffix(".stdout.log").write_text(result.stdout)
    output.with_suffix(".stderr.log").write_text(result.stderr)
    if result.returncode:
        raise RuntimeError(f"{shlex.join(command)} exited {result.returncode}: {result.stderr[-5000:]}")
    return {"exit_code": result.returncode, "mode": engine_mode(result.stderr),
            "elapsed_seconds_diagnostic_only": elapsed, "stderr": result.stderr[-8000:]}


def compare(actual, expected, timeout):
    result = subprocess.run([sys.executable, str(ROOT / "scripts/compare.py"), str(actual), str(expected)],
                            capture_output=True, text=True, timeout=timeout)
    if result.returncode or result.stdout.splitlines()[-1:] != ["MATCH"]:
        raise RuntimeError(f"exact comparison failed for {actual}:\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("generator", type=Path)
    parser.add_argument("--candidate", required=True, help="shell-quoted command; executed as argv, not by a shell")
    parser.add_argument("--baseline", help="optional independent native baseline, also checked against the Python oracle")
    parser.add_argument("--reference", default=shlex.join([sys.executable, str(ROOT / "pipeline/pipeline.py")]))
    parser.add_argument("--events", type=int, default=100000)
    parser.add_argument("--seeds", type=int, nargs="+", default=[41, 73])
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 4, 8])
    parser.add_argument("--cases", nargs="+", choices=("normal",) + MUTATIONS,
                        default=["normal", *MUTATIONS], help="mutations use the first seed; normal/shuffled use every seed")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--mode-check", choices=["strict", "off"], default="strict")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--work-dir", type=Path, help="new directory to retain generated cases and logs")
    args = parser.parse_args()
    if args.events < 1000 or min(args.threads) < 1 or args.timeout < 1:
        parser.error("events >= 1000, positive thread counts and timeout required")
    work = args.work_dir.resolve() if args.work_dir else Path(tempfile.mkdtemp(prefix="generator-fastpath-"))
    if args.work_dir:
        work.mkdir(parents=True, exist_ok=False)
    report = {"schema_version": 1, "success": False, "mode_checks": args.mode_check,
              "seeds": args.seeds, "events_requested": args.events, "threads": args.threads,
              "candidate": shlex.split(args.candidate), "baseline": shlex.split(args.baseline) if args.baseline else None,
              "reference": shlex.split(args.reference), "work_dir": str(work), "cases": []}
    try:
        for si, seed in enumerate(args.seeds):
            original = work / f"seed-{seed}"
            subprocess.run([str(args.generator.resolve()), "--seed", str(seed), "--events", str(args.events),
                            "--clicks", str(args.events // 4), "--threads", "2", "--output", str(original)],
                           check=True, timeout=args.timeout, stdout=subprocess.DEVNULL)
            normal_reference = None
            names = [name for name in args.cases if si == 0 or name in ("normal", "shuffled")]
            for name in names:
                case_dir = work / f"seed-{seed}-{name}"
                case_dir.mkdir()
                data = case_dir / "data"
                shutil.copytree(original, data)
                case = {"seed": seed, "name": name, "mutation": {} if name == "normal" else mutate(data, name, seed),
                        "runs": []}
                report["cases"].append(case)
                reference = case_dir / "reference"
                run(report["reference"], data, reference, 1, args.timeout)
                if name == "normal":
                    normal_reference = reference
                if args.baseline:
                    baseline = case_dir / "baseline"
                    case["baseline"] = run(report["baseline"], data, baseline, 1, args.timeout)
                    case["baseline"]["comparison"] = compare(baseline, reference, args.timeout)
                for threads in args.threads:
                    output = case_dir / f"candidate-{threads}"
                    if name == "partial_output_fallback":
                        if normal_reference is None:
                            normal_reference = case_dir / "normal-reference"
                            run(report["reference"], original, normal_reference, 1, args.timeout)
                        for table in TABLES:
                            (output / table).mkdir(parents=True)
                            for i, source in enumerate(sorted((normal_reference / table).glob("*.parquet"))):
                                shutil.copyfile(source, output / table / f"stale-{i}.parquet")
                    sample = {"threads": threads, **run(report["candidate"], data, output, threads, args.timeout)}
                    case["runs"].append(sample)
                    sample["comparison"] = compare(output, reference, args.timeout)
                    expected_mode = "fast" if name in ("normal", "shuffled") else (
                        "fallback" if name in ("opaque_ids", "unusual_catalog", "wide_window", "partial_output_fallback") else None)
                    if args.mode_check == "strict" and expected_mode and sample["mode"] != expected_mode:
                        raise RuntimeError(f"seed={seed} {name} threads={threads}: expected {expected_mode}, got {sample['mode']}")
                    if name == "partial_output_fallback" and any(output.glob("*/stale-*.parquet")):
                        raise RuntimeError("fallback retained seeded stale Parquet parts")
                    print(f"PASS seed={seed} {name} threads={threads} mode={sample['mode']}", flush=True)
        report["success"] = True
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        print(report["error"], file=sys.stderr, flush=True)
    finally:
        report["run_count"] = sum(len(case["runs"]) for case in report["cases"])
        report["mode_counts"] = dict(Counter(run["mode"] for case in report["cases"] for run in case["runs"]))
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        print("BEGIN_FASTPATH_VALIDATION_JSON\n" + json.dumps(report, indent=2) + "\nEND_FASTPATH_VALIDATION_JSON", flush=True)
        if report["success"] and not args.work_dir:
            shutil.rmtree(work)
        elif not report["success"]:
            print(f"Failure artifacts retained: {work}", file=sys.stderr)
    return 0 if report["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
