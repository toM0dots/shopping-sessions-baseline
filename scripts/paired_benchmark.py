#!/usr/bin/env python3
"""Alternate two native executables on identical input; compare every output.

Each invocation is a fresh process with a fresh output directory. Timing ends
after wait4 reports process exit. Correctness comparison and directory cleanup
are outside that interval. No warmup samples are discarded or caches flushed.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--data", type=Path, required=True)
parser.add_argument("--baseline", type=Path, required=True)
parser.add_argument("--candidate", type=Path, required=True)
parser.add_argument("--rounds", type=int, default=5)
parser.add_argument("--threads", type=int, default=8)
parser.add_argument("--report", type=Path, required=True)
args = parser.parse_args()
if args.rounds < 1 or args.threads < 1:
    parser.error("rounds and threads must be positive")
commands = {name: str(getattr(args, name).resolve()) for name in ("baseline", "candidate")}
root = Path(__file__).resolve().parents[1]
samples = []
for round_index in range(args.rounds):
    order = ["baseline", "candidate"] if round_index % 2 == 0 else ["candidate", "baseline"]
    with tempfile.TemporaryDirectory(prefix="shopping-paired-") as tmp:
        for name in order:
            env = dict(os.environ, DATA_DIR=str(args.data.resolve()), OUTPUT_DIR=f"{tmp}/{name}",
                       SCRATCH_DIR=f"{tmp}/scratch", NCPU=str(args.threads))
            with tempfile.TemporaryFile(mode="w+b") as stdout, tempfile.TemporaryFile(mode="w+b") as stderr:
                start = time.perf_counter()
                process = subprocess.Popen([commands[name]], env=env, stdout=stdout, stderr=stderr)
                _, status, usage = os.wait4(process.pid, 0)
                elapsed = time.perf_counter() - start
                process.returncode = os.waitstatus_to_exitcode(status)
                stderr.seek(0)
                diagnostic = stderr.read().decode()
            if process.returncode:
                raise SystemExit(diagnostic)
            sample = {"round": round_index + 1, "implementation": name, "wall_seconds": elapsed,
                      "peak_rss_bytes": usage.ru_maxrss * (1 if platform.system() == "Darwin" else 1024),
                      "exit_code": process.returncode, "stderr": diagnostic.strip()}
            samples.append(sample)
            print(json.dumps(sample), flush=True)
        comparison = subprocess.run([sys.executable, str(root / "scripts/compare.py"),
                                     f"{tmp}/baseline", f"{tmp}/candidate"],
                                    text=True, capture_output=True, check=True)
        print(comparison.stdout, flush=True)
summary = {}
for name in commands:
    values = [s for s in samples if s["implementation"] == name]
    times = [s["wall_seconds"] for s in values]
    summary[name] = {"min_seconds": min(times), "median_seconds": statistics.median(times),
                     "max_seconds": max(times), "peak_rss_bytes": max(s["peak_rss_bytes"] for s in values)}
report = {"platform": platform.platform(), "data": str(args.data), "threads": args.threads,
          "commands": {name: str(getattr(args, name)) for name in commands},
          "method": "alternating order; fresh processes and outputs; caches not flushed; no samples discarded",
          "correctness": f"Both complete tables matched exactly in all {args.rounds} rounds",
          "summary": summary, "runs": samples}
args.report.parent.mkdir(parents=True, exist_ok=True)
args.report.write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps({k: v for k, v in report.items() if k != "runs"}), flush=True)
