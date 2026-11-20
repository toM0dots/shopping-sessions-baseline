#!/usr/bin/env python3
"""Time fresh processes from start through exit, including input/output I/O.

No warmup is discarded. Repeated runs normally have warm filesystem caches.
Generation, compilation, and answer comparison are outside the timed interval.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import resource
import statistics
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--data", type=Path, required=True)
parser.add_argument("--runs", type=int, default=5)
parser.add_argument("--threads", type=int, default=4)
parser.add_argument("--report", type=Path)
parser.add_argument("command", nargs=argparse.REMAINDER)
args = parser.parse_args()
command = args.command[1:] if args.command[:1] == ["--"] else args.command
if not command or args.runs < 1 or args.threads < 1:
    parser.error("provide a command and positive run/thread counts")
measurements = []
for run in range(args.runs):
    # Fresh empty output directories prevent reuse of earlier results.
    with tempfile.TemporaryDirectory(prefix="shopping-bench-") as work:
        env = dict(os.environ, DATA_DIR=str(args.data.resolve()), OUTPUT_DIR=work + "/out",
                   SCRATCH_DIR=work + "/scratch", NCPU=str(args.threads))
        started = time.perf_counter()
        result = subprocess.run(command, env=env, text=True, capture_output=True)
        elapsed = time.perf_counter() - started
        measurement = {"run": run + 1, "wall_seconds": elapsed, "exit_code": result.returncode,
                       "stderr": result.stderr.strip()}
        measurements.append(measurement)
        print(json.dumps(measurement), flush=True)
        if result.returncode:
            raise SystemExit(result.returncode)
times = [r["wall_seconds"] for r in measurements]
report = {"platform": platform.platform(), "command": command, "data": str(args.data.resolve()),
          "threads": args.threads, "filesystem_cache": "not flushed; repeated runs may be warm",
          "includes": "process startup, all input reads, computation, Parquet writing, process exit",
          "min_seconds": min(times), "median_seconds": statistics.median(times),
          "max_seconds": max(times), "runs": measurements}
report["peak_child_rss_bytes"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * (1 if platform.system() == "Darwin" else 1024)
print(json.dumps({k: v for k, v in report.items() if k != "runs"}), flush=True)
if args.report:
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
