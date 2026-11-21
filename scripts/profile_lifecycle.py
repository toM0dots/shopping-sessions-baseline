#!/usr/bin/env python3
"""Separate process launch, pipeline work, local cleanup, and final shutdown.

The child and parent both sample CLOCK_MONOTONIC. Launch includes the shell,
loader and global initialization. Shutdown includes global destructors, OS exit
and the parent's wait/pipe overhead. Normal C++ cleanup remains enabled.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--data", type=Path, required=True)
parser.add_argument("--runs", type=int, default=5)
parser.add_argument("--threads", type=int, default=8)
parser.add_argument("--report", type=Path)
parser.add_argument("command", nargs=argparse.REMAINDER)
args = parser.parse_args()
command = args.command[1:] if args.command[:1] == ["--"] else args.command
if not command or args.runs < 1 or args.threads < 1:
    parser.error("provide a command and positive run/thread counts")
samples = []
for run in range(args.runs):
    with tempfile.TemporaryDirectory(prefix="shopping-lifecycle-") as tmp:
        env = dict(os.environ, DATA_DIR=str(args.data.resolve()), OUTPUT_DIR=tmp + "/out",
                   SCRATCH_DIR=tmp + "/scratch", NCPU=str(args.threads), PROFILE_LIFECYCLE="1")
        start = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
        result = subprocess.run(command, env=env, capture_output=True, text=True)
        end = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
        if result.returncode:
            raise SystemExit(result.stderr)
        markers = {k: int(v) for k, v in re.findall(r"lifecycle_(main_enter|main_exit)_ns=(\d+)", result.stderr)}
        summary = next(json.loads(line) for line in result.stderr.splitlines() if line.startswith('{'))
        body = (markers["main_exit"] - markers["main_enter"]) / 1e9
        sample = {
            "run": run + 1, "wall_seconds": (end - start) / 1e9,
            "launch_seconds": (markers["main_enter"] - start) / 1e9,
            "pipeline_seconds": summary["elapsed_seconds"],
            "local_cleanup_seconds": body - summary["elapsed_seconds"],
            "shutdown_seconds": (end - markers["main_exit"]) / 1e9,
            "totals": {k: v for k, v in summary.items() if k != "elapsed_seconds"},
        }
        samples.append(sample)
        print(json.dumps(sample), flush=True)
keys = [k for k in samples[0] if k.endswith("_seconds")]
report = {"platform": platform.platform(), "command": command, "data": str(args.data),
          "threads": args.threads, "clock": "CLOCK_MONOTONIC in both processes",
          "filesystem_cache": "not flushed; repeated runs may be warm",
          "medians": {k: statistics.median(s[k] for s in samples) for k in keys}, "runs": samples}
print(json.dumps({k: v for k, v in report.items() if k != "runs"}))
if args.report:
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
