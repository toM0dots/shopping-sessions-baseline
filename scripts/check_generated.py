#!/usr/bin/env python3
"""Differential checks against the retained Python reference on varied seeds."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("generator", type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
generator = args.generator.resolve()
for seed, events, wide_window in [(2, 1000, False), (17, 20000, False),
                                  (42, 100000, False), (2028, 30000, False),
                                  (17, 20000, True)]:
    with tempfile.TemporaryDirectory(prefix="shopping-differential-") as tmp:
        work = Path(tmp)
        subprocess.run([str(generator), "--seed", str(seed), "--events", str(events),
                        "--clicks", str(events // 4), "--threads", "2", "--output", str(work / "data")], check=True)
        if wide_window:
            # A large date/campaign domain exercises the bounded accumulator's
            # sparse fallback at both worker counts without generating huge data.
            (work / "data/manifest.json").write_text(json.dumps({
                "window_start": "2020-01-01T00:00:00Z", "window_end": "2030-01-01T00:00:00Z"}))
        print(f"seed={seed} events={events} wide_window={wide_window}", flush=True)
        for name, command, threads in [
            ("reference", [sys.executable, str(root / "pipeline/pipeline.py")], "2"),
            ("native-1", [str(root / "build/shopping-sessions")], "1"),
            ("native-8", [str(root / "build/shopping-sessions")], "8"),
        ]:
            env = dict(os.environ, DATA_DIR=str(work / "data"), OUTPUT_DIR=str(work / name),
                       SCRATCH_DIR=str(work / "scratch"), NCPU=threads)
            subprocess.run(command, env=env, check=True, stdout=subprocess.DEVNULL)
            if name != "reference":
                subprocess.run([sys.executable, str(root / "scripts/compare.py"),
                                str(work / name), str(work / "reference")], check=True)
print("All generated differential checks passed.")
