#!/usr/bin/env python3
"""Check the official seed-1 million-event answer with the forced fast engine.

python scripts/check_public_fastpath.py /path/to/generate \
    --candidate build/fastpath-ci/bin/shopping-sessions-dispatch \
    --report logs/public-fastpath.json

The candidate is a shell-quoted argv string executed without a shell. Golden
CSV contents are pinned to the already validated official dev-answer download.
--answer-zip permits an offline copy but enforces the same hashes. --mode-check
off is only for harness self-tests against an untraced baseline executable.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import urllib.request
import zipfile

from check_generator_fastpath import compare, engine_mode

ANSWER_URL = "https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/dev-answer.zip"
EXPECTED_SHA256 = {
    "user_daily.csv": "ea8d4b9cab25bf06bea7a4fec1d8b1520e5002b7f7c4153c256f607b90c9798b",
    "campaign_daily.csv": "5b8f728ac09a8b179d265ca0ca231f9d38d24ac3e885285d6ba956a5bcda14e5",
}
LIMIT = 32 * 1024 * 1024


def load_answers(archive, destination):
    destination.mkdir()
    found = {}
    with zipfile.ZipFile(archive) as zipped:
        for info in zipped.infolist():
            path = PurePosixPath(info.filename)
            if path.is_absolute() or ".." in path.parts or "\\" in info.filename or stat.S_ISLNK(info.external_attr >> 16):
                raise ValueError(f"unsafe answer ZIP member: {info.filename!r}")
            if info.is_dir() or path.name not in EXPECTED_SHA256:
                continue
            if path.name in found or info.file_size > LIMIT:
                raise ValueError(f"duplicate or oversized answer ZIP member: {info.filename!r}")
            content = zipped.read(info)
            digest = hashlib.sha256(content).hexdigest()
            if digest != EXPECTED_SHA256[path.name]:
                raise ValueError(f"golden SHA256 mismatch for {path.name}: {digest}")
            # Never extract paths supplied by an archive: write fixed basenames.
            (destination / path.name).write_bytes(content)
            found[path.name] = {"member": info.filename, "sha256": digest, "bytes": len(content)}
    if set(found) != set(EXPECTED_SHA256):
        raise ValueError("answer ZIP does not contain exactly one copy of both expected tables")
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("generator", type=Path)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--answer-zip", type=Path, help="offline ZIP; content hashes remain mandatory")
    parser.add_argument("--mode-check", choices=("strict", "off"), default="strict")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--work-dir", type=Path, help="new directory to retain inputs and output logs")
    args = parser.parse_args()
    command = shlex.split(args.candidate)
    if not command or args.timeout < 1:
        parser.error("a candidate command and positive timeout are required")
    work = args.work_dir.resolve() if args.work_dir else Path(tempfile.mkdtemp(prefix="public-fastpath-"))
    if args.work_dir:
        work.mkdir(parents=True, exist_ok=False)
    report = {"schema_version": 1, "success": False, "answer_url": ANSWER_URL,
              "answer_source": str(args.answer_zip) if args.answer_zip else ANSWER_URL,
              "expected_sha256": EXPECTED_SHA256, "seed": 1, "events_requested": 1000000,
              "clicks_requested": 250000, "candidate": command, "mode_checks": args.mode_check,
              "work_dir": str(work), "runs": []}
    try:
        archive = args.answer_zip.resolve() if args.answer_zip else work / "answer.zip"
        if args.answer_zip is None:
            with urllib.request.urlopen(ANSWER_URL, timeout=60) as response:
                payload = response.read(LIMIT + 1)
            if len(payload) > LIMIT:
                raise ValueError("answer download exceeds 32 MiB limit")
            archive.write_bytes(payload)
        report["golden_tables"] = load_answers(archive, work / "golden")
        data = work / "data"
        subprocess.run([str(args.generator.resolve()), "--seed", "1", "--events", "1000000",
                        "--clicks", "250000", "--output", str(data)],
                       check=True, timeout=args.timeout, stdout=subprocess.DEVNULL)
        for threads in (1, 8):
            output, scratch = work / f"output-{threads}", work / f"scratch-{threads}"
            output.mkdir()
            scratch.mkdir()
            env = {k: v for k, v in os.environ.items() if not k.startswith("XL_") and k not in
                   ("SS_ENGINE", "SS_ENGINE_TRACE", "SS_BASELINE_BIN")}
            env.update(DATA_DIR=str(data), OUTPUT_DIR=str(output), SCRATCH_DIR=str(scratch),
                       NCPU=str(threads), SS_ENGINE="fast", SS_ENGINE_TRACE="1", XL_TIMING="1")
            result = subprocess.run(command, env=env, capture_output=True, text=True,
                                    errors="replace", timeout=args.timeout)
            (work / f"run-{threads}.stdout.log").write_text(result.stdout)
            (work / f"run-{threads}.stderr.log").write_text(result.stderr)
            run = {"threads": threads, "exit_code": result.returncode,
                   "mode": engine_mode(result.stderr), "stderr": result.stderr[-8000:]}
            report["runs"].append(run)
            if result.returncode:
                raise RuntimeError(f"candidate exited {result.returncode}: {result.stderr[-5000:]}")
            run["comparison"] = compare(output, work / "golden", args.timeout)
            if args.mode_check == "strict" and run["mode"] != "fast":
                raise RuntimeError(f"NCPU={threads}: expected fast execution without fallback, got {run['mode']}")
            print(f"PASS official 1M golden NCPU={threads} mode={run['mode']}", flush=True)
        report["success"] = True
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        print(report["error"], file=sys.stderr, flush=True)
    finally:
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        print("BEGIN_PUBLIC_FASTPATH_JSON\n" + json.dumps(report, indent=2) + "\nEND_PUBLIC_FASTPATH_JSON", flush=True)
        if report["success"] and not args.work_dir:
            shutil.rmtree(work)
        elif not report["success"]:
            print(f"Failure artifacts retained: {work}", file=sys.stderr)
    return 0 if report["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
