#!/usr/bin/env python3
"""Compare native containers on the official generated 20M-event workload.

Requires Linux, Docker, Cargo, Git, and the Python checker dependencies. Builds
the previous revision with the same Docker dependency layers, generates fresh
input, alternates full container runs, and compares both complete output tables.
Docker client startup and runner cleanup are included in wall time. This is not
the portal's exact timer, hardware, or hidden input.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import urllib.request
import zipfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--baseline-ref", default="a7cd5c1d43390cfb3960f2a11a9e13a5d44c8f9f")
parser.add_argument("--candidate-image", default="shopping-sessions:ci")
parser.add_argument("--rounds", type=int, default=6)
parser.add_argument("--threads", type=int, default=4)
parser.add_argument("--memory", default="12g")
parser.add_argument("--report", type=Path, default=Path("logs/linux-v3-final-20m.json"))
args = parser.parse_args()
if args.rounds < 1 or args.threads < 1:
    parser.error("rounds and threads must be positive")
root = Path(__file__).resolve().parents[1]
report_path = args.report.resolve()
generator_url = "https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip"
generator_sha = "13724cadbac5a99d847e27cea911cd2e338ff972b46c2039da87452011bb101e"

def run(command, **kwargs):
    return subprocess.run([str(part) for part in command], check=True, **kwargs)

with tempfile.TemporaryDirectory(prefix="shopping-linux-") as temporary:
    work = Path(temporary)
    baseline = work / "baseline"
    baseline.mkdir()
    run(["git", "fetch", "--depth", "1", "origin", args.baseline_ref], cwd=root)
    archive = work / "baseline.tar"
    run(["git", "archive", "--format=tar", "--output", archive, args.baseline_ref], cwd=root)
    run(["tar", "-xf", archive, "-C", baseline])
    run(["docker", "build", "--build-arg", f"BUILD_JOBS={args.threads}",
         "-t", "shopping-sessions:benchmark-baseline", baseline])

    generator_archive = work / "generator.zip"
    urllib.request.urlretrieve(generator_url, generator_archive)
    if hashlib.sha256(generator_archive.read_bytes()).hexdigest() != generator_sha:
        raise SystemExit("Official generator archive checksum changed; inspect it before benchmarking")
    with zipfile.ZipFile(generator_archive) as zipped:
        zipped.extractall(work / "generator-source")
    manifests = list((work / "generator-source").rglob("Cargo.toml"))
    if len(manifests) != 1:
        raise SystemExit("Expected exactly one generator Cargo.toml")
    manifest = manifests[0]
    generator_target = work / "generator-target"
    run(["cargo", "build", "--release", "--manifest-path", manifest, "--target-dir", generator_target])
    data = work / "data"
    run([generator_target / "release/generate", "--seed", "17", "--events", "20000000",
         "--clicks", "5000000", "--threads", args.threads, "--output", data])
    wrappers = {}
    for name, image in [("baseline", "shopping-sessions:benchmark-baseline"),
                        ("candidate", args.candidate_image)]:
        wrapper = work / f"{name}.sh"
        wrapper.write_text("#!/bin/sh\nset -eu\n" +
                           f"export IMAGE={shlex.quote(image)} CPUS={args.threads} MEM={shlex.quote(args.memory)}\n" +
                           f"exec {shlex.quote(str(root / 'scripts/docker_run.sh'))} \"$DATA_DIR\" \"$OUTPUT_DIR\"\n")
        wrapper.chmod(0o755)
        wrappers[name] = wrapper
    run([sys.executable, root / "scripts/paired_benchmark.py", "--data", data,
         "--baseline", wrappers["baseline"], "--candidate", wrappers["candidate"],
         "--rounds", args.rounds, "--threads", args.threads, "--report", report_path])
    report = json.loads(report_path.read_text())
    # wait4 measures the launcher, while Docker's daemon owns the container.
    # Omit those RSS values instead of mislabeling them as pipeline memory.
    for sample in report["runs"]:
        sample.pop("peak_rss_bytes", None)
    for summary in report["summary"].values():
        summary.pop("peak_rss_bytes", None)
    report.update({
        "data": "official generator: seed 17, 20M events, 5M clicks",
        "commands": {"baseline_image": "shopping-sessions:benchmark-baseline",
                     "candidate_image": args.candidate_image},
        "baseline_commit": args.baseline_ref,
        "candidate_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "generator_url": generator_url, "generator_sha256": generator_sha,
        "input_csv_bytes": sum(p.stat().st_size for p in data.rglob("*.csv")),
        "host_logical_cpus": os.cpu_count(),
        "host_memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
        "container_cpu_limit": args.threads, "container_memory_limit": args.memory,
        "memory_measurement": "Not measured: Docker daemon owns the pipeline process",
        "timing_scope": "Docker client startup through runner cleanup, including native pipeline; comparison excluded",
        "notes": "Linux CI comparison, not the hidden portal score; fresh containers and outputs, caches not flushed, all samples retained",
    })
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print("BEGIN_LINUX_BENCHMARK_JSON", flush=True)
    print(report_path.read_text(), end="", flush=True)
    print("END_LINUX_BENCHMARK_JSON", flush=True)
