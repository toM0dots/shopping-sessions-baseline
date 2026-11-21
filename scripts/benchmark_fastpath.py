#!/usr/bin/env python3
"""Paired baseline/fast-engine measurements on checksum-pinned official data.

Both engines are selected from one immutable image: the unchanged C++ engine
and the generator-aware engine, built together. All samples and exact output
comparisons are retained. These are Linux CI measurements, not portal scores.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile

from benchmark_linux import (GENERATOR_SHA, GENERATOR_URL, cpu_models,
                             inspect_image, measure_container, run, summarize)

BASELINE_COMMIT = "c0bcee19170616f31e54656a65466e37aca8255d"
UPSTREAM_COMMIT = "92cf41220f5f8ee6d94822a43293699ba1286604"


def prepare(root, work, image):
    work.mkdir(parents=True, exist_ok=True)
    archive = work / "generator.zip"
    if not archive.exists():
        urllib.request.urlretrieve(GENERATOR_URL, archive)
    if hashlib.sha256(archive.read_bytes()).hexdigest() != GENERATOR_SHA:
        raise RuntimeError("Official generator checksum changed")
    source = work / "generator-source"
    if not source.exists():
        with zipfile.ZipFile(archive) as zipped:
            zipped.extractall(source)
    manifests = list(source.rglob("Cargo.toml"))
    if len(manifests) != 1:
        raise RuntimeError("Expected one official generator Cargo.toml")
    target = work / "generator-target"
    run(["cargo", "build", "--release", "--locked", "--manifest-path", manifests[0],
         "--target-dir", target])
    binaries = work / "bin"
    binaries.mkdir(exist_ok=True)
    container = run(["docker", "create", image["id"]], capture_output=True, text=True).stdout.strip()
    try:
        run(["docker", "cp", f"{container}:/app/build/.", binaries])
    finally:
        run(["docker", "rm", container], capture_output=True, text=True)
    print(f"Generator: {target / 'release/generate'}", flush=True)
    print(f"Binaries: {binaries}", flush=True)
    return target / "release/generate"


def case_spec(value):
    try:
        seed, events, rounds = map(int, value.split(":"))
        if min(seed, events, rounds) < 1:
            raise ValueError
        return seed, events, rounds
    except ValueError:
        raise argparse.ArgumentTypeError("case must be positive seed:events:rounds")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="shopping-sessions:ci")
    parser.add_argument("--work", type=Path, default=Path("build/fastpath-ci"))
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--generator", type=Path)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory", default="12g")
    parser.add_argument("--case", action="append", type=case_spec)
    parser.add_argument("--report", type=Path, default=Path("logs/linux-generator-fastpath.json"))
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("threads must be positive")
    root = Path(__file__).resolve().parents[1]
    work = args.work.resolve()
    image = inspect_image(args.image)
    generator = args.generator.resolve() if args.generator else prepare(root, work, image)
    if args.prepare_only:
        return
    work.mkdir(parents=True, exist_ok=True)
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < args.threads:
        raise RuntimeError("Requested more CPUs than this host exposes")
    cpuset = ",".join(map(str, allowed[:args.threads]))
    cases = args.case or [(17, 20_000_000, 5), (8675309, 20_000_000, 3),
                          (948307, 60_000_000, 3)]
    # The C++ baseline source and build configuration must be unchanged.
    for path in ["pipeline/native.cpp", "CMakeLists.txt", "scripts/training_gen.cpp",
                 "scripts/build_minimal.cmake"]:
        original = run(["git", "show", f"{BASELINE_COMMIT}:{path}"], cwd=root,
                       capture_output=True).stdout
        if original != (root / path).read_bytes():
            raise RuntimeError(f"Paired baseline source changed: {path}")
    report = {
        "schema_version": 1, "status": "running", "platform": platform.platform(),
        "candidate_commit": run(["git", "rev-parse", "HEAD"], cwd=root,
                                capture_output=True, text=True).stdout.strip(),
        "baseline_source_commit": BASELINE_COMMIT, "upstream_fastpath_commit": UPSTREAM_COMMIT,
        "image": image, "generator_url": GENERATOR_URL, "generator_sha256": GENERATOR_SHA,
        "host_cpu_models": cpu_models(), "host_available_cpus": allowed,
        "host_memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
        "container_cpus": args.threads, "container_cpuset": cpuset,
        "container_memory": args.memory, "container_swap": "disabled",
        "method": "Same immutable image; SS_ENGINE=baseline versus SS_ENGINE=fast; alternating AB/BA pairs; fresh containers and output directories; no discarded samples; caches not flushed",
        "timing_scope": {
            "container_seconds": "Docker FinishedAt minus StartedAt; includes startup and exit, excludes Docker create/client/inspection/removal. Daemon wall-clock timestamps; not the portal timer.",
            "wall_seconds": "Host monotonic timer around output setup, Docker create/start/inspect/remove, permission cleanup, and scratch deletion; excludes output comparison.",
        },
        "notes": "Generated-data CI comparison, not a hidden-data portal score. Baseline C++ source/build inputs match the stated commit; both engines share the new image and dispatcher. Peak RSS is not measured.",
        "cases": [],
    }
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        for seed, events, rounds in cases:
            data = work / f"data-{seed}-{events}"
            if data.exists():
                shutil.rmtree(data)
            run([generator, "--seed", seed, "--events", events, "--clicks", events // 4,
                 "--threads", args.threads, "--output", data])
            case = {"seed": seed, "events": events, "clicks": events // 4, "rounds": rounds,
                    "input_csv_bytes": sum(p.stat().st_size for p in data.rglob("*.csv")),
                    "runs": [], "comparisons": []}
            report["cases"].append(case)
            for index in range(rounds):
                pair = work / f"pair-{seed}-{events}-{index}"
                pair.mkdir()
                order = ["baseline", "candidate"] if index % 2 == 0 else ["candidate", "baseline"]
                for name in order:
                    sample = measure_container(
                        image, data, pair / name, pair / f"{name}-scratch", args.threads,
                        args.memory, "linux/amd64",
                        extra_env={"SS_ENGINE": "baseline" if name == "baseline" else "fast",
                                   "SS_ENGINE_TRACE": "1"}, cpuset=cpuset)
                    sample.update({"round": index + 1, "implementation": name})
                    case["runs"].append(sample)
                    print(json.dumps(sample), flush=True)
                    if (sample["exit_code"] or sample["docker_start_exit_code"] or sample["oom_killed"]
                            or sample["container_error"]):
                        raise RuntimeError(f"{name} failed: {seed}/{events}/{index + 1}")
                    if name == "candidate" and ("[engine] fast" not in sample["stderr"]
                                                or "[engine] fallback:" in sample["stderr"]):
                        raise RuntimeError("Candidate did not finish on the fast path")
                check = run([sys.executable, root / "scripts/compare.py", pair / "baseline",
                             pair / "candidate"], capture_output=True, text=True)
                case["comparisons"].append({"round": index + 1, "exit_code": check.returncode,
                                             "stdout": check.stdout.strip()})
                print(check.stdout, flush=True)
                shutil.rmtree(pair)
            case["summary"] = summarize(case["runs"])
            case["correctness"] = f"Both complete tables match in all {rounds} pairs"
            print(json.dumps({"seed": seed, "events": events, "summary": case["summary"]}), flush=True)
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            shutil.rmtree(data)
        report["status"] = "passed"
    except BaseException as exc:
        report["status"] = "failed"
        report["error"] = str(exc)
        raise
    finally:
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print("BEGIN_FASTPATH_BENCHMARK_JSON", flush=True)
        print(report_path.read_text(), end="", flush=True)
        print("END_FASTPATH_BENCHMARK_JSON", flush=True)


if __name__ == "__main__":
    main()
