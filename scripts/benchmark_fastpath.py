#!/usr/bin/env python3
"""Paired baseline/fast-engine measurements on checksum-pinned official data.

By default, compare the unchanged C++ engine and generator-aware engine in one
immutable image. Supply --baseline-image and --baseline-ref to compare a prior
fast engine against the candidate instead. All samples and exact output
comparisons are retained. These are Linux CI measurements, not portal scores.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
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


def environment_spec(value):
    key, separator, setting = value.partition("=")
    # Keep the engine selection, trace, and mounted paths under harness control.
    if not separator or not re.fullmatch(r"NCPU|XL_[A-Z0-9_]+", key):
        raise argparse.ArgumentTypeError("environment must be NCPU=value or XL_NAME=value")
    if key == "NCPU" and (not setting.isdigit() or int(setting) < 1):
        raise argparse.ArgumentTypeError("NCPU must be a positive integer")
    return key, setting


def cpu_topology(cpus):
    topology = []
    for cpu in cpus:
        directory = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        record = {"cpu": cpu}
        for field in ("physical_package_id", "core_id", "thread_siblings_list"):
            try:
                record[field] = (directory / field).read_text().strip()
            except OSError:
                record[field] = None
        topology.append(record)
    return topology


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="shopping-sessions:ci")
    parser.add_argument("--baseline-image", help="Prior fast-engine image; both images must finish on the fast path")
    parser.add_argument("--baseline-ref", help="Git revision used to build --baseline-image")
    parser.add_argument("--baseline-env", action="append", type=environment_spec, default=[], metavar="KEY=VALUE")
    parser.add_argument("--candidate-env", action="append", type=environment_spec, default=[], metavar="KEY=VALUE")
    parser.add_argument("--work", type=Path, default=Path("build/fastpath-ci"))
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--generator", type=Path)
    parser.add_argument("--threads", type=int, default=4,
                        help="Fixed CPU quota and cpuset size; NCPU defaults to this for both engines")
    parser.add_argument("--memory", default="12g")
    parser.add_argument("--case", action="append", type=case_spec)
    parser.add_argument("--report", type=Path, default=Path("logs/linux-generator-fastpath.json"))
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("threads must be positive")
    if bool(args.baseline_image) != bool(args.baseline_ref):
        parser.error("--baseline-image and --baseline-ref must be supplied together")
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
    images = {"baseline": inspect_image(args.baseline_image) if args.baseline_image else image,
              "candidate": image}
    baseline_commit = (run(["git", "rev-parse", "--verify", f"{args.baseline_ref}^{{commit}}"],
                           cwd=root, capture_output=True, text=True).stdout.strip()
                       if args.baseline_image else BASELINE_COMMIT)
    environments = {
        "baseline": {"SS_ENGINE": "fast" if args.baseline_image else "baseline",
                     "SS_ENGINE_TRACE": "1", **dict(args.baseline_env)},
        "candidate": {"SS_ENGINE": "fast", "SS_ENGINE_TRACE": "1", **dict(args.candidate_env)},
    }
    cases = args.case or [(17, 20_000_000, 5), (8675309, 20_000_000, 3),
                          (948307, 60_000_000, 3)]
    if not args.baseline_image:
        # The C++ baseline source and build configuration must be unchanged.
        for path in ["pipeline/native.cpp", "CMakeLists.txt", "scripts/training_gen.cpp",
                     "scripts/build_minimal.cmake"]:
            original = run(["git", "show", f"{BASELINE_COMMIT}:{path}"], cwd=root,
                           capture_output=True).stdout
            if original != (root / path).read_bytes():
                raise RuntimeError(f"Paired baseline source changed: {path}")
    comparison_method = ("Two immutable images; prior fast engine versus candidate fast engine"
                         if args.baseline_image else
                         "Same immutable image; SS_ENGINE=baseline versus SS_ENGINE=fast")
    report = {
        "schema_version": 2, "status": "running", "platform": platform.platform(),
        "candidate_commit": run(["git", "rev-parse", "HEAD"], cwd=root,
                                capture_output=True, text=True).stdout.strip(),
        "baseline_source_commit": baseline_commit, "upstream_fastpath_commit": UPSTREAM_COMMIT,
        "image": image, "images": images, "environments": environments,
        "generator_url": GENERATOR_URL, "generator_sha256": GENERATOR_SHA,
        "host_cpu_models": cpu_models(), "host_available_cpus": allowed,
        "pinned_cpu_topology": cpu_topology(allowed[:args.threads]),
        "host_memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
        "container_cpus": args.threads, "container_cpuset": cpuset,
        "container_memory": args.memory, "container_swap": "disabled",
        "worker_threads": {name: int(env.get("NCPU", args.threads)) for name, env in environments.items()},
        "method": comparison_method + "; alternating AB/BA pairs; fresh containers and output directories; no discarded samples; caches not flushed",
        "timing_scope": {
            "container_seconds": "Docker FinishedAt minus StartedAt; includes startup and exit, excludes Docker create/client/inspection/removal. Daemon wall-clock timestamps; not the portal timer.",
            "wall_seconds": "Host monotonic timer around output setup, Docker create/start/inspect/remove, permission cleanup, and scratch deletion; excludes output comparison.",
        },
        "notes": ("Generated-data CI comparison, not a hidden-data portal score. "
                  + ("The supplied baseline image is identified by its immutable ID and declared source commit. Both images are required to finish on the fast path. "
                     if args.baseline_image else
                     "Baseline C++ source/build inputs match the stated commit; both engines share the new image and dispatcher. ")
                  + "CPU quota and cpuset stay fixed even when NCPU overrides vary worker counts. Peak RSS is not measured."),
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
                        images[name], data, pair / name, pair / f"{name}-scratch", args.threads,
                        args.memory, "linux/amd64",
                        extra_env=environments[name], cpuset=cpuset)
                    sample.update({"round": index + 1, "implementation": name})
                    case["runs"].append(sample)
                    print(json.dumps(sample), flush=True)
                    if (sample["exit_code"] or sample["docker_start_exit_code"] or sample["oom_killed"]
                            or sample["container_error"]):
                        raise RuntimeError(f"{name} failed: {seed}/{events}/{index + 1}")
                    if environments[name]["SS_ENGINE"] == "fast" and (
                            "[engine] fast" not in sample["stderr"] or "[engine] fallback:" in sample["stderr"]):
                        raise RuntimeError(f"{name} did not finish on the fast path")
                check = subprocess.run([sys.executable, str(root / "scripts/compare.py"),
                                        str(pair / "baseline"), str(pair / "candidate")],
                                       capture_output=True, text=True, check=False)
                case["comparisons"].append({"round": index + 1, "exit_code": check.returncode,
                                             "stdout": check.stdout.strip(), "stderr": check.stderr.strip()})
                print(check.stdout, flush=True)
                if check.returncode:
                    raise RuntimeError(f"Complete output tables differ: {seed}/{events}/{index + 1}")
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
