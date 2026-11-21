#!/usr/bin/env python3
"""Compare native containers on the official generated 20M-event workload.

Requires Linux, Docker, Cargo, Git, and the Python checker dependencies. Reports
both Docker's StartedAt-to-FinishedAt lifecycle and the outer host wall time.
The latter includes create/start/inspect/remove and permission cleanup. Neither
is a measurement on the portal's hardware or hidden input. All samples remain;
there are no discarded warmups or filesystem-cache flushes.
"""
import argparse
import calendar
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile

BASELINE_REF = "c330a0494b304029e28d5bae370c6efff6049cb8"
GENERATOR_URL = "https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip"
GENERATOR_SHA = "13724cadbac5a99d847e27cea911cd2e338ff972b46c2039da87452011bb101e"


def run(command, **kwargs):
    return subprocess.run([str(part) for part in command], check=True, **kwargs)


def docker_timestamp_ns(value):
    """Parse RFC3339Nano UTC without dropping Docker's sub-microsecond digits."""
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z", value)
    if not match or value.startswith("0001-"):
        raise ValueError(f"Missing or invalid Docker lifecycle timestamp: {value!r}")
    seconds = calendar.timegm(datetime.strptime(match[1], "%Y-%m-%dT%H:%M:%S").timetuple())
    fraction = int((match[2] or "").ljust(9, "0"))
    return seconds * 1_000_000_000 + fraction


def inspect_image(reference):
    image = json.loads(run(["docker", "image", "inspect", reference],
                           capture_output=True, text=True).stdout)[0]
    return {"reference": reference, "id": image["Id"], "os": image["Os"],
            "architecture": image["Architecture"], "repo_digests": image.get("RepoDigests", [])}


def needs_permission_cleanup(*directories):
    # Match docker_run.sh: container-created files may belong to UID 1000 while
    # the runner has another UID. Keep output tables readable until comparison.
    uid = os.getuid()
    for directory in directories:
        for current, names, files in os.walk(directory):
            if Path(current).stat().st_uid != uid:
                return True
            if any((Path(current) / name).lstat().st_uid != uid for name in names + files):
                return True
    return False


def measure_container(image, data, output, scratch, threads, memory, docker_platform):
    """Measure one fresh container; output comparison is the caller's job."""
    container = None
    state = None
    started = None
    description = None
    beginning = time.perf_counter()
    output.mkdir()
    scratch.mkdir()
    output.chmod(0o777)
    scratch.chmod(0o777)
    try:
        # Pin the inspected immutable image ID so a tag update cannot change
        # the executable midway through an alternating benchmark batch.
        command = [
            "docker", "create", "--platform", docker_platform,
            "--network", "none", "--read-only", "--tmpfs", "/tmp:rw,size=64m,mode=1777",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            "--pids-limit", "4096", "--cpus", str(threads),
            "--memory", memory, "--memory-swap", memory, "--user", "1000:1000",
            "-v", f"{data}:/data:ro,z", "-v", f"{output}:/output:z", "-v", f"{scratch}:/scratch:z",
            "-e", "DATA_DIR=/data", "-e", "OUTPUT_DIR=/output", "-e", "SCRATCH_DIR=/scratch",
            "-e", "HOME=/scratch", "-e", f"NCPU={threads}", "-e", f"MEM_GB={memory.removesuffix('g')}",
            image["id"],
        ]
        container = run(command, capture_output=True, text=True).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{64}", container):
            raise RuntimeError(f"Docker did not return a container ID: {container!r}")
        # No --rm: the stopped container must remain available for inspection.
        started = subprocess.run(["docker", "start", "--attach", container],
                                 capture_output=True, text=True, check=False)
        description = json.loads(run(["docker", "inspect", container],
                                     capture_output=True, text=True).stdout)[0]
        state = description["State"]
        if state["Running"] or state["Status"] != "exited":
            raise RuntimeError(f"Container did not exit: {state}")
        if description["Image"] != image["id"]:
            raise RuntimeError("Container image ID differs from the pinned benchmark image")
        container_ns = docker_timestamp_ns(state["FinishedAt"]) - docker_timestamp_ns(state["StartedAt"])
        if container_ns <= 0:
            raise RuntimeError(f"Non-positive Docker lifecycle interval: {state}")
    finally:
        if container and re.fullmatch(r"[0-9a-f]{64}", container):
            # Also clean up a still-running container if startup/inspection fails.
            run(["docker", "rm", "--force", container], capture_output=True, text=True)
        if needs_permission_cleanup(output, scratch):
            # The mount roots belong to the host runner, so UID 1000 cannot
            # chmod those directories even when they are writable. Only change
            # entries owned by the container user; retain strict error handling.
            run(["docker", "run", "--rm", "--platform", docker_platform, "--network", "none",
                 "--user", "1000:1000", "-v", f"{output}:/output:z", "-v", f"{scratch}:/scratch:z",
                 "--entrypoint", "find", image["id"], "/output", "/scratch", "-uid", "1000",
                 "-exec", "chmod", "a+rwX", "{}", "+"],
                capture_output=True, text=True)
        shutil.rmtree(scratch)
    wall_seconds = time.perf_counter() - beginning
    limits = description["HostConfig"]
    return {
        "wall_seconds": wall_seconds, "container_seconds": container_ns / 1_000_000_000,
        "container_started_at": state["StartedAt"], "container_finished_at": state["FinishedAt"],
        "image_id": description["Image"], "exit_code": state["ExitCode"],
        "docker_start_exit_code": started.returncode, "oom_killed": state["OOMKilled"],
        "container_error": state.get("Error", ""),
        "cpu_limit_nanocpus": limits["NanoCpus"], "memory_limit_bytes": limits["Memory"],
        "memory_plus_swap_limit_bytes": limits["MemorySwap"], "pids_limit": limits["PidsLimit"],
        "stdout": started.stdout.strip(), "stderr": started.stderr.strip(),
    }


def summarize(samples):
    result = {}
    for name in ("baseline", "candidate"):
        result[name] = {}
        for metric in ("container_seconds", "wall_seconds"):
            values = [sample[metric] for sample in samples if sample["implementation"] == name]
            result[name][metric] = {"min": min(values), "median": statistics.median(values), "max": max(values)}
    return result


def cpu_models():
    return sorted({line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                   if line.startswith("model name")})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", default=BASELINE_REF)
    parser.add_argument("--candidate-image", default="shopping-sessions:ci")
    parser.add_argument("--rounds", type=int, default=6)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory", default="12g")
    parser.add_argument("--platform", default="linux/amd64")
    parser.add_argument("--report", type=Path, default=Path("logs/linux-v8-final-20m.json"))
    args = parser.parse_args()
    if args.rounds < 1 or args.threads < 1:
        parser.error("rounds and threads must be positive")
    root = Path(__file__).resolve().parents[1]
    report_path = args.report.resolve()
    with tempfile.TemporaryDirectory(prefix="shopping-linux-") as temporary:
        work = Path(temporary)
        baseline = work / "baseline"
        baseline.mkdir()
        run(["git", "fetch", "--depth", "1", "origin", args.baseline_ref], cwd=root)
        baseline_commit = run(["git", "rev-parse", f"{args.baseline_ref}^{{commit}}"],
                              cwd=root, capture_output=True, text=True).stdout.strip()
        archive = work / "baseline.tar"
        run(["git", "archive", "--format=tar", "--output", archive, baseline_commit], cwd=root)
        run(["tar", "-xf", archive, "-C", baseline])
        run(["docker", "build", "--platform", args.platform, "--build-arg", f"BUILD_JOBS={args.threads}",
             "-t", "shopping-sessions:benchmark-baseline", baseline])
        images = {"baseline": inspect_image("shopping-sessions:benchmark-baseline"),
                  "candidate": inspect_image(args.candidate_image)}
        docker_server = json.loads(run(["docker", "version", "--format", "{{json .Server}}"],
                                       capture_output=True, text=True).stdout)

        generator_archive = work / "generator.zip"
        urllib.request.urlretrieve(GENERATOR_URL, generator_archive)
        if hashlib.sha256(generator_archive.read_bytes()).hexdigest() != GENERATOR_SHA:
            raise SystemExit("Official generator archive checksum changed; inspect it before benchmarking")
        with zipfile.ZipFile(generator_archive) as zipped:
            zipped.extractall(work / "generator-source")
        manifests = list((work / "generator-source").rglob("Cargo.toml"))
        if len(manifests) != 1:
            raise SystemExit("Expected exactly one generator Cargo.toml")
        generator_target = work / "generator-target"
        run(["cargo", "build", "--release", "--manifest-path", manifests[0], "--target-dir", generator_target])
        data = work / "data"
        run([generator_target / "release/generate", "--seed", "17", "--events", "20000000",
             "--clicks", "5000000", "--threads", args.threads, "--output", data])

        samples, comparisons = [], []
        for round_index in range(args.rounds):
            order = ["baseline", "candidate"] if round_index % 2 == 0 else ["candidate", "baseline"]
            with tempfile.TemporaryDirectory(prefix=f"pair-{round_index + 1}-", dir=work) as pair_directory:
                pair = Path(pair_directory)
                for name in order:
                    sample = measure_container(images[name], data, pair / name, pair / f"{name}-scratch",
                                               args.threads, args.memory, args.platform)
                    sample.update({"round": round_index + 1, "implementation": name})
                    samples.append(sample)
                    print(json.dumps(sample), flush=True)
                    if (sample["exit_code"] or sample["docker_start_exit_code"] or sample["oom_killed"]
                            or sample["container_error"]):
                        raise RuntimeError(f"{name} failed in round {round_index + 1}; see recorded sample")
                comparison = run([sys.executable, root / "scripts/compare.py", pair / "baseline", pair / "candidate"],
                                 capture_output=True, text=True)
                comparisons.append({"round": round_index + 1, "exit_code": comparison.returncode,
                                    "stdout": comparison.stdout.strip()})
                print(comparison.stdout, flush=True)

        report = {
            "schema_version": 2, "platform": platform.platform(),
            "data": "official generator: seed 17, 20M events, 5M clicks", "threads": args.threads,
            "baseline_commit": baseline_commit,
            "candidate_commit": run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True).stdout.strip(),
            "images": images,
            "docker_server": {key: docker_server.get(key) for key in ("Version", "Os", "Arch", "KernelVersion")},
            "generator_url": GENERATOR_URL, "generator_sha256": GENERATOR_SHA,
            "input_csv_bytes": sum(path.stat().st_size for path in data.rglob("*.csv")),
            "host_cpu_models": cpu_models(), "host_logical_cpus": os.cpu_count(),
            "host_available_cpus": len(os.sched_getaffinity(0)),
            "host_memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
            "container_cpu_limit": args.threads, "container_memory_limit": args.memory,
            "container_platform": args.platform,
            "memory_measurement": "Peak process memory not measured; enforced container limits recorded per sample",
            "timing_scope": {
                "container_seconds": "Docker State.FinishedAt minus State.StartedAt, parsed at nanosecond precision. Includes native process startup and normal exit; excludes Docker create and host client/inspection/removal/permission cleanup. Uses daemon wall-clock timestamps, not a monotonic or portal timer.",
                "wall_seconds": "Host monotonic timer from output/scratch setup through Docker create/start/inspect/remove, permission cleanup and scratch deletion; complete-table comparison and output deletion are excluded.",
            },
            "method": "Alternating AB/BA pairs; fresh containers and outputs; caches not flushed; all samples retained",
            "correctness": f"Both complete tables matched exactly in all {args.rounds} rounds",
            "notes": "Generated-data Linux CI comparison, not the hidden portal score. Wall times include this harness's inspect/remove calls and are not identical in scope to the older docker_run.sh wall benchmark.",
            "summary": summarize(samples), "runs": samples, "comparisons": comparisons,
        }
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print("BEGIN_LINUX_BENCHMARK_JSON", flush=True)
        print(report_path.read_text(), end="", flush=True)
        print("END_LINUX_BENCHMARK_JSON", flush=True)


if __name__ == "__main__":
    main()
