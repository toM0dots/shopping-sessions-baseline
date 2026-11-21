#!/usr/bin/env python3
"""Build a CI-only Rust variant over an immutable existing runtime image.

Example: --tag shopping-sessions:bb14 --set XL_BB=14
         --report build/variants/bb14.json

Only the fast binary is replaced. The production Dockerfile and source files
are unchanged, and the resulting image retains the base image's run settings.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import uuid

from benchmark_linux import run


ALLOWED = {"XL_BB": {14, 15}, "XL_SUB_DEF": {4096, 8192}, "XL_INLINE_PURCHASE": {0, 1}}
CONFIG_FIELDS = ("Entrypoint", "Cmd", "Env", "User", "WorkingDir")
FAST_BINARY = "/app/build/shopping-sessions-fast"


def setting(value):
    key, separator, raw = value.partition("=")
    if not separator or key not in ALLOWED or not raw.isdigit() or int(raw) not in ALLOWED[key]:
        raise argparse.ArgumentTypeError("supported settings: XL_BB=14|15, XL_SUB_DEF=4096|8192, XL_INLINE_PURCHASE=0|1")
    return key, str(int(raw))


def inspect(reference):
    description = json.loads(run(["docker", "image", "inspect", reference],
                                 capture_output=True, text=True).stdout)[0]
    if description["Os"] != "linux" or description["Architecture"] != "amd64":
        raise RuntimeError("The benchmark variant requires a Linux amd64 base image")
    return description


def create(image_id):
    container = run(["docker", "create", image_id], capture_output=True, text=True).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{64}", container):
        raise RuntimeError(f"Invalid Docker container ID: {container!r}")
    return container


def builder_dockerfile(production, settings):
    stages = list(re.finditer(r"^FROM\s+[^\n]+", production, re.MULTILINE | re.IGNORECASE))
    if len(stages) < 2 or not stages[0].group().lower().endswith(" as fast-builder"):
        raise RuntimeError("Expected the production Dockerfile to start with the fast-builder stage")
    stage = production[:stages[1].start()]
    marker = "RUN mkdir -p /fast-bin"
    if stage.count(marker) != 1 or "cargo build --locked --release --target x86_64-unknown-linux-musl" not in stage:
        raise RuntimeError("Fast-builder recipe changed; inspect it before injecting variant settings")
    environment = "".join(f"ENV {key}={value}\n" for key, value in sorted(settings.items()))
    return stage.replace(marker, environment + marker, 1) + (
        "\nRUN test -s /fast-bin/shopping-sessions-fast && test -x /fast-bin/shopping-sessions-fast\n")


def binary_hash(path):
    data = path.read_bytes()
    if (len(data) < 64 or data[:6] != b"\x7fELF\x02\x01"
            or int.from_bytes(data[18:20], "little") != 62 or not path.stat().st_mode & 0o111):
        raise RuntimeError("Expected a nonempty, executable ELF64 x86-64 fast binary")
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-image", default="shopping-sessions:ci")
    parser.add_argument("--tag", required=True, help="Tag for the resulting experimental image")
    parser.add_argument("--set", dest="settings", action="append", type=setting, required=True, metavar="KEY=VALUE")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    settings = dict(args.settings)
    if len(settings) != len(args.settings):
        parser.error("each compile-time setting may be supplied only once")
    if args.tag == args.base_image:
        parser.error("variant tag must differ from the base image reference")
    root = Path(__file__).resolve().parents[1]
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    tag_directory = re.sub(r"[^A-Za-z0-9_.-]", "_", args.tag)
    binary_path = root / "build/fast-variants" / tag_directory / "shopping-sessions-fast"
    binary_path.parent.mkdir(parents=True, exist_ok=True)
    report = {"schema_version": 1, "status": "running", "settings": settings,
              "variant_tag": args.tag, "base_reference": args.base_image,
              "source_commit": run(["git", "rev-parse", "HEAD"], cwd=root,
                                   capture_output=True, text=True).stdout.strip(),
              "source_dirty_status": run(["git", "status", "--porcelain", "--", "fast", "Dockerfile"],
                                         cwd=root, capture_output=True, text=True).stdout.strip(),
              "notes": "CI experiment only. Production source and Dockerfile are not modified. Only the fast binary is overlaid on a stopped container from the pinned base image."}
    builder_tag = "shopping-sessions:variant-builder-" + uuid.uuid4().hex[:12]
    containers = []
    try:
        base = inspect(args.base_image)
        report["base_image_id"] = base["Id"]
        report["preserved_runtime_config"] = {key: base["Config"].get(key) for key in CONFIG_FIELDS}
        with tempfile.TemporaryDirectory(prefix="shopping-fast-variant-") as temporary:
            context = Path(temporary)
            (context / "fast").mkdir()
            for filename in ("Cargo.toml", "Cargo.lock", "LICENSE"):
                shutil.copy2(root / "fast" / filename, context / "fast" / filename)
            for directory in (".cargo", "src"):
                shutil.copytree(root / "fast" / directory, context / "fast" / directory)
            report["source_sha256"] = {
                path.relative_to(context).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted((context / "fast").rglob("*")) if path.is_file()
            }
            recipe = builder_dockerfile((root / "Dockerfile").read_text(), settings)
            (context / "Dockerfile").write_text(recipe)
            report["builder_dockerfile"] = recipe
            report["rust_config"] = (context / "fast/.cargo/config.toml").read_text()
            run(["docker", "build", "--platform", "linux/amd64", "--target", "fast-builder",
                 "-t", builder_tag, context])
            builder = inspect(builder_tag)
            report["builder_image_id"] = builder["Id"]
            built = create(builder["Id"])
            containers.append(built)
            run(["docker", "cp", f"{built}:/fast-bin/shopping-sessions-fast", binary_path])
            report["fast_binary_sha256"] = binary_hash(binary_path)
            report["fast_binary_bytes"] = binary_path.stat().st_size
            report["fast_binary_path"] = str(binary_path)
            overlay = create(base["Id"])
            containers.append(overlay)
            run(["docker", "cp", binary_path, f"{overlay}:{FAST_BINARY}"])
            changes = run(["docker", "diff", overlay], capture_output=True, text=True).stdout.splitlines()
            if not changes or any(line[2:] not in ("/app", "/app/build", FAST_BINARY) for line in changes):
                raise RuntimeError(f"Unexpected runtime filesystem changes: {changes}")
            report["runtime_filesystem_changes"] = changes
            committed = run(["docker", "commit", "--pause=false", overlay, args.tag],
                            capture_output=True, text=True).stdout.strip()
            variant = inspect(args.tag)
            if variant["Id"] != committed:
                raise RuntimeError("Variant tag does not point to the committed immutable image")
            for field in CONFIG_FIELDS:
                if variant["Config"].get(field) != base["Config"].get(field):
                    raise RuntimeError(f"Runtime configuration changed: {field}")
            report["variant_image_id"] = variant["Id"]
            verified = create(variant["Id"])
            containers.append(verified)
            roundtrip = context / "verified-fast"
            run(["docker", "cp", f"{verified}:{FAST_BINARY}", roundtrip])
            if binary_hash(roundtrip) != report["fast_binary_sha256"]:
                raise RuntimeError("Variant image does not contain the exact compiled fast binary")
        report["status"] = "built"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = str(error)
        raise
    finally:
        for container in containers:
            subprocess.run(["docker", "rm", "--force", container], check=False,
                           capture_output=True, text=True)
        subprocess.run(["docker", "image", "rm", builder_tag], check=False,
                       capture_output=True, text=True)
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print("BEGIN_FAST_VARIANT_BUILD_JSON", flush=True)
        print(report_path.read_text(), end="", flush=True)
        print("END_FAST_VARIANT_BUILD_JSON", flush=True)


if __name__ == "__main__":
    main()
