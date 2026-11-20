# Shopping Sessions

Native C++ implementation of the [Shopping Sessions challenge](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions),
with the original Python/DuckDB baseline retained in `pipeline/pipeline.py`.
The default runner executes the compiled native pipeline and writes both Parquet tables.

## Runtime and correctness

The public seed-1 dataset requests **1,000,000 events and 250,000 clicks**.
Its CSV inputs occupy 96,210,343 bytes, including lookups, duplicates, and malformed rows.

The final native batch measured **0.918 seconds median end to end**, with
**all 10 runs below one second**. The Python reference measured 24.194 seconds
median on this same machine: approximately **26.4× faster**.

| Implementation | Runs | Minimum | Median | Maximum | Peak child RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python/DuckDB reference | 3 | 22.879 s | 24.194 s | 26.268 s | 811 MiB |
| Native C++ via `run.sh` | 10 | 0.848 s | 0.918 s | 0.954 s | 223 MiB |

Every sample and environment details are saved in `logs/native-final.json`,
`logs/baseline-final.json`, and `logs/environment.json`. An earlier tuning batch
included a 1.262-second first run after linking new Arrow libraries
(`logs/native-8-tuned.json`); startup can exceed one second.
Published logs replace the local checkout path with `.`; measured values are unchanged.

Timings include process startup, reading every input, all computation, Parquet
writing and process exit. They exclude compilation, data generation and answer
comparison. Every run is a fresh process with an empty output directory; the
filesystem cache is **not flushed**. This establishes subsecond repeated runs,
not a guaranteed cold-start or competition time.

Measured on macOS 15.2, Intel Core i5-1038NG7 at 2.00 GHz, 4 cores / 8 hardware
threads, 16 GiB RAM, with `NCPU=8`. The binary uses portable C++17 and `-O3`,
without `-march=native`. Arrow/Parquet 19.0.1 supplies output encoding.

Validation:

- **36/36 official fixtures pass** through `run.sh` (`logs/native-fixtures.log`).
- **Exact public answer match**: 117,038 user/day rows and 27,400 campaign/day
  rows, no differing rows or duplicate keys (`logs/native-dev-compare.log`).
- Totals: 179,366 sessions, 977,930 surviving events, 376,461,330 revenue cents.
- Four additional official-generator seeds, 1,000–100,000 events, match the
  Python reference at both `NCPU=1` and `NCPU=8` (`logs/generated-checks.log`).

The earlier session measured the Python baseline at 61.41 seconds on a Linux
Xeon with different software and resources. That figure is historical context,
not a directly comparable speedup measurement. This Mac uses Python 3.11.7,
DuckDB 1.4.5, and Polars 1.38.1 for the retained reference/checker; the previous
session's DuckDB 1.5.6 and Polars 2.1.0 pins were unavailable in this setup.

**Docker is unavailable on the development machine.** The updated Dockerfile
has not been built or run; Linux/scorer resource limits, cold-cache performance,
and the larger 20M/60M workloads remain unvalidated. No competition submission
is made by these scripts.

## Build and run locally

Requires a C++17 compiler, Python 3.10+, and a PyArrow binary wheel (Linux or
macOS; PyArrow 19's Intel Mac wheel requires macOS 12+).

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --only-binary=:all: -r requirements-dev.txt
python scripts/build_native.py

python scripts/run_fixtures.py fixtures -- sh run.sh
DATA_DIR="$PWD/dev" OUTPUT_DIR="$PWD/out" NCPU=8 sh run.sh
python scripts/compare.py out dev-answer
python scripts/benchmark.py --data dev --threads 8 --runs 10 \
  --report logs/local-benchmark.json -- sh run.sh
```

If an older Python build reports macOS as 10.16 on a newer Mac, prefix the pip
command with `SYSTEM_VERSION_COMPAT=0` so it selects the compatible binary wheel.
The build script also supports existing conda Arrow C++ installations, although
their extra dynamic dependencies can increase startup time.

`NCPU` defaults to 8 and controls worker concurrency. Compilation is always a
separate step. The runner reads `DATA_DIR` (default `/data`) and writes
`OUTPUT_DIR` (default `/output`); `SCRATCH_DIR` defaults to `/scratch`.
Use a fresh output directory for each independent dataset.

For convenience: `make native`, `make native-fixtures`, and `make benchmark`.
Use `make PYTHON=.venv/bin/python native` without activating the environment.

## Reproduce the input and differential checks

Download `generator.zip` and `dev-answer.zip` from the
[official challenge page](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions).
Unzip the answer to `dev-answer/`, build the unmodified generator with Rust 1.85+
and run:

```sh
cargo build --release --manifest-path generator/Cargo.toml
generator/target/release/generate --seed 1 --events 1000000 \
  --clicks 250000 --threads 4 --output dev
python scripts/check_generated.py generator/target/release/generate
```

The baseline can still be run independently:

```sh
DATA_DIR="$PWD/dev" OUTPUT_DIR="$PWD/out-reference" \
  SCRATCH_DIR="$PWD/scratch" NCPU=8 python pipeline/pipeline.py
```

With Docker available, `make fixtures` builds and runs the native container checks;
`make run-dev check-dev` runs and validates the public input. The multi-stage
Dockerfile compiles during image construction. Its runtime starts the native
binary directly, without Python initialization or network access.

## Implementation

- Read-only memory maps retain input strings without copying each field.
- Parallel CSV parsing preserves LF-only records, carriage returns, exact
  whitespace and quoting, and strict ASCII digit/calendar validation.
- Transitive union-find links use the smallest UTF-8 bytewise ID as the person.
- ID-partitioned deduplication retains invalid early event copies as blockers;
  precomputed validation flags are applied only after choosing the winner.
- Events and clicks are grouped by person, then sorted by time. Session gaps,
  midnight ownership and click-ID ties follow the official rules exactly.
- A sliding seven-day window counts each distinct assisting campaign once.
  Category lookups are performed only for purchases.
- Arrow C++ writes typed DATE, string, nullable category and integer Parquet
  columns directly; neither Python imports nor Python-to-DuckDB conversion are
  part of the optimized runtime.

The native pipeline reads only the input dataset. It never loads reference
answers or persists computed results between invocations. It holds intermediate
records in memory and does not spill or enforce `MEM_GB` internally; the scorer's
container memory limit would provide the hard cap.

The fixtures and unmodified comparison scripts come from the official DuckDB
starter. The Dockerfile and runner have been adapted for the native executable.
Exact semantics are defined by the
[official rules](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1).
Generated inputs, local tools, build products and output tables are gitignored.
