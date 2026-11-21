# Shopping Sessions

Native C++ implementation of the [Shopping Sessions challenge](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions),
with the original Python/DuckDB baseline retained in `pipeline/pipeline.py`.
The default runner executes the compiled native pipeline and writes both Parquet tables.

## Runtime and correctness

The portal practice workload uses **about 20M events, 8 vCPUs and 24 GB RAM**,
timed from container start to exit; the public 1M input is the correctness gate.
The final workload is about 60M events. See the
[official scoring configuration](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions).
The reported **7.3-second portal score** is therefore different from the earlier
local subsecond result. This update has not yet been scored on the portal.

Fresh-process comparisons against the published `c697d57` native implementation:

| Input | Paired rounds | Previous median | Updated median | Runtime reduction |
| --- | ---: | ---: | ---: | ---: |
| Public 1M events / 250K clicks | 10 | 0.911 s | **0.593 s** | **34.9%** |
| Generated 20M events / 5M clicks, seed 17 | 4 | 18.130 s | **14.081 s** | **22.3%** |

The updated 20M runs ranged from 13.022–17.080 seconds; all samples are retained.
This generated input contains 1,921,192,469 CSV bytes and is a surrogate for the
hidden portal data. Both complete output tables matched the previous version in
every paired round. Peak child RSS on 20M fell from 3.76 to 3.62 GiB.
A separate ten-run batch through `sh run.sh` measured **0.592 s median** on 1M
(0.558–0.735 s). Raw samples and provenance are in `logs/paired-final-1m.json`,
`logs/paired-final-20m.json`, `logs/native-v2-final.json` and
`logs/environment-v2.json`.

These are local macOS measurements on an Intel Core i5-1038NG7, 4 physical cores /
8 hardware threads, 16 GiB RAM, `NCPU=8`. Paired runs alternate baseline/candidate
order. Timing includes process startup, reading, computation, Parquet output and
normal process exit; comparison and compilation are outside the timer. Every
invocation uses a fresh process and output directory. Filesystem caches are not
flushed and no warmup samples are discarded. These results do not establish the
new portal score or cold-cache performance.

### Where the overhead went

Monotonic-clock markers separate time outside the internal pipeline timer:

| Phase, public 1M | Previous median | Updated median |
| --- | ---: | ---: |
| Launch, loader and global initialization | 0.229 s | 0.081 s |
| Local object cleanup after pipeline timer | 0.065 s | 0.016 s |
| Final shutdown and parent wait overhead | 0.012 s | 0.005 s |

The shared Arrow libraries dominated launch: a no-op linked against them took
0.190 s end to end, versus 0.009 s without Arrow. The default build now links a
smaller static Arrow/Parquet build and strips unused code; OS/C++ libraries remain
dynamic. The shell runner also avoids an extra `dirname` process. First launches
after linking can still be slower and are retained in the diagnostic logs.
See `logs/lifecycle-before.json`, `logs/lifecycle-final.json`, the
`logs/lifecycle-probe-*.json` controls and `scripts/loader_probe.cpp`. Phase medians
are calculated independently and need not sum to the median total.

The larger-data improvements use compact integer person IDs for sorting, cached
event-ID hashes and smaller deduplication tables, a contiguous frozen identity
lookup, and reusable campaign/date accumulators. Dense accumulators have a shared
64 MiB budget and fall back to sparse maps for wide date windows. Full IDs are
still compared on hash matches. Normal destructors and file closing remain enabled.

Validation of the updated binary:

- **36/36 official fixtures pass** (`logs/native-v2-fixtures.log`).
- **Exact public answer match**, including all rows and duplicate-key checks
  (`logs/native-v2-dev-compare.log`).
- Four additional generator seeds plus a wide date-window case match the Python
  reference at both `NCPU=1` and `NCPU=8` (`logs/native-v2-generated-checks.log`).
- All 14 paired rounds match both complete tables, including the generated 20M
  input: 2,249,563 user/day rows and 28,014 campaign/day rows.
- `.github/workflows/native.yml` builds the Linux container and runs all fixtures
  with read-only root, network disabled and UID 1000, at 2 CPUs / 6 GiB. This is a
  portability check, not a competition performance benchmark.

Docker is unavailable on the development machine; the workflow provides the Linux
check after publishing. The hidden portal input, 60M final workload and cold-cache
performance remain unmeasured. No submission is made by these scripts.
The exploratory `logs/paired-20m-initial.json` overlapped correctness checks and is
explicitly excluded from reported speedups. Published logs replace the local
checkout path with `.` without changing measurements.

## Previous baseline results

The public seed-1 dataset requests **1,000,000 events and 250,000 clicks**.
Its CSV inputs occupy 96,210,343 bytes, including lookups, duplicates, and malformed rows.

The first published native batch measured **0.918 seconds median end to end**, with
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

## Build and run locally

Requires CMake 3.18+, make, a C++17 compiler and network access for the first build.
Building the pinned Arrow/Parquet source dependencies takes several minutes;
subsequent builds reuse them. Python 3.10+ and a PyArrow wheel are needed only for
the retained reference and checkers (PyArrow 19's Intel Mac wheel requires macOS 12+).

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --only-binary=:all: -r requirements-dev.txt
cmake -DJOBS=4 -P scripts/build_minimal.cmake

python scripts/run_fixtures.py fixtures -- sh run.sh
DATA_DIR="$PWD/dev" OUTPUT_DIR="$PWD/out" NCPU=8 sh run.sh
python scripts/compare.py out dev-answer
python scripts/benchmark.py --data dev --threads 8 --runs 10 \
  --report logs/local-benchmark.json -- sh run.sh
```

If an older Python build reports macOS as 10.16 on a newer Mac, prefix the pip
command with `SYSTEM_VERSION_COMPAT=0` so it selects the compatible binary wheel.
`make native-wheel` retains the faster-to-build PyArrow shared-library route,
which has higher process launch overhead. Its helper also supports existing conda
Arrow C++ installations, whose dynamic dependencies can increase startup time.

`NCPU` defaults to 8 and controls worker concurrency. Compilation is always a
separate step. The runner reads `DATA_DIR` (default `/data`) and writes
`OUTPUT_DIR` (default `/output`); `SCRATCH_DIR` defaults to `/scratch`.
Use a fresh output directory for each independent dataset.

For convenience: `make native`, `make native-fixtures`, and `make benchmark`.
Use `make PYTHON=.venv/bin/python native-fixtures` without activating the environment.

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

To reproduce the larger comparison, set `BASELINE` to an executable built from
commit `c697d57` using that revision's `scripts/build_native.py`, then run:

```sh
generator/target/release/generate --seed 17 --events 20000000 \
  --clicks 5000000 --threads 4 --output dev-tools/scale-20m
python scripts/paired_benchmark.py --data dev-tools/scale-20m \
  --baseline "$BASELINE" --candidate build/shopping-sessions \
  --rounds 4 --threads 8 --report logs/local-paired-20m.json
python scripts/profile_lifecycle.py --data dev --runs 5 --threads 8 \
  --report logs/local-lifecycle.json -- sh run.sh
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
- Transitive union-find links use the smallest UTF-8 bytewise ID as the person;
  a contiguous immutable lookup serves subsequent identity queries.
- ID-partitioned deduplication retains invalid early event copies as blockers;
  cached hashes select buckets and full IDs resolve collisions. Validation flags
  are applied only after choosing the winner.
- Events and clicks are grouped by compact person ID, then sorted by time. Session gaps,
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
