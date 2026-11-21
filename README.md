# Shopping Sessions

Native C++ implementation of the [Shopping Sessions challenge](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions),
with the original Python/DuckDB baseline retained in `pipeline/pipeline.py`.
The default runner executes the compiled native pipeline and writes both Parquet tables.

## Latest update: parallel identity and leaner partitions

Fresh-process paired runs against the previous native build (`a7cd5c1`), alternating order,
`NCPU=4`. Both complete tables matched in every round:

| Input (surrogate) | Rounds | Previous median | Updated median | Runtime reduction |
| --- | ---: | ---: | ---: | ---: |
| 1M events / 250K clicks / 12.5K links | 10 | 0.294 s | **0.214 s** | **27%** |
| 20M events / 5M clicks / 250K links | 6 | 7.075 s | **3.328 s** | **53%** |
| 20M events / 5M clicks / 2M links | 6 | 11.088 s | **3.886 s** | **65%** |

Measured in a Linux x86-64 cloud container, 4 vCPUs, 15 GiB RAM, against Arrow from the PyArrow
19.0.1 wheel (`make native-wheel`). The official generator was not reachable from that
environment, so these inputs come from `scripts/surrogate_gen.cpp`. It reproduces the file formats,
duplicate and malformed rows, mixed timestamp formats and linked device IDs. It is **not** the
official generator, and absolute times are not comparable with the macOS figures below. The
link-heavy row matters because the previous version resolved links on one thread: there,
setup alone took 4.3 s. Peak RSS was 4.58 GiB versus 4.14 GiB on the 250K-link input and 4.72
versus 4.89 GiB on the 2M-link input. Reports: `logs/paired-v3-surrogate-*.json`.

What changed, by phase (20M, 250K links, single runs):

| Phase | Previous | Updated | Change |
| --- | ---: | ---: | --- |
| setup (links) | 0.34 s | 0.08 s | Parallel hash-sharded interning, integer union-find, concurrent naming |
| parse_events | 1.80 s | 1.05 s | One-pass SSE2 splitter, append-only block buffers, batched lookup prefetch |
| dedup_events | 0.92 s | 0.45 s | Person hashes computed once at parse time; prefetched probes |
| clicks | 0.72 s | 0.56 s | Cached click-ID hashes; same buffers and prefetch |
| sessionize_aggregate | 2.31 s | 0.67 s | Shard-local open addressing plus counting sort, not hash map plus full sort |
| write_parquet | 0.94 s | 0.27 s | Large `user_daily` tables are written as concurrently encoded part files |

Large `user_daily` outputs, at least 262,144 rows per part, are written as up to `NCPU` files,
`part-0.parquet` … `part-N.parquet`, in the same directory. The unmodified official checker
(`scripts/run_fixtures.py`, `scripts/compare.py`) reads every `*.parquet` file there. Smaller
outputs, including all fixtures and the public 1M answer, stay a single `part-0.parquet`.

Validation of this update:

- 36/36 official fixtures pass, with the SSE2 splitter and with the portable scalar fallback.
- All three surrogate inputs match the previous build exactly. The 1M surrogate also matches the
  Python reference.
- `scripts/fuzz_reference.py` checks 150 adversarial small inputs against the Python reference
  at `NCPU=1` and `NCPU=3` with no differences. The inputs include quotes, CRs, missing final LFs,
  malformed rows, self and chained links, duplicates and boundary timestamps.
- The static-Arrow Linux container build could not be run in that environment, because
  GitHub archive downloads were blocked. The `native.yml` workflow covers it on push.
- The public 1M answer, the hidden portal input and the 60M workload were not available
  for this update.

## Runtime and correctness (previous update)

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
- **Linux container build and all 36 fixtures pass** in
  [GitHub Actions](https://github.com/toM0dots/shopping-sessions-baseline/actions/runs/44842344593),
  with read-only root, network disabled and UID 1000, at 2 CPUs / 6 GiB.
  See `logs/linux-container-ci.json`. This is a portability check, not a competition
  performance benchmark; `.github/workflows/native.yml` repeats it on code changes.

Docker is unavailable on the development machine; Linux validation ran in CI.
The hidden portal input, 60M final workload and cold-cache
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
python scripts/fuzz_reference.py 150 -- sh run.sh
# Surrogate inputs used for the latest update (not the official generator):
c++ -O2 -pthread -std=c++17 scripts/surrogate_gen.cpp -o build/surrogate_gen
LINK_STYLE=anon build/surrogate_gen dev-tools/surrogate/s1m 1000000 250000 4 1
LINK_STYLE=anon build/surrogate_gen dev-tools/surrogate/s20m 20000000 5000000 4 17
LINKS_PER_100_USERS=200 build/surrogate_gen dev-tools/surrogate/s20mL 20000000 5000000 4 23
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
  whitespace and quoting, and strict ASCII digit/calendar validation. One SSE2
  pass finds commas, quotes and LFs; lines containing a quote use the exact
  field-by-field parser. A scalar fallback covers non-x86 builds.
- Link IDs are interned in parallel into 64 hash-sharded tables. Union-find runs on
  dense integers, and each component is named by its smallest UTF-8 bytewise ID
  (concurrent compare-and-swap minimum). The frozen shard tables then serve every
  identity query, and rows are buffered so their lookups can be prefetched.
- ID-partitioned deduplication retains invalid early event copies as blockers;
  cached hashes select buckets and full IDs resolve collisions. Validation flags
  are applied only after choosing the winner.
- Person hashes are computed once during parsing and carried with each record.
  Each person shard interns names in an open-addressing table (full names are still
  compared on hash matches) and groups records with a counting sort. Only each person's
  short run is sorted by time. Session gaps, midnight ownership and click-ID ties follow
  the official rules exactly.
- Partition buffers are append-only block lists, so records are written once and never
  copied on growth.
- A sliding seven-day window counts each distinct assisting campaign once.
  Category lookups are performed only for purchases.
- Arrow C++ writes typed DATE, string, nullable category and integer Parquet
  columns directly; neither Python imports nor Python-to-DuckDB conversion are
  part of the optimized runtime. Column encoding is multithreaded, and large user
  tables are split into concurrently written part files.

The native pipeline reads only the input dataset. It never loads reference
answers or persists computed results between invocations. It holds intermediate
records in memory and does not spill or enforce `MEM_GB` internally; the scorer's
container memory limit would provide the hard cap.

The fixtures and unmodified comparison scripts come from the official DuckDB
starter. The Dockerfile and runner have been adapted for the native executable.
Exact semantics are defined by the
[official rules](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1).
Generated inputs, local tools, build products and output tables are gitignored.
