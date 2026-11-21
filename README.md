# Shopping Sessions

Native C++ implementation of the [Shopping Sessions challenge](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions),
with the original Python/DuckDB baseline retained in `pipeline/pipeline.py`.
The default runner executes the compiled native pipeline and writes both Parquet tables.

## Latest update: faster hashing, lookups and memory reuse

Fresh-process paired runs against the previous `main` (`f296c69`), alternating order, `NCPU=4`,
on the same surrogate inputs and machine as the section below. Both complete tables matched in
every round:

| Input (surrogate) | Rounds | Previous median | Updated median | Runtime reduction |
| --- | ---: | ---: | ---: | ---: |
| 1M events / 250K clicks / 12.5K links | 10 | 0.195 s | **0.185 s** | **5%** |
| 20M events / 5M clicks / 250K links | 6 | 2.899 s | **2.401 s** | **17%** |
| 20M events / 5M clicks / 2M links | 6 | 3.421 s | **2.771 s** | **19%** |

Peak RSS fell from 4.41 to 3.98 GiB and from 4.68 to 4.25 GiB. Reports:
`logs/paired-v5-surrogate-*.json`.

- **Hash function** (largest gain, ~12%): a wyhash-style mixer uses one 128-bit multiply
  per 16 bytes instead of two serial multiplies per 8 bytes. A micro-benchmark measured
  14 ns → 5.5 ns per ID, over about 55M hashes per 20M run. Every table still compares
  full keys, and a test build with a degenerate length-only hash passes all fixtures and
  40 reference fuzz cases.
- **Per-shard release** (~5%, −0.5 GiB): each dedup shard frees its raw rows once winners
  are copied out. This lowers the peak, and later allocations reuse warm memory.
- **Flat lookups and interning prefetch** (~3%): campaign and product lookups use a
  contiguous open-addressing copy of their maps. Person interning prefetches table slots
  and name bytes.
- **Cold-cache start:** chunk boundaries are now found by the worker that parses each 1 MiB
  range, not by a serial pre-scan that touched one page per MiB. Cold 20M runs (page cache
  dropped before each) went from 3.556 s to 3.406 s median; warm runs are unchanged.
- Short per-person runs use insertion sort.

Tried and rejected after measuring: huge-page or pre-populated buffers, `MADV_WILLNEED`,
allocator retention tuning, write-combining scatter buffers, a lower identity-table load
factor, a linked-ID bit filter, and parsing clicks before events.

Validation: 36/36 fixtures (SSE2 and scalar), 150 reference fuzz cases at two thread counts,
`check_parser_semantics.py` (SIMD and scalar), exact matches on all three surrogate inputs,
and chunk-boundary resolution tested with 7-byte chunks against the fixtures and 60 fuzz cases.

## Previous update: parallel identity and leaner partitions

Fresh-process paired runs against the previous `main` (`b617b8a`), alternating order, `NCPU=4`.
Both complete tables matched in every round:

| Input (surrogate) | Rounds | Previous median | Updated median | Runtime reduction |
| --- | ---: | ---: | ---: | ---: |
| 1M events / 250K clicks / 12.5K links | 10 | 0.256 s | **0.208 s** | **19%** |
| 20M events / 5M clicks / 250K links | 6 | 6.042 s | **3.022 s** | **50%** |
| 20M events / 5M clicks / 2M links | 6 | 7.488 s | **3.584 s** | **52%** |

Measured in a Linux x86-64 cloud container, 4 vCPUs, 15 GiB RAM, against Arrow from the PyArrow
19.0.1 wheel (`make native-wheel`). The official generator was not reachable from that
environment, so these inputs come from `scripts/surrogate_gen.cpp`. It reproduces the file formats,
duplicate and malformed rows, mixed timestamp formats and linked device IDs. It is **not** the
official generator, and absolute times are not comparable with the figures below. Peak RSS was
4.39 GiB versus 4.58 GiB on the 250K-link input and 4.68 versus 4.75 GiB on the 2M-link input.
Reports: `logs/paired-v4-surrogate-*.json`.

What changed, by phase (20M, 250K links, first paired round):

| Phase | Previous | Updated | Change |
| --- | ---: | ---: | --- |
| setup (links) | 0.20 s | 0.07 s | Parallel hash-sharded interning, integer union-find, concurrent naming |
| parse_events | 1.52 s | 0.95 s | Per-chunk SSE2 splitter, append-only block buffers, batched lookup prefetch |
| dedup_events | 0.97 s | 0.46 s | Person hashes computed once at parse time; prefetched probes |
| clicks | 0.76 s | 0.50 s | Cached click-ID hashes; same buffers and prefetch |
| sessionize_aggregate | 1.72 s | 0.63 s | Grouping uses cached hashes and shard-local tables, never rehashing input |
| write_parquet | 0.79 s | 0.24 s | Large `user_daily` tables are written as concurrently encoded part files |

The validated date-prefix cache in `timestamp()` and the SSE2 `rows()` scanner from
`af0ae36` are retained. `rows()` still parses lookups and any line that contains a quote.

Large `user_daily` outputs, at least 262,144 rows per part, are written as up to `NCPU` files,
`part-0.parquet` … `part-N.parquet`, in the same directory. The unmodified official checker
(`scripts/run_fixtures.py`, `scripts/compare.py`) reads every `*.parquet` file there. Smaller
outputs, including all fixtures and the public 1M answer, stay a single `part-0.parquet`.

Validation of this update:

- 36/36 official fixtures pass, with the SSE2 splitter and with the portable scalar fallback.
- All three surrogate inputs match the previous `main` exactly. The 1M surrogate also matches the
  Python reference.
- `scripts/fuzz_reference.py` checks 150 adversarial small inputs against the Python reference
  at `NCPU=1` and `NCPU=3` with no differences. The inputs include quotes, CRs, missing final LFs,
  malformed rows, self and chained links, duplicates and boundary timestamps.
- `scripts/check_parser_semantics.py` passes, SIMD and scalar: 361,548 CSV and 203,052 timestamp
  comparisons.
- The static-Arrow container build and all fixtures pass in the `native.yml` workflow.
- The public 1M answer, the hidden portal input and the 60M workload were not available
  for this update.

## Runtime and measurement context

The [portal practice workload](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)
uses **about 20M hidden events, 8 vCPUs and 24 GB RAM**, timed from container start
through exit. Its final workload is about 60M events. The public 1M input is a
smaller correctness check. The last reported **7.3-second portal result is not a
measurement of this revision**; a new portal run is still needed.

The latest **Linux container comparison** against `a7cd5c1` measured
**7.285 → 5.993 s median on generated 20M events / 5M clicks: 17.7% lower runtime**.
The candidate was faster in all six alternating pairs, and both complete output
tables matched in every pair. This ran on a GitHub Linux runner with **4 vCPUs
and a 12 GiB container limit**. Timing includes Docker client startup and cleanup;
it is separate from the portal's 7.3 s result and its 8-vCPU environment.

Candidate runs ranged from 5.971–7.146 s; baseline runs from 7.191–16.257 s.
The slower first baseline invocation is retained. All samples and provenance are
in `logs/linux-v3-final-20m.json`; the
[successful workflow](https://github.com/toM0dots/shopping-sessions-baseline/actions/runs/44845023821)
also verifies the Linux build, sanitizer checks and all 36 fixtures.

The latest local comparisons use the immediately previous published version,
`a7cd5c1`, as baseline. Each round alternates execution order and compares both
complete output tables after timing:

| Local input | Paired rounds | Previous median | Updated median | Runtime reduction |
| --- | ---: | ---: | ---: | ---: |
| Generated 20M events / 5M clicks, seed 17 | 6 | 11.627 s | 11.296 s | 2.8% |
| Public 1M events / 250K clicks, seed 1 | 10 | 0.466 s | 0.427 s | 8.3% |

The Mac 20M candidate ranged from **9.230 to 13.297 seconds**, versus 10.607–13.089 s
for the baseline. This batch shows a modest median improvement amid substantial
variation, not a repeatable multi-second gain. All samples, including slower
candidate runs, are retained in `logs/paired-v3-final-20m.json` and
`logs/paired-v3-final-1m.json`. Environment and source/binary hashes are in
`logs/environment-v3.json`. `logs/native-v3-final.json` records a separate ten-run
batch through `sh run.sh`; exploratory per-change comparisons are named
`logs/paired-v3-{identity,grouping,parser}-20m.json`.

These measurements use macOS 15.2, an Intel Core i5-1038NG7, 4 physical cores /
8 hardware threads, 16 GiB RAM, and `NCPU=8`. Every invocation is a fresh process
with fresh outputs. Timing includes startup, all input reads, computation,
Parquet writing and normal cleanup/exit. Compilation and answer comparison are
outside the timer. Filesystem caches are not flushed and no samples are dropped.
Compare paired runs within one batch: changes in machine load and cache state
make separate batches unsuitable for calculating speedup. These local times do
not predict the portal score or establish cold-cache performance.

### Changes in this pass

- Reuse one contiguous string-to-ID table throughout identity joining and lookup;
  canonicalize its values in place. Setup's 20M median fell from 2.167 to 0.902 s
  in the final local batch.
- Reuse that map implementation to intern aggregation IDs, group records by dense
  person ID in linear time, then sort timestamps within each person's group.
  Large shared-device groups and click-ID tie rules remain supported.
- Scan CSV delimiters and quotes together using SSE2 on x86, with the original
  scalar parser on other architectures. LF-only records, exact quoting, CR,
  whitespace, nulls and malformed-field rules remain unchanged.
- Cache fully validated date prefixes within each worker, accelerate short
  numeric timestamps, and fix a transient signed overflow at `INT64_MAX` in the
  old numeric parser. No cache survives a process and no answers are loaded.

### Correctness and Linux verification

- **36/36 official fixtures pass** (`logs/native-v3-fixtures.log`).
- **Exact public answer match:** 117,038 user/day rows and 27,400 campaign/day rows,
  with no differing rows or duplicate keys (`logs/native-v3-dev-compare.log`).
- Four generated seeds and a wide-date-window case match the Python reference at
  `NCPU=1` and `NCPU=8` (`logs/native-v3-generated-checks.log`).
- Each SIMD/scalar parser run passes **361,548 CSV and 203,052 timestamp
  comparisons** under AddressSanitizer and UndefinedBehaviorSanitizer, including
  protected-page input boundaries (`logs/native-v3-parser-*.log`).
- **Linux container build, both sanitizer modes, and 36/36 container fixtures
  pass** (`logs/linux-container-v3-ci.json`).
- All 16 local and 6 Linux final paired rounds match both complete tables. The 20M input contains
  1,921,192,469 CSV bytes and yields 2,249,563 user/day and 28,014 campaign/day rows.

The Linux workflow builds the native container and runs parser checks plus all
fixtures with read-only root, network disabled and UID 1000. A push commit whose
message contains `[benchmark]` additionally compares the previous and current
containers on the generated 20M input. This optional comparison uses 4 vCPUs /
12 GiB on a GitHub runner and includes Docker client startup and runner cleanup;
it is not the portal's exact environment or timer. The benchmark JSON is printed
in the workflow log for retrieval. Docker is unavailable on the local Mac.
The hidden portal input and the 60M final workload remain unmeasured.

### Earlier measurements

The previous optimization pass (`b4df5c3`, documented at `a7cd5c1`) compared against
`c697d57`: its paired medians were 18.130 → 14.081 s on 20M and 0.911 → 0.593 s on
1M (`logs/paired-final-*.json`). Those belong to a different measurement batch.
That pass also replaced large shared Arrow libraries with a minimal static
Arrow/Parquet build: local launch/initialization fell from 0.229 to 0.081 s,
cleanup from 0.065 to 0.016 s, and final shutdown from 0.012 to 0.005 s
(`logs/lifecycle-before.json`, `logs/lifecycle-final.json`). Normal destruction
remains enabled; phase medians need not sum to the median total.

The original native version measured 0.918 s median on public 1M, compared with
24.194 s for the retained Python reference (`logs/native-final.json`,
`logs/baseline-final.json`). A first launch after linking can exceed one second.
Published logs replace the local checkout path with `.` without changing values.

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
commit `a7cd5c1` using that revision's `scripts/build_minimal.cmake`, then run:

```sh
generator/target/release/generate --seed 17 --events 20000000 \
  --clicks 5000000 --threads 4 --output dev-tools/scale-20m
python scripts/paired_benchmark.py --data dev-tools/scale-20m \
  --baseline "$BASELINE" --candidate build/shopping-sessions \
  --rounds 6 --threads 8 --report logs/local-paired-20m.json
python scripts/profile_lifecycle.py --data dev --runs 5 --threads 8 \
  --report logs/local-lifecycle.json -- sh run.sh
python scripts/check_parser_semantics.py
python scripts/check_parser_semantics.py --scalar
```

With Linux, Docker and Cargo available, reproduce the container comparison with:

```sh
make build IMAGE=shopping-sessions:ci
python scripts/benchmark_linux.py
```

This downloads the checksum-pinned official generator, builds `a7cd5c1` as the
baseline, generates fresh 20M input, and writes `logs/linux-v3-final-20m.json`.

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
