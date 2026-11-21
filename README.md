# Shopping Sessions

Native implementation of the [Shopping Sessions challenge](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions),
with a generator-aware Rust engine and the existing C++ engine as a general fallback.
The original Python/DuckDB reference remains in `pipeline/pipeline.py`.
Both production engines read the supplied CSVs and write both Parquet tables.

## Generator-aware fast engine

The fast engine in `fast/` derives from Team 67's MIT-licensed
[Express Lane, commit `92cf412`](https://github.com/jason-scheffel/express-lane/tree/92cf41220f5f8ee6d94822a43293699ba1286604).
See [third-party notices](THIRD_PARTY_NOTICES.md) for attribution and local changes.
It infers reversible ID encodings from the current input, uses decoded event indices
for direct-slot deduplication, and exploits checked user/time ordering. It also uses
SIMD field decoding and a direct Parquet writer. It does not ship expected output tables.

The dispatcher selects this engine only on Linux amd64 with the required CPU/OS
features. Unsupported CPUs use the existing C++ engine. When input constraints fail,
the fast engine stops its workers, removes partial Parquet parts, and restarts the
complete input through the C++ engine. The C++ source and PGO build are unchanged
from `c0bcee1`.

`SS_ENGINE=auto` is the default; `baseline` selects C++, and `fast` requires a supported
CPU. `SS_ENGINE_TRACE=1` reports selection and any fallback on stderr. Build with
`docker build --build-arg BUILD_JOBS=4 -t shopping-sessions:ci .`; native macOS builds
continue to use C++.

The latest production Docker build uses compile-time **`XL_BB=14`**, reducing each
event partition's slot array from 512 KiB to 256 KiB. On an Intel Xeon 6975P-C
runner with **four pinned logical CPUs spanning two physical cores, 12 GiB and no
swap**, this produced a small improvement on both 20M inputs against `fd56b794`:

| Official generated input | Pairs | Previous fast engine | `XL_BB=14` | Runtime change |
| --- | ---: | ---: | ---: | ---: |
| 20M events / 5M clicks, seed 17 | 5 | 0.346031 s | **0.341081 s** | **1.43% faster** |
| 20M events / 5M clicks, seed 8675309 | 5 | 0.345152 s | **0.342634 s** | **0.73% faster** |
| 60M events / 15M clicks, seed 948307 | 3 | **0.904092 s** | 0.910598 s | **0.72% slower** |

These are medians of alternating fresh-container runs, from container start through
exit. The candidate was faster in 5/5, 4/5 and 1/3 pairs, respectively. Both complete
output tables matched exactly in **all 13 pairs**; all samples were retained and
caches were not flushed. The 60M regression limits the improvement to the tested
20M workloads. These CI results do not establish a portal runtime below 0.30 s.

Ten screening configurations passed output comparison; `XL_BB=14` had the best
screening median and was selected for the independent holdout comparisons above.
Inlining purchase parsing was slower and remains disabled, as do direct click
indexing and batched release. The selected build passed **36 official fixtures**,
**48 differential checks** against the Python reference (**33 fast, 15 fallback**),
and the **official public 1M golden answer at one and eight threads**. The expanded
checks include duplicate and permuted click IDs, purchase price boundaries,
unknown products and quoted fields.

Measured source: [`973221f`](https://github.com/toM0dots/shopping-sessions-baseline/commit/973221fbfa142a5c6822904105b9a4f9f934fe84)
compiled with `XL_BB=14`, in [CI run 44956950677](https://github.com/toM0dots/shopping-sessions-baseline/actions/runs/44956950677).
See the [paired measurements](logs/linux-selected-parser-paired.json),
[selection](logs/fast-parser-selection.json), [screening results](logs/linux-fast-parser-sweep.json),
[differential validation](logs/generator-selected-parser-validation.json), and
[public golden validation](logs/public-selected-parser-validation.json).
The [release manifest](logs/fast-release-selection.json) pins the measured binary's
SHA-256 (`696661fb73cd0fc86ea74a7cf132158da99f8924261d4fe5f5eead2b3f09e459`);
verification of the release build against that binary is pending.

The most recent verified portal measurement predates this tuning update.
On the [Datathon portal](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions),
submission **`BB-1408C9EE`** at
[`fd56b794`](https://github.com/toM0dots/shopping-sessions-baseline/commit/fd56b794f7efe74bcb4e8d32bd653f4df923c7fb)
reached **Scored: 0.31 s** on the hidden practice input and was marked
**Entered in the final (your best score)**, observed on 2026-11-21 at 21:32:58 UTC.
The portal's `shopping-sessions-4` scorer uses about 20M events, eight pinned vCPUs,
24 GB RAM and no swap; its policy reports the median of three runs when the first
finishes below 20 seconds. The earlier submission `BB-4EC0915E` at `1e2e1695b224`
scored **1.37 s**. The portal accepted the corrected submission, but did not expose
individual timings, detailed correctness results or engine-selection logs.
See [`logs/portal-generator-fastpath.json`](logs/portal-generator-fastpath.json).

The original fast-engine comparison used source [`98d5116`](https://github.com/toM0dots/shopping-sessions-baseline/commit/98d51161f0cfbcd6364e3ae513e6a00fbf2ec77d).
On an Intel Xeon 6975P-C Linux runner, with **four pinned logical CPUs, 12 GiB and no
swap**, alternating fresh-container runs produced these medians:

| Official generated input | Pairs | C++ baseline | Fast engine | Runtime reduction |
| --- | ---: | ---: | ---: | ---: |
| 20M events / 5M clicks, seed 17 | 5 | 1.697 s | **0.347 s** | **79.6%** |
| 20M events / 5M clicks, seed 8675309 | 3 | 1.689 s | **0.345 s** | **79.6%** |
| 60M events / 15M clicks, seed 948307 | 3 | 4.977 s | **0.903 s** | **81.9%** |

These measure Docker container start through exit, including CSV reads, computation
and Parquet writing. Both engines run from the same immutable image; the baseline
selects the unchanged C++ engine. This is approximately a **4.9x speedup at 20M** and
**5.5x at 60M** in CI. These four-CPU measurements are separate from the **0.31 s
portal result** above, which uses eight vCPUs and different inputs/hardware.
**0.30 s remains the portal target.**

Both complete output tables matched exactly, with no duplicate keys, in **all 11
pairs**. The 60M comparison includes 6,601,416 user-day rows and 28,014 campaign-day
rows. Every timed candidate finished on the fast path. Caches were not flushed and
no samples were discarded. The report also retains outer Docker-management times,
including first-baseline outliers; those are a different timing scope. Peak RSS was
not measured. Raw samples, limits, immutable image ID and complete comparison results:
[`logs/linux-generator-fastpath.json`](logs/linux-generator-fastpath.json).
[Successful full build, validation and benchmark](https://github.com/toM0dots/shopping-sessions-baseline/actions/runs/44949538888).

Additional validation:

- **36/36 official fixtures** in the isolated production container; existing CSV/key
  sanitizer checks and the new partial-output cleanup test passed.
- **39 differential runs** on two generated holdout seeds and mutations, at one,
  four and eight threads, against the independent Python reference. Of these, 24
  completed on the fast path and 15 exercised full C++ fallback. Cases include
  shuffled rows, long/mixed/opaque IDs, permuted event IDs, cross-person links,
  invalid earliest duplicate winners, unusual timestamps, large catalogs, wide
  windows, and removal of seeded stale output parts. See
  [`logs/generator-fastpath-validation.json`](logs/generator-fastpath-validation.json).
- **Official public 1M golden answer**, with both published CSV hashes pinned:
  all 117,038 user rows and 27,400 campaign rows matched at one and eight threads,
  with fast execution required. See
  [`logs/public-generator-fastpath.json`](logs/public-generator-fastpath.json) and
  [the public-answer CI run](https://github.com/toM0dots/shopping-sessions-baseline/actions/runs/44949923318).
  This check's commit changes tests only; its production engine is identical to
  the benchmarked source.

An intermediate AMD CI run caught an overly conservative CPU gate: it selected C++
because the VM omitted PKU/OSPKE. Its output matched, but the required-fast check
correctly rejected the run. The build now explicitly disables unused PKU and the
dispatcher checks the matching feature contract. The rejection and initial Intel
measurements remain in `logs/*generator-fastpath*`; they are not substituted for
the successful final measurements above.

Reproduce the differential and large-data checks on a supported Linux Docker host:

```sh
docker build --build-arg BUILD_JOBS=4 -t shopping-sessions:ci .
python scripts/benchmark_fastpath.py --prepare-only
python scripts/check_generator_fastpath.py build/fastpath-ci/generator-target/release/generate \
  --candidate build/fastpath-ci/bin/shopping-sessions-dispatch \
  --baseline build/fastpath-ci/bin/shopping-sessions --report logs/generator-fastpath-validation.json
python scripts/check_public_fastpath.py build/fastpath-ci/generator-target/release/generate \
  --candidate build/fastpath-ci/bin/shopping-sessions-dispatch --report logs/public-generator-fastpath.json
python scripts/benchmark_fastpath.py --generator build/fastpath-ci/generator-target/release/generate
```

Install `requirements-dev.txt` and Rust/Cargo for the verification tools. The container
itself does not use Python or build anything during its timed execution.

## Tuning follow-up: first round

The first tuning round at [`291a066`](https://github.com/toM0dots/shopping-sessions-baseline/commit/291a066f8f78b317bfe2351509bc64b97ded64b8)
found **no consistent improvement on either 20M input**. Seven experiments, including
a diagnostic profile, retained all **56 alternating container pairs** on an AMD EPYC
9V45 with four pinned logical CPUs spanning two physical cores, 12 GiB and no swap.
Both complete output tables matched in every pair. These are separate CI measurements;
the verified portal result remains **0.31 s at `fd56b794`**.

- Core-first CPU placement was essentially unchanged: **0.25818 → 0.25816 s** and
  **0.25240 → 0.25134 s** for the two 20M seeds.
- Direct click indexing was slightly slower: **0.25910 → 0.26114 s** and
  **0.25657 → 0.26126 s**. Using two workers instead of four was **18–21% slower**.
- Releasing input mappings in batches of eight had mixed 20M results. Its apparent
  **8.4% reduction at 60M** came from only two pairs, with a slow baseline sample;
  this does not establish a repeatable improvement.

Direct click indexing and batched release remain disabled by default
(`XL_CKIX=0`, `XL_REL_BATCH=1`). This round alone was not promoted; core-first
placement was retained for the subsequent parser experiments reported above.
Raw samples, environment settings, comparisons and diagnostic output are retained in
`logs/linux-fast-tuning-round1-*.json`.
[Completed CI run](https://github.com/toM0dots/shopping-sessions-baseline/actions/runs/44954335639).

All **135 differential checks** passed against the independent Python reference:
[default candidate](logs/generator-tuning-round1-default.json),
[click indexing](logs/generator-tuning-round1-click-index.json), and
[combined indexing, batched release and pread](logs/generator-tuning-round1-combined-pread.json).
The 45 runs per configuration include duplicate clicks across files and permuted
click IDs, alongside the earlier holdouts and mutations. The official public 1M
golden answer also matched at one and eight threads for both the
[default candidate](logs/public-tuning-round1-default.json) and
[click-index candidate](logs/public-tuning-round1-click-index.json).

## Previous update: measured against the latest merged code (v8)

On the official generator's **20M events / 5M clicks, seed 17**, six alternating
fresh-container pairs against `c330a049` gave:

| Linux CI measurement | Previous median | Updated median | Reduction |
| --- | ---: | ---: | ---: |
| Container start to exit | 1.461 s | **1.391 s** | **4.8%** |
| Entire benchmark invocation, including Docker management and cleanup | 1.860 s | **1.770 s** | **4.8%** |

The candidate was faster in all six container pairs; both complete output tables matched
exactly in every pair. Host: AMD EPYC 9V45, four available CPUs; each container was limited
to four CPUs and 12 GiB, with the existing GCC profile-guided build used for both versions.
All samples are retained, including the first baseline's 11.438 s outer-wall outlier.
Caches were not flushed. This is **not a new portal score**, and these times cannot be
compared directly with earlier sections' different machines, inputs and baselines.

Changes:

- Pack supported ID characters into six-bit fields, replacing base-36 arithmetic with
  shifts while preserving exact equality, byte ordering, leading zeros and fallback IDs.
- Halve normal deduplication table slots from 16 to eight bytes using a fingerprint and
  bucket index. Full key comparisons still resolve collisions; larger inputs retain a
  wider-index fallback.
- Reuse aggregation buffers per worker across partitions and release them before writing
  Parquet. In this batch, median aggregation time fell from 0.253 s to 0.211 s.
- Reserve exact Arrow string capacities before appending output IDs and categories.

The benchmark now records Docker's `StartedAt`–`FinishedAt` interval separately from the
host's monotonic timer around create/start/inspect/remove and permission cleanup. The first
uses daemon wall-clock timestamps; neither is the portal's timer. The outer interval differs
from the older `docker_run.sh` benchmark. Image IDs, actual resource limits, raw timings,
phase diagnostics and all output comparisons are in
[`logs/linux-v8-final-20m.json`](logs/linux-v8-final-20m.json).

Local macOS/Apple Clang checks (PGO disabled) are retained too: the public 1M median changed
from 0.401 s to 0.386 s over ten pairs, while 20M was effectively tied/slightly slower,
3.870 s to 3.912 s over six pairs. See `logs/paired-v8-local-*.json` and
[`logs/environment-v8.json`](logs/environment-v8.json). The Linux improvement should not be
assumed to apply equally to every CPU or workload.

Validation: 36/36 fixtures locally and inside the isolated Linux container; exact public
reference output; five generated datasets at one and eight threads; 80 adversarial cases at
one and three threads; and a constant-hash build passing all fixtures plus 20 adversarial
cases. The new `scripts/check_keys.py` runs 82,513 ID cases, 100,000 pair comparisons and 66
protected-buffer/lifetime checks under ASan/UBSan in each of SIMD and scalar mode. Parser
sanitizers also passed in Linux CI.
[Successful build and benchmark](https://github.com/toM0dots/shopping-sessions-baseline/actions/runs/44940974903).

Reproduce on a Linux Docker host with the checker dependencies installed:

```sh
docker build --build-arg BUILD_JOBS=4 -t shopping-sessions:ci .
.venv/bin/python scripts/benchmark_linux.py
```

## Previous update: restructured partitioning

Paired runs against `main` (`f296c69`) on the official generator's datasets, alternating order,
`NCPU=4`, on the 4-vCPU Linux container described below. Both complete tables matched in every
round:

| Input (official generator) | Rounds | `main` median | Updated median | Runtime reduction |
| --- | ---: | ---: | ---: | ---: |
| Public 1M events / 250K clicks | 10 | 0.223 s | **0.170 s** | **24%** |
| 20M events / 5M clicks, seed 17 | 6 | 3.994 s | **2.364 s** | **41%** |

Peak RSS on 20M fell from 4.34 to 3.49 GiB. A single-threaded 20M run now costs 8.1 CPU-seconds
in total: 7.13 s user and 0.97 s system. Reports: `logs/paired-v7-official-*.json`.

This includes the previous section's changes (inline `Key`s, one-pass quoted lines, slimmer
records). The restructure:

- **Partition by raw user.** Parsing stores each valid event once, 32 bytes with its raw user ID,
  in that user's shard. It also writes a 32-byte dedup record (event ID, ingest time, pointer) to
  the event ID's shard. No identity lookup happens while parsing.
- **Deduplicate by reference.** The earliest copy of each event ID wins; every other stored copy is
  tombstoned in place by clearing its user. Winners are never copied into new partitions.
- **Resolve identity per shard.** Identity tables are sharded by the same hash as raw users, so a
  shard resolves its users against its own small table, rewriting them in place. A record whose
  person lives in another shard stays put: that shard gets an 8-byte pointer, and the home shard
  skips the record because its user now hashes elsewhere. About 37% of records cross shards.
- **No global click deduplication.** Duplicate click lines are identical after unquoting (the
  property the previous code relied on), and identical clicks change neither the crediting click
  nor the set of assisting campaigns.

Tried and rejected after measuring on this machine: transparent huge pages for partition
blocks (faults fell from 577K to 167K, but zeroing each 2 MiB page evicts the core's L2, and
parsing slowed); `MADV_POPULATE_READ` on the input; cache-sized identity shard tables; radix
grouping by time instead of per-person sorts; 8-byte packed events in aggregation; and a
line-at-a-time 128-bit mask splitter. Each was neutral or slower.

Validation: 36/36 fixtures, SSE2, scalar and degenerate-hash builds; 150 reference fuzz cases;
60 long-ID cases; exact matches on both official datasets.

## Previous update: measured on the official generator's data

All results here use datasets from the official generator: the public 1M set (seed 1) and the
20M set from the earlier 20M benchmarks (seed 17, 1,921,192,554 bytes). The native output on the
public set matches the Python reference exactly, with the published totals: 117,038 user rows,
27,400 campaign rows, 179,366 sessions and 376,461,330 revenue cents. Paired runs against
`main` (`f296c69`), alternating order, `NCPU=4`, on the 4-vCPU Linux container described below.
Both complete tables matched in every round:

| Input (official generator) | Rounds | Previous median | Updated median | Runtime reduction |
| --- | ---: | ---: | ---: | ---: |
| Public 1M events / 250K clicks | 10 | 0.227 s | **0.195 s** | **14%** |
| 20M events / 5M clicks, seed 17 | 6 | 4.151 s | **2.842 s** | **32%** |

Peak RSS on 20M fell from 4.33 to 3.62 GiB. Reports: `logs/paired-v6-official-*.json`.

The official data differs from the earlier surrogate in two ways that mattered. About a fifth of
all fields are quoted, so most lines contain a quote. And most user IDs are linked, so most
identity lookups hit. The changes, on top of the hashing, lookup and memory-reuse round below:

- **One-pass quoted lines:** the per-chunk splitter now uses `rows()`, which validates quotes while
  it scans. Before, every line containing a quote was split twice.
- **IDs held by value (`Key`):** IDs of up to 15 bytes are stored inline, with the length in
  byte 15; generated IDs have at most 9 bytes. Longer IDs point into the input. Equality is two
  word compares, and byte-wise order is a big-endian word compare, so identity hits, interning
  and dedup never read cold input. Identity entries also hold the canonical person, so a hit
  resolves within one cache line.
- **Slimmer records:** records carry keys but not hashes (raw events 72 → 56 bytes, events
  40 → 32, clicks 64 → 48); a key rehashes in a few nanoseconds.

Validation: 36/36 fixtures, SSE2 and scalar builds; 150 reference fuzz cases; 80 more with long
and mixed-length IDs that cross the inline/pointer boundary; a degenerate-hash build;
`check_parser_semantics.py`, SIMD and scalar; and exact matches on both official datasets.

Tried and rejected after measuring: exactly sized identity tables (a higher load factor made the
frequent misses probe longer) and counting quotes per field from bitmasks (neutral at 4 threads).

## Previous update: faster hashing, lookups and memory reuse

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

## Historical v3 measurements and timing context

The [portal practice workload](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)
uses **about 20M hidden events, 8 vCPUs and 24 GB RAM**, timed from container start
through exit. Its final workload is about 60M events. The public 1M input is a
smaller correctness check. The historical **7.3-second portal result came from
an earlier revision**. The current portal result and generator-aware engine's CI
measurements are recorded separately at the top of this document.

The v3 **Linux container comparison** against `a7cd5c1` measured
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

The v3 local comparisons used its immediately previous published version,
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
The hidden portal input and the 60M final workload were not measured in this
historical update.

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
# Surrogate inputs used for the historical v3 update (not the official generator):
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
  whitespace and quoting, and strict ASCII digit/calendar validation. One SSE2 pass
  finds commas, quotes and LFs and validates quotes per field as it scans; a scalar
  fallback covers non-x86 builds. Workers resolve their own 1 MiB chunk boundaries.
- IDs of up to 15 bytes (every generated ID) are held by value in a 16-byte `Key`,
  with the length in byte 15; longer IDs point into the input. Equality and byte-wise
  order never read cold input, and records carry keys rather than hashes.
- Link IDs are interned in parallel into 64 hash-sharded tables. Union-find runs on
  dense integers, and each component is named by its smallest UTF-8 bytewise ID
  (concurrent compare-and-swap minimum). Each entry holds the ID and its canonical ID.
- Parsing stores each valid event once in the shard of its raw user, and writes a
  dedup record for every event row (valid or not) to its event ID's shard. The earliest
  copy wins and the others are tombstoned in place, so invalid early copies still block
  later valid ones; validation flags are applied before choosing the winner only to
  decide what is stored.
- Each shard resolves its users against its own identity shard (same hash), rewriting
  them in place. A record whose person lives elsewhere stays put; that shard receives a
  pointer. Duplicate click lines are identical after unquoting and change no result, so
  clicks need no global deduplication.
- Each person shard interns canonical IDs in an open-addressing table, with full keys
  compared, and groups records with a counting sort. Short per-person runs are
  insertion-sorted by time. Session gaps, midnight ownership and click-ID ties follow
  the official rules exactly.
- Partition buffers are append-only block lists: records never move, so dedup records
  and cross-shard pointers can address them directly.
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
