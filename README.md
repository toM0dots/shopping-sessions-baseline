# Shopping Sessions baseline

Correctness-first Python implementation with DuckDB Parquet output, based on the
[official DuckDB starter](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions).
This is an independent local trial, not a competition submission.

## Validation

- All 36 official edge-case fixtures passed.
- Public seed-1 dataset (1 million requested events, 250,000 clicks) matched the
  official answer exactly: 117,038 user/day rows and 27,400 campaign/day rows.
- One native run: 61.41 seconds wall time, 780 MiB peak RSS, Linux x86_64 Xeon
  Platinum 8573C, Python 3.12.14, DuckDB 1.5.6, NCPU=2 and MEM_GB=4.
- Docker was unavailable: container compatibility and scorer resource limits
  have not been validated. Larger workloads remain untested.

## Run locally

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt polars==2.1.0
mkdir -p scratch
export SCRATCH_DIR="$PWD/scratch" NCPU=2 MEM_GB=4
python scripts/run_fixtures.py fixtures -- python pipeline/pipeline.py
```

For the public dev check, obtain generator.zip and dev-answer.zip from the
[official challenge page](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions).
Build the official generator with Rust 1.85 or newer and run:

```sh
generate --seed 1 --events 1000000 --clicks 250000 --output dev
DATA_DIR="$PWD/dev" OUTPUT_DIR="$PWD/out" python pipeline/pipeline.py
python scripts/compare.py out dev-answer
```

With Docker available, `make fixtures` builds and runs the container checks;
`make run-dev check-dev` runs and checks public dev data.

## Implementation

Strict LF-only CSV parsing, global deduplication before validation, union-find
identity resolution, session gaps strictly over 30 minutes, seven-day click
attribution and distinct-campaign assists, and exact integer daily aggregation.
The pipeline reads only input data, never expected answers. Python dictionaries
and sorting hold data in memory; the DuckDB memory limit does not cap Python.

The Dockerfile, runner, comparison scripts and fixtures come from the official
public starter. Exact semantics are defined in the
[official rules](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1).
Generated datasets, local environments and output files are excluded.
