# Express Lane integration

Vendored MIT source: Team 67's
[`jason-scheffel/express-lane@92cf41220f5f8ee6d94822a43293699ba1286604`](https://github.com/jason-scheffel/express-lane/tree/92cf41220f5f8ee6d94822a43293699ba1286604).
See `LICENSE` and `../THIRD_PARTY_NOTICES.md` for attribution and local changes.

Build from this directory with Rust 1.100.0 and the Linux musl target:

```sh
cargo build --locked --release --target x86_64-unknown-linux-musl
```

The executable is `target/x86_64-unknown-linux-musl/release/pipeline`. Install it
as `shopping-sessions-fast` beside the existing `shopping-sessions` C++ executable.
The build targets Cascade Lake and must only run after a CPU/OS feature check.
Inputs are read from `DATA_DIR` (default `/data`) and outputs go to `OUTPUT_DIR`
(default `/output`); `NCPU` defaults to 8. No external Rust crates are required.

If the fast engine cannot represent the input, it re-execs itself with
`--generic` to stop all workers, clears partial Parquet files, and replaces itself
with the sibling C++ executable. `SS_BASELINE_BIN` can override that path for
testing. `SS_ENGINE_TRACE=1` logs the fallback reason. Direct `--generic` mode
also forces this complete C++ retry; it does not execute the upstream generic code.

`XL_SKIP`, `XL_NOSINK`, `XL_MEMTEST`, `XL_SORTBENCH`, and synthetic key overrides
are not available. Remaining tuning and diagnostic options do not bypass the
required processing.
