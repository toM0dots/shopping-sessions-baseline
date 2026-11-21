# Third-party notices

## Express Lane

The Rust sources, Cargo manifests, build configuration, and license in `fast/`
derive from [Team 67's Express Lane](https://github.com/jason-scheffel/express-lane),
commit [`92cf41220f5f8ee6d94822a43293699ba1286604`](https://github.com/jason-scheffel/express-lane/tree/92cf41220f5f8ee6d94822a43293699ba1286604).
Copyright (c) 2026 Team 67. Used under the MIT License; the complete copyright
notice, permission notice, and warranty disclaimer are preserved in
[`fast/LICENSE`](fast/LICENSE).

Local changes integrate that implementation as a guarded fast engine:

- Constraint failures restart into a single-threaded cleanup step, remove partial
  Parquet output, then execute this repository's C++ engine over the complete input.
  The fallback binary is the sibling `shopping-sessions`, or `SS_BASELINE_BIN`
  when explicitly supplied. `DATA_DIR`, `OUTPUT_DIR`, and `NCPU` are preserved.
- Work-skipping, synthetic-key overrides, and successful early-exit benchmark
  entry points are removed. Production execution always processes the input.
- Descriptor preallocation uses `F_DUPFD_CLOEXEC` to avoid replacing an existing
  descriptor. Optional engine tracing reports the fallback reason.
- The Rust build disables the unused PKU feature explicitly. CPU dispatch still
  requires the remaining Cascade Lake instructions and OS AVX-512 state support;
  it accepts AVX-512 VMs that do not expose protection keys.
- The original `src/generic/` implementation remains as uncompiled upstream
  reference material; it is replaced at runtime by the C++ fallback.

The generator-aware ID representation, SIMD parsing, direct-indexed processing,
and custom Parquet encoding in this engine are upstream work, not original work
of this repository. No claim is made that an upstream leaderboard result measures
this modified integration; its performance and correctness are validated separately.
