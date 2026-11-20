#!/usr/bin/env python3
"""Build the native executable against the Arrow C++ libraries in PyArrow.

Supports wheel and conda layouts; no machine-specific CPU flags are used.
"""
import os
from pathlib import Path
import shlex
import subprocess
import sys

import pyarrow

root = Path(__file__).resolve().parents[1]
prefixes = [Path(sys.prefix), Path(sys.base_prefix), Path(pyarrow.__file__).parents[3]]
includes = [Path(pyarrow.get_include()), *(p / "include" for p in prefixes)]
include = next((p for p in includes if (p / "arrow/api.h").is_file()), None)
library_dirs = [*(Path(p) for p in pyarrow.get_library_dirs()), *(p / "lib" for p in prefixes)]
libraries = []
used_dirs = []
for name in ("parquet", "arrow"):
    matches = []
    for directory in library_dirs:
        for pattern in (f"lib{name}.so", f"lib{name}.so.*", f"lib{name}.dylib", f"lib{name}.*.dylib"):
            matches.extend(sorted(directory.glob(pattern)))
    if not matches:
        sys.exit(f"Cannot find lib{name}; install the pyarrow wheel or Arrow C++ libraries.")
    libraries.append(str(matches[0]))
    used_dirs.append(str(matches[0].parent))
if include is None:
    sys.exit("Cannot find Arrow headers; install the pyarrow wheel or Arrow C++ libraries.")
(root / "build").mkdir(exist_ok=True)
cmd = [*shlex.split(os.environ.get("CXX", "c++")), "-O3", "-DNDEBUG", "-std=c++17", "-pthread",
       f"-I{include}", str(root / "pipeline/native.cpp"), *libraries,
       *(f"-Wl,-rpath,{p}" for p in dict.fromkeys(used_dirs)),
       "-o", str(root / "build/shopping-sessions")]
print(shlex.join(cmd), flush=True)
subprocess.run(cmd, check=True)
