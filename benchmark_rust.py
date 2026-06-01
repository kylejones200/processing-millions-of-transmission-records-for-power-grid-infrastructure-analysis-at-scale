#!/usr/bin/env python3
"""Python vs Rust kernel benchmark."""

from __future__ import annotations

import time
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
from compute_kernel import group_sum_by_key  # noqa: E402

def main() -> None:
    n = 10000
    keys = np.ascontiguousarray(np.arange(n) % 50, dtype=np.int64)
    values = np.ascontiguousarray(np.sin(np.arange(n) * 0.01) + 1.0)
    n_groups = 50
    t0 = time.perf_counter()
    for _ in range(200):
        group_sum_by_key(keys, values, n_groups)
    py_s = time.perf_counter() - t0
    try:
        import processing_millions_of_transmission_records_for_power_grid_infrastructure_analysis_at_scale_rs as rs
    except ImportError:
        print("Build: maturin develop --release -m rust/py/Cargo.toml")
        print(f"Python {py_s:.3f}s")
        return
    rs_s = rs.bench_kernel_py(keys, values, n_groups, 5000)
    print(f"Python {py_s:.3f}s Rust {rs_s:.3f}s speedup {py_s / max(rs_s, 1e-9):.1f}x")
    np.testing.assert_allclose(
        group_sum_by_key(keys, values, n_groups),
        np.asarray(rs.group_sum_by_key_py(keys, values, n_groups)),
        rtol=1e-10,
    )
    print("Correctness: OK")

if __name__ == "__main__":
    main()
