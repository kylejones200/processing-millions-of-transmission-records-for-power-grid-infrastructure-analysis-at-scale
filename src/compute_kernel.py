"""Group sum aggregation by integer keys."""

from __future__ import annotations

import numpy as np


def group_sum_by_key(
    keys: np.ndarray, values: np.ndarray, n_groups: int
) -> np.ndarray:
    k = np.asarray(keys, dtype=np.int64)
    v = np.asarray(values, dtype=float)
    sums = np.zeros(n_groups, dtype=float)
    for key, val in zip(k, v):
        idx = int(key)
        if 0 <= idx < n_groups:
            sums[idx] += val
    return sums
