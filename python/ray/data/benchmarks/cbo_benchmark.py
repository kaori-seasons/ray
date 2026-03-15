#!/usr/bin/env python3
"""Synthetic Ray Data benchmark for evaluating the planned CBO toggles.

The script runs two representative workloads:

1. Batch-style pipeline with heavy filtering, UDF expansion, repartitioning, and sort.
2. Streaming-style pipeline that is consumed via ``iter_batches`` to mimic online
   inference latency.

For each workload the script executes two runs: once with the best-effort
``enable_cost_based_optimization`` flag disabled and once enabled. The branch does
not yet wire CBO into the optimizer, but collecting identical before/after metrics
exposes the readiness gaps in an auditable way.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from dataclasses import asdict, dataclass
from typing import Dict, List

import ray
from ray.data import DataContext, Dataset

# ---------------------------------------------------------------------------
# Workload definitions
# ---------------------------------------------------------------------------


def _double_values(batch: Dict[str, "np.ndarray"]) -> Dict[str, "np.ndarray"]:
    import numpy as np

    return {"id": batch["id"] * 2, "squared": np.square(batch["id"])}


def _normalize(batch: Dict[str, "np.ndarray"]) -> Dict[str, "np.ndarray"]:
    import numpy as np

    arr = batch["id"].astype(np.float64)
    arr = (arr - np.mean(arr)) / (np.std(arr) + 1e-6)
    return {"id": arr}


def build_batch_dataset(num_rows: int) -> Dataset:
    ds = ray.data.range(num_rows)
    ds = ds.filter(lambda row: row["id"] % 3 == 0)
    ds = ds.map_batches(
        _double_values,
        batch_size=2048,
        batch_format="numpy",
    )
    ds = ds.repartition(64)
    ds = ds.sort(key="id")
    return ds


def build_streaming_dataset(num_rows: int) -> Dataset:
    ds = ray.data.range(num_rows)
    ds = ds.map_batches(
        _normalize,
        batch_size=1024,
        batch_format="numpy",
    )
    ds = ds.random_shuffle(seed=1234)
    ds = ds.repartition(32)
    return ds


# ---------------------------------------------------------------------------
# Benchmark harness
# ---------------------------------------------------------------------------


@dataclass
class RunMetrics:
    mode: str
    enable_cbo: bool
    duration_s: float
    reservation_ratio: float
    operator_count: int
    operators: List[Dict[str, float]]
    global_bytes_spilled: int
    dataset_bytes_spilled: int
    streaming_schedule_s: float
    total_input_rows: int


def _summarize_stats(ds: Dataset, duration_s: float, mode: str, enable_cbo: bool) -> RunMetrics:
    stats = ds.stats()
    summary = stats.to_summary()
    operators_payload = []
    total_input_rows = 0
    for op in summary.operators_stats:
        op_rows = (
            op.output_num_rows.get("sum", 0)
            if isinstance(op.output_num_rows, dict)
            else 0
        )
        operators_payload.append(
            {
                "operator": op.operator_name,
                "time_total_s": round(op.time_total_s, 4),
                "output_rows": op_rows,
                "num_rows_per_s": round(op.num_rows_per_s, 4),
            }
        )
        total_input_rows += op.total_input_num_rows or 0

    ctx = DataContext.get_current()
    return RunMetrics(
        mode=mode,
        enable_cbo=enable_cbo,
        duration_s=duration_s,
        reservation_ratio=getattr(ctx, "op_resource_reservation_ratio", 0.5),
        operator_count=len(operators_payload),
        operators=operators_payload,
        global_bytes_spilled=summary.global_bytes_spilled,
        dataset_bytes_spilled=summary.dataset_bytes_spilled,
        streaming_schedule_s=summary.streaming_exec_schedule_s,
        total_input_rows=total_input_rows,
    )


def _configure_context(streaming: bool, enable_cbo: bool) -> DataContext:
    ctx = DataContext.get_current().copy()
    setattr(ctx, "enable_cost_based_optimization", enable_cbo)
    setattr(ctx, "_user_set_reservation_ratio", False)
    ctx.execution_options.actor_locality_enabled = streaming
    ctx.execution_options.verbose_progress = False
    return ctx


def run_batch_workload(num_rows: int, enable_cbo: bool) -> RunMetrics:
    ctx = _configure_context(streaming=False, enable_cbo=enable_cbo)
    with DataContext.current(ctx):
        ds = build_batch_dataset(num_rows)
        start = time.perf_counter()
        ds.materialize()
        duration = time.perf_counter() - start
        return _summarize_stats(ds, duration, mode="batch", enable_cbo=enable_cbo)


def run_streaming_workload(num_rows: int, enable_cbo: bool) -> RunMetrics:
    ctx = _configure_context(streaming=True, enable_cbo=enable_cbo)
    with DataContext.current(ctx):
        ds = build_streaming_dataset(num_rows)
        start = time.perf_counter()
        consumed = 0
        for batch in ds.iter_batches(batch_size=4096, batch_format="numpy"):
            consumed += len(batch["id"])
        duration = time.perf_counter() - start
        metrics = _summarize_stats(ds, duration, mode="streaming", enable_cbo=enable_cbo)
        metrics.total_input_rows = consumed
        return metrics


def aggregate_runs(runs: List[RunMetrics]) -> Dict[str, Dict[str, float]]:
    grouped: Dict[str, List[RunMetrics]] = {}
    for run in runs:
        key = f"{run.mode}-cbo-{run.enable_cbo}"
        grouped.setdefault(key, []).append(run)

    aggregates: Dict[str, Dict[str, float]] = {}
    for key, values in grouped.items():
        aggregates[key] = {
            "duration_s_mean": statistics.mean(v.duration_s for v in values),
            "duration_s_stdev": statistics.pstdev(v.duration_s for v in values),
            "reservation_ratio_mean": statistics.mean(v.reservation_ratio for v in values),
            "operator_count": statistics.mean(v.operator_count for v in values),
        }
    return aggregates


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-rows", type=int, default=200_000, help="Rows for batch workload.")
    parser.add_argument("--streaming-rows", type=int, default=150_000, help="Rows for streaming workload.")
    parser.add_argument("--repetitions", type=int, default=1, help="Runs per configuration.")
    parser.add_argument("--output", type=str, default="cbo_benchmark_results.json", help="Where to write JSON.")
    args = parser.parse_args()

    ray.init(ignore_reinit_error=True)

    runs: List[RunMetrics] = []
    try:
        for _ in range(args.repetitions):
            runs.append(run_batch_workload(args.batch_rows, enable_cbo=False))
            runs.append(run_batch_workload(args.batch_rows, enable_cbo=True))
            runs.append(run_streaming_workload(args.streaming_rows, enable_cbo=False))
            runs.append(run_streaming_workload(args.streaming_rows, enable_cbo=True))
    finally:
        ray.shutdown()

    output_payload = {
        "runs": [asdict(run) for run in runs],
        "aggregates": aggregate_runs(runs),
    }

    with open(args.output, "w", encoding="utf-8") as fp:
        json.dump(output_payload, fp, indent=2)

    print(f"Wrote benchmark results to {os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
