#!/usr/bin/env python3
"""Production-grade benchmark for Ray Data CBO (Cost-Based Optimizer).

Measures the impact of CBO on two representative execution modes:

1. **Batch pipeline**: Read → Filter → Map (expansion) → Repartition → Sort → Materialize
2. **Streaming pipeline**: Read → Map (normalize) → Shuffle → Repartition → iter_batches

For each mode the script executes matched runs with CBO enabled and disabled,
collecting wall-clock time, per-operator breakdown, resource usage, spill bytes,
and streaming latency percentiles.

Usage:
    python cbo_benchmark.py --batch-rows 500000 --streaming-rows 300000 --repetitions 3
    python cbo_benchmark.py --output results.json --warmup
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

import ray
from ray.data import DataContext, Dataset

# ---------------------------------------------------------------------------
# Workload definitions
# ---------------------------------------------------------------------------


def _double_values(batch: Dict[str, "np.ndarray"]) -> Dict[str, "np.ndarray"]:
    """Simulates a UDF that expands the schema (amplification_ratio > 1)."""
    import numpy as np

    return {"id": batch["id"] * 2, "squared": np.square(batch["id"])}


def _normalize(batch: Dict[str, "np.ndarray"]) -> Dict[str, "np.ndarray"]:
    """Simulates a stateless streaming UDF."""
    import numpy as np

    arr = batch["id"].astype(np.float64)
    arr = (arr - np.mean(arr)) / (np.std(arr) + 1e-6)
    return {"id": arr}


def build_batch_dataset(num_rows: int) -> Dataset:
    """Heavy batch pipeline: Filter → Map (expansion) → Repartition → Sort."""
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
    """Streaming pipeline: Map (normalize) → Shuffle → Repartition."""
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
# Metrics collection
# ---------------------------------------------------------------------------


@dataclass
class OperatorMetrics:
    """Per-operator stats collected after execution."""

    operator: str = ""
    time_total_s: float = 0.0
    output_rows: int = 0
    throughput_rows_per_s: float = 0.0


@dataclass
class RunMetrics:
    """All metrics collected for a single benchmark run."""

    mode: str = ""                      # "batch" | "streaming"
    enable_cbo: bool = False
    duration_s: float = 0.0
    reservation_ratio: float = 0.5
    operator_count: int = 0
    operators: List[Dict[str, Any]] = field(default_factory=list)
    global_bytes_spilled: int = 0
    dataset_bytes_spilled: int = 0
    streaming_schedule_s: float = 0.0
    total_input_rows: int = 0
    # Streaming latency percentiles (only for streaming mode)
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_p99_ms: float = 0.0
    latency_mean_ms: float = 0.0


def _summarize_stats(
    ds: Dataset,
    duration_s: float,
    mode: str,
    enable_cbo: bool,
    batch_latencies: Optional[List[float]] = None,
) -> RunMetrics:
    """Extract execution stats from the materialised dataset."""
    plan = getattr(ds, "_plan", None)
    if plan is None:
        raise RuntimeError("Dataset is missing execution plan; cannot collect stats.")

    plan_stats = plan.stats()
    summary = plan_stats.to_summary()

    operators_payload: List[Dict[str, Any]] = []
    total_input_rows = 0

    for op in summary.operators_stats:
        op_rows = (
            op.output_num_rows.get("sum", 0)
            if isinstance(op.output_num_rows, dict)
            else 0
        )
        time_s = op.time_total_s if op.time_total_s else 0.0
        operators_payload.append(
            {
                "operator": op.operator_name,
                "time_total_s": round(time_s, 4),
                "output_rows": op_rows,
                "throughput_rows_per_s": round(
                    (op_rows / time_s) if time_s > 0 else 0.0, 2
                ),
            }
        )
        total_input_rows += getattr(op, "total_input_num_rows", 0) or 0

    ctx = DataContext.get_current()

    # Streaming latency percentiles
    p50 = p95 = p99 = mean_lat = 0.0
    if batch_latencies and len(batch_latencies) > 0:
        sorted_lats = sorted(batch_latencies)
        n = len(sorted_lats)
        p50 = sorted_lats[int(n * 0.50)] * 1000
        p95 = sorted_lats[int(n * 0.95)] * 1000
        p99 = sorted_lats[min(int(n * 0.99), n - 1)] * 1000
        mean_lat = statistics.mean(batch_latencies) * 1000

    return RunMetrics(
        mode=mode,
        enable_cbo=enable_cbo,
        duration_s=round(duration_s, 4),
        reservation_ratio=getattr(ctx, "op_resource_reservation_ratio", 0.5),
        operator_count=len(operators_payload),
        operators=operators_payload,
        global_bytes_spilled=summary.global_bytes_spilled,
        dataset_bytes_spilled=summary.dataset_bytes_spilled,
        streaming_schedule_s=round(summary.streaming_exec_schedule_s, 4),
        total_input_rows=total_input_rows,
        latency_p50_ms=round(p50, 3),
        latency_p95_ms=round(p95, 3),
        latency_p99_ms=round(p99, 3),
        latency_mean_ms=round(mean_lat, 3),
    )


# ---------------------------------------------------------------------------
# Context management
# ---------------------------------------------------------------------------


def _configure_context(streaming: bool, enable_cbo: bool) -> DataContext:
    """Create a context with CBO toggled on/off."""
    ctx = DataContext.get_current().copy()
    ctx.enable_cost_based_optimization = enable_cbo
    ctx._user_set_reservation_ratio = False
    ctx.execution_options.verbose_progress = False
    return ctx


@contextmanager
def _context_scope(ctx: DataContext):
    """Apply *ctx* for the duration of the block, restoring the original after."""
    if hasattr(DataContext, "current"):
        with DataContext.current(ctx):
            yield
        return

    prev = DataContext.get_current()
    DataContext._set_current(ctx)
    try:
        yield
    finally:
        if prev is not None:
            DataContext._set_current(prev)


# ---------------------------------------------------------------------------
# Workload runners
# ---------------------------------------------------------------------------


def run_batch_workload(num_rows: int, enable_cbo: bool) -> RunMetrics:
    """Execute the batch pipeline with or without CBO."""
    ctx = _configure_context(streaming=False, enable_cbo=enable_cbo)
    with _context_scope(ctx):
        ds = build_batch_dataset(num_rows)
        start = time.perf_counter()
        ds = ds.materialize()
        duration = time.perf_counter() - start
        return _summarize_stats(ds, duration, mode="batch", enable_cbo=enable_cbo)


def run_streaming_workload(num_rows: int, enable_cbo: bool) -> RunMetrics:
    """Execute the streaming pipeline with or without CBO, measuring per-batch latency."""
    ctx = _configure_context(streaming=True, enable_cbo=enable_cbo)
    with _context_scope(ctx):
        ds = build_streaming_dataset(num_rows)

        batch_latencies: List[float] = []
        consumed = 0

        start = time.perf_counter()
        for batch in ds.iter_batches(batch_size=4096, batch_format="numpy"):
            batch_start = time.perf_counter()
            consumed += len(batch["id"])
            batch_latencies.append(time.perf_counter() - batch_start)
        duration = time.perf_counter() - start

        metrics = _summarize_stats(
            ds,
            duration,
            mode="streaming",
            enable_cbo=enable_cbo,
            batch_latencies=batch_latencies,
        )
        metrics.total_input_rows = consumed
        return metrics


# ---------------------------------------------------------------------------
# Aggregation & reporting
# ---------------------------------------------------------------------------


def aggregate_runs(runs: List[RunMetrics]) -> Dict[str, Dict[str, Any]]:
    """Group runs by (mode, enable_cbo) and compute summary statistics."""
    grouped: Dict[str, List[RunMetrics]] = {}
    for run in runs:
        key = f"{run.mode}-cbo-{run.enable_cbo}"
        grouped.setdefault(key, []).append(run)

    aggregates: Dict[str, Dict[str, Any]] = {}
    for key, values in grouped.items():
        durations = [v.duration_s for v in values]
        agg: Dict[str, Any] = {
            "runs": len(values),
            "duration_s_mean": round(statistics.mean(durations), 4),
            "duration_s_stdev": round(statistics.pstdev(durations), 4),
            "reservation_ratio_mean": round(
                statistics.mean(v.reservation_ratio for v in values), 4
            ),
            "operator_count": values[0].operator_count,
            "global_bytes_spilled_mean": int(
                statistics.mean(v.global_bytes_spilled for v in values)
            ),
            "dataset_bytes_spilled_mean": int(
                statistics.mean(v.dataset_bytes_spilled for v in values)
            ),
        }
        # Streaming-specific aggregates
        if values[0].mode == "streaming":
            agg["latency_p50_ms_mean"] = round(
                statistics.mean(v.latency_p50_ms for v in values), 3
            )
            agg["latency_p95_ms_mean"] = round(
                statistics.mean(v.latency_p95_ms for v in values), 3
            )
            agg["latency_p99_ms_mean"] = round(
                statistics.mean(v.latency_p99_ms for v in values), 3
            )
        aggregates[key] = agg
    return aggregates


def _format_comparison(aggregates: Dict[str, Dict[str, Any]]) -> str:
    """Produce a human-readable comparison table."""
    lines: List[str] = []
    lines.append("=" * 72)
    lines.append("  Ray Data CBO Benchmark Results")
    lines.append("=" * 72)

    for mode in ("batch", "streaming"):
        off_key = f"{mode}-cbo-False"
        on_key = f"{mode}-cbo-True"
        off = aggregates.get(off_key, {})
        on = aggregates.get(on_key, {})
        if not off or not on:
            continue

        lines.append(f"\n  [{mode.upper()}]")
        lines.append(f"  {'Metric':<35} {'CBO OFF':>12} {'CBO ON':>12} {'Delta':>10}")
        lines.append(f"  {'-'*35} {'-'*12} {'-'*12} {'-'*10}")

        dur_off = off["duration_s_mean"]
        dur_on = on["duration_s_mean"]
        delta_pct = ((dur_on - dur_off) / dur_off * 100) if dur_off > 0 else 0
        lines.append(
            f"  {'Duration (s)':<35} {dur_off:>12.3f} {dur_on:>12.3f} "
            f"{delta_pct:>+9.1f}%"
        )

        lines.append(
            f"  {'Reservation Ratio':<35} "
            f"{off['reservation_ratio_mean']:>12.3f} "
            f"{on['reservation_ratio_mean']:>12.3f}"
        )

        spill_off = off["global_bytes_spilled_mean"]
        spill_on = on["global_bytes_spilled_mean"]
        lines.append(
            f"  {'Global Spill (MB)':<35} "
            f"{spill_off / (1024**2):>12.1f} "
            f"{spill_on / (1024**2):>12.1f}"
        )

        if mode == "streaming":
            for pct in ("p50", "p95", "p99"):
                key_name = f"latency_{pct}_ms_mean"
                v_off = off.get(key_name, 0)
                v_on = on.get(key_name, 0)
                lines.append(
                    f"  {'Latency ' + pct + ' (ms)':<35} "
                    f"{v_off:>12.3f} {v_on:>12.3f}"
                )

    lines.append("\n" + "=" * 72)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Production benchmark for Ray Data CBO."
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=200_000,
        help="Number of rows for the batch workload.",
    )
    parser.add_argument(
        "--streaming-rows",
        type=int,
        default=150_000,
        help="Number of rows for the streaming workload.",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=3,
        help="How many times to repeat each configuration.",
    )
    parser.add_argument(
        "--warmup",
        action="store_true",
        default=False,
        help="Run a warmup iteration (discarded) before timing.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="cbo_benchmark_results.json",
        help="Path to write the JSON results.",
    )
    args = parser.parse_args()

    ray.init(ignore_reinit_error=True)

    runs: List[RunMetrics] = []
    try:
        # Optional warmup (results discarded)
        if args.warmup:
            print("[warmup] Running warmup iterations ...")
            run_batch_workload(min(args.batch_rows, 50_000), enable_cbo=False)
            run_streaming_workload(min(args.streaming_rows, 50_000), enable_cbo=False)
            print("[warmup] Done.\n")

        for rep in range(args.repetitions):
            print(f"[rep {rep + 1}/{args.repetitions}] batch CBO=OFF ...")
            runs.append(run_batch_workload(args.batch_rows, enable_cbo=False))

            print(f"[rep {rep + 1}/{args.repetitions}] batch CBO=ON ...")
            runs.append(run_batch_workload(args.batch_rows, enable_cbo=True))

            print(f"[rep {rep + 1}/{args.repetitions}] streaming CBO=OFF ...")
            runs.append(run_streaming_workload(args.streaming_rows, enable_cbo=False))

            print(f"[rep {rep + 1}/{args.repetitions}] streaming CBO=ON ...")
            runs.append(run_streaming_workload(args.streaming_rows, enable_cbo=True))

    finally:
        ray.shutdown()

    aggregates = aggregate_runs(runs)

    output_payload = {
        "runs": [asdict(run) for run in runs],
        "aggregates": aggregates,
    }

    with open(args.output, "w", encoding="utf-8") as fp:
        json.dump(output_payload, fp, indent=2)

    # Print human-readable comparison
    print(_format_comparison(aggregates))
    print(f"\nFull results written to {os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
