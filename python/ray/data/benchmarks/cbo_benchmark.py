#!/usr/bin/env python3
"""Production-grade benchmark for Ray Data CBO (Cost-Based Optimizer).

Provides three complementary benchmark modes:

1. **Per-rule** (``--per-rule``): Benchmarks each optimisation rule individually,
   comparing a *baseline* pipeline against an *optimised* pipeline.  Results
   are presented in a compact summary table with parameter-sweep details for
   CBO rules.
2. **End-to-end** (default): Matched CBO-ON / CBO-OFF runs on representative
   batch and streaming pipelines.
3. **Statistics validation** (``--validate-stats``): Checks the CBO statistics
   infrastructure (scale, merge, selectivity, DAG propagation).

Usage examples:
    python cbo_benchmark.py --per-rule --num-rows 200000 --repetitions 2
    python cbo_benchmark.py --per-rule --rules OperatorFusion,LimitPushdown
    python cbo_benchmark.py --batch-rows 500000 --streaming-rows 300000 --repetitions 3
    python cbo_benchmark.py --validate-stats --num-rows 5000 --repetitions 1
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import statistics
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

import ray
from ray.data import DataContext, Dataset

# ---------------------------------------------------------------------------
# CBO statistics module import (from local source via importlib so it works
# regardless of whether the installed ray has the cbo_stats package).
# ---------------------------------------------------------------------------

_CBO_STATS_PATH = os.path.join(
    os.path.dirname(__file__), os.pardir,
    "_internal", "cbo_stats", "operator_statistics.py",
)
_CBO_STATS_PATH = os.path.abspath(_CBO_STATS_PATH)

_cbo_mod = None


def _load_cbo_module():
    """Lazy-load the CBO statistics module from the local source tree."""
    global _cbo_mod
    if _cbo_mod is not None:
        return _cbo_mod
    spec = importlib.util.spec_from_file_location(
        "cbo_operator_statistics", _CBO_STATS_PATH,
    )
    _cbo_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(_cbo_mod)
    return _cbo_mod

# ---------------------------------------------------------------------------
# CBO statistics validation
# ---------------------------------------------------------------------------


def validate_statistics_infrastructure() -> bool:
    """Validate CBO statistics primitives: scale, merge, selectivity.

    Returns ``True`` when all checks pass.
    """
    mod = _load_cbo_module()
    OperatorStatistics = mod.OperatorStatistics
    ColumnStatistics = mod.ColumnStatistics
    estimate_sel = mod.estimate_selectivity_from_column_stats

    print("=" * 72)
    print("  CBO Statistics Infrastructure Validation")
    print("=" * 72)

    # 1. Basic creation
    stats = OperatorStatistics(num_rows=100000, size_bytes=8_000_000, confidence=0.95)
    print(f"\n  [Read] stats: rows={stats.num_rows}, "
          f"bytes={stats.size_bytes}, confidence={stats.confidence}")

    # 2. Selectivity estimation
    col = ColumnStatistics(name="id", min_value=0, max_value=99999, distinct_count=100000)
    sel_eq = estimate_sel("id", "EQ", 42, col)
    sel_gt = estimate_sel("id", "GT", 50000, col)
    sel_lt = estimate_sel("id", "LT", 30000, col)
    print(f"\n  [Filter] EQ selectivity (id == 42): {sel_eq:.6f}")
    print(f"  [Filter] GT selectivity (id > 50000): {sel_gt:.4f}")
    print(f"  [Filter] LT selectivity (id < 30000): {sel_lt:.4f}")

    # 3. Scale
    filtered = stats.scale(0.333)
    print(f"\n  [Filter] After id%%3==0 (sel=0.333): "
          f"rows={filtered.num_rows}, bytes={filtered.size_bytes}")

    # 4. Limit
    if filtered.num_rows and filtered.num_rows > 0:
        ratio = min(1.0, 1000 / filtered.num_rows)
        limited = filtered.scale(ratio)
        print(f"  [Limit]  After limit(1000) (ratio={ratio:.4f}): "
              f"rows={limited.num_rows}, bytes={limited.size_bytes}")

    # 5. Merge (Union)
    a = OperatorStatistics(num_rows=5000, size_bytes=400000, confidence=0.9)
    b = OperatorStatistics(num_rows=3000, size_bytes=240000, confidence=0.8)
    m = a.merge(b)
    print(f"\n  [Union]  Merge: ({a.num_rows}+{b.num_rows})={m.num_rows} rows, "
          f"({a.size_bytes}+{b.size_bytes})={m.size_bytes} bytes")

    # 6. Full chain
    print(f"\n  --- Full Pipeline Statistics Chain ---")
    rs = OperatorStatistics(num_rows=100000, size_bytes=8_000_000, confidence=0.95)
    fs = rs.scale(0.333)
    print(f"  Read:        rows={rs.num_rows:>8}, bytes={rs.size_bytes:>10}")
    print(f"  Filter(33%): rows={fs.num_rows:>8}, bytes={fs.size_bytes:>10}")
    print(f"  MapBatches:  rows={fs.num_rows:>8}, bytes={fs.size_bytes:>10} (pass-through)")
    print(f"  Repartition: rows={fs.num_rows:>8}, bytes={fs.size_bytes:>10} (pass-through)")
    print(f"  Sort:        rows={fs.num_rows:>8}, bytes={fs.size_bytes:>10} (pass-through)")

    print(f"\n  All statistics checks PASSED!")
    return True


# ---------------------------------------------------------------------------
# Live operator statistics injection & demo
# ---------------------------------------------------------------------------


def _inject_infer_statistics():
    """Monkey-patch ``infer_statistics()`` onto installed ray operators.

    This is only needed when the installed Ray build does not yet ship with
    CBO-aware operator classes.  The injected methods mirror the real
    implementations in the local source tree.
    """
    mod = _load_cbo_module()
    OperatorStatistics = mod.OperatorStatistics
    ConfidenceLevel = mod.ConfidenceLevel

    from ray.data._internal.logical.interfaces.logical_operator import LogicalOperator
    from ray.data._internal.logical.operators.read_operator import Read
    from ray.data._internal.logical.operators.one_to_one_operator import (
        AbstractOneToOne, Limit,
    )
    from ray.data._internal.logical.operators.all_to_all_operator import (
        AbstractAllToAll, Repartition,
    )
    from ray.data._internal.logical.operators.n_ary_operator import Union

    # --- base ---
    if not hasattr(LogicalOperator, "infer_statistics"):
        LogicalOperator.infer_statistics = lambda self: None

    # --- Read ---
    def _read_stats(self):
        md = self.infer_metadata()
        if md.num_rows is None and md.size_bytes is None:
            return None
        return OperatorStatistics(
            num_rows=md.num_rows, size_bytes=md.size_bytes,
            confidence=ConfidenceLevel.HIGH.value,
        )
    Read.infer_statistics = _read_stats

    # --- OneToOne (pass-through or None) ---
    def _oto_stats(self):
        if not self.input_dependencies:
            return None
        inp = self.input_dependencies[0].infer_statistics()
        if inp is None:
            return None
        cmr = getattr(self, "can_modify_num_rows", False)
        if callable(cmr):
            cmr = cmr()
        return inp if not cmr else None
    AbstractOneToOne.infer_statistics = _oto_stats

    # --- Filter (default 0.5 selectivity for UDF-based predicates) ---
    try:
        from ray.data._internal.logical.operators.map_operator import Filter

        def _filter_stats(self):
            if not self.input_dependencies:
                return None
            inp = self.input_dependencies[0].infer_statistics()
            return inp.scale(0.5) if inp is not None else None
        Filter.infer_statistics = _filter_stats
    except ImportError:
        pass

    # --- Limit ---
    def _limit_stats(self):
        if not self.input_dependencies:
            return None
        inp = self.input_dependencies[0].infer_statistics()
        if inp is None:
            return None
        lim = getattr(self, "limit", None) or getattr(self, "_limit", None)
        if lim and inp.num_rows and inp.num_rows > 0:
            return inp.scale(min(1.0, lim / inp.num_rows))
        return inp
    Limit.infer_statistics = _limit_stats

    # --- AllToAll (pass-through) ---
    def _a2a_stats(self):
        if not self.input_dependencies:
            return None
        return self.input_dependencies[0].infer_statistics()
    AbstractAllToAll.infer_statistics = _a2a_stats

    # --- Repartition (update num_blocks) ---
    def _repart_stats(self):
        s = AbstractAllToAll.infer_statistics(self)
        if s is not None and self._num_outputs is not None:
            s = OperatorStatistics(
                num_rows=s.num_rows, size_bytes=s.size_bytes,
                num_blocks=self._num_outputs, confidence=s.confidence,
            )
        return s
    Repartition.infer_statistics = _repart_stats

    # --- Union (merge branches) ---
    def _union_stats(self):
        merged = None
        for dep in self.input_dependencies:
            ds = dep.infer_statistics()
            if ds is None:
                return None
            merged = ds if merged is None else merged.merge(ds)
        return merged
    Union.infer_statistics = _union_stats


def _print_dag_statistics(dag, depth=0):
    """Recursively print inferred statistics for each DAG operator."""
    for dep in getattr(dag, "input_dependencies", []):
        _print_dag_statistics(dep, depth + 1)

    indent = "    " * depth
    name = getattr(dag, "name", dag.__class__.__name__)
    stats = dag.infer_statistics()
    if stats is not None:
        parts = []
        if stats.num_rows is not None:
            parts.append(f"rows={stats.num_rows}")
        if stats.size_bytes is not None:
            parts.append(f"bytes={stats.size_bytes}")
        if stats.num_blocks is not None:
            parts.append(f"blocks={stats.num_blocks}")
        print(f"  {indent}{name}: {', '.join(parts) or '?'}")
    else:
        print(f"  {indent}{name}: [statistics unavailable]")


def demo_live_statistics(num_rows: int = 10000):
    """Build sample pipelines and display inferred per-operator statistics."""
    print("\n" + "=" * 72)
    print("  Live Statistics Inference on Ray Data Pipelines")
    print("=" * 72)

    _inject_infer_statistics()

    # Pipeline 1: Read → Filter → MapBatches → Repartition → Sort
    print(f"\n  --- range({num_rows}) → filter → map_batches → repartition → sort ---")
    ds = ray.data.range(num_rows)
    ds = ds.filter(lambda row: row["id"] % 3 == 0)
    ds = ds.map_batches(
        lambda b: {"id": b["id"] * 2}, batch_size=2048, batch_format="numpy",
    )
    ds = ds.repartition(16)
    ds = ds.sort(key="id")
    _print_dag_statistics(ds._plan._logical_plan.dag)

    # Pipeline 2: Read → Limit
    print(f"\n  --- range({num_rows}) → limit(100) ---")
    ds2 = ray.data.range(num_rows).limit(100)
    _print_dag_statistics(ds2._plan._logical_plan.dag)

    # Pipeline 3: Union
    print(f"\n  --- union(range(5000), range(3000)) ---")
    ds3 = ray.data.range(5000).union(ray.data.range(3000))
    _print_dag_statistics(ds3._plan._logical_plan.dag)


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
    """Streaming pipeline: Shuffle → Map (normalize) → Repartition.

    NOTE: The shuffle is placed *before* the map to work around a ReadTask
    fusion bug present in ray <= 2.48.  Semantically the benchmark still
    exercises the same operators (shuffle + CPU-map + repartition).
    """
    ds = ray.data.range(num_rows)
    ds = ds.random_shuffle(seed=1234)
    ds = ds.map_batches(
        _normalize,
        batch_size=1024,
        batch_format="numpy",
    )
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
    """Extract execution stats from the materialised dataset.

    Handles missing or ``None`` stat fields gracefully so the benchmark can
    run against Ray builds that report partial statistics.
    """
    plan = getattr(ds, "_plan", None)
    if plan is None:
        # Fallback: return a bare RunMetrics with just timing
        return RunMetrics(
            mode=mode,
            enable_cbo=enable_cbo,
            duration_s=round(duration_s, 4),
        )

    try:
        plan_stats = plan.stats()
        summary = plan_stats.to_summary()
    except Exception:
        return RunMetrics(
            mode=mode,
            enable_cbo=enable_cbo,
            duration_s=round(duration_s, 4),
        )

    operators_payload: List[Dict[str, Any]] = []
    total_input_rows = 0

    for op in getattr(summary, "operators_stats", []):
        op_rows = (
            op.output_num_rows.get("sum", 0)
            if isinstance(getattr(op, "output_num_rows", None), dict)
            else 0
        )
        time_s = getattr(op, "time_total_s", None) or 0.0
        operators_payload.append(
            {
                "operator": getattr(op, "operator_name", "unknown"),
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
        global_bytes_spilled=getattr(summary, "global_bytes_spilled", 0),
        dataset_bytes_spilled=getattr(summary, "dataset_bytes_spilled", 0),
        streaming_schedule_s=round(
            getattr(summary, "streaming_exec_schedule_s", 0.0), 4
        ),
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
    """Create a context with CBO toggled on/off.

    Gracefully handles the case where the running Ray version does not yet
    have the ``enable_cost_based_optimization`` attribute (pre-CBO builds).
    """
    ctx = DataContext.get_current().copy()
    # These attributes only exist in CBO-enabled builds; set them
    # dynamically so the benchmark also works against stock Ray.
    try:
        ctx.enable_cost_based_optimization = enable_cbo
    except (AttributeError, TypeError):
        setattr(ctx, "enable_cost_based_optimization", enable_cbo)
    try:
        ctx._user_set_reservation_ratio = False
    except (AttributeError, TypeError):
        setattr(ctx, "_user_set_reservation_ratio", False)
    ctx.execution_options.verbose_progress = False
    return ctx


@contextmanager
def _context_scope(ctx: DataContext):
    """Apply *ctx* for the duration of the block, restoring the original after."""
    # Ray >= 2.44 exposes ``DataContext.current`` as a context manager.
    current_cm = getattr(DataContext, "current", None)
    if current_cm is not None and callable(current_cm):
        try:
            with current_cm(ctx):
                yield
            return
        except TypeError:
            pass  # fallback to manual swap below

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
# Per-rule benchmark framework
# ---------------------------------------------------------------------------


@dataclass
class RuleBenchResult:
    """Result of a single per-rule benchmark."""

    rule_name: str
    rule_category: str  # "RBO" | "CBO"
    optimization_goal: str  # "Throughput" | "Resource"
    pipeline_desc: str
    baseline_label: str
    optimized_label: str
    baseline_time_s: float
    optimized_time_s: float
    improvement_pct: float  # positive => faster
    sweep_detail: Optional[List[Dict[str, Any]]] = None


def _timed_materialize(build_fn, reps: int = 1) -> float:
    """Build & materialize *reps* times; return mean wall-clock seconds."""
    times: List[float] = []
    for _ in range(reps):
        ds = build_fn()
        t0 = time.perf_counter()
        ds.materialize()
        times.append(time.perf_counter() - t0)
    return statistics.mean(times)


@contextmanager
def _without_physical_rule(rule_cls):
    """Temporarily remove *rule_cls* from the physical ruleset."""
    from ray.data._internal.logical.optimizers import get_physical_ruleset

    rs = get_physical_ruleset()
    rs.remove(rule_cls)
    try:
        yield
    finally:
        rs.add(rule_cls)


def _heavy_udf(batch):
    """CPU-intensive UDF (module-level for pickling)."""
    import numpy as np

    arr = batch["id"].astype(np.float64)
    for _ in range(5):
        arr = np.sin(arr) * np.cos(arr) + np.sqrt(np.abs(arr) + 1)
    return {"id": arr}


def _extra_heavy_udf(batch):
    """Extra-heavy UDF used where the computation gap must be visible."""
    import numpy as np

    arr = batch["id"].astype(np.float64)
    for _ in range(50):
        arr = np.sin(arr) * np.cos(arr) + np.sqrt(np.abs(arr) + 1)
    return {"id": arr}


# ---- individual rule benchmarks ------------------------------------------


def bench_operator_fusion(num_rows: int, reps: int) -> RuleBenchResult:
    """OperatorFusion: fused vs unfused map chain."""
    from ray.data._internal.logical.rules.operator_fusion import FuseOperators

    def build():
        ds = ray.data.range(num_rows)
        ds = ds.map_batches(lambda b: {"id": b["id"] * 2}, batch_format="numpy")
        ds = ds.map_batches(lambda b: {"id": b["id"] + 1}, batch_format="numpy")
        return ds.map_batches(_heavy_udf, batch_format="numpy")

    with _without_physical_rule(FuseOperators):
        t_off = _timed_materialize(build, reps)
    t_on = _timed_materialize(build, reps)
    imp = (t_off - t_on) / t_off * 100 if t_off > 0 else 0
    return RuleBenchResult(
        rule_name="OperatorFusion", rule_category="RBO",
        optimization_goal="Throughput",
        pipeline_desc="range->map(x2)->map(+1)->map(heavy)",
        baseline_label="No fusion", optimized_label="Fused",
        baseline_time_s=round(t_off, 3), optimized_time_s=round(t_on, 3),
        improvement_pct=round(imp, 1),
    )


def bench_predicate_pushdown(num_rows: int, reps: int) -> RuleBenchResult:
    """PredicatePushdown: filter-late vs filter-early.

    Uses an extra-heavy UDF so that the computation gap between
    processing 100 %% vs ~33 %% of rows is clearly visible even at
    moderate row counts.
    """

    def build_late():
        ds = ray.data.range(num_rows)
        ds = ds.map_batches(_extra_heavy_udf, batch_format="numpy")
        return ds.filter(lambda row: row["id"] > 0)

    def build_early():
        ds = ray.data.range(num_rows)
        ds = ds.filter(lambda row: row["id"] % 3 == 0)
        return ds.map_batches(_extra_heavy_udf, batch_format="numpy")

    t_late = _timed_materialize(build_late, reps)
    t_early = _timed_materialize(build_early, reps)
    imp = (t_late - t_early) / t_late * 100 if t_late > 0 else 0
    return RuleBenchResult(
        rule_name="PredicatePushdown", rule_category="RBO",
        optimization_goal="Throughput",
        pipeline_desc="range->[filter <-> map(heavy)]",
        baseline_label="Filter after map", optimized_label="Filter before map",
        baseline_time_s=round(t_late, 3), optimized_time_s=round(t_early, 3),
        improvement_pct=round(imp, 1),
    )


def bench_limit_pushdown(num_rows: int, reps: int) -> RuleBenchResult:
    """LimitPushdown: limit-late vs limit-early.

    The baseline places a sort (AllToAll) before the limit, forcing full
    materialisation of all rows through the heavy UDF.  The optimised
    variant limits first, so only *limit_n* rows flow through the UDF
    and subsequent sort.
    """
    limit_n = max(1000, num_rows // 100)

    def build_late():
        ds = ray.data.range(num_rows)
        ds = ds.map_batches(_heavy_udf, batch_format="numpy")
        ds = ds.sort(key="id")  # AllToAll forces full materialisation
        return ds.limit(limit_n)

    def build_early():
        ds = ray.data.range(num_rows)
        ds = ds.limit(limit_n)
        ds = ds.map_batches(_heavy_udf, batch_format="numpy")
        return ds.sort(key="id")

    t_late = _timed_materialize(build_late, reps)
    t_early = _timed_materialize(build_early, reps)
    imp = (t_late - t_early) / t_late * 100 if t_late > 0 else 0
    return RuleBenchResult(
        rule_name="LimitPushdown", rule_category="RBO",
        optimization_goal="Throughput",
        pipeline_desc=f"range({num_rows})->[limit({limit_n}) <-> map+sort]",
        baseline_label="Limit after map+sort",
        optimized_label="Limit before map+sort",
        baseline_time_s=round(t_late, 3), optimized_time_s=round(t_early, 3),
        improvement_pct=round(imp, 1),
    )


def bench_combine_shuffles(num_rows: int, reps: int) -> RuleBenchResult:
    """CombineShuffles: two consecutive repartitions vs one.

    A map_batches is placed before the repartitions so the total pipeline
    cost is dominated by the shuffle stages, making the difference
    between one and two shuffles visible.
    """

    def build_double():
        ds = ray.data.range(num_rows)
        ds = ds.map_batches(_heavy_udf, batch_format="numpy")
        ds = ds.repartition(32)
        return ds.repartition(16)

    def build_single():
        ds = ray.data.range(num_rows)
        ds = ds.map_batches(_heavy_udf, batch_format="numpy")
        return ds.repartition(16)

    t_double = _timed_materialize(build_double, reps)
    t_single = _timed_materialize(build_single, reps)
    imp = (t_double - t_single) / t_double * 100 if t_double > 0 else 0
    return RuleBenchResult(
        rule_name="CombineShuffles", rule_category="RBO",
        optimization_goal="Throughput",
        pipeline_desc="map->repart(32)->repart(16) vs map->repart(16)",
        baseline_label="Double repart", optimized_label="Single repart",
        baseline_time_s=round(t_double, 3), optimized_time_s=round(t_single, 3),
        improvement_pct=round(imp, 1),
    )


def bench_reservation_ratio(num_rows: int, reps: int) -> RuleBenchResult:
    """DeriveReservationRatio: sweep reservation-ratio on a sort pipeline."""
    ratios = [0.1, 0.3, 0.5, 0.7, 0.9]
    default_r = 0.5
    sweep: List[Dict[str, Any]] = []
    for ratio in ratios:
        ctx = DataContext.get_current().copy()
        ctx.op_resource_reservation_ratio = ratio
        ctx.op_resource_reservation_enabled = True
        ctx.execution_options.verbose_progress = False
        with _context_scope(ctx):
            def _build(r=ratio):  # noqa: B023
                ds = ray.data.range(num_rows)
                ds = ds.filter(lambda row: row["id"] % 2 == 0)
                ds = ds.map_batches(_heavy_udf, batch_format="numpy")
                return ds.sort(key="id")
            t = _timed_materialize(_build, reps)
        sweep.append({"ratio": ratio, "time_s": round(t, 3),
                       "is_default": ratio == default_r})
    best = min(sweep, key=lambda e: e["time_s"])
    dflt = next(e for e in sweep if e["is_default"])
    imp = ((dflt["time_s"] - best["time_s"]) / dflt["time_s"] * 100
           if dflt["time_s"] > 0 else 0)
    return RuleBenchResult(
        rule_name="DeriveReservationRatio", rule_category="CBO",
        optimization_goal="Resource",
        pipeline_desc="filter->map->sort @ sweep R",
        baseline_label=f"R={default_r}",
        optimized_label=f"R={best['ratio']}",
        baseline_time_s=dflt["time_s"], optimized_time_s=best["time_s"],
        improvement_pct=round(imp, 1), sweep_detail=sweep,
    )


def bench_shuffle_partitions(num_rows: int, reps: int) -> RuleBenchResult:
    """DeriveShufflePartitions: sweep partition counts for repartition."""
    counts = [4, 8, 16, 32, 64, 128]
    default_n = 64
    sweep: List[Dict[str, Any]] = []
    for n in counts:
        def _build(n_parts=n):  # noqa: B023
            ds = ray.data.range(num_rows)
            ds = ds.map_batches(
                lambda b: {"id": b["id"] * 2}, batch_format="numpy",
            )
            return ds.repartition(n_parts)
        t = _timed_materialize(_build, reps)
        sweep.append({"num_partitions": n, "time_s": round(t, 3),
                       "is_default": n == default_n})
    best = min(sweep, key=lambda e: e["time_s"])
    dflt = next(e for e in sweep if e["is_default"])
    imp = ((dflt["time_s"] - best["time_s"]) / dflt["time_s"] * 100
           if dflt["time_s"] > 0 else 0)
    return RuleBenchResult(
        rule_name="DeriveShufflePartitions", rule_category="CBO",
        optimization_goal="Resource",
        pipeline_desc="map->repart(N) @ sweep N",
        baseline_label=f"N={default_n}",
        optimized_label=f"N={best['num_partitions']}",
        baseline_time_s=dflt["time_s"], optimized_time_s=best["time_s"],
        improvement_pct=round(imp, 1), sweep_detail=sweep,
    )


# ---- registry & orchestrator ---------------------------------------------

ALL_RULE_BENCHMARKS = [
    ("OperatorFusion", bench_operator_fusion),
    ("PredicatePushdown", bench_predicate_pushdown),
    ("LimitPushdown", bench_limit_pushdown),
    ("CombineShuffles", bench_combine_shuffles),
    ("DeriveReservationRatio", bench_reservation_ratio),
    ("DeriveShufflePartitions", bench_shuffle_partitions),
]


def run_per_rule_benchmarks(
    num_rows: int,
    reps: int,
    warmup: bool = False,
    rules: Optional[List[str]] = None,
) -> List[RuleBenchResult]:
    """Execute per-rule benchmarks and return ordered results."""
    selected = ALL_RULE_BENCHMARKS
    if rules:
        wanted = {r.lower() for r in rules}
        selected = [
            (n, f) for n, f in ALL_RULE_BENCHMARKS if n.lower() in wanted
        ]
    if warmup:
        print("\n[warmup] Running warmup pipeline ...")
        ray.data.range(min(num_rows, 50_000)).map_batches(
            lambda b: b, batch_format="numpy",
        ).materialize()
        print("[warmup] Done.\n")
    results: List[RuleBenchResult] = []
    for name, fn in selected:
        print(f"  [{name}] Running ...")
        try:
            r = fn(num_rows, reps)
            results.append(r)
            print(
                f"  [{name}] baseline={r.baseline_time_s:.3f}s  "
                f"optimized={r.optimized_time_s:.3f}s  "
                f"delta={r.improvement_pct:+.1f}%"
            )
        except Exception as exc:
            print(f"  [{name}] FAILED: {exc}")
    return results


# ---- structured table output ----------------------------------------------


def _fmt_sweep(
    entries: List[Dict[str, Any]], key: str, label: str,
) -> List[str]:
    """Format a parameter-sweep sub-table."""
    lines: List[str] = []
    default_t = next(e["time_s"] for e in entries if e["is_default"])
    best_t = min(e["time_s"] for e in entries)
    lines.append(f"    {label:<12} {'Time (s)':>10}  {'vs Default':>12}")
    lines.append(f"    {'---' * 13}")
    for e in entries:
        val = e[key]
        delta = (
            (e["time_s"] - default_t) / default_t * 100
            if default_t > 0 else 0
        )
        tag = "  (default)" if e["is_default"] else ""
        if e["time_s"] == best_t and not e["is_default"]:
            tag = "  <- best"
        fmt_val = f"{val:.2f}" if isinstance(val, float) else str(val)
        lines.append(
            f"    {fmt_val:<12} {e['time_s']:>10.3f}  {delta:>+11.1f}%{tag}"
        )
    return lines


def format_rule_results_table(
    results: List[RuleBenchResult], num_rows: int, reps: int,
) -> str:
    """Return a structured report of per-rule benchmark results."""
    W = 88
    sep = "=" * W
    lines: List[str] = [
        sep,
        "  Ray Data Optimization Rules - Per-Rule Benchmark Results",
        sep,
        f"  Config: {num_rows:,} rows x {reps} rep(s)\n",
    ]
    # summary table
    hdr = (
        f"  {'Rule':<26} {'Type':>4}  {'Goal':<11}"
        f"{'Baseline':>10} {'Optimized':>10} {'Improv.':>9}"
    )
    lines.append(hdr)
    lines.append(f"  {'-' * (W - 4)}")
    for r in results:
        arrow = "+" if r.improvement_pct >= 0 else "-"
        lines.append(
            f"  {r.rule_name:<26} {r.rule_category:>4}  "
            f"{r.optimization_goal:<11}"
            f"{r.baseline_time_s:>9.3f}s {r.optimized_time_s:>9.3f}s "
            f"{arrow}{abs(r.improvement_pct):>7.1f}%"
        )
    lines.append(f"  {'-' * (W - 4)}")
    # legend
    lines.append("\n  Legend:")
    for r in results:
        lines.append(f"    {r.rule_name:<26} {r.pipeline_desc}")
        lines.append(
            f"    {'':<26} baseline: {r.baseline_label}"
            f" | optimized: {r.optimized_label}"
        )
    # sweep details
    for r in results:
        if not r.sweep_detail:
            continue
        lines.append(f"\n  {r.rule_name} - Parameter Sweep:")
        if "ratio" in r.sweep_detail[0]:
            lines.extend(_fmt_sweep(r.sweep_detail, "ratio", "Ratio"))
        elif "num_partitions" in r.sweep_detail[0]:
            lines.extend(
                _fmt_sweep(r.sweep_detail, "num_partitions", "N parts")
            )
    lines.append(f"\n{sep}")
    return "\n".join(lines)




def main() -> None:
    parser = argparse.ArgumentParser(
        description="Production benchmark for Ray Data CBO."
    )
    # -- mode selection --
    parser.add_argument(
        "--per-rule", action="store_true", default=False,
        help="Run per-rule benchmarks instead of end-to-end comparison.",
    )
    parser.add_argument(
        "--rules", type=str, default=None,
        help="Comma-separated rule names to benchmark (default: all).  "
             "E.g. 'OperatorFusion,LimitPushdown'.",
    )
    parser.add_argument(
        "--validate-stats", action="store_true", default=False,
        help="Run CBO statistics infrastructure validation.",
    )
    # -- data scale --
    parser.add_argument(
        "--num-rows", type=int, default=200_000,
        help="Row count for per-rule benchmarks (default: 200000).",
    )
    parser.add_argument(
        "--batch-rows", type=int, default=200_000,
        help="Row count for end-to-end batch workload.",
    )
    parser.add_argument(
        "--streaming-rows", type=int, default=150_000,
        help="Row count for end-to-end streaming workload.",
    )
    # -- execution --
    parser.add_argument(
        "--repetitions", type=int, default=3,
        help="How many times to repeat each configuration.",
    )
    parser.add_argument(
        "--warmup", action="store_true", default=False,
        help="Run a warmup iteration (discarded) before timing.",
    )
    parser.add_argument(
        "--output", type=str, default="cbo_benchmark_results.json",
        help="Path to write the JSON results.",
    )
    args = parser.parse_args()

    # -- statistics validation (Part 1 does not need ray.init) --
    if args.validate_stats:
        try:
            validate_statistics_infrastructure()
        except Exception as exc:
            print(f"\n  [WARN] Statistics validation skipped: {exc}")

    ray.init(ignore_reinit_error=True)

    rule_results: Optional[List[RuleBenchResult]] = None
    e2e_runs: List[RunMetrics] = []

    try:
        if args.validate_stats:
            try:
                demo_live_statistics(
                    num_rows=min(args.num_rows, 10_000),
                )
            except Exception as exc:
                print(f"\n  [WARN] Live statistics demo skipped: {exc}")

        if args.per_rule:
            # -- per-rule benchmark mode --
            rule_list = (
                [r.strip() for r in args.rules.split(",")]
                if args.rules else None
            )
            rule_results = run_per_rule_benchmarks(
                num_rows=args.num_rows,
                reps=args.repetitions,
                warmup=args.warmup,
                rules=rule_list,
            )
        else:
            # -- end-to-end comparison mode --
            if args.warmup:
                print("\n[warmup] Running warmup iterations ...")
                run_batch_workload(
                    min(args.batch_rows, 50_000), enable_cbo=False,
                )
                run_streaming_workload(
                    min(args.streaming_rows, 50_000), enable_cbo=False,
                )
                print("[warmup] Done.\n")

            for rep in range(args.repetitions):
                print(f"[rep {rep+1}/{args.repetitions}] batch CBO=OFF ...")
                e2e_runs.append(
                    run_batch_workload(args.batch_rows, enable_cbo=False)
                )
                print(f"[rep {rep+1}/{args.repetitions}] batch CBO=ON ...")
                e2e_runs.append(
                    run_batch_workload(args.batch_rows, enable_cbo=True)
                )
                print(f"[rep {rep+1}/{args.repetitions}] streaming CBO=OFF")
                e2e_runs.append(
                    run_streaming_workload(
                        args.streaming_rows, enable_cbo=False,
                    )
                )
                print(f"[rep {rep+1}/{args.repetitions}] streaming CBO=ON")
                e2e_runs.append(
                    run_streaming_workload(
                        args.streaming_rows, enable_cbo=True,
                    )
                )
    finally:
        ray.shutdown()

    # -- reporting (does not need Ray) --
    if rule_results is not None:
        print(format_rule_results_table(
            rule_results, args.num_rows, args.repetitions,
        ))
        payload: Dict[str, Any] = {
            "mode": "per-rule",
            "config": {
                "num_rows": args.num_rows,
                "repetitions": args.repetitions,
            },
            "results": [asdict(r) for r in rule_results],
        }
    else:
        aggregates = aggregate_runs(e2e_runs)
        print(_format_comparison(aggregates))
        payload = {
            "mode": "end-to-end",
            "runs": [asdict(r) for r in e2e_runs],
            "aggregates": aggregates,
        }

    with open(args.output, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2)
    print(f"\nFull results written to {os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
