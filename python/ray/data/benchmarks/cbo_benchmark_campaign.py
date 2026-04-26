from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from ray.data.benchmarks import cbo_benchmark_harness as benchmark_harness
except ImportError:
    import cbo_benchmark_harness as benchmark_harness  # type: ignore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)


def _resolve_path(path: Optional[str], base_dir: str) -> Optional[str]:
    if not path:
        return None
    if os.path.isabs(path):
        return path
    return os.path.abspath(os.path.join(base_dir, path))


def _join_csv(values: Sequence[Any]) -> str:
    return ",".join(str(value) for value in values)


@dataclass
class CampaignTask:
    task_id: str
    experiment_type: str
    stage: str
    description: str
    command: List[str]
    output_path: str
    log_path: str
    dependency_ids: List[str] = field(default_factory=list)
    cwd: Optional[str] = None
    resume_skip_if_output_exists: bool = True


def build_campaign_spec_template() -> Dict[str, Any]:
    return {
        "campaign_name": "cbo_phase6_campaign",
        "output_dir": "./cbo_campaign_output",
        "report_title": "Ray Data CBO Benchmark Campaign",
        "environment_metadata_json": None,
        "working_dir": None,
        "python_executable": sys.executable,
        "benchmark_script": None,
        "feedback_alpha": 0.7,
        "feedback_tolerance": 0.02,
        "feedback_max_iterations": 5,
        "executor": {
            "max_parallelism": 4,
            "local_max_parallelism": 1,
            "allow_local_fallback_for_remote_stages": True,
            "state_path": "./cbo_campaign_output/execution_state.json",
            "remote_targets": [],
        },
        "experiments": {
            "r_value_matrix": {
                "enabled": True,
                "row_scales": [200000, 500000],
                "operator_counts": [2, 4, 6],
                "sweep_ratios": [0.1, 0.3, 0.5, 0.7, 0.9],
                "repetitions": 2,
                "subtask_count": 2,
            },
            "memory_profile": {
                "enabled": True,
                "profile_preset": "10gb_10m",
                "operator_counts": [2, 4, 6],
                "repetitions": 1,
                "subtask_count": 2,
                "memory_poll_interval_s": 0.1,
            },
            "per_rule": {
                "enabled": False,
                "num_rows": 200000,
                "repetitions": 2,
                "warmup": False,
                "rules": [],
            },
            "end_to_end": {
                "enabled": False,
                "batch_rows": 200000,
                "streaming_rows": 150000,
                "repetitions": 2,
                "warmup": False,
            },
        },
    }


def load_campaign_spec_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fp:
        payload = json.load(fp)
    if not isinstance(payload, dict):
        raise ValueError(f"Campaign spec must be a JSON object, got {type(payload)!r}")
    payload["__spec_path__"] = os.path.abspath(path)
    payload["__spec_dir__"] = os.path.dirname(os.path.abspath(path))
    return payload


def write_campaign_spec_template(path: str) -> str:
    _ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(build_campaign_spec_template(), fp, indent=2)
    return path


def _normalize_plan_context(
    spec: Dict[str, Any],
    script_path: Optional[str] = None,
    python_executable: Optional[str] = None,
) -> Dict[str, Any]:
    spec_dir = spec.get("__spec_dir__") or os.getcwd()
    output_dir = _resolve_path(spec.get("output_dir"), spec_dir)
    if not output_dir:
        raise ValueError("campaign spec must provide output_dir")
    resolved_script_path = script_path or spec.get("benchmark_script") or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "cbo_benchmark.py")
    )
    if not os.path.isabs(resolved_script_path):
        resolved_script_path = os.path.abspath(os.path.join(spec_dir, resolved_script_path))

    resolved_python = python_executable or spec.get("python_executable") or sys.executable
    working_dir = _resolve_path(spec.get("working_dir"), spec_dir) or os.getcwd()
    environment_metadata_json = _resolve_path(
        spec.get("environment_metadata_json"),
        spec_dir,
    )
    return {
        "spec_dir": spec_dir,
        "output_dir": output_dir,
        "script_path": resolved_script_path,
        "python_executable": resolved_python,
        "working_dir": working_dir,
        "environment_metadata_json": environment_metadata_json,
        "feedback_alpha": spec.get("feedback_alpha", 0.7),
        "feedback_tolerance": spec.get("feedback_tolerance", 0.02),
        "feedback_max_iterations": spec.get("feedback_max_iterations", 5),
    }


def _normalize_executor_config(
    spec: Dict[str, Any],
    context: Dict[str, Any],
) -> Dict[str, Any]:
    raw_executor = dict(spec.get("executor") or {})
    max_parallelism = int(raw_executor.get("max_parallelism", 1))
    local_max_parallelism = int(
        raw_executor.get("local_max_parallelism", max_parallelism)
    )
    if max_parallelism <= 0 or local_max_parallelism <= 0:
        raise ValueError("executor parallelism values must be positive")
    remote_targets: List[Dict[str, Any]] = []
    for index, target in enumerate(raw_executor.get("remote_targets") or []):
        if not isinstance(target, dict):
            raise ValueError("executor.remote_targets entries must be objects")
        name = str(target.get("name") or f"remote-{index}")
        capacity = int(target.get("capacity", 1))
        if capacity <= 0:
            raise ValueError(f"executor.remote_targets[{index}].capacity must be positive")
        command_template = target.get("command_template") or []
        if not isinstance(command_template, list) or not command_template:
            raise ValueError(
                f"executor.remote_targets[{index}].command_template must be a non-empty list"
            )
        stages = [str(stage) for stage in (target.get("stages") or ["run_shard"])]
        remote_targets.append(
            {
                "name": name,
                "capacity": capacity,
                "stages": stages,
                "command_template": [str(part) for part in command_template],
            }
        )

    state_path = _resolve_path(
        raw_executor.get("state_path")
        or os.path.join(context["output_dir"], "execution_state.json"),
        context["spec_dir"],
    )
    return {
        "max_parallelism": max_parallelism,
        "local_max_parallelism": local_max_parallelism,
        "allow_local_fallback_for_remote_stages": bool(
            raw_executor.get("allow_local_fallback_for_remote_stages", True)
        ),
        "remote_targets": remote_targets,
        "state_path": state_path,
    }


def _plan_file(path: str, payload: Any) -> Dict[str, Any]:
    return {"path": path, "payload": payload}


def _subtask_file_name(subtask_index: int, subtask_count: int) -> str:
    width = max(3, len(str(max(subtask_count - 1, 0))))
    return f"shard_{subtask_index:0{width}d}_of_{subtask_count}.json"


def _build_base_command(context: Dict[str, Any]) -> List[str]:
    return [context["python_executable"], context["script_path"]]


def _stringify_optional_bool_flag(command: List[str], flag: str, enabled: bool) -> None:
    if enabled:
        command.append(flag)


def _extend_common_finalize_flags(
    command: List[str],
    experiment_dir: str,
    context: Dict[str, Any],
    report_title: Optional[str],
) -> None:
    if report_title:
        command.extend(["--report-title", report_title])
    if context["environment_metadata_json"]:
        command.extend(
            ["--environment-metadata-json", context["environment_metadata_json"]]
        )
    command.extend(
        [
            "--feedback-alpha",
            str(context["feedback_alpha"]),
            "--feedback-tolerance",
            str(context["feedback_tolerance"]),
            "--feedback-max-iterations",
            str(context["feedback_max_iterations"]),
            "--report-bundle-dir",
            os.path.join(experiment_dir, "bundle"),
        ]
    )


def _build_r_value_matrix_plan(
    spec: Dict[str, Any],
    context: Dict[str, Any],
    report_title: Optional[str],
) -> Dict[str, Any]:
    experiment_type = "r_value_matrix"
    experiment_dir = os.path.join(context["output_dir"], experiment_type)
    shard_dir = os.path.join(experiment_dir, "shards")
    log_dir = os.path.join(experiment_dir, "logs")
    manifest_path = os.path.join(experiment_dir, "manifest.json")
    input_results_path = os.path.join(experiment_dir, "input_results.json")
    merged_output_path = os.path.join(experiment_dir, "merged.json")
    bundle_manifest_path = os.path.join(experiment_dir, "bundle", "bundle_manifest.json")

    row_scales = list(spec.get("row_scales") or [])
    operator_counts = list(spec.get("operator_counts") or [])
    sweep_ratios = list(spec.get("sweep_ratios") or [])
    repetitions = int(spec.get("repetitions", 1))
    subtask_count = int(spec.get("subtask_count", 1))
    input_results = spec.get("input_results")
    manifest_override = spec.get("manifest_path")

    support_files: List[Dict[str, Any]] = []
    run_tasks: List[CampaignTask] = []
    dependency_ids: List[str] = []

    if input_results:
        resolved_input_results = [
            _resolve_path(path, context["spec_dir"]) for path in input_results
        ]
        resolved_manifest_path = _resolve_path(manifest_override, context["spec_dir"])
    else:
        if not row_scales or not operator_counts or not sweep_ratios:
            raise ValueError(
                "r_value_matrix campaign spec requires row_scales, "
                "operator_counts, and sweep_ratios unless input_results is provided"
            )
        workloads = benchmark_harness.build_parameterized_workloads(
            row_counts=row_scales,
            operator_counts=operator_counts,
            payload_bytes_per_row=0,
        )
        manifest_payload = {
            experiment_type: [
                asdict(item)
                for item in benchmark_harness.build_parameterized_subtask_manifest(
                    workloads,
                    subtask_count=subtask_count,
                    experiment_type=experiment_type,
                )
            ]
        }
        support_files.append(_plan_file(manifest_path, manifest_payload))
        resolved_manifest_path = manifest_path
        resolved_input_results = []
        for subtask_index in range(subtask_count):
            output_path = os.path.join(
                shard_dir, _subtask_file_name(subtask_index, subtask_count)
            )
            log_path = os.path.join(
                log_dir,
                f"run_shard_{subtask_index:03d}.log",
            )
            command = _build_base_command(context)
            command.extend(
                [
                    "--r-value-matrix",
                    "--matrix-row-scales",
                    _join_csv(row_scales),
                    "--matrix-operator-counts",
                    _join_csv(operator_counts),
                    "--matrix-ratios",
                    _join_csv(sweep_ratios),
                    "--repetitions",
                    str(repetitions),
                    "--subtask-count",
                    str(subtask_count),
                    "--subtask-index",
                    str(subtask_index),
                    "--output",
                    output_path,
                ]
            )
            task = CampaignTask(
                task_id=f"{experiment_type}.run.{subtask_index}",
                experiment_type=experiment_type,
                stage="run_shard",
                description=(
                    f"Run {experiment_type} shard {subtask_index}/{subtask_count}"
                ),
                command=command,
                output_path=output_path,
                log_path=log_path,
                cwd=context["working_dir"],
            )
            run_tasks.append(task)
            dependency_ids.append(task.task_id)
            resolved_input_results.append(output_path)

    support_files.append(_plan_file(input_results_path, resolved_input_results))
    finalize_command = _build_base_command(context)
    finalize_command.extend(
        [
            "--merge-results",
            "--input-results-file",
            input_results_path,
            "--output",
            merged_output_path,
        ]
    )
    if resolved_manifest_path:
        finalize_command.extend(["--merge-manifest", resolved_manifest_path])
    _extend_common_finalize_flags(
        finalize_command,
        experiment_dir,
        context,
        report_title,
    )
    finalize_task = CampaignTask(
        task_id=f"{experiment_type}.finalize",
        experiment_type=experiment_type,
        stage="finalize",
        description=f"Merge and bundle {experiment_type}",
        command=finalize_command,
        output_path=bundle_manifest_path,
        log_path=os.path.join(log_dir, "finalize.log"),
        dependency_ids=dependency_ids,
        cwd=context["working_dir"],
    )
    return {
        "tasks": [asdict(task) for task in [*run_tasks, finalize_task]],
        "support_files": support_files,
    }


def _build_memory_profile_plan(
    spec: Dict[str, Any],
    context: Dict[str, Any],
    report_title: Optional[str],
) -> Dict[str, Any]:
    experiment_type = "memory_profile"
    experiment_dir = os.path.join(context["output_dir"], experiment_type)
    shard_dir = os.path.join(experiment_dir, "shards")
    log_dir = os.path.join(experiment_dir, "logs")
    manifest_path = os.path.join(experiment_dir, "manifest.json")
    input_results_path = os.path.join(experiment_dir, "input_results.json")
    merged_output_path = os.path.join(experiment_dir, "merged.json")
    bundle_manifest_path = os.path.join(experiment_dir, "bundle", "bundle_manifest.json")

    repetitions = int(spec.get("repetitions", 1))
    subtask_count = int(spec.get("subtask_count", 1))
    input_results = spec.get("input_results")
    manifest_override = spec.get("manifest_path")
    memory_poll_interval_s = float(spec.get("memory_poll_interval_s", 0.1))

    support_files: List[Dict[str, Any]] = []
    run_tasks: List[CampaignTask] = []
    dependency_ids: List[str] = []

    if input_results:
        resolved_input_results = [
            _resolve_path(path, context["spec_dir"]) for path in input_results
        ]
        resolved_manifest_path = _resolve_path(manifest_override, context["spec_dir"])
    else:
        operator_counts = list(spec.get("operator_counts") or [])
        if not operator_counts:
            raise ValueError(
                "memory_profile campaign spec requires operator_counts "
                "unless input_results is provided"
            )
        if spec.get("profile_preset"):
            profile_inputs = benchmark_harness.resolve_memory_profile_preset(
                str(spec["profile_preset"]),
                operator_counts=operator_counts,
            )
        else:
            row_scales = list(spec.get("row_scales") or [])
            if not row_scales:
                raise ValueError(
                    "memory_profile campaign spec requires row_scales "
                    "when profile_preset is not provided"
                )
            profile_inputs = {
                "preset_name": None,
                "row_counts": row_scales,
                "operator_counts": operator_counts,
                "payload_bytes_per_row": int(spec["payload_bytes_per_row"]),
            }
        memory_plan = benchmark_harness.build_memory_profile_plan(
            row_counts=profile_inputs["row_counts"],
            operator_counts=profile_inputs["operator_counts"],
            repetitions=repetitions,
            payload_bytes_per_row=profile_inputs["payload_bytes_per_row"],
        )
        manifest_payload = {
            experiment_type: [
                asdict(item)
                for item in benchmark_harness.build_memory_profile_subtask_manifest(
                    memory_plan,
                    subtask_count=subtask_count,
                )
            ]
        }
        support_files.append(_plan_file(manifest_path, manifest_payload))
        resolved_manifest_path = manifest_path
        resolved_input_results = []

        for subtask_index in range(subtask_count):
            output_path = os.path.join(
                shard_dir, _subtask_file_name(subtask_index, subtask_count)
            )
            log_path = os.path.join(
                log_dir,
                f"run_shard_{subtask_index:03d}.log",
            )
            command = _build_base_command(context)
            command.extend(
                [
                    "--memory-profile",
                    "--profile-operator-counts",
                    _join_csv(profile_inputs["operator_counts"]),
                    "--repetitions",
                    str(repetitions),
                    "--subtask-count",
                    str(subtask_count),
                    "--subtask-index",
                    str(subtask_index),
                    "--memory-poll-interval-s",
                    str(memory_poll_interval_s),
                    "--output",
                    output_path,
                ]
            )
            if profile_inputs.get("preset_name"):
                command.extend(["--profile-preset", profile_inputs["preset_name"]])
            else:
                command.extend(
                    [
                        "--profile-row-scales",
                        _join_csv(profile_inputs["row_counts"]),
                        "--payload-bytes-per-row",
                        str(profile_inputs["payload_bytes_per_row"]),
                    ]
                )
            task = CampaignTask(
                task_id=f"{experiment_type}.run.{subtask_index}",
                experiment_type=experiment_type,
                stage="run_shard",
                description=(
                    f"Run {experiment_type} shard {subtask_index}/{subtask_count}"
                ),
                command=command,
                output_path=output_path,
                log_path=log_path,
                cwd=context["working_dir"],
            )
            run_tasks.append(task)
            dependency_ids.append(task.task_id)
            resolved_input_results.append(output_path)

    support_files.append(_plan_file(input_results_path, resolved_input_results))
    finalize_command = _build_base_command(context)
    finalize_command.extend(
        [
            "--merge-results",
            "--input-results-file",
            input_results_path,
            "--output",
            merged_output_path,
        ]
    )
    if resolved_manifest_path:
        finalize_command.extend(["--merge-manifest", resolved_manifest_path])
    _extend_common_finalize_flags(
        finalize_command,
        experiment_dir,
        context,
        report_title,
    )
    finalize_task = CampaignTask(
        task_id=f"{experiment_type}.finalize",
        experiment_type=experiment_type,
        stage="finalize",
        description=f"Merge and bundle {experiment_type}",
        command=finalize_command,
        output_path=bundle_manifest_path,
        log_path=os.path.join(log_dir, "finalize.log"),
        dependency_ids=dependency_ids,
        cwd=context["working_dir"],
    )
    return {
        "tasks": [asdict(task) for task in [*run_tasks, finalize_task]],
        "support_files": support_files,
    }


def _build_per_rule_plan(
    spec: Dict[str, Any],
    context: Dict[str, Any],
    report_title: Optional[str],
) -> Dict[str, Any]:
    experiment_type = "per_rule"
    experiment_dir = os.path.join(context["output_dir"], experiment_type)
    log_dir = os.path.join(experiment_dir, "logs")
    output_path = os.path.join(experiment_dir, "results.json")
    bundle_manifest_path = os.path.join(experiment_dir, "bundle", "bundle_manifest.json")
    command = _build_base_command(context)
    command.extend(
        [
            "--per-rule",
            "--num-rows",
            str(spec["num_rows"]),
            "--repetitions",
            str(spec["repetitions"]),
            "--output",
            output_path,
        ]
    )
    rules = spec.get("rules") or []
    if rules:
        command.extend(["--rules", _join_csv(rules)])
    _stringify_optional_bool_flag(command, "--warmup", bool(spec.get("warmup")))
    if report_title:
        command.extend(["--report-title", report_title])
    if context["environment_metadata_json"]:
        command.extend(
            ["--environment-metadata-json", context["environment_metadata_json"]]
        )
    command.extend(
        [
            "--report-bundle-dir",
            os.path.join(experiment_dir, "bundle"),
        ]
    )
    task = CampaignTask(
        task_id=f"{experiment_type}.run",
        experiment_type=experiment_type,
        stage="run",
        description="Run and bundle per_rule benchmark",
        command=command,
        output_path=bundle_manifest_path,
        log_path=os.path.join(log_dir, "run.log"),
        cwd=context["working_dir"],
    )
    return {"tasks": [asdict(task)], "support_files": []}


def _build_end_to_end_plan(
    spec: Dict[str, Any],
    context: Dict[str, Any],
    report_title: Optional[str],
) -> Dict[str, Any]:
    experiment_type = "end_to_end"
    experiment_dir = os.path.join(context["output_dir"], experiment_type)
    log_dir = os.path.join(experiment_dir, "logs")
    output_path = os.path.join(experiment_dir, "results.json")
    bundle_manifest_path = os.path.join(experiment_dir, "bundle", "bundle_manifest.json")
    command = _build_base_command(context)
    command.extend(
        [
            "--batch-rows",
            str(spec["batch_rows"]),
            "--streaming-rows",
            str(spec["streaming_rows"]),
            "--repetitions",
            str(spec["repetitions"]),
            "--output",
            output_path,
        ]
    )
    _stringify_optional_bool_flag(command, "--warmup", bool(spec.get("warmup")))
    if report_title:
        command.extend(["--report-title", report_title])
    if context["environment_metadata_json"]:
        command.extend(
            ["--environment-metadata-json", context["environment_metadata_json"]]
        )
    command.extend(
        [
            "--report-bundle-dir",
            os.path.join(experiment_dir, "bundle"),
        ]
    )
    task = CampaignTask(
        task_id=f"{experiment_type}.run",
        experiment_type=experiment_type,
        stage="run",
        description="Run and bundle end_to_end benchmark",
        command=command,
        output_path=bundle_manifest_path,
        log_path=os.path.join(log_dir, "run.log"),
        cwd=context["working_dir"],
    )
    return {"tasks": [asdict(task)], "support_files": []}


def build_campaign_plan(
    spec: Dict[str, Any],
    script_path: Optional[str] = None,
    python_executable: Optional[str] = None,
) -> Dict[str, Any]:
    context = _normalize_plan_context(
        spec,
        script_path=script_path,
        python_executable=python_executable,
    )
    executor = _normalize_executor_config(spec, context)
    campaign_name = spec.get("campaign_name") or "cbo_campaign"
    experiments = spec.get("experiments") or {}
    report_title = spec.get("report_title")
    tasks: List[Dict[str, Any]] = []
    support_files: List[Dict[str, Any]] = []

    builders = {
        "r_value_matrix": _build_r_value_matrix_plan,
        "memory_profile": _build_memory_profile_plan,
        "per_rule": _build_per_rule_plan,
        "end_to_end": _build_end_to_end_plan,
    }
    for experiment_type, builder in builders.items():
        experiment_spec = experiments.get(experiment_type) or {}
        if not experiment_spec.get("enabled"):
            continue
        partial = builder(experiment_spec, context, report_title)
        tasks.extend(partial["tasks"])
        support_files.extend(partial["support_files"])

    if not tasks:
        raise ValueError("campaign spec does not enable any experiment")

    support_files.append(
        _plan_file(
            os.path.join(context["output_dir"], "campaign_spec.snapshot.json"),
            {
                key: value
                for key, value in spec.items()
                if not key.startswith("__")
            },
        )
    )

    return {
        "campaign_name": campaign_name,
        "generated_at": _utc_now(),
        "output_dir": context["output_dir"],
        "working_dir": context["working_dir"],
        "script_path": context["script_path"],
        "python_executable": context["python_executable"],
        "environment_metadata_json": context["environment_metadata_json"],
        "executor": executor,
        "tasks": tasks,
        "support_files": support_files,
    }


def materialize_campaign_support_files(plan: Dict[str, Any]) -> List[str]:
    written: List[str] = []
    for entry in plan.get("support_files", []):
        path = entry["path"]
        _ensure_parent_dir(path)
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(entry["payload"], fp, indent=2)
        written.append(path)
    return written


def write_campaign_plan(path: str, plan: Dict[str, Any]) -> str:
    materialize_campaign_support_files(plan)
    _ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(plan, fp, indent=2)
    return path


def _task_output_exists(task: Dict[str, Any]) -> bool:
    return os.path.exists(task["output_path"])


def render_campaign_plan_summary(plan: Dict[str, Any]) -> str:
    executor = plan.get("executor") or {}
    lines = [
        f"Campaign: {plan['campaign_name']}",
        f"Output dir: {plan['output_dir']}",
        f"Task count: {len(plan.get('tasks', []))}",
        (
            "Executor: "
            f"max_parallelism={executor.get('max_parallelism', 1)}, "
            f"local_max_parallelism={executor.get('local_max_parallelism', 1)}, "
            f"remote_targets={len(executor.get('remote_targets') or [])}"
        ),
        "Tasks:",
    ]
    for task in plan.get("tasks", []):
        status = "completed" if _task_output_exists(task) else "pending"
        lines.append(
            f"  - {task['task_id']} [{task['stage']}] "
            f"{task['experiment_type']} -> {task['output_path']} ({status})"
        )
    return "\n".join(lines)


def _format_wrapped_command(
    task: Dict[str, Any],
    target: Optional[Dict[str, Any]],
) -> Tuple[List[str], str]:
    if target is None:
        command = list(task["command"])
        return command, shlex.join(command)

    rendered_command = shlex.join(task["command"])
    cwd = task.get("cwd") or os.getcwd()
    substitutions = {
        "command": rendered_command,
        "cwd": cwd,
        "task_id": task["task_id"],
        "output_path": task["output_path"],
        "log_path": task["log_path"],
        "target_name": target["name"],
    }
    command_template = list(target["command_template"])
    has_placeholder = any("{" in part and "}" in part for part in command_template)
    if has_placeholder:
        formatted = [part.format(**substitutions) for part in command_template]
        return formatted, shlex.join(formatted)
    wrapped = command_template + list(task["command"])
    return wrapped, shlex.join(wrapped)


def _stage_has_remote_target(executor: Dict[str, Any], stage: str) -> bool:
    for target in executor.get("remote_targets") or []:
        if stage in (target.get("stages") or []):
            return True
    return False


def _select_dispatch_target(
    task: Dict[str, Any],
    executor: Dict[str, Any],
    active_remote: Dict[str, int],
    active_local: int,
    round_robin_state: Dict[str, int],
) -> Optional[Dict[str, Any]]:
    stage = task["stage"]
    remote_targets = [
        target
        for target in (executor.get("remote_targets") or [])
        if stage in (target.get("stages") or [])
    ]
    if remote_targets:
        start = round_robin_state.get(stage, 0)
        for offset in range(len(remote_targets)):
            index = (start + offset) % len(remote_targets)
            target = remote_targets[index]
            if active_remote.get(target["name"], 0) < int(target["capacity"]):
                round_robin_state[stage] = (index + 1) % len(remote_targets)
                return target
        if not executor.get("allow_local_fallback_for_remote_stages", True):
            return None

    if active_local < int(executor.get("local_max_parallelism", 1)):
        return {
            "name": "local",
            "capacity": int(executor.get("local_max_parallelism", 1)),
            "stages": [],
            "command_template": [],
        }
    return None


def _build_initial_execution_state(plan: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "campaign_name": plan["campaign_name"],
        "generated_at": _utc_now(),
        "updated_at": _utc_now(),
        "state_path": (plan.get("executor") or {}).get("state_path"),
        "tasks": {
            task["task_id"]: {
                "status": "pending",
                "experiment_type": task["experiment_type"],
                "stage": task["stage"],
                "output_path": task["output_path"],
                "log_path": task["log_path"],
                "dependency_ids": list(task.get("dependency_ids", [])),
                "attempts": 0,
            }
            for task in plan.get("tasks", [])
        },
    }


def _write_execution_state(executor: Dict[str, Any], state: Dict[str, Any]) -> None:
    state["updated_at"] = _utc_now()
    state_path = executor.get("state_path")
    if not state_path:
        return
    _ensure_parent_dir(state_path)
    with open(state_path, "w", encoding="utf-8") as fp:
        json.dump(state, fp, indent=2)


def _run_campaign_task(
    task: Dict[str, Any],
    target: Dict[str, Any],
) -> Dict[str, Any]:
    command, launched_command = _format_wrapped_command(
        task,
        None if target.get("name") == "local" else target,
    )
    completed = subprocess.run(
        command,
        cwd=task.get("cwd") or None,
        capture_output=True,
        text=True,
        check=False,
    )
    output_text = completed.stdout or ""
    if completed.stderr:
        output_text += completed.stderr
    return {
        "returncode": completed.returncode,
        "output_text": output_text,
        "launched_command": launched_command,
        "assigned_target": target["name"],
    }


def execute_campaign_plan(
    plan: Dict[str, Any],
    resume: bool = True,
    dry_run: bool = False,
    log_fn=print,
) -> Dict[str, Any]:
    materialize_campaign_support_files(plan)
    executor = plan.get("executor") or {
        "max_parallelism": 1,
        "local_max_parallelism": 1,
        "remote_targets": [],
        "allow_local_fallback_for_remote_stages": True,
        "state_path": os.path.join(plan["output_dir"], "execution_state.json"),
    }
    task_results: List[Dict[str, Any]] = []
    tasks_by_id = {task["task_id"]: task for task in plan.get("tasks", [])}
    pending_ids = list(tasks_by_id)
    completed_ids: set[str] = set()
    submitted_ids: set[str] = set()
    active_local = 0
    active_remote: Dict[str, int] = {}
    round_robin_state: Dict[str, int] = {}
    state = _build_initial_execution_state(plan)
    state_lock = Lock()
    _write_execution_state(executor, state)

    def _update_task_state(
        task_id: str,
        status: str,
        **extra: Any,
    ) -> None:
        with state_lock:
            task_state = state["tasks"][task_id]
            task_state["status"] = status
            if status == "running":
                task_state["started_at"] = _utc_now()
                task_state["attempts"] = int(task_state.get("attempts", 0)) + 1
            if status in {"completed", "failed", "skipped_existing", "dry_run"}:
                task_state["finished_at"] = _utc_now()
            task_state.update(extra)
            _write_execution_state(executor, state)

    futures: Dict[Future, Tuple[str, Dict[str, Any]]] = {}
    max_workers = int(executor.get("max_parallelism", 1))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while len(completed_ids) < len(tasks_by_id):
            progress_made = False
            for task_id in pending_ids:
                if task_id in completed_ids or task_id in submitted_ids:
                    continue
                task = tasks_by_id[task_id]
                if any(dep not in completed_ids for dep in task.get("dependency_ids", [])):
                    continue

                output_exists = _task_output_exists(task)
                if resume and task.get("resume_skip_if_output_exists", True) and output_exists:
                    log_fn(f"[skip] {task['task_id']} output exists: {task['output_path']}")
                    task_results.append(
                        {
                            "task_id": task["task_id"],
                            "status": "skipped_existing",
                            "output_path": task["output_path"],
                        }
                    )
                    _update_task_state(
                        task["task_id"],
                        "skipped_existing",
                        output_path=task["output_path"],
                    )
                    completed_ids.add(task["task_id"])
                    progress_made = True
                    continue

                if dry_run:
                    display_command = _format_wrapped_command(task, None)[1]
                    log_fn(f"[dry-run] {task['task_id']}: {display_command}")
                    task_results.append(
                        {
                            "task_id": task["task_id"],
                            "status": "dry_run",
                            "output_path": task["output_path"],
                        }
                    )
                    _update_task_state(
                        task["task_id"],
                        "dry_run",
                        output_path=task["output_path"],
                    )
                    completed_ids.add(task["task_id"])
                    progress_made = True
                    continue

                dispatch_target = _select_dispatch_target(
                    task,
                    executor,
                    active_remote,
                    active_local,
                    round_robin_state,
                )
                if dispatch_target is None:
                    continue

                _ensure_parent_dir(task["log_path"])
                _ensure_parent_dir(task["output_path"])
                log_fn(f"[run] {task['task_id']} -> {dispatch_target['name']}")
                submitted_ids.add(task["task_id"])
                if dispatch_target["name"] == "local":
                    active_local += 1
                else:
                    active_remote[dispatch_target["name"]] = (
                        active_remote.get(dispatch_target["name"], 0) + 1
                    )
                launch_preview = _format_wrapped_command(
                    task,
                    None if dispatch_target["name"] == "local" else dispatch_target,
                )[1]
                _update_task_state(
                    task["task_id"],
                    "running",
                    assigned_target=dispatch_target["name"],
                    launched_command=launch_preview,
                )
                futures[
                    pool.submit(_run_campaign_task, task, dispatch_target)
                ] = (task["task_id"], dispatch_target)
                progress_made = True

            if futures:
                done, _ = wait(list(futures), return_when=FIRST_COMPLETED)
                for future in done:
                    task_id, dispatch_target = futures.pop(future)
                    task = tasks_by_id[task_id]
                    if dispatch_target["name"] == "local":
                        active_local = max(active_local - 1, 0)
                    else:
                        active_remote[dispatch_target["name"]] = max(
                            active_remote.get(dispatch_target["name"], 1) - 1,
                            0,
                        )
                    result = future.result()
                    with open(task["log_path"], "w", encoding="utf-8") as fp:
                        fp.write(result["output_text"])
                    if result["returncode"] != 0:
                        _update_task_state(
                            task_id,
                            "failed",
                            assigned_target=result["assigned_target"],
                            launched_command=result["launched_command"],
                            returncode=result["returncode"],
                            output_path=task["output_path"],
                            log_path=task["log_path"],
                        )
                        raise RuntimeError(
                            f"Task {task_id} failed with exit code {result['returncode']}. "
                            f"See log: {task['log_path']}"
                        )
                    task_results.append(
                        {
                            "task_id": task_id,
                            "status": "completed",
                            "output_path": task["output_path"],
                            "log_path": task["log_path"],
                            "assigned_target": result["assigned_target"],
                        }
                    )
                    _update_task_state(
                        task_id,
                        "completed",
                        assigned_target=result["assigned_target"],
                        launched_command=result["launched_command"],
                        returncode=result["returncode"],
                        output_path=task["output_path"],
                        log_path=task["log_path"],
                    )
                    completed_ids.add(task_id)
                    progress_made = True
                continue

            if not progress_made:
                remaining = [
                    task_id
                    for task_id in pending_ids
                    if task_id not in completed_ids and task_id not in submitted_ids
                ]
                raise RuntimeError(
                    "Campaign execution made no progress. Remaining tasks: "
                    + ", ".join(remaining)
                )

    return {
        "campaign_name": plan["campaign_name"],
        "executed_at": _utc_now(),
        "state_path": executor.get("state_path"),
        "task_results": task_results,
    }
