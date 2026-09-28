#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Rebuild an ``airflow.sdk.DAG`` from the serialized Dag that a Lang-SDK runtime sends."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

from airflow.sdk import DAG, BaseOperator, TaskGroup
from airflow.sdk._shared.module_loading import import_string
from airflow.sdk._shared.timezones.timezone import parse_timezone
from airflow.sdk.definitions.timetables.simple import ContinuousTimetable, NullTimetable, OnceTimetable
from airflow.sdk.definitions.timetables.trigger import CronTriggerTimetable

if TYPE_CHECKING:
    from pendulum.tz.timezone import FixedTimezone, Timezone

    from airflow.sdk.bases.timetable import BaseTimetable

_STUB_OPERATOR = "airflow.providers.standard.decorators.stub._StubOperator"

_DAG_KEYS = frozenset(
    {
        "catchup",
        "dag_display_name",
        "description",
        "disable_bundle_versioning",
        "doc_md",
        "fail_fast",
        "is_paused_upon_creation",
        "max_active_runs",
        "max_active_tasks",
        "max_consecutive_failed_dag_runs",
        "render_template_as_native_obj",
        "tags",
    }
)
_UNSUPPORTED_DAG_KEYS = ("params", "edge_info", "deadline", "allowed_run_types")

# The BaseOperator arguments a Lang-SDK task can set.
_OPERATOR_KEYS = frozenset(
    {
        "depends_on_past",
        "do_xcom_push",
        "doc_md",
        "email",
        "email_on_failure",
        "email_on_retry",
        "end_date",
        "execution_timeout",
        "executor",
        "ignore_first_depends_on_past",
        "map_index_template",
        "max_active_tis_per_dag",
        "max_active_tis_per_dagrun",
        "max_retry_delay",
        "owner",
        "pool",
        "pool_slots",
        "priority_weight",
        "queue",
        "retries",
        "retry_delay",
        "retry_exponential_backoff",
        "start_date",
        "trigger_rule",
        "wait_for_downstream",
        "wait_for_past_depends_before_skipping",
        "weight_rule",
    }
)
_TIMEDELTA_KEYS = frozenset({"execution_timeout", "max_retry_delay", "retry_delay"})
_DATETIME_KEYS = frozenset({"end_date", "start_date"})

# Keys the serializer derives from the operator class or the task graph, never constructor arguments.
_DERIVED_TASK_KEYS = frozenset(
    {
        "_arg_bindings",
        "_can_skip_downstream",
        "_is_empty",
        "_is_mapped",
        "_needs_expansion",
        "_operator_extra_links",
        "_operator_name",
        "_task_module",
        "downstream_task_ids",
        "has_retry_policy",
        "is_stub",
        "language",
        "python_callable_name",
        "task_id",
        "task_type",
        "template_ext",
        "template_fields",
        "template_fields_renderers",
        "ui_color",
        "ui_fgcolor",
    }
)


def _native_task(): ...


def _decode_datetime(
    value: float, tz: datetime.tzinfo | FixedTimezone | Timezone = datetime.timezone.utc
) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(value, tz=tz)


def _decode_timedelta(value: float) -> datetime.timedelta:
    return datetime.timedelta(seconds=value)


def _decode_timetable(encoded: dict[str, Any]) -> BaseTimetable:
    kind, var = encoded["__type"], encoded.get("__var") or {}
    if kind == "airflow.timetables.simple.NullTimetable":
        return NullTimetable()
    if kind == "airflow.timetables.simple.OnceTimetable":
        return OnceTimetable()
    if kind == "airflow.timetables.simple.ContinuousTimetable":
        return ContinuousTimetable()
    if kind == "airflow.timetables.trigger.CronTriggerTimetable":
        run_immediately = var.get("run_immediately", False)
        if not isinstance(run_immediately, bool):
            run_immediately = _decode_timedelta(run_immediately)
        return CronTriggerTimetable(
            var["expression"],
            timezone=var["timezone"],
            interval=_decode_timedelta(var.get("interval", 0)),
            run_immediately=run_immediately,
        )
    raise ValueError(f"the {kind} timetable is not supported")


def _build_task_groups(dag: DAG, encoded: dict[str, Any]) -> dict[str, TaskGroup]:
    """Build the Dag's task groups and return the group each task id belongs to."""
    groups: dict[str, TaskGroup] = {}

    def build(group_data: dict[str, Any], group: TaskGroup) -> None:
        group.upstream_group_ids.update(group_data.get("upstream_group_ids", []))
        group.downstream_group_ids.update(group_data.get("downstream_group_ids", []))
        group.upstream_task_ids.update(group_data.get("upstream_task_ids", []))
        group.downstream_task_ids.update(group_data.get("downstream_task_ids", []))
        for kind, child in group_data["children"].values():
            if kind == "operator":
                groups[child] = group
                continue
            build(
                child,
                TaskGroup(
                    group_id=child["_group_id"],
                    parent_group=group,
                    dag=dag,
                    prefix_group_id=child.get("prefix_group_id", True),
                    tooltip=child.get("tooltip", ""),
                    ui_color=child.get("ui_color", "CornflowerBlue"),
                    ui_fgcolor=child.get("ui_fgcolor", "#000"),
                    group_display_name=child.get("group_display_name", ""),
                ),
            )

    build(encoded, dag.task_group)
    return groups


def _get_local_task_id(task_id: str, group: TaskGroup) -> str:
    prefix = f"{group.group_id}."
    if group.group_id and group.prefix_group_id and task_id.startswith(prefix):
        return task_id[len(prefix) :]
    return task_id


def _decode_operator_kwargs(var: dict[str, Any]) -> dict[str, Any]:
    kwargs = {}
    for key in _OPERATOR_KEYS & var.keys():
        value = var[key]
        if value is not None and key in _TIMEDELTA_KEYS:
            value = _decode_timedelta(value)
        elif value is not None and key in _DATETIME_KEYS:
            value = _decode_datetime(value)
        kwargs[key] = value
    return kwargs


def _import_operator_class(var: dict[str, Any]) -> type[BaseOperator]:
    path = f"{var['_task_module']}.{var['task_type']}"
    try:
        operator_class = import_string(path)
    except ImportError as e:
        raise ValueError(f"cannot import the operator {path!r}: {e}") from None
    if not (isinstance(operator_class, type) and issubclass(operator_class, BaseOperator)):
        raise ValueError(f"{path!r} is not an operator")
    return operator_class


def _build_task(var: dict[str, Any], dag: DAG, group: TaskGroup, *, import_operators: bool) -> BaseOperator:
    if var.get("_is_mapped"):
        raise ValueError("mapped tasks are not supported")
    kwargs = {
        "task_id": _get_local_task_id(var["task_id"], group),
        "dag": dag,
        "task_group": group,
        **_decode_operator_kwargs(var),
    }
    task: BaseOperator
    if not import_operators:
        task = BaseOperator(**kwargs)
    elif var.get("language") or var.get("is_stub"):
        task = import_string(_STUB_OPERATOR)(python_callable=_native_task, **kwargs)
        if "_arg_bindings" in var:
            task._arg_bindings = var["_arg_bindings"]
    else:
        arguments = {k: v for k, v in var.items() if k not in _DERIVED_TASK_KEYS | _OPERATOR_KEYS}
        task = _import_operator_class(var)(**kwargs, **arguments)
    if var.get("_can_skip_downstream"):
        task._can_skip_downstream = True
    return task


def materialize_dag(data: dict[str, Any], *, import_operators: bool = True) -> DAG:
    """
    Build the ``airflow.sdk.DAG`` that a Lang-SDK runtime serialized into ``data``.

    A task the runtime executes becomes a ``@task.stub`` operator. A task that names a Python
    operator becomes that operator, so a Python worker can run it.

    :param import_operators: Whether to import the operator classes. Without them, every task is
        a plain ``BaseOperator``, which checks the Dag's structure and its tasks' common arguments
        without importing any module the payload names.
    :raises ValueError: if the Dag uses a feature that cannot be rebuilt yet, or cannot be built.
    """
    encoded = data["dag"]
    dag_id = encoded["dag_id"]
    for key in _UNSUPPORTED_DAG_KEYS:
        if encoded.get(key):
            raise ValueError(f"Dag {dag_id!r} sets {key!r}, which a Lang-SDK Dag cannot use yet")
    kwargs: dict[str, Any] = {key: encoded[key] for key in _DAG_KEYS & encoded.keys()}
    tz = parse_timezone(encoded.get("timezone", "UTC"))
    for key in ("start_date", "end_date"):
        if encoded.get(key) is not None:
            kwargs[key] = _decode_datetime(encoded[key], tz)
    if encoded.get("dagrun_timeout") is not None:
        kwargs["dagrun_timeout"] = _decode_timedelta(encoded["dagrun_timeout"])
    try:
        timetable = _decode_timetable(encoded["timetable"])
    except ValueError as e:
        raise ValueError(f"Dag {dag_id!r}: {e}") from None

    dag = DAG(dag_id, schedule=timetable, **kwargs)
    # A Dag without a start date would otherwise take the deployment's default timezone.
    dag.timezone = tz
    dag.fileloc = encoded["fileloc"]
    dag.relative_fileloc = encoded.get("relative_fileloc")
    groups = _build_task_groups(dag, encoded["task_group"])
    tasks: dict[str, BaseOperator] = {}
    for task_data in encoded["tasks"]:
        var = task_data["__var"]
        try:
            tasks[var["task_id"]] = _build_task(
                var, dag, groups.get(var["task_id"], dag.task_group), import_operators=import_operators
            )
        except Exception as e:
            reason = e if isinstance(e, ValueError) else f"{type(e).__name__}: {e}"
            raise ValueError(f"Dag {dag_id!r}, task {var['task_id']!r}: {reason}") from e
    for task_data in encoded["tasks"]:
        var = task_data["__var"]
        for downstream_task_id in var.get("downstream_task_ids", []):
            if downstream_task_id not in tasks:
                raise ValueError(
                    f"Dag {dag_id!r}, task {var['task_id']!r}: the downstream task "
                    f"{downstream_task_id!r} does not exist"
                )
            tasks[var["task_id"]].set_downstream(tasks[downstream_task_id])
    return dag
