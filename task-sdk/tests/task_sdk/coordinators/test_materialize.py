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
"""
Tests for rebuilding a Lang-SDK Dag as an ``airflow.sdk.DAG``.

The fixtures are ``DagFileParsingResult`` payloads recorded from the TypeScript and Java runtimes.
"""

from __future__ import annotations

import copy
import datetime
import json
from pathlib import Path
from unittest import mock

import pytest

from airflow.providers.standard.decorators.stub import _StubOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from airflow.sdk.coordinators._materialize import materialize_dag
from airflow.sdk.exceptions import DagRunTriggerException
from airflow.serialization.serialized_objects import DagSerialization

FIXTURES = Path(__file__).parent / "fixtures"

# What re-serializing a rebuilt stub task changes: the serializer describes the stub operator class
# instead of the runtime's task class, and the runtime's language marker is dropped.
_STUB_TASK_KEYS = frozenset(
    {
        "_operator_name",
        "_task_module",
        "is_stub",
        "language",
        "op_args",
        "op_kwargs",
        "python_callable_name",
        "task_type",
        "template_fields",
        "template_fields_renderers",
    }
)
# Arguments the Python serializer leaves out, or adds at their defaults, for TriggerDagRunOperator.
_TRIGGER_TASK_KEYS = frozenset(
    {"logical_date", "note", "poke_interval", "reset_dag_run", "skip_when_already_exists"}
)
# Written by the Python serializer for every task.
_ADDED_TASK_KEYS = frozenset({"_needs_expansion", "has_retry_policy"})
_DEFAULT_RETRY_DELAY = 300.0
_CRON_PRESETS = {"@daily": "0 0 * * *"}


def _load_payloads() -> list:
    params = []
    for path in sorted(FIXTURES.glob("*.json")):
        for serialized in json.loads(path.read_text())["serialized_dags"]:
            data = serialized["data"]
            params.append(pytest.param(data, id=f"{path.stem}-{data['dag']['dag_id']}"))
    return params


def _get_payload(dag_id: str) -> dict:
    return next(p.values[0] for p in _load_payloads() if p.values[0]["dag"]["dag_id"] == dag_id)


def _normalize(data: dict) -> dict:
    dag = {key: value for key, value in data["dag"].items() if key != "_processor_dags_folder"}
    tasks = []
    for task in dag.pop("tasks"):
        var = dict(task["__var"])
        ignored = set(_ADDED_TASK_KEYS)
        if var.get("task_type") == "TriggerDagRunOperator":
            ignored |= _TRIGGER_TASK_KEYS
        elif var.get("language") or var.get("is_stub") or var.get("task_type") == "_StubOperator":
            ignored |= _STUB_TASK_KEYS
        var = {key: value for key, value in var.items() if key not in ignored}
        var.setdefault("retry_delay", _DEFAULT_RETRY_DELAY)
        tasks.append({**task, "__var": var})
    timetable = copy.deepcopy(dag.pop("timetable"))
    if expression := timetable["__var"].get("expression"):
        timetable["__var"]["expression"] = _CRON_PRESETS.get(expression, expression)
    return {"dag": dag, "tasks": tasks, "timetable": timetable}


@pytest.mark.parametrize("data", _load_payloads())
def test_round_trip_keeps_the_runtime_payload(data):
    dag = materialize_dag(data)
    again = json.loads(json.dumps(DagSerialization.to_dict(dag)))

    DagSerialization.validate_schema(again)
    DagSerialization.from_dict(copy.deepcopy(again))
    assert _normalize(again) == _normalize(data)


def test_runtime_tasks_become_stub_tasks():
    dag = materialize_dag(_get_payload("native_rich"))

    extract, gate = dag.task_dict["extract"], dag.task_dict["gate"]
    assert type(extract) is _StubOperator
    assert extract.is_stub is True
    assert (extract.queue, extract.retries) == ("ts", 3)
    assert extract.execution_timeout == datetime.timedelta(seconds=120)
    assert extract.downstream_task_ids == {"gate", "load.load_a"}
    assert gate._arg_bindings == [{"name": "n", "kind": "xcom", "task_id": "extract"}]
    assert gate.inherits_from_skipmixin is True
    assert dag.task_dict["load.load_a"].task_group.group_id == "load"


def test_python_operator_task_is_rebuilt_as_that_operator():
    dag = materialize_dag(_get_payload("native_rich"))

    trigger = dag.task_dict["trigger_downstream"]
    assert type(trigger) is TriggerDagRunOperator
    with pytest.raises(DagRunTriggerException) as exc_info:
        trigger.execute(mock.MagicMock())

    assert exc_info.value.trigger_dag_id == "downstream_etl"
    assert exc_info.value.conf == {"source": "native_rich", "ds": "{{ ds }}"}
    assert exc_info.value.reset_dag_run is True
    assert exc_info.value.note == "from ts"


def test_dag_attributes_are_rebuilt():
    data = _get_payload("java_native")

    dag = materialize_dag(data)

    assert isinstance(dag, DAG)
    assert (dag.fileloc, dag.relative_fileloc) == ("/bundles/app/dags.jar", "dags.jar")
    assert dag.timetable.expression == "0 0 * * *"
    assert dag.start_date == datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    assert dag.dagrun_timeout == datetime.timedelta(minutes=5)
    assert (dag.catchup, dag.max_active_runs, dag.tags) == (True, 3, {"a", "b"})


def test_leaves_the_payload_unchanged():
    data = _get_payload("native_rich")
    original = copy.deepcopy(data)

    materialize_dag(data)

    assert data == original


def _set_task_key(data: dict, key: str, value: object) -> dict:
    data["dag"]["tasks"][0]["__var"][key] = value
    return data


@pytest.mark.parametrize(
    ("change", "message"),
    [
        pytest.param(
            lambda d: d["dag"].update(params=[["x", {"default": 1}]]),
            r"Dag 'conformance_minimal' sets 'params', which a Lang-SDK Dag cannot use yet",
            id="params",
        ),
        pytest.param(
            lambda d: d["dag"].update(
                timetable={"__type": "airflow.timetables.interval.DeltaDataIntervalTimetable", "__var": {}}
            ),
            r"Dag 'conformance_minimal': the .*DeltaDataIntervalTimetable timetable is not supported",
            id="timetable",
        ),
        pytest.param(
            lambda d: _set_task_key(d, "_is_mapped", True),
            r"Dag 'conformance_minimal', task 'solo': mapped tasks are not supported",
            id="mapped-task",
        ),
        pytest.param(
            lambda d: _set_task_key(_set_task_key(d, "language", None), "_task_module", "not.a.module"),
            r"task 'solo': cannot import the operator 'not\.a\.module\.TypeScriptOperator'",
            id="unimportable-operator",
        ),
        pytest.param(
            lambda d: _set_task_key(
                _set_task_key(_set_task_key(d, "language", None), "_task_module", "airflow.sdk"),
                "task_type",
                "DAG",
            ),
            r"task 'solo': 'airflow\.sdk\.DAG' is not an operator",
            id="not-an-operator",
        ),
    ],
)
def test_rejects_what_cannot_be_rebuilt(change, message):
    data = copy.deepcopy(_get_payload("conformance_minimal"))
    data["dag"]["tasks"][0]["__var"].pop("is_stub", None)
    data["dag"]["tasks"][0]["__var"]["language"] = "typescript"
    change(data)

    with pytest.raises(ValueError, match=message):
        materialize_dag(data)
