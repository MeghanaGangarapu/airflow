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
from __future__ import annotations

import logging
from unittest import mock

import pytest

from airflow.dag_processing.importer_routing import (
    _find_bundle_file,
    get_claiming_coordinator,
    get_task_sdk_registry,
)

from unit.dag_processing.fake_importers import FAKE_IMPORTER, task_sdk_importers, write_jar
from unit.dag_processing.fake_lang_sdk import FakeCoordinator, fake_coordinator


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        pytest.param("native.jar", "native.jar", id="relative-to-the-bundle"),
        pytest.param("native.jar/Main.java", "native.jar", id="archive-member"),
        pytest.param("{bundle}/native.jar", "native.jar", id="absolute-inside"),
        pytest.param("../outside.jar", None, id="relative-outside"),
        pytest.param("{root}/outside.jar", None, id="absolute-outside"),
        pytest.param("missing.jar", None, id="missing"),
    ],
)
def test_find_bundle_file(tmp_path, reference, expected):
    bundle_path = tmp_path / "bundle"
    bundle_path.mkdir()
    write_jar(bundle_path / "native.jar", "native_dag")
    write_jar(tmp_path / "outside.jar", "outside_dag")

    found = _find_bundle_file(
        bundle_path, bundle_path.joinpath(reference.format(bundle=bundle_path, root=tmp_path))
    )

    assert found == (bundle_path / expected if expected else None)


def test_legacy_extension_mapping_is_warned_once(caplog):
    with task_sdk_importers({"classpath": FAKE_IMPORTER, "extensions": [".fake", ".py"]}):
        with caplog.at_level(logging.WARNING, logger="airflow.dag_processing.importer_routing"):
            get_task_sdk_registry("testing")
            get_task_sdk_registry("testing")

    warnings = [r.getMessage() for r in caplog.records if r.name == "airflow.dag_processing.importer_routing"]
    assert warnings == [
        "Ignoring the Dag importer configured for .py files: they always use the legacy importer"
    ]


def test_get_claiming_coordinator_returns_the_coordinator_of_its_importer(tmp_path):
    with fake_coordinator(), task_sdk_importers(FAKE_IMPORTER):
        coordinator = get_claiming_coordinator(tmp_path / "dags.native", "testing")
        others = [get_claiming_coordinator(tmp_path / name, "testing") for name in ("dags.fake", "dag.py")]

    assert isinstance(coordinator, FakeCoordinator)
    assert others == [None, None]


@mock.patch("airflow.dag_processing.importer_routing.get_task_sdk_registry", autospec=True, return_value=None)
def test_get_claiming_coordinator_without_a_registry(mock_registry, tmp_path):
    assert get_claiming_coordinator(tmp_path / "dags.native", "testing") is None
