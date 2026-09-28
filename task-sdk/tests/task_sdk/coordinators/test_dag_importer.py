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

import copy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

import pytest

from airflow.dag_processing.processor import DagFileParsingResult, LangSDKDagFileProcessorProcess
from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.exceptions import AirflowConfigException
from airflow.sdk.importers import DagSourceCode, FilesystemDagDefinition
from airflow.serialization.serialized_objects import LazyDeserializedDAG

FIXTURES = Path(__file__).parent / "fixtures"


class _BundleImporter(CoordinatorDagImporter):
    artifact_suffix = ".min.mjs"

    def get_source_code(self, definition) -> DagSourceCode:
        return DagSourceCode("", "typescript")

    def might_contain_dag(self, definition, safe_mode: bool) -> bool:
        return not definition.path.name.startswith("skip")


def _get_payload(dag_id: str) -> dict:
    serialized_dags = json.loads((FIXTURES / "ts_conformance.json").read_text())["serialized_dags"]
    return copy.deepcopy(next(d["data"] for d in serialized_dags if d["data"]["dag"]["dag_id"] == dag_id))


@pytest.fixture
def importer() -> _BundleImporter:
    return _BundleImporter(coordinator=MagicMock(spec=SubprocessCoordinator))


@pytest.mark.parametrize(("artifact_suffix", "extension"), [(".min.mjs", ".mjs"), (".jar", ".jar")])
def test_claims_the_last_suffix_of_its_artifacts(artifact_suffix, extension):
    importer_class = type("_Importer", (_BundleImporter,), {"artifact_suffix": artifact_suffix})

    importer = importer_class(coordinator=MagicMock(spec=SubprocessCoordinator))

    assert importer.supported_extensions == [extension]
    assert importer.can_handle(f"dags/main{artifact_suffix}") is True
    assert importer.can_handle("dags/main.txt") is False


def test_does_not_handle_other_files_with_its_extension(importer):
    assert importer.can_handle("dags/main.mjs") is False


def test_lists_only_its_artifacts(importer, tmp_path):
    for name in ("main.min.mjs", "helper.mjs", "skip.min.mjs"):
        (tmp_path / name).write_text("")

    definitions = list(importer.list_dag_definitions(SimpleNamespace(name="testing", path=tmp_path)))

    assert [d.path.name for d in definitions] == ["main.min.mjs"]


class TestImportDefinition:
    @staticmethod
    def _import(importer, tmp_path):
        definition = FilesystemDagDefinition(tmp_path / "main.min.mjs")
        return importer.import_definition(definition, SimpleNamespace(name="testing", path=tmp_path))

    @patch("airflow.settings.get_dagbag_import_timeout", autospec=True, return_value=12)
    @patch.object(LangSDKDagFileProcessorProcess, "run", autospec=True)
    def test_rebuilds_the_dags_the_runtime_returns(self, mock_run, mock_timeout, importer, tmp_path):
        mock_run.return_value = DagFileParsingResult(
            fileloc=str(tmp_path / "main.min.mjs"),
            serialized_dags=[LazyDeserializedDAG(data=_get_payload("conformance_minimal"))],
            import_errors={"main.min.mjs": "one failed", "main.ts": "two failed"},
        )

        result = self._import(importer, tmp_path)

        assert [dag.dag_id for dag in result.dags] == ["conformance_minimal"]
        assert [(e.source_reference, e.message) for e in result.errors] == [
            (str(tmp_path / "main.min.mjs"), "one failed"),
            (str(tmp_path / "main.min.mjs"), "main.ts: two failed"),
        ]
        mock_run.assert_called_once_with(
            coordinator=importer.coordinator,
            path=tmp_path / "main.min.mjs",
            bundle_path=tmp_path,
            bundle_name="testing",
            dag_file_rel_path="main.min.mjs",
            timeout=12,
            logger=ANY,
        )

    @patch.object(LangSDKDagFileProcessorProcess, "run", autospec=True, side_effect=TimeoutError("too slow"))
    def test_reports_a_timeout(self, mock_run, importer, tmp_path):
        [error] = self._import(importer, tmp_path).errors

        assert (error.message, error.error_type) == ("too slow", "timeout")

    @patch.object(LangSDKDagFileProcessorProcess, "run", autospec=True)
    def test_reports_a_dag_that_cannot_be_rebuilt(self, mock_run, importer, tmp_path):
        payload = _get_payload("conformance_minimal")
        payload["dag"]["params"] = [["x", {"default": 1}]]
        mock_run.return_value = DagFileParsingResult(
            fileloc=str(tmp_path / "main.min.mjs"), serialized_dags=[LazyDeserializedDAG(data=payload)]
        )

        result = self._import(importer, tmp_path)

        assert result.dags == []
        assert [e.message for e in result.errors] == [
            "Dag 'conformance_minimal' sets 'params', which a Lang-SDK Dag cannot use yet"
        ]

    @pytest.mark.parametrize(("configured", "expected"), [(30, 30), (0, None), (-1, None)])
    @patch("airflow.settings.get_dagbag_import_timeout", autospec=True)
    @patch.object(LangSDKDagFileProcessorProcess, "run", autospec=True)
    def test_timeout_follows_the_dagbag_import_timeout(
        self, mock_run, mock_timeout, importer, tmp_path, configured, expected
    ):
        mock_timeout.return_value = configured
        mock_run.return_value = DagFileParsingResult(fileloc="main.min.mjs", serialized_dags=[])

        self._import(importer, tmp_path)

        assert mock_run.call_args.kwargs["timeout"] == expected

    @patch("airflow.settings.get_dagbag_import_timeout", autospec=True, return_value="30")
    def test_timeout_must_be_a_number(self, mock_timeout, importer, tmp_path):
        with pytest.raises(AirflowConfigException, match="must be int or float"):
            self._import(importer, tmp_path)
