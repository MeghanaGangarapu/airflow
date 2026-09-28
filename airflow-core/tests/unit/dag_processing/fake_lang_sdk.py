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
"""A coordinator that parses ``.native`` Dag files with the fake runtime, for Dag processing tests."""

from __future__ import annotations

import contextlib
import json
import os
import shlex
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import attrs

from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.importers import DagSourceCode, reset_importer_registry

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from collections.abc import Iterator

FAKE_RUNTIME = Path(__file__).with_name("fake_lang_sdk_runtime.py")
# The oldest supervisor schema version, so requests to the runtime are downgraded.
SCHEMA_VERSION = "2026-06-16"


@attrs.define(kw_only=True)
class FakeCoordinator(SubprocessCoordinator):
    """
    Parse ``.native`` files with the fake runtime.

    The file names the command's schema version, or a ``command_error`` to raise instead. With
    ``launcher``, a shell starts the runtime as its child.
    """

    def _build_parse_dag_command(self, *, path: Path) -> tuple[list[str], str | None]:
        spec = json.loads(path.read_text())
        if error := spec.get("command_error"):
            raise FileNotFoundError(error)
        command = [sys.executable, os.fspath(FAKE_RUNTIME), os.fspath(path)]
        if spec.get("launcher"):
            # A shell that starts the runtime as its child and waits for it.
            command = ["/bin/sh", "-c", f'{shlex.join(command)} "$@" & wait', "fake-launcher"]
        return command, spec.get("schema_version", SCHEMA_VERSION)

    def get_dag_importer(self) -> FakeCoordinatorDagImporter:
        return FakeCoordinatorDagImporter(coordinator=self)


class FakeCoordinatorDagImporter(CoordinatorDagImporter):
    artifact_suffix = ".native"

    def get_source_code(self, definition) -> DagSourceCode:
        return DagSourceCode(definition.read_text(), "fake")


@contextlib.contextmanager
def fake_coordinator(**kwargs: Any) -> Iterator[None]:
    """Configure a ``FakeCoordinator``, with fresh coordinators and registries inside and after the block."""
    spec = {"fake": {"classpath": f"{__name__}.FakeCoordinator", "kwargs": kwargs}}
    reset_importer_registry()
    try:
        with conf_vars({("sdk", "coordinators"): json.dumps(spec)}):
            yield
    finally:
        reset_importer_registry()


def write_native_file(path: Path, **spec: Any) -> Path:
    """Write a ``.native`` file whose keys tell the fake runtime what to do."""
    path.write_text(json.dumps(spec))
    return path
