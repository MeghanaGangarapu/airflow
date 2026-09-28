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

import json
import os
import sys
from pathlib import Path
from typing import Any

import attrs

from airflow.sdk.coordinators._subprocess import SubprocessCoordinator

FAKE_RUNTIME = Path(__file__).with_name("fake_lang_sdk_runtime.py")
# The oldest supervisor schema version, so requests to the runtime are downgraded.
SCHEMA_VERSION = "2026-06-16"


@attrs.define(kw_only=True)
class FakeCoordinator(SubprocessCoordinator):
    """
    Parse ``.native`` files with the fake runtime.

    The file names the command's schema version, or a ``command_error`` to raise instead.
    """

    def _build_parse_dag_command(self, *, path: Path) -> tuple[list[str], str | None]:
        spec = json.loads(path.read_text())
        if error := spec.get("command_error"):
            raise FileNotFoundError(error)
        return [sys.executable, os.fspath(FAKE_RUNTIME), os.fspath(path)], spec.get(
            "schema_version", SCHEMA_VERSION
        )


def write_native_file(path: Path, **spec: Any) -> Path:
    """Write a ``.native`` file whose keys tell the fake runtime what to do."""
    path.write_text(json.dumps(spec))
    return path
