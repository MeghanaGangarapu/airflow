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
"""Task SDK Dag importers that claim non-Python files, for Dag processing tests."""

from __future__ import annotations

import contextlib
import json
import zipfile
from typing import TYPE_CHECKING, Any

from airflow.sdk import DAG, BaseOperator
from airflow.sdk.importers import (
    AbstractDagImporter,
    DagImportError,
    DagImportResult,
    DagImportWarning,
    DagSourceCode,
    FilesystemDagDefinition,
    find_file_dag_definitions,
    get_file_suffix,
    reset_importer_registry,
)

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

FAKE_IMPORTER = f"{__name__}.FakeDagImporter"
JAR_IMPORTER = f"{__name__}.JarDagImporter"
FAILING_SOURCE_JAR_IMPORTER = f"{__name__}.FailingSourceJarImporter"
ERROR_LISTING_JAR_IMPORTER = f"{__name__}.ErrorListingJarImporter"
RAISING_LISTING_JAR_IMPORTER = f"{__name__}.RaisingListingJarImporter"


def build_dag(dag_id: str, definition: FilesystemDagDefinition, bundle: Any) -> DAG:
    with DAG(dag_id=dag_id, schedule=None) as dag:
        BaseOperator(task_id="task")
    dag.fileloc = repr(definition)
    dag.relative_fileloc = definition.get_relative_loc(bundle.path)
    return dag


class FakeDagImporter(AbstractDagImporter[FilesystemDagDefinition]):
    """Import ``.fake`` files: each line is a Dag id, ``error: <message>`` or ``warn: <message>``."""

    supported_extensions = [".fake"]

    def can_handle(self, definition) -> bool:
        return get_file_suffix(definition) in self.supported_extensions

    def list_dag_definitions(self, bundle, *, safe_mode=True) -> Iterator[FilesystemDagDefinition]:
        yield from find_file_dag_definitions(bundle.path, self.supported_extensions)

    def import_definition(self, definition, bundle) -> DagImportResult:
        result = DagImportResult(definition=definition)
        for line_number, line in enumerate(definition.read_text().splitlines(), start=1):
            if line.startswith("error:"):
                result.errors.append(
                    DagImportError(source_reference=repr(definition), message=line[6:].strip())
                )
            elif line.startswith("warn:"):
                result.warnings.append(
                    DagImportWarning(
                        source_reference=repr(definition),
                        message=line[5:].strip(),
                        line_number=line_number,
                    )
                )
            elif line.strip():
                result.dags.append(build_dag(line.strip(), definition, bundle))
        return result

    def get_source_code(self, definition) -> DagSourceCode:
        return DagSourceCode(source_code=definition.read_text(), language="fake")


class JarDagImporter(AbstractDagImporter[FilesystemDagDefinition]):
    """Import ``.jar`` archives that list their Dag ids in a ``dags.txt`` member."""

    supported_extensions = [".jar"]

    def can_handle(self, definition) -> bool:
        return get_file_suffix(definition) in self.supported_extensions

    def list_dag_definitions(self, bundle, *, safe_mode=True) -> Iterator[FilesystemDagDefinition]:
        for definition in find_file_dag_definitions(bundle.path, self.supported_extensions):
            with zipfile.ZipFile(definition.path) as jar:
                if "dags.txt" in jar.namelist():
                    yield definition

    def import_definition(self, definition, bundle) -> DagImportResult:
        with zipfile.ZipFile(definition.path) as jar:
            dag_ids = jar.read("dags.txt").decode().split()
        return DagImportResult(
            definition=definition, dags=[build_dag(dag_id, definition, bundle) for dag_id in dag_ids]
        )

    def get_source_code(self, definition) -> DagSourceCode:
        with zipfile.ZipFile(definition.path) as jar:
            return DagSourceCode(source_code=jar.read("Main.java").decode(), language="java")


class FailingSourceJarImporter(JarDagImporter):
    def get_source_code(self, definition) -> DagSourceCode:
        raise RuntimeError("cannot read the embedded source")


class ErrorListingJarImporter(JarDagImporter):
    """Report a JAR without ``dags.txt`` as a discovery error instead of skipping it."""

    def list_dag_definitions(self, bundle, *, safe_mode=True) -> Iterator[Any]:
        for definition in find_file_dag_definitions(bundle.path, self.supported_extensions):
            with zipfile.ZipFile(definition.path) as jar:
                if "dags.txt" in jar.namelist():
                    yield definition
                else:
                    yield DagImportError(source_reference=repr(definition), message="no dags.txt")


class RaisingListingJarImporter(JarDagImporter):
    """List the readable JARs, then fail as if the next one were corrupt."""

    def list_dag_definitions(self, bundle, *, safe_mode=True) -> Iterator[FilesystemDagDefinition]:
        yield from super().list_dag_definitions(bundle, safe_mode=safe_mode)
        raise zipfile.BadZipFile("corrupt archive")


def write_jar(path: Path, *dag_ids: str, source: str = "class Main {}\n") -> Path:
    """Write a JAR that lists ``dag_ids``, with a member the legacy zip discovery would expand."""
    with zipfile.ZipFile(path, "w") as jar:
        if dag_ids:
            jar.writestr("dags.txt", "\n".join(dag_ids))
        jar.writestr("Main.java", source)
        jar.writestr("dag.py", "from airflow.sdk import DAG\n")
    return path


@contextlib.contextmanager
def task_sdk_importers(*configs: Any) -> Iterator[None]:
    """Configure global Task SDK importers, with fresh registries inside and after the block."""
    reset_importer_registry()
    try:
        with conf_vars({("dag_processor", "dag_importer_configs"): json.dumps(list(configs))}):
            yield
    finally:
        reset_importer_registry()
