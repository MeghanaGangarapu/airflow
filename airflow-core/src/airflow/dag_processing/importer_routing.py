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
Route Dag files that a Task SDK importer claims.

A file is claimed when its extension is registered to a Task SDK importer other than the default
``PythonDagImporter`` and ``ZipImporter``. Every other file, including ``.py`` and ``.zip``, stays
on the legacy importer in :mod:`airflow.dag_processing.importers`.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from airflow.sdk.importers import (
    AbstractDagImporter,
    DagImporterRegistry,
    DagImportError,
    DagImportResult,
    FilesystemDagDefinition,
    PythonDagImporter,
    ZipImporter,
    ZipMemberDagDefinition,
    get_file_suffix,
    get_importer_registry,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from airflow.dag_processing.bundles.base import BaseDagBundle

log = logging.getLogger(__name__)

_DEFAULT_IMPORTER_TYPES: tuple[type[AbstractDagImporter[Any]], ...] = (PythonDagImporter, ZipImporter)

_reported_registry_failures: set[str | None] = set()
_reported_load_failures: set[str] = set()


@dataclass(frozen=True)
class BundleRef:
    """The part of a Dag bundle that importers read: its name and its root path."""

    name: str | None
    path: Path | None


def _as_bundle(bundle: BaseDagBundle | BundleRef) -> BaseDagBundle:
    return cast("BaseDagBundle", bundle)


def get_task_sdk_registry(bundle_name: str | None) -> DagImporterRegistry | None:
    """
    Return the Task SDK importer registry for a bundle, or ``None`` when it cannot be built.

    A broken importer configuration must not stop ``.py`` and ``.zip`` files from parsing, so the
    failure is logged once per bundle and the caller keeps the legacy importer.
    """
    try:
        return get_importer_registry(bundle_name)
    except Exception:
        if bundle_name not in _reported_registry_failures:
            _reported_registry_failures.add(bundle_name)
            log.exception("Cannot build the Task SDK Dag importer registry for bundle %s", bundle_name)
        return None


def claimed_importer(
    registry: DagImporterRegistry, path: str | os.PathLike[str]
) -> AbstractDagImporter[Any] | None:
    """
    Return the Task SDK importer that claims ``path``, or ``None``.

    Only registered extensions claim files; an importer that matches through ``can_handle`` alone
    is not used.

    :raises AirflowConfigException: if the importer configured for the extension cannot be loaded.
    """
    file_path = Path(path)
    if get_file_suffix(file_path) not in registry.supported_extensions():
        return None
    importer = registry.get_importer(file_path)
    if importer is None or type(importer) in _DEFAULT_IMPORTER_TYPES:
        return None
    return importer


def _claimed_importer_or_none(
    registry: DagImporterRegistry, path: str | os.PathLike[str]
) -> AbstractDagImporter[Any] | None:
    try:
        return claimed_importer(registry, path)
    except Exception as e:
        if (reason := str(e)) not in _reported_load_failures:
            _reported_load_failures.add(reason)
            log.exception("Cannot load the Dag importer for %s", path)
        return None


def is_claimed(registry: DagImporterRegistry, path: str | os.PathLike[str]) -> bool:
    """
    Return whether a Task SDK importer claims ``path``.

    An importer that fails to load counts as no claim. The file then keeps its legacy handling,
    and parsing it reports the failure as an import error.
    """
    return _claimed_importer_or_none(registry, path) is not None


def _claiming_importers(registry: DagImporterRegistry) -> list[AbstractDagImporter[Any]]:
    importers: list[AbstractDagImporter[Any]] = []
    for ext in registry.supported_extensions():
        # Any file name works: the registry routes by its suffix.
        importer = _claimed_importer_or_none(registry, f"_{ext}")
        if importer is not None and importer not in importers:
            importers.append(importer)
    return importers


def has_claiming_importers(registry: DagImporterRegistry) -> bool:
    """Return whether any registered extension belongs to a claiming Task SDK importer."""
    return bool(_claiming_importers(registry))


def iter_claimed_paths(
    registry: DagImporterRegistry,
    bundle: BaseDagBundle | BundleRef,
    *,
    safe_mode: bool,
) -> Iterator[Path]:
    """
    Yield each file under ``bundle.path`` that a Task SDK importer claims, once, in walk order.

    An archive member is reported as its archive, which is the file that gets parsed. Discovery
    errors are logged and skipped.
    """
    seen: set[Path] = set()
    for importer in _claiming_importers(registry):
        try:
            items = list(importer.list_dag_definitions(_as_bundle(bundle), safe_mode=safe_mode))
        except Exception:
            log.exception("Cannot list the Dag files that %s claims", type(importer).__name__)
            continue
        for item in items:
            if isinstance(item, DagImportError):
                log.warning("Skipping a Dag definition: %s", item.format_message())
                continue
            if isinstance(item, ZipMemberDagDefinition):
                path = item.zip_path
            elif isinstance(item, FilesystemDagDefinition):
                path = item.path
            else:
                log.warning("Skipping %r: %s did not list a file", item, type(importer).__name__)
                continue
            if path in seen or _claimed_importer_or_none(registry, path) is not importer:
                continue
            seen.add(path)
            yield path


def _failed_result(source_reference: str, error: Exception) -> DagImportResult:
    return DagImportResult(
        errors=[DagImportError(source_reference=source_reference, message=f"{type(error).__name__}: {error}")]
    )


def iter_claimed_results(
    importer: AbstractDagImporter[Any],
    path: str | os.PathLike[str],
    *,
    bundle_name: str | None,
    bundle_path: Path | None,
    safe_mode: bool,
) -> Iterator[DagImportResult]:
    """
    Import every Dag definition that ``importer`` lists for one claimed file.

    Yields one result per definition. A discovery error, or an exception from the importer, is
    yielded as a result that holds only that error, so it is reported rather than raised.
    """
    file_path = Path(path)
    try:
        items = list(
            importer.list_dag_definitions(
                _as_bundle(BundleRef(name=bundle_name, path=file_path)), safe_mode=safe_mode
            )
        )
    except Exception as e:
        log.exception("Cannot list the Dag definitions in %s", file_path)
        yield _failed_result(str(file_path), e)
        return

    bundle = _as_bundle(BundleRef(name=bundle_name, path=bundle_path))
    for item in items:
        if isinstance(item, DagImportError):
            yield DagImportResult(errors=[item])
            continue
        try:
            result = importer.import_definition(item, bundle)
        except Exception as e:
            log.exception("Cannot import the Dag definition %r", item)
            result = _failed_result(repr(item), e)
        yield result


def read_claimed_source(fileloc: str, bundle_name: str | None) -> str | None:
    """
    Return the Dag source of a claimed file, read through its importer.

    Returns ``None`` when no Task SDK importer claims the file.

    :raises Exception: whatever the importer raises while loading or reading the source.
    """
    registry = get_task_sdk_registry(bundle_name)
    if registry is None or (importer := claimed_importer(registry, fileloc)) is None:
        return None
    return importer.get_source_code(FilesystemDagDefinition(Path(fileloc))).source_code
