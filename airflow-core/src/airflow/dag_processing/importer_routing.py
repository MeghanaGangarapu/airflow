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

A file is claimed when its extension is registered in the bundle's Task SDK importer registry,
unless it is one of the legacy extensions ``.py``, ``.pyc`` and ``.zip``. Those always stay on the
legacy importer in :mod:`airflow.dag_processing.importers`.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import weakref
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from airflow.exceptions import AirflowConfigException
from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter
from airflow.sdk.importers import (
    AbstractDagImporter,
    DagImporterRegistry,
    DagImportError,
    DagImportResult,
    FilesystemDagDefinition,
    PythonDagImporter,
    ZipImporter,
    ZipMemberDagDefinition,
    find_file_dag_definitions,
    get_file_suffix,
    get_importer_registry,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from airflow.dag_processing.bundles.base import BaseDagBundle
    from airflow.sdk.coordinators._subprocess import SubprocessCoordinator

log = logging.getLogger(__name__)

LEGACY_EXTENSIONS = frozenset({".py", ".pyc", ".zip"})

_reported_registry_failures: set[str | None] = set()
_checked_registries: weakref.WeakSet[DagImporterRegistry] = weakref.WeakSet()


@dataclass(frozen=True)
class BundleRef:
    """
    The part of a Dag bundle that importers read: its name and its root path.

    Core passes it in place of a ``BaseDagBundle`` when a Dag bag lists a folder or imports a file,
    so an importer must not rely on any other bundle attribute.
    """

    name: str | None
    path: Path | None


def _cast_to_bundle(bundle: BaseDagBundle | BundleRef) -> BaseDagBundle:
    return cast("BaseDagBundle", bundle)


def get_task_sdk_registry(bundle_name: str | None) -> DagImporterRegistry | None:
    """
    Return the Task SDK importer registry for a bundle, or ``None`` when it cannot be built.

    A broken importer configuration must not stop ``.py`` and ``.zip`` files from parsing, so the
    failure is logged once until the registry builds again, and the caller keeps the legacy importer.
    """
    try:
        registry = get_importer_registry(bundle_name)
    except Exception:
        if bundle_name not in _reported_registry_failures:
            _reported_registry_failures.add(bundle_name)
            log.exception("Cannot build the Task SDK Dag importer registry for bundle %s", bundle_name)
        return None
    _reported_registry_failures.discard(bundle_name)
    _warn_about_ignored_legacy_importers(registry)
    return registry


def _warn_about_ignored_legacy_importers(registry: DagImporterRegistry) -> None:
    if registry in _checked_registries:
        return
    _checked_registries.add(registry)
    for ext in sorted(LEGACY_EXTENSIONS.intersection(registry.supported_extensions())):
        try:
            importer = registry.get_importer(f"_{ext}")
        except Exception:
            importer = None
        if type(importer) not in (PythonDagImporter, ZipImporter):
            log.warning(
                "Ignoring the Dag importer configured for %s files: they always use the legacy importer", ext
            )


def _get_claimed_extensions(registry: DagImporterRegistry) -> list[str]:
    return [ext for ext in registry.supported_extensions() if ext not in LEGACY_EXTENSIONS]


def is_claimed(registry: DagImporterRegistry, path: str | os.PathLike[str]) -> bool:
    """
    Return whether a Task SDK importer claims ``path``, judged by its extension alone.

    This loads no importer. A claimed file whose importer cannot be loaded is still claimed, and
    parsing it reports the failure as an import error.
    """
    return get_file_suffix(Path(path)) in _get_claimed_extensions(registry)


def get_claiming_importer(
    registry: DagImporterRegistry, path: str | os.PathLike[str]
) -> AbstractDagImporter[Any] | None:
    """
    Return the Task SDK importer that claims ``path``, or ``None`` when the file is not claimed.

    :raises AirflowConfigException: if the importer configured for the extension cannot be loaded,
        or imports archive members, which is not supported for a claimed file.
    """
    if not is_claimed(registry, path):
        return None
    importer = registry.get_importer(Path(path))
    if isinstance(importer, ZipImporter):
        raise AirflowConfigException(
            f"{type(importer).__name__} cannot claim {get_file_suffix(Path(path))} files: an importer "
            "that claims files must import each file as one Dag definition, not as archive members."
        )
    return importer


def _get_claiming_importer_or_none(
    registry: DagImporterRegistry, path: str | os.PathLike[str]
) -> AbstractDagImporter[Any] | None:
    try:
        return get_claiming_importer(registry, path)
    except Exception as e:
        log.warning("Cannot load the Dag importer for %s: %s", path, e)
        return None


def get_claiming_coordinator(
    path: str | os.PathLike[str], bundle_name: str | None
) -> SubprocessCoordinator | None:
    """
    Return the coordinator whose runtime parses ``path``, or ``None`` when a Python child parses it.

    A runtime parses the file when the importer that claims it is a coordinator's Dag importer.
    """
    if (registry := get_task_sdk_registry(bundle_name)) is None:
        return None
    importer = _get_claiming_importer_or_none(registry, path)
    return importer.coordinator if isinstance(importer, CoordinatorDagImporter) else None


def _group_claiming_importers(
    registry: DagImporterRegistry,
) -> list[tuple[AbstractDagImporter[Any] | None, list[str]]]:
    """Pair each claiming importer with its extensions; an importer that cannot load is ``None``."""
    groups: list[tuple[AbstractDagImporter[Any] | None, list[str]]] = []
    for ext in _get_claimed_extensions(registry):
        # Any file name works: the registry routes by its suffix.
        importer = _get_claiming_importer_or_none(registry, f"_{ext}")
        group = next((g for g in groups if importer is not None and g[0] is importer), None)
        if group is None:
            groups.append((importer, [ext]))
        else:
            group[1].append(ext)
    return groups


def has_claiming_importers(registry: DagImporterRegistry) -> bool:
    """Return whether the registry claims any extension."""
    return bool(_get_claimed_extensions(registry))


def _find_bundle_file(bundle_path: Path, reference: Path) -> Path | None:
    """
    Return the file in the bundle that ``reference`` names, or ``None``.

    A relative reference is relative to the bundle. A reference into an archive, such as
    ``x.jar/member.py``, names the archive.
    """
    bundle_path = Path(os.path.normpath(bundle_path))
    path = Path(os.path.normpath(bundle_path / reference))
    for candidate in (path, *path.parents):
        if not candidate.is_relative_to(bundle_path):
            return None
        if candidate.is_file():
            return candidate
    return None


def _get_listed_path(
    registry: DagImporterRegistry, importer: AbstractDagImporter[Any], item: object, bundle_path: Path
) -> Path | None:
    """Return the file to parse for a listed item, or ``None`` when ``importer`` does not own one."""
    if isinstance(item, DagImportError):
        log.warning("Dag discovery error: %s", item.format_message())
        # Parsing the file lists it again and records the error as its import error.
        reference = Path(item.source_reference)
    elif isinstance(item, ZipMemberDagDefinition):
        reference = item.zip_path
    elif isinstance(item, FilesystemDagDefinition):
        reference = item.path
    else:
        log.warning("Skipping %r: %s did not list a file", item, type(importer).__name__)
        return None
    path = _find_bundle_file(bundle_path, reference)
    if path is None or _get_claiming_importer_or_none(registry, path) is not importer:
        return None
    return path


def merge_claimed_paths(
    registry: DagImporterRegistry,
    bundle: BaseDagBundle | BundleRef,
    legacy_paths: list[str],
    *,
    safe_mode: bool,
) -> list[str]:
    """
    Replace the claimed files in ``legacy_paths`` with the files that claiming importers list.

    An archive member is reported as its archive, which is the file that gets parsed. A file named
    by a discovery error is kept, so parsing it records the error. When an importer cannot load,
    or raises while listing, every file with its extensions is kept: parsing each one then reports
    the failure, instead of the files' Dags being treated as deleted.
    """
    bundle_path = Path(_cast_to_bundle(bundle).path)
    claimed_paths: dict[str, None] = {}
    for importer, extensions in _group_claiming_importers(registry):
        if importer is not None:
            try:
                for item in importer.list_dag_definitions(_cast_to_bundle(bundle), safe_mode=safe_mode):
                    if (path := _get_listed_path(registry, importer, item, bundle_path)) is not None:
                        claimed_paths.setdefault(os.fspath(path))
                continue
            except Exception:
                log.exception("Cannot list the Dag files that %s claims", type(importer).__name__)
        for definition in find_file_dag_definitions(bundle_path, extensions):
            claimed_paths.setdefault(os.fspath(definition.path))
    return [path for path in legacy_paths if not is_claimed(registry, path)] + list(claimed_paths)


def _build_failed_result(source_reference: str, error: Exception) -> DagImportResult:
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
                _cast_to_bundle(BundleRef(name=bundle_name, path=file_path)), safe_mode=safe_mode
            )
        )
    except Exception as e:
        log.exception("Cannot list the Dag definitions in %s", file_path)
        yield _build_failed_result(str(file_path), e)
        return

    bundle = _cast_to_bundle(BundleRef(name=bundle_name, path=bundle_path))
    for item in items:
        if isinstance(item, DagImportError):
            # The listing root is the file itself, so a relative reference is relative to it.
            reference = os.fspath(file_path / item.source_reference)
            yield DagImportResult(errors=[dataclasses.replace(item, source_reference=reference)])
            continue
        try:
            result = importer.import_definition(item, bundle)
        except Exception as e:
            log.exception("Cannot import the Dag definition %r", item)
            result = _build_failed_result(repr(item), e)
        yield result


def read_claimed_source(fileloc: str, bundle_name: str | None) -> str | None:
    """
    Return the Dag source of a claimed file, read through its importer.

    Returns ``None`` when no Task SDK importer claims the file.

    :raises Exception: whatever the importer raises while loading or reading the source.
    """
    registry = get_task_sdk_registry(bundle_name)
    if registry is None or (importer := get_claiming_importer(registry, fileloc)) is None:
        return None
    return importer.get_source_code(FilesystemDagDefinition(Path(fileloc))).source_code
