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

import contextlib
import copy
import functools
import importlib
import json
import logging
import os
import selectors
import signal
import time
import traceback
from collections.abc import Callable, Sequence
from pathlib import Path
from socket import socket, socketpair
from typing import TYPE_CHECKING, Annotated, Any, BinaryIO, ClassVar, Literal, NoReturn, cast

import attrs
import psutil
from pydantic import BaseModel, Field, TypeAdapter
from uuid6 import uuid7

from airflow._shared.observability.metrics import stats
from airflow.callbacks.callback_requests import (
    CallbackRequest,
    DagCallbackRequest,
    EmailRequest,
    TaskCallbackRequest,
)
from airflow.configuration import conf
from airflow.dag_processing.bundles.base import BundleVersionLock
from airflow.dag_processing.dagbag import BundleDagBag, DagBag
from airflow.models.dag import DagModel
from airflow.sdk.coordinators._subprocess import _is_connection_from_pid, _ResourceTracker, _start_server
from airflow.sdk.exceptions import AirflowRuntimeError, TaskNotFound
from airflow.sdk.execution_time import supervisor, task_runner
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    DeleteVariable,
    ErrorResponse,
    GetConnection,
    GetPreviousDagRun,
    GetPreviousTI,
    GetPrevSuccessfulDagRun,
    GetTaskStates,
    GetTICount,
    GetVariable,
    GetVariableKeys,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    MaskSecret,
    OKResponse,
    PreviousDagRunResult,
    PreviousTIResult,
    PrevSuccessfulDagRunResult,
    PutVariable,
    TaskStatesResult,
    VariableKeysResult,
    VariableResult,
    XComCountResponse,
    XComResult,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.supervisor import (
    PsutilTracker,
    WatchedSubprocess,
    length_prefixed_frame_reader,
    make_buffered_socket_reader,
    process_log_messages_from_subprocess,
    register_request_method,
)
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, _send_error_email_notification
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
from airflow.utils.dag_version_inflation_checker import (
    DagVersionInflationCheckLevel,
    DagVersionInflationCheckResult,
    check_dag_file_stability,
)
from airflow.utils.file import iter_airflow_imports
from airflow.utils.helpers import prune_dict
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
    from airflow.sdk.api.client import Client
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.sdk.execution_time.supervisor import RequestHandler, RequestResult
    from airflow.typing_compat import Self


class DagFileParseRequest(BaseModel):
    """
    Request for DAG File Parsing.

    This is the request that the manager will send to the DAG parser with the dag file and
    any other necessary metadata.
    """

    file: str

    bundle_path: Path
    """Passing bundle path around lets us figure out relative file path."""

    bundle_name: str
    """Bundle name for team-specific executor validation."""

    callback_requests: list[CallbackRequest] = Field(default_factory=list)
    type: Literal["DagFileParseRequest"] = "DagFileParseRequest"


class DagFileParsingResult(BaseModel):
    """
    Result of DAG File Parsing.

    This is the result of a successful DAG parse, in this class, we gather all serialized DAGs,
    import errors and warnings to send back to the scheduler to store in the DB.
    """

    fileloc: str
    serialized_dags: list[LazyDeserializedDAG]
    warnings: list | None = None
    import_errors: dict[str, str] | None = None
    type: Literal["DagFileParsingResult"] = "DagFileParsingResult"


ToManager = Annotated[
    DagFileParsingResult
    | GetConnection
    | GetVariable
    | GetVariableKeys
    | PutVariable
    | GetTaskStates
    | GetTICount
    | DeleteVariable
    | GetPrevSuccessfulDagRun
    | GetPreviousDagRun
    | GetPreviousTI
    | GetXCom
    | GetXComCount
    | GetXComSequenceItem
    | GetXComSequenceSlice
    | MaskSecret,
    Field(discriminator="type"),
]

ToDagProcessor = Annotated[
    DagFileParseRequest
    | ConnectionResult
    | VariableResult
    | VariableKeysResult
    | TaskStatesResult
    | PreviousDagRunResult
    | PreviousTIResult
    | PrevSuccessfulDagRunResult
    | ErrorResponse
    | OKResponse
    | XComCountResponse
    | XComResult
    | XComSequenceIndexResult
    | XComSequenceSliceResult,
    Field(discriminator="type"),
]


def _is_python_source(file_path: str | os.PathLike[str]) -> bool:
    """Return whether a Dag file is Python source, which the AST-based pre-parse steps can read."""
    return Path(file_path).suffix.lower() == ".py"


def _pre_import_airflow_modules(file_path: str, log: FilteringBoundLogger) -> None:
    """
    Pre-import Airflow modules found in the given file.

    This prevents modules from being re-imported in each processing process,
    saving CPU time and memory.
    (The default value of "parsing_pre_import_modules" is set to True)

    :param file_path: Path to the file to scan for imports
    :param log: Logger instance to use for warnings
    """
    if not _is_python_source(file_path):
        return
    if not conf.getboolean("dag_processor", "parsing_pre_import_modules", fallback=True):
        return

    for module in iter_airflow_imports(file_path):
        try:
            importlib.import_module(module)
        except Exception as e:
            log.warning("Error when trying to pre-import module '%s' found in %s: %s", module, file_path, e)


def _parse_file_entrypoint():
    # Mark as client-side (runs user DAG code)
    # Prevents inheriting server context from parent DagProcessorManager
    os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "client"

    import structlog

    from airflow.sdk.execution_time import comms, task_runner

    # Parse DAG file, send JSON back up!
    comms_decoder = comms.CommsDecoder[ToDagProcessor, ToManager](
        body_decoder=TypeAdapter[ToDagProcessor](ToDagProcessor),
    )

    msg = comms_decoder._get_response()
    if not isinstance(msg, DagFileParseRequest):
        raise RuntimeError(f"Required first message to be a DagFileParseRequest, it was {msg}")

    task_runner.SUPERVISOR_COMMS = comms_decoder
    log = structlog.get_logger(logger_name="task")

    result = _parse_file(msg, log)

    if result is not None:
        comms_decoder.send(result)


def _parse_file(msg: DagFileParseRequest, log: FilteringBoundLogger) -> DagFileParsingResult | None:
    # TODO: Set known_pool names on DagBag!

    stability_check_result = (
        check_dag_file_stability(os.fspath(msg.file))
        if _is_python_source(msg.file)
        else DagVersionInflationCheckResult(check_level=DagVersionInflationCheckLevel.off)
    )

    # Callback runs must not be blocked by the stability check: callbacks for
    # already-scheduled runs still have to execute, and they never produce a
    # parsing result anyway.
    if not msg.callback_requests and (
        stability_check_error_dict := stability_check_result.get_error_format_dict(msg.file, msg.bundle_path)
    ):
        # If Dag stability check level is error, we shouldn't parse the Dags and return the result early
        return DagFileParsingResult(
            fileloc=msg.file,
            serialized_dags=[],
            import_errors=stability_check_error_dict,
        )

    bag = BundleDagBag(
        dag_folder=msg.file,
        bundle_path=msg.bundle_path,
        bundle_name=msg.bundle_name,
        load_op_links=False,
    )

    if msg.callback_requests:
        # If the request is for callback, we shouldn't serialize the Dags
        _execute_callbacks(bag, msg.callback_requests, log)
        return None

    serialized_dags, serialization_import_errors = _serialize_dags(bag, log)
    bag.import_errors.update(serialization_import_errors)
    result = DagFileParsingResult(
        fileloc=msg.file,
        serialized_dags=serialized_dags,
        import_errors=bag.import_errors,
        warnings=stability_check_result.get_formatted_warnings(bag.dag_ids),
    )
    return result


def _serialize_dags(
    bag: DagBag,
    log: FilteringBoundLogger,
) -> tuple[list[LazyDeserializedDAG], dict[str, str]]:
    serialization_import_errors = {}
    serialized_dags = []
    for dag in bag.dags.values():
        try:
            data = DagSerialization.to_dict(dag)
            serialized_dags.append(LazyDeserializedDAG(data=data, last_loaded=dag.last_loaded))
        except Exception:
            log.exception("Failed to serialize DAG: %s", dag.fileloc)
            dagbag_import_error_traceback_depth = conf.getint(
                "core", "dagbag_import_error_traceback_depth", fallback=None
            )
            # Use relative_fileloc if available, fall back to fileloc
            error_path = dag.relative_fileloc or dag.fileloc
            serialization_import_errors[error_path] = traceback.format_exc(
                limit=-dagbag_import_error_traceback_depth
            )
    return serialized_dags, serialization_import_errors


def _get_dag_with_task(
    dagbag: DagBag, dag_id: str, task_id: str | None = None
) -> tuple[DAG, BaseOperator | MappedOperator | None]:
    """
    Retrieve a DAG and optionally a task from the DagBag.

    :param dagbag: DagBag to retrieve from
    :param dag_id: DAG ID to retrieve
    :param task_id: Optional task ID to retrieve from the DAG
    :return: tuple of (dag, task) where task is None if not requested
    :raises ValueError: If DAG or task is not found
    """
    if dag_id not in dagbag.dags:
        raise ValueError(
            f"DAG '{dag_id}' not found in DagBag. "
            f"This typically indicates a race condition where the DAG was removed or failed to parse."
        )

    dag = dagbag.dags[dag_id]

    if task_id is not None:
        try:
            task = dag.get_task(task_id)
            return dag, task
        except TaskNotFound:
            raise ValueError(
                f"Task '{task_id}' not found in DAG '{dag_id}'. "
                f"This typically indicates a race condition where the task was removed or the DAG structure changed."
            ) from None

    return dag, None


def _execute_callbacks(
    dagbag: DagBag, callback_requests: list[CallbackRequest], log: FilteringBoundLogger
) -> None:
    for request in callback_requests:
        if isinstance(request, (TaskCallbackRequest, EmailRequest)):
            log_extra = {
                "dag_id": request.ti.dag_id,
                "run_id": request.ti.run_id,
                "ti_id": str(request.ti.id),
            }
        else:
            log_extra = {"dag_id": request.dag_id, "run_id": request.run_id}
        # context_from_server can carry user-supplied run conf, and the masker cannot
        # redact inside an already-serialized string, so keep it out of log payloads.
        request_json = request.to_json(exclude={"context_from_server"})
        log.debug("Processing Callback Request", request=request_json, **log_extra)
        # A failed request (e.g. the Dag or task was removed since the callback
        # was scheduled) must not abort the remaining requests in this batch --
        # they were already popped from the manager's queue and would be lost.
        try:
            with BundleVersionLock(
                bundle_name=request.bundle_name,
                bundle_version=request.bundle_version,
            ):
                if isinstance(request, TaskCallbackRequest):
                    _execute_task_callbacks(dagbag, request, log)
                elif isinstance(request, DagCallbackRequest):
                    _execute_dag_callbacks(dagbag, request, log)
                elif isinstance(request, EmailRequest):
                    _execute_email_callbacks(dagbag, request, log)
        except Exception:
            log.exception("Failed to execute callback request", request=request_json, **log_extra)


def _execute_dag_callbacks(dagbag: DagBag, request: DagCallbackRequest, log: FilteringBoundLogger) -> None:
    from airflow.sdk.api.datamodels._generated import TIRunContext

    dag, _ = _get_dag_with_task(dagbag, request.dag_id)
    callbacks = dag.on_failure_callback if request.is_failure_callback else dag.on_success_callback
    if not callbacks:
        log.warning("Callback requested, but dag didn't have any", dag_id=request.dag_id)
        return

    callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
    ctx_from_server = request.context_from_server

    context: Context = {
        "dag": dag,
        "run_id": request.run_id,
        "reason": request.msg,
    }
    if ctx_from_server is not None and ctx_from_server.last_ti is not None:
        try:
            task = dag.get_task(ctx_from_server.last_ti.task_id)
        except TaskNotFound:
            # The task only enriches the callback context; a task removed since the
            # run must not cost the user the callback itself (produce_dag_callback
            # makes the same call for an unrepresentable last_ti).
            log.warning(
                "Task from callback context no longer exists in the Dag; running callback with minimal context",
                dag_id=request.dag_id,
                task_id=ctx_from_server.last_ti.task_id,
            )
        else:
            runtime_ti = RuntimeTaskInstance.model_construct(
                **ctx_from_server.last_ti.model_dump(exclude_unset=True),
                task=task,
                _ti_context_from_server=TIRunContext.model_construct(
                    dag_run=ctx_from_server.dag_run,
                    max_tries=task.retries,
                ),
            )
            context = runtime_ti.get_template_context()
            context["reason"] = request.msg

    for callback in callbacks:
        log.info(
            "Executing on_%s dag callback",
            "failure" if request.is_failure_callback else "success",
            dag_id=request.dag_id,
        )
        try:
            callback(context)
        except Exception:
            log.exception("Callback failed", dag_id=request.dag_id)
            stats.incr(
                "dag.callback_exceptions",
                tags=prune_dict(
                    {
                        "dag_id": request.dag_id,
                        "team_name": (
                            DagModel.get_team_name(request.dag_id)
                            if conf.getboolean("core", "multi_team")
                            else None
                        ),
                    }
                ),
            )


def _execute_task_callbacks(dagbag: DagBag, request: TaskCallbackRequest, log: FilteringBoundLogger) -> None:
    if not request.is_failure_callback:
        log.warning(
            "Task callback requested but is not a failure callback",
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
            ti_id=str(request.ti.id),
        )
        return

    dag, task = _get_dag_with_task(dagbag, request.ti.dag_id, request.ti.task_id)

    if TYPE_CHECKING:
        assert task is not None

    if request.task_callback_type is TaskInstanceState.UP_FOR_RETRY:
        callbacks = task.on_retry_callback
    else:
        callbacks = task.on_failure_callback

    if not callbacks:
        log.warning(
            "Callback requested but no callback found",
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
            ti_id=request.ti.id,
        )
        return

    callbacks = callbacks if isinstance(callbacks, Sequence) else [callbacks]
    ctx_from_server = request.context_from_server

    if ctx_from_server is not None:
        runtime_ti = RuntimeTaskInstance.model_construct(
            **request.ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=ctx_from_server,
            max_tries=ctx_from_server.max_tries,
        )
    else:
        runtime_ti = RuntimeTaskInstance.model_construct(
            **request.ti.model_dump(exclude_unset=True),
            task=task,
        )
    context = runtime_ti.get_template_context()

    def get_callback_representation(callback):
        with contextlib.suppress(AttributeError):
            return callback.__name__
        with contextlib.suppress(AttributeError):
            return callback.__class__.__name__
        return callback

    for idx, callback in enumerate(callbacks):
        callback_repr = get_callback_representation(callback)
        log.info(
            "Executing Task callback at index %d: %s (ti_id=%s)",
            idx,
            callback_repr,
            request.ti.id,
        )
        try:
            callback(context)
        except Exception:
            log.exception(
                "Error in callback at index %d: %s (ti_id=%s)",
                idx,
                callback_repr,
                request.ti.id,
            )


def _execute_email_callbacks(dagbag: DagBag, request: EmailRequest, log: FilteringBoundLogger) -> None:
    """Execute email notification for task failure/retry."""
    dag, task = _get_dag_with_task(dagbag, request.ti.dag_id, request.ti.task_id)

    if TYPE_CHECKING:
        assert task is not None

    if not task.email:
        log.warning(
            "Email callback requested but no email configured",
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
        )
        return

    # Check if email should be sent based on task configuration
    should_send_email = False
    if request.email_type == "failure" and task.email_on_failure:
        should_send_email = True
    elif request.email_type == "retry" and task.email_on_retry:
        should_send_email = True

    if not should_send_email:
        log.info(
            "Email not sent - task configured with email_on_%s=False",
            request.email_type,
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
        )
        return

    ctx_from_server = request.context_from_server

    runtime_ti = RuntimeTaskInstance.model_construct(
        **request.ti.model_dump(exclude_unset=True),
        task=task,
        _ti_context_from_server=ctx_from_server,
        max_tries=ctx_from_server.max_tries,
    )

    log.info(
        "Sending %s email for task %s",
        request.email_type,
        request.ti.task_id,
        dag_id=request.ti.dag_id,
        run_id=request.ti.run_id,
    )

    try:
        context = runtime_ti.get_template_context()
        error = Exception(request.msg) if request.msg else None
        _send_error_email_notification(task, runtime_ti, context, error, log)
    except Exception:
        log.exception(
            "Failed to send %s email",
            request.email_type,
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
        )


def in_process_api_server() -> InProcessExecutionAPI:
    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI

    api = InProcessExecutionAPI()
    return api


@attrs.define(kw_only=True)
class DagFileProcessorProcess(WatchedSubprocess, LoggingMixin):
    """
    Parses dags with Task SDK API.

    This class provides a wrapper and management around a subprocess to parse a specific DAG file.

    Since DAGs are written with the Task SDK, we need to parse them in a task SDK process such that
    we can use the Task SDK definitions when serializing. This prevents potential conflicts with classes
    in core Airflow.
    """

    logger_filehandle: BinaryIO
    parsing_result: DagFileParsingResult | None = None
    decoder: ClassVar[TypeAdapter[ToManager]] = TypeAdapter[ToManager](ToManager)
    had_callbacks: bool = False  # Track if this process was started with callbacks to prevent stale DAG detection false positives

    client: Client
    """The HTTP client to use for communication with the API server."""

    bundle_name: str
    dag_file_rel_path: str

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
        dag_file_rel_path: str,
        callbacks: list[CallbackRequest],
        target: Callable[[], None] = _parse_file_entrypoint,
        client: Client,
        **kwargs,
    ) -> Self:
        logger = kwargs["logger"]

        # Parsing DAG files runs user code that can trigger macOS-unsafe ObjC
        # initialization (secret backends, connection/variable lookups, HTTP
        # clients). Fork+exec a clean interpreter there. Tests override `target`
        # with a stub to exercise the base infrastructure; keep bare fork for those.
        use_exec = target is _parse_file_entrypoint and supervisor._should_use_exec()

        # Pre-importing only helps the bare-fork child (it inherits the imports via
        # copy-on-write). An exec'd child re-imports from scratch, so skip it there
        # to avoid leaking user modules into the long-lived processor manager.
        if not use_exec:
            _pre_import_airflow_modules(os.fspath(path), logger)

        proc: Self = super().start(
            target=target,
            client=client,
            bundle_name=bundle_name,
            dag_file_rel_path=dag_file_rel_path,
            use_exec=use_exec,
            **kwargs,
        )
        proc.had_callbacks = bool(callbacks)  # Track if this process had callbacks
        proc._on_child_started(callbacks, path, bundle_path, bundle_name)
        return proc

    def _on_child_started(
        self,
        callbacks: list[CallbackRequest],
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
    ) -> None:
        msg = DagFileParseRequest(
            file=os.fspath(path),
            bundle_path=bundle_path,
            bundle_name=bundle_name,
            callback_requests=callbacks,
        )
        self.send_msg(msg, request_id=0)

    def _get_target_loggers(self) -> tuple[FilteringBoundLogger, ...]:
        base = super()._get_target_loggers()
        if not self.subprocess_logs_to_stdout:
            return base
        return tuple(
            logger.bind(dag_file=self.dag_file_rel_path, bundle_name=self.bundle_name) for logger in base
        )

    def _create_log_forwarder(
        self,
        loggers: tuple[FilteringBoundLogger, ...],
        name: str,
        *,
        data: bytes,
        log_level: int = logging.INFO,
    ) -> Callable[[socket], bool]:
        return super()._create_log_forwarder(
            loggers,
            name.replace("task.", "dag_processor.", 1),
            data=data,
            log_level=log_level,
        )

    def _handle_parsing_result(
        self, msg: DagFileParsingResult, log: FilteringBoundLogger, req_id: int
    ) -> RequestResult:
        self.parsing_result = msg
        return None, {}

    _request_handlers: ClassVar[dict[type[BaseModel], RequestHandler[DagFileProcessorProcess]]] = {
        **WatchedSubprocess._get_shared_request_handlers(
            DeleteVariable,
            GetConnection,
            GetPrevSuccessfulDagRun,
            GetPreviousDagRun,
            GetPreviousTI,
            GetTICount,
            GetTaskStates,
            GetVariable,
            GetVariableKeys,
            GetXCom,
            GetXComCount,
            GetXComSequenceItem,
            GetXComSequenceSlice,
            MaskSecret,
            PutVariable,
        ),
        **dict([register_request_method(DagFileParsingResult, _handle_parsing_result)]),
    }

    def _reject_request(self, msg, log: FilteringBoundLogger, req_id: int) -> None:
        log.error("Unhandled request", msg=msg)
        self.send_msg(
            None,
            request_id=req_id,
            error=ErrorResponse(detail={"status_code": 400, "message": "Unhandled request"}),
        )

    @property
    def is_ready(self) -> bool:
        if self._check_subprocess_exit() is None:
            # Process still alive, def can't be finished yet
            return False

        return not self._open_sockets

    def wait(self) -> int:
        raise NotImplementedError(f"Don't call wait on {type(self).__name__} objects")

    def close(self):
        self.cleanup_sockets_after_kill()
        try:
            self.logger_filehandle.close()
        except OSError:
            self.log.warning(
                "Failed to close log file handle for %s",
                self.dag_file_rel_path,
                exc_info=True,
            )


def _exec_lang_sdk_runtime(
    coordinator: SubprocessCoordinator,
    *,
    path: Path,
    bundle_path: Path,
    comm_address: tuple[str, int],
    logs_address: tuple[str, int],
    status: socket,
) -> NoReturn:
    """
    Replace this Dag-parse child with the coordinator's runtime.

    The runtime's schema version, or the reason it cannot start, is written to *status* as a JSON
    line. The socket is closed on exec, which tells the parent that the runtime started.
    """

    def report_schema_version(schema_version: str | None) -> None:
        status.sendall(json.dumps({"schema_version": schema_version}).encode() + b"\n")

    try:
        coordinator.parse_dag(
            path=path,
            bundle_path=bundle_path,
            comm_address=comm_address,
            logs_address=logs_address,
            report_schema_version=report_schema_version,
        )
    except BaseException as e:
        with contextlib.suppress(BaseException):
            status.sendall(json.dumps({"error": f"{type(e).__name__}: {e}"}).encode() + b"\n")
    os._exit(127)


def _get_dag_id(data: Any) -> str | None:
    try:
        return data["dag"]["dag_id"]
    except (KeyError, TypeError):
        return None


@attrs.define(kw_only=True)
class LangSDKDagFileProcessorProcess(DagFileProcessorProcess):
    """
    Parse a native Lang-SDK Dag file with its coordinator's runtime.

    The parse child execs the runtime instead of running Python, and the runtime answers the
    ``DagFileParseRequest`` itself. It connects back to two listeners this process owns, so the
    request is sent once the runtime has connected and its schema version is known.
    """

    stdin: socket | None = None  # type: ignore[assignment]
    """The runtime's comm connection, set once the runtime connects."""

    client: Client | None = None  # type: ignore[assignment]
    """Answers the runtime's requests; without one they are relayed to ``SUPERVISOR_COMMS``."""

    logger_filehandle: BinaryIO | None = None  # type: ignore[assignment]

    _fileloc: str = attrs.field(default="", init=False)
    _parse_request: DagFileParseRequest | None = attrs.field(default=None, init=False)
    _schema_version_known: bool = attrs.field(default=False, init=False)
    _status_buffer: bytearray = attrs.field(factory=bytearray, init=False)
    _unverified_connections: list[tuple[socket, socket, str]] = attrs.field(factory=list, init=False)

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        coordinator: SubprocessCoordinator,
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
        dag_file_rel_path: str,
        selector: selectors.BaseSelector,
        logger: FilteringBoundLogger,
        **kwargs,
    ) -> Self:
        with _ResourceTracker(timeout=0) as tracker:
            comm_listener, logs_listener = tracker.track(_start_server(), _start_server())
            comm_listener.setblocking(False)
            logs_listener.setblocking(False)
            stdout_r, stdout_w = tracker.track(*socketpair())
            stderr_r, stderr_w = tracker.track(*socketpair())
            status_r, status_w = tracker.track(*socketpair())
            child_ends = (stdout_w, stderr_w, status_w)
            parent_ends = (comm_listener, logs_listener, stdout_r, stderr_r, status_r)

            pid = os.fork()
            if pid == 0:
                cls._run_child(
                    coordinator,
                    path=Path(path),
                    bundle_path=bundle_path,
                    comm_address=comm_listener.getsockname()[:2],
                    logs_address=logs_listener.getsockname()[:2],
                    parent_ends=parent_ends,
                    stdout=stdout_w,
                    stderr=stderr_w,
                    status=status_w,
                )
            for sock in tracker.untrack(*child_ends):
                sock.close()

            proc = cls(
                pid=pid,
                process=PsutilTracker(psutil.Process(pid)),
                process_log=logger,
                start_time=time.monotonic(),
                selector=selector,
                bundle_name=bundle_name,
                dag_file_rel_path=dag_file_rel_path,
                **kwargs,
            )
            proc._register_runtime_sockets(
                stdout=stdout_r,
                stderr=stderr_r,
                status=status_r,
                comm_listener=comm_listener,
                logs_listener=logs_listener,
            )
            tracker.untrack(*parent_ends)

        proc._fileloc = os.fspath(path)
        proc._parse_request = DagFileParseRequest(
            file=os.fspath(path), bundle_path=bundle_path, bundle_name=bundle_name
        )
        return proc

    @classmethod
    def run(
        cls,
        *,
        coordinator: SubprocessCoordinator,
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
        dag_file_rel_path: str,
        timeout: float | None,
        logger: FilteringBoundLogger,
    ) -> DagFileParsingResult:
        """
        Parse *path* outside the Dag processor and wait for the result.

        The runtime's requests are relayed to the process this one runs in, such as a task.

        :raises TimeoutError: if the parse does not finish within *timeout* seconds. The runtime is
            killed.
        """
        with selectors.DefaultSelector() as selector:
            proc = cls.start(
                id=uuid7(),
                coordinator=coordinator,
                path=path,
                bundle_path=bundle_path,
                bundle_name=bundle_name,
                dag_file_rel_path=dag_file_rel_path,
                selector=selector,
                logger=logger,
            )
            try:
                while not proc.is_ready:
                    wait = 0.1
                    if timeout is not None:
                        if (remaining := proc.start_time + timeout - time.monotonic()) <= 0:
                            proc.kill(signal.SIGKILL)
                            raise TimeoutError(
                                f"The Lang-SDK runtime did not parse {os.fspath(path)} within {timeout}s"
                            )
                        wait = min(wait, remaining)
                    proc._service_subprocess(max_wait_time=wait)
            finally:
                proc.close()
        return cast("DagFileParsingResult", proc.parsing_result)

    @staticmethod
    def _run_child(
        coordinator: SubprocessCoordinator,
        *,
        path: Path,
        bundle_path: Path,
        comm_address: tuple[str, int],
        logs_address: tuple[str, int],
        parent_ends: tuple[socket, ...],
        stdout: socket,
        stderr: socket,
        status: socket,
    ) -> NoReturn:
        try:
            supervisor._reset_signals()
            signal.pthread_sigmask(signal.SIG_SETMASK, set())
            for sock in parent_ends:
                sock.close()
            devnull = os.open(os.devnull, os.O_RDONLY)
            os.dup2(devnull, 0)
            os.dup2(stdout.fileno(), 1)
            os.dup2(stderr.fileno(), 2)
            _exec_lang_sdk_runtime(
                coordinator,
                path=path,
                bundle_path=bundle_path,
                comm_address=comm_address,
                logs_address=logs_address,
                status=status,
            )
        finally:
            os._exit(127)

    def _register_runtime_sockets(
        self,
        *,
        stdout: socket,
        stderr: socket,
        status: socket,
        comm_listener: socket,
        logs_listener: socket,
    ) -> None:
        self._open_sockets.update(
            (
                (stdout, "stdout"),
                (stderr, "stderr"),
                (status, "status"),
                (comm_listener, "comm-listener"),
                (logs_listener, "logs-listener"),
            )
        )
        target_loggers = self._get_target_loggers()
        self.selector.register(
            stdout, selectors.EVENT_READ, self._create_log_forwarder(target_loggers, "task.stdout", data=b"")
        )
        self.selector.register(
            stderr,
            selectors.EVENT_READ,
            self._create_log_forwarder(target_loggers, "task.stderr", data=b"", log_level=logging.ERROR),
        )
        self.selector.register(status, selectors.EVENT_READ, (self._read_status, self._on_socket_closed))
        for listener, kind in ((comm_listener, "comm"), (logs_listener, "logs")):
            self.selector.register(
                listener,
                selectors.EVENT_READ,
                (functools.partial(self._accept_connection, kind=kind), self._on_socket_closed),
            )

    def _accept_connection(self, listener: socket, *, kind: str) -> bool:
        try:
            conn, _ = listener.accept()
        except BlockingIOError:
            # cleanup_sockets_after_kill() calls this until it returns False.
            return self._exit_code is None
        conn.setblocking(True)
        self._unverified_connections.append((conn, listener, kind))
        self._verify_connections()
        return True

    def _verify_connections(self) -> None:
        """
        Use each accepted connection once it is confirmed to come from the runtime.

        A connection that is not visible yet stays pending and is checked again on the next
        ``is_ready`` poll, so the caller's loop never waits here.
        """
        pending = []
        for conn, listener, kind in self._unverified_connections:
            if listener.fileno() == -1:
                # The runtime already connected this channel.
                conn.close()
                continue
            try:
                owned = _is_connection_from_pid(conn, self.pid)
            except OSError:
                conn.close()
                continue
            if not owned:
                pending.append((conn, listener, kind))
                continue
            self._on_socket_closed(listener)
            listener.close()
            if kind == "comm":
                self._register_comm(conn)
            else:
                self._register_logs(conn)
        self._unverified_connections = pending

    def _register_comm(self, conn: socket) -> None:
        self.stdin = conn
        self._open_sockets[conn] = "requests"
        self.selector.register(
            conn,
            selectors.EVENT_READ,
            length_prefixed_frame_reader(
                self.handle_requests(self.process_log), on_close=self._on_socket_closed
            ),
        )
        self._send_parse_request()

    def _register_logs(self, conn: socket) -> None:
        self._open_sockets[conn] = "logs"
        self.selector.register(
            conn,
            selectors.EVENT_READ,
            make_buffered_socket_reader(
                process_log_messages_from_subprocess(self._get_target_loggers()),
                on_close=self._on_socket_closed,
            ),
        )

    def _read_status(self, sock: socket) -> bool:
        if chunk := sock.recv(4096):
            self._status_buffer.extend(chunk)
            return True
        try:
            status = json.loads(self._status_buffer.splitlines()[-1])
        except (IndexError, ValueError):
            status = {"error": "the parse process exited before starting the runtime"}
        if "error" in status:
            self._set_import_error(f"Cannot start the Lang-SDK runtime: {status['error']}")
        else:
            self._subprocess_schema_version = status["schema_version"]
            self._schema_version_known = True
            self._send_parse_request()
        return False

    def _send_parse_request(self) -> None:
        if not self._schema_version_known or self.stdin is None or self._parse_request is None:
            return
        request, self._parse_request = self._parse_request, None
        self.send_msg(request, request_id=0)

    def _set_import_error(self, message: str) -> None:
        self.parsing_result = DagFileParsingResult(
            fileloc=self._fileloc,
            serialized_dags=[],
            import_errors={self.dag_file_rel_path: message},
        )

    def _handle_request(self, msg, log: FilteringBoundLogger, req_id: int) -> None:
        if self.client is None and not isinstance(msg, DagFileParsingResult):
            self._relay_request(msg, log, req_id)
            return
        super()._handle_request(msg, log, req_id)

    def _relay_request(self, msg: BaseModel, log: FilteringBoundLogger, req_id: int) -> None:
        """Answer the runtime's request through ``SUPERVISOR_COMMS`` of the process this one runs in."""
        if isinstance(msg, MaskSecret):
            # This process forwards the runtime's logs, so it masks the secret as well.
            self._request_handlers[MaskSecret](self, msg, log, req_id)
        comms = getattr(task_runner, "SUPERVISOR_COMMS", None)
        if comms is None:
            self.send_msg(
                None,
                request_id=req_id,
                error=ErrorResponse(
                    detail={
                        "message": f"{type(msg).__name__} is answered only in a task or the Dag processor"
                    }
                ),
            )
            return
        try:
            response = comms.send(msg)
        except AirflowRuntimeError as e:
            self.send_msg(None, request_id=req_id, error=e.error)
            return
        self.send_msg(response, request_id=req_id)

    def _handle_parsing_result(
        self, msg: DagFileParsingResult, log: FilteringBoundLogger, req_id: int
    ) -> RequestResult:
        import_errors = dict(msg.import_errors or {})
        serialized_dags = []
        for dag in msg.serialized_dags:
            try:
                DagSerialization.validate_schema(dag.data)
                DagSerialization.from_dict(copy.deepcopy(dag.data))
            except Exception as e:
                message = (
                    f"Cannot load the serialized Dag {_get_dag_id(dag.data)!r}: "
                    f"{type(e).__name__}: {getattr(e, 'message', e)}"
                )
                self.process_log.warning(message)
                previous = import_errors.get(self.dag_file_rel_path)
                import_errors[self.dag_file_rel_path] = f"{previous}\n{message}" if previous else message
                continue
            serialized_dags.append(dag)
        self.parsing_result = msg.model_copy(
            update={"serialized_dags": serialized_dags, "import_errors": import_errors or None}
        )
        return None, {}

    @property
    def is_ready(self) -> bool:
        self._verify_connections()
        if self._check_subprocess_exit() is None:
            return False
        self._close_unused_connections()
        if self._open_sockets:
            return False
        if self.parsing_result is None:
            self._set_import_error(
                f"The Lang-SDK runtime exited with code {self._exit_code} without a parse result"
            )
        return True

    def _close_unused_connections(self) -> None:
        """Close the listeners of a runtime that never connected, and connections never verified."""
        for sock, socket_type in list(self._open_sockets.items()):
            if socket_type.endswith("-listener"):
                self._on_socket_closed(sock)
                sock.close()
        for conn, _, _ in self._unverified_connections:
            conn.close()
        self._unverified_connections = []

    def close(self) -> None:
        for conn, _, _ in self._unverified_connections:
            conn.close()
        self._unverified_connections = []
        if self.logger_filehandle is None:
            self.cleanup_sockets_after_kill()
        else:
            super().close()
