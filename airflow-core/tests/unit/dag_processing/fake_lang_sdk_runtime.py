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
A stand-in Lang-SDK runtime that parses a ``.native`` Dag file.

It speaks the supervisor framing over ``--comm`` and ``--logs`` as the TypeScript and Java runtimes
do, and never imports Airflow. The ``.native`` file is JSON; its keys choose what the runtime does:

* ``dags``: the Dag ids to return, each with one stub task ``extract``.
* ``get_variable``: a variable to request before replying; the reply is recorded in each Dag's
  description.
* ``exit_before_connect``: exit with this code without connecting.
* ``sleep``: seconds to wait after the request arrives.
* ``invalid``: return Dags that do not deserialize.
* ``import_errors``: import errors to return, keyed as the runtime keys them.
"""

from __future__ import annotations

import datetime
import json
import os
import socket
import sys
import time
from pathlib import Path

import msgspec


def _get_option(name: str) -> str:
    return next(arg.split("=", 1)[1] for arg in sys.argv[2:] if arg.startswith(f"--{name}="))


def _connect(address: str) -> socket.socket:
    host, port = address.rsplit(":", 1)
    return socket.create_connection((host, int(port)))


def _send_frame(sock: socket.socket, frame: list) -> None:
    body = msgspec.msgpack.encode(frame)
    sock.sendall(len(body).to_bytes(4, "big") + body)


def _read_exactly(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        if not (chunk := sock.recv(size - len(data))):
            raise EOFError("comm closed")
        data += chunk
    return data


def _receive_frame(sock: socket.socket) -> list:
    size = int.from_bytes(_read_exactly(sock, 4), "big")
    return msgspec.msgpack.decode(_read_exactly(sock, size))


def _build_payload(dag_id: str, *, fileloc: str, bundle_path: str, description: str, invalid: bool) -> dict:
    timetable = "no.such.Timetable" if invalid else "airflow.timetables.simple.NullTimetable"
    return {
        "__version": 3,
        "dag": {
            "dag_id": dag_id,
            "description": description,
            "fileloc": fileloc,
            "relative_fileloc": os.path.relpath(fileloc, bundle_path),
            "timezone": "UTC",
            "timetable": {"__type": timetable, "__var": {}},
            "tasks": [
                {
                    "__type": "operator",
                    "__var": {
                        "task_id": "extract",
                        "task_type": "FakeOperator",
                        "_task_module": "fake.runtime",
                        "language": "fake",
                        "template_fields": [],
                        "is_stub": True,
                    },
                }
            ],
            "dag_dependencies": [],
            "task_group": {
                "_group_id": None,
                "group_display_name": "",
                "prefix_group_id": True,
                "tooltip": "",
                "ui_color": "CornflowerBlue",
                "ui_fgcolor": "#000",
                "children": {"extract": ["operator", "extract"]},
                "upstream_group_ids": [],
                "downstream_group_ids": [],
                "upstream_task_ids": [],
                "downstream_task_ids": [],
            },
            "edge_info": {},
            "params": [],
            "deadline": None,
            "allowed_run_types": None,
            "max_active_tasks": 16,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "catchup": False,
            "disable_bundle_versioning": False,
        },
    }


def main() -> None:
    spec = json.loads(Path(sys.argv[1]).read_text())
    if (code := spec.get("exit_before_connect")) is not None:
        print("exiting before connecting", file=sys.stderr, flush=True)
        os._exit(code)

    logs = _connect(_get_option("logs"))
    comm = _connect(_get_option("comm"))
    print("fake runtime started", flush=True)
    record = {
        "event": "fake runtime connected",
        "level": "info",
        "logger": "fake_runtime",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    logs.sendall(json.dumps(record).encode() + b"\n")

    request_id, request = _receive_frame(comm)[:2]
    time.sleep(spec.get("sleep", 0))

    reply = None
    if key := spec.get("get_variable"):
        _send_frame(comm, [1, {"type": "GetVariable", "key": key}])
        _, body, error = _receive_frame(comm)
        reply = {"body": body, "error": error}
    description = json.dumps({"request": request, "reply": reply})

    result = {
        "type": "DagFileParsingResult",
        "fileloc": request["file"],
        "serialized_dags": [
            {
                "data": _build_payload(
                    dag_id,
                    fileloc=request["file"],
                    bundle_path=request["bundle_path"],
                    description=description,
                    invalid=spec.get("invalid", False),
                )
            }
            for dag_id in spec.get("dags", [])
        ],
        "import_errors": spec.get("import_errors"),
    }
    _send_frame(comm, [request_id, result])
    _receive_frame(comm)
    comm.close()
    logs.close()


if __name__ == "__main__":
    main()
