# Copyright (c) 2022-2024 curoky(cccuroky@gmail.com).
#
# This file is part of thriftoy.
# See https://github.com/curoky/thriftoy for further info.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import threading
import time
from datetime import datetime
from enum import Enum
from pathlib import Path

import sqlmodel
import typer
from thriftpy2.rpc import TThreadedServer
from thriftpy2.transport import TServerSocket

from ..common.message import TMessage
from ..common.message_extracted_processor import TMessageExtractedProcessor
from ..common.types import ProtocolType, TransportType


class StorageType(str, Enum):
    DIRECTORY = "dir"
    SQLITE = "sqlite"
    JSON = "json"


class TMessageDumpProcessor(TMessageExtractedProcessor):
    def __init__(
        self,
        engine,
        transport_type: TransportType,
        save_size_limit=-1,
        save_time_limit=-1,
        monitor_step_duration=10,
    ) -> None:
        self.engine = engine
        self.start_time = datetime.now()
        self.saved_size = 0
        self.save_size_limit = save_size_limit
        self.save_time_limit = save_time_limit
        self.monitor_step_duration = monitor_step_duration
        self.monitor_stop = False
        self.monitor_thread = threading.Thread(target=self.monitor)
        self.monitor_thread.start()
        super().__init__(transport_type)

    def set_close_server_cb(self, close_server_cb):
        self.close_server_cb = close_server_cb

    def monitor(self):
        while not self.monitor_stop:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            logging.info("elapsed %ds, dumped size: %d", elapsed, self.saved_size)
            time.sleep(self.monitor_step_duration)

    def check_stop(self):
        if self.save_size_limit > 0 and self.saved_size > self.save_size_limit:
            logging.info(
                "stop: saved_size %d > save_size_limit %d", self.saved_size, self.save_size_limit
            )
            self.close_server_cb()
            self.monitor_stop = True
        sec = (datetime.now() - self.start_time).total_seconds()
        if self.save_time_limit > 0 and sec > self.save_time_limit:
            logging.info("stop: duration sec %d > save_time_limit %d", sec, self.save_time_limit)
            self.close_server_cb()
            self.monitor_stop = True

    def handle_message(self, message: TMessage, iprot, oprot):
        logging.debug("[handle_message]: method=%s, size=%d", message.method, len(message.data))
        self.saved_size += 1
        self.check_stop()
        with sqlmodel.Session(self.engine) as session:
            session.add(message)
            session.commit()


def startDumpService(
    host: str,
    port: int,
    processor,
    protocol_type: ProtocolType = ProtocolType.BINARY,
    transport_type: TransportType = TransportType.FRAMED,
):
    server_socket = TServerSocket(host=host, port=port, client_timeout=10000)
    server = TThreadedServer(
        processor=processor,
        trans=server_socket,
        itrans_factory=transport_type.get_factory(),
        iprot_factory=protocol_type.get_factory(),
    )

    def close_server():
        server.close()
        logging.info("Server closed")

    processor.set_close_server_cb(close_server)
    server.serve()


app = typer.Typer()


@app.command()
def main(
    db_path: Path,
    listen_host: str = "0.0.0.0",
    listen_port: int = 6000,
    dump_limit: int = 100,
    transport_type: TransportType = TransportType.FRAMED,
    protocol_type: ProtocolType = ProtocolType.BINARY,
    clean_db: bool = False,
    verbose: bool = False,
):
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    logging.info("start dumpping server on %s:%s", listen_host, listen_port)

    storage_engine = sqlmodel.create_engine(f"sqlite:///{db_path}", echo=verbose)
    if clean_db:
        sqlmodel.SQLModel.metadata.drop_all(storage_engine)
    sqlmodel.SQLModel.metadata.create_all(storage_engine, tables=[TMessage.__table__])

    processor = TMessageDumpProcessor(
        storage_engine, save_size_limit=dump_limit, transport_type=transport_type
    )
    startDumpService(
        listen_host,
        listen_port,
        processor,
        transport_type=transport_type,
        protocol_type=protocol_type,
    )
