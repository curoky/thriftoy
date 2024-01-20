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
import multiprocessing
import os
import threading
import time
from collections.abc import Callable
from datetime import datetime
from enum import Enum
from queue import Queue
from typing import Any

import sqlmodel
from thriftpy2.rpc import TThreadedServer
from thriftpy2.transport import TServerSocket, TSocket

from ..common.message import TMessage
from ..common.types import ProtocolType, TransportType
from .memory_wrapped_transport import TMemoryWrappedTransportFactory
from .message_extracted_processor import TMessageExtractedProcessor


class StorageType(str, Enum):
    DIRECTORY = "dir"
    SQLITE = "sqlite"
    JSON = "json"


class SimpleDBSaver:
    def __init__(
        self,
        engine,
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
        self.monitor_thread = threading.Thread(target=self.monitor)
        self.monitor_thread.start()

    def monitor(self):
        while True:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            logging.info("elapsed %ds, dumped size: %d", elapsed, self.saved_size)
            time.sleep(self.monitor_step_duration)

    def check_stop(self):
        if self.save_size_limit > 0 and self.saved_size > self.save_size_limit:
            logging.info(
                "stop: saved_size %d > save_size_limit %d", self.saved_size, self.save_size_limit
            )
            os._exit(0)
        sec = (datetime.now() - self.start_time).total_seconds()
        if self.save_time_limit > 0 and sec > self.save_time_limit:
            logging.info("stop: duration sec %d > save_time_limit %d", sec, self.save_time_limit)
            os._exit(0)

    def push(self, message: TMessage):
        self.saved_size += 1
        self.check_stop()
        while True:
            with sqlmodel.Session(self.engine) as session:
                session.add(message)
                session.commit()


class MultiProcessorDBSaver(SimpleDBSaver):
    def __init__(
        self,
        engine,
        transform: Callable[[TMessage], Any],
        save_size_limit=-1,
        save_time_limit=-1,
        monitor_step_duration=10,
    ) -> None:
        super().__init__(
            engine,
            save_size_limit=save_size_limit,
            save_time_limit=save_time_limit,
            monitor_step_duration=monitor_step_duration,
        )
        self.pool = multiprocessing.Pool(10)
        self.result_queue = Queue()
        self.save_bg_thread = threading.Thread(target=self.save)
        self.save_bg_thread.start()
        self.transform = transform

    def push(self, message: TMessage):
        self.saved_size += 1
        self.check_stop()
        self.result_queue.put(self.pool.apply_async(self.transform, args=(message,)))

    def save(self):
        logging.info("start thread for saving")
        while True:
            result = self.result_queue.get().get()
            logging.debug("MultiProcessorDBSaver: get messages from queue")
            if result and len(result) != 0:
                with sqlmodel.Session(self.engine) as session:
                    session.add_all(result)
                    session.commit()


class TMessageDumpProcessor(TMessageExtractedProcessor):
    def __init__(self, saver) -> None:
        self.saver = saver

    def handle_message(self, socket: TSocket, message: TMessage):
        logging.debug("[handle_message]: method=%s, size=%d", message.method, len(message.data))
        self.saver.push(message)


def startDumpService(
    host: str,
    port: int,
    processor,
    protocol_type: ProtocolType = ProtocolType.BINARY,
    transport_type: TransportType = TransportType.FRAMED,
):
    server_socket = TServerSocket(host=host, port=port, client_timeout=10000)
    logging.info("start dump service on %s:%d", host, port)
    server = TThreadedServer(
        processor=processor,
        trans=server_socket,
        itrans_factory=TMemoryWrappedTransportFactory(transport_type),
        iprot_factory=protocol_type.get_factory(),
    )
    server.serve()
