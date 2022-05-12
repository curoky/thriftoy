# Copyright (c) 2023-2023 curoky(cccuroky@gmail.com).
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

from enum import Enum
from pathlib import Path

import sqlmodel
from thriftpy2.transport import TSocket

from . import ProtocolType, TransportType
from .ThriftMessage import ThriftMessage
from .TUnPackedProcessor import TUnPackedProcessor


class StorageType(str, Enum):
    DIRECTORY = "dir"
    SQLITE = "sqlite"
    JSON = "json"


class TMessageDumpProcessor(TUnPackedProcessor):
    def __init__(
        self,
        output_path: Path,
        limit: int,
        storage_engine,
        transport_type: TransportType = TransportType.FRAMED,
        protocol_type: ProtocolType = ProtocolType.BINARY,
    ) -> None:
        super().__init__(transport_type=transport_type, protocol_type=protocol_type)
        self.output_path = output_path
        self.limit = limit
        self.processed_size = 0
        self.engine = storage_engine

    def process_message(self, socket: TSocket, message: ThriftMessage):
        self.processed_size += 1
        if self.processed_size > self.limit:
            return
        with sqlmodel.Session(self.engine) as session:
            session.add(message)
            session.commit()
