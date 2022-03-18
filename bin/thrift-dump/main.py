#!/usr/bin/env python3
# Copyright (c) -2023 curoky(cccuroky@gmail.com).
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
from enum import Enum
from pathlib import Path

import sqlmodel
import typer
from thriftpy2.rpc import TThreadedServer
from thriftpy2.transport import TServerSocket, TSocket

from thriftoy import ProtocolType, TransportType
from thriftoy.TMemoryComplexTransport import TMemoryComplexTransportFactory
from thriftoy.TUnPackedProcessor import ThriftMessage, TUnPackedProcessor

app = typer.Typer()


class StorageType(str, Enum):
    DIRECTORY = "dir"
    SQLITE = "sqlite"
    JSON = "json"


class DumpProcessor(TUnPackedProcessor):
    def __init__(self, storage_type: StorageType, output_dir: Path, limit: int) -> None:
        self.storage_type = storage_type
        self.output_dir = output_dir
        self.limit = limit

        if self.storage_type == StorageType.SQLITE:
            self.engine = sqlmodel.create_engine(f"sqlite:///{output_dir}/data.db", echo=True)
            sqlmodel.SQLModel.metadata.create_all(self.engine)

    # def process(self, client: TSocket, message: ThriftMessage):
    def process_message(self, socket: TSocket, message: ThriftMessage):
        if self.storage_type == StorageType.SQLITE:
            with sqlmodel.Session(self.engine) as session:
                session.add(message)
                session.commit()


@app.command()
def main(
    host: str = "0.0.0.0",
    port: int = 6000,
    output_dir: Path = Path("."),
    limit: int = 100,
    transport_type: TransportType = TransportType.FRAMED,
    protocol_type: ProtocolType = ProtocolType.BINARY,
    storage_type: StorageType = StorageType.JSON,
):
    logging.info("start recording server on %s:%s", host, port)
    server_socket = TServerSocket(host=host, port=port, client_timeout=10000)
    processor = DumpProcessor(storage_type=storage_type, output_dir=output_dir, limit=limit)

    server = TThreadedServer(
        processor=processor,
        trans=server_socket,
        iprot_factory=protocol_type.get_factory(),
        itrans_factory=TMemoryComplexTransportFactory(transport_type),
    )

    # server = TRecordServer(
    #     trans=server_socket,
    #     processor=processor,
    #     transport_type=transport_type,
    #     protocol_type=protocol_type,
    # )
    server.serve()


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    app()
