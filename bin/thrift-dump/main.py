#!/usr/bin/env python3
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
import logging
from pathlib import Path

import typer
from thriftpy2.rpc import TThreadedServer
from thriftpy2.transport import TServerSocket

from thriftoy import ProtocolType, TransportType
from thriftoy.TMemoryComplexTransport import TMemoryComplexTransportFactory
from thriftoy.TMessageDumpProcessor import TMessageDumpProcessor

app = typer.Typer()


@app.command()
def main(
    output_path: Path,
    host: str = "0.0.0.0",
    port: int = 6000,
    limit: int = 100,
    transport_type: TransportType = TransportType.FRAMED,
    protocol_type: ProtocolType = ProtocolType.BINARY,
):
    logging.info("start recording server on %s:%s", host, port)
    server_socket = TServerSocket(host=host, port=port, client_timeout=10000)
    processor = TMessageDumpProcessor(
        output_path=output_path,
        limit=limit,
        transport_type=transport_type,
        protocol_type=protocol_type,
    )

    server = TThreadedServer(
        processor=processor,
        trans=server_socket,
        iprot_factory=protocol_type.get_factory(),
        itrans_factory=TMemoryComplexTransportFactory(transport_type),
    )

    server.serve()


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    app()
