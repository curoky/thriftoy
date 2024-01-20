#!/usr/bin/env python3
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

import typer
from thriftpy2.rpc import TServerSocket, TSocket, TThreadedServer
from thriftpy2.transport.framed import TFramedTransportFactory

from thriftoy.common.memory_wrapped_transport import TMemoryWrappedTransportFactory
from thriftoy.common.message import TMessage
from thriftoy.common.message_extracted_processor import TMessageExtractedProcessor
from thriftoy.common.types import ProtocolType, TransportType


class ProxyProcessor(TMessageExtractedProcessor):
    def __init__(self, to_host: str, to_port: int) -> None:
        self.to_host = to_host
        self.to_port = to_port

    def handle_message(self, from_socket: TSocket, message: TMessage):
        to_socket = TSocket(self.to_host, self.to_port)
        to_otrans = TMemoryWrappedTransportFactory(TransportType.FRAMED).get_transport(to_socket)
        to_otrans.open()
        to_otrans.write(message.data)
        to_otrans.flush()

        to_itrans = TFramedTransportFactory().get_transport(to_socket)
        to_itrans.read(4)
        from_socket.write(to_itrans._trans._rbuf.getvalue())


app = typer.Typer()


@app.command()
def main(host: str = "0.0.0.0", port: int = 6000, to_host: str = "0.0.0.0", to_port: int = 6001):
    logging.info(f"start: {host}:{port} -> {to_host}:{to_port}")
    server_socket = TServerSocket(host=host, port=port, client_timeout=10000)
    processor = ProxyProcessor(to_host=to_host, to_port=to_port)
    server = TThreadedServer(
        processor=processor,
        trans=server_socket,
        iprot_factory=ProtocolType.BINARY.get_factory(),
        itrans_factory=TMemoryWrappedTransportFactory(TransportType.BUFFERED),
    )

    server.serve()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app()
