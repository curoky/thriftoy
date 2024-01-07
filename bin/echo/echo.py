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

import thriftpy2
import typer
from thriftpy2.rpc import make_client, make_server

from thriftoy.common.TTypes import ProtocolType, TransportType

app = typer.Typer()
echo_thrift = thriftpy2.load("echo.thrift", module_name="echo_thrift")


class Dispatcher:
    def echo(self, param):
        print("receive:" + param)
        return param


@app.command()
def server(
    host: str = "0.0.0.0",
    port: int = 6000,
    protocol_type: ProtocolType = ProtocolType.BINARY,
    transport_type: TransportType = TransportType.FRAMED,
):
    logging.info("start server on %s:%d", host, port)
    server = make_server(
        echo_thrift.EchoService,
        Dispatcher(),
        host=host,
        port=port,
        trans_factory=transport_type.get_factory(),
        proto_factory=protocol_type.get_factory(),
    )
    server.serve()


@app.command()
def client(
    host: str = "0.0.0.0",
    port: int = 6000,
    protocol_type: ProtocolType = ProtocolType.BINARY,
    transport_type: TransportType = TransportType.FRAMED,
):
    client = make_client(
        echo_thrift.EchoService,
        host=host,
        port=port,
        trans_factory=transport_type.get_factory(),
        proto_factory=protocol_type.get_factory(),
    )

    print(client.echo("hello, world"))
    client.close()


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    app()
