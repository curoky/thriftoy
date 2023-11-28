#!/usr/bin/env python3
# Copyright (c) 2022-2023 curoky(cccuroky@gmail.com).
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

import thriftpy2
import typer
from thriftpy2.protocol.binary import TBinaryProtocolFactory
from thriftpy2.protocol.json import struct_to_json
from thriftpy2.rpc import make_client
from thriftpy2.transport.framed import TFramedTransportFactory

from thriftoy.DBUtils import get_thrift_message

app = typer.Typer()

echo_thrift = thriftpy2.load("../echo/echo.thrift", module_name="echo_thrift")


@app.command()
def send(db_path: Path, host: str = "0.0.0.0", port: int = 6000):
    messages = get_thrift_message(f"sqlite:///{db_path}", limit=100)
    for message in messages[0:1]:
        args = message.extract_args(echo_thrift.EchoService, method="echo")
        client = make_client(
            echo_thrift.EchoService,
            host=host,
            port=port,
            proto_factory=TBinaryProtocolFactory(),
            trans_factory=TFramedTransportFactory(),
        )
        rsp = client.echo(args.param)
        print(rsp)


@app.command()
def save(db_path: Path, save_dir: Path):
    messages = get_thrift_message(f"sqlite:///{db_path}", limit=100)
    for message in messages[0:1]:
        args = message.extract_args(echo_thrift.EchoService, method="echo")
        data = struct_to_json(args)
        print(data)


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    app()
