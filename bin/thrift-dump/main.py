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
from pathlib import Path

import sqlmodel
import typer

from thriftoy.common.TMessage import TMessage
from thriftoy.common.TTypes import ProtocolType, TransportType
from thriftoy.dump.TDumpService import SimpleDBSaver, TMessageDumpProcessor, startDumpService

app = typer.Typer()


@app.command()
def main(
    db_path: Path,
    listen_host: str = "0.0.0.0",
    listen_port: int = 6000,
    dump_limit: int = 100,
    transport_type: TransportType = TransportType.FRAMED,
    protocol_type: ProtocolType = ProtocolType.BINARY,
):
    logging.info("start recording server on %s:%s", listen_host, listen_port)

    storage_engine = sqlmodel.create_engine(f"sqlite:///{db_path}", echo=True)
    sqlmodel.SQLModel.metadata.create_all(storage_engine, tables=[TMessage.__table__])

    saver = SimpleDBSaver(storage_engine, save_size_limit=dump_limit)
    processor = TMessageDumpProcessor(saver=saver)
    startDumpService(
        listen_host,
        listen_port,
        processor,
        transport_type=transport_type,
        protocol_type=protocol_type,
    )


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    app()
