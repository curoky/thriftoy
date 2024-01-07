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
from concurrent.futures import ThreadPoolExecutor

import thriftpy2
from sqlmodel import Session, create_engine, select
from thriftpy2.protocol.binary import TBinaryProtocolFactory
from thriftpy2.rpc import TSocket, make_client
from thriftpy2.transport.framed import TFramedTransportFactory

from thriftoy.common.TMessage import TMessage

echo_thrift = thriftpy2.load("../echo/echo.thrift", module_name="echo_thrift")


def get_thrift_message(path: str, method: str, limit: int) -> list[TMessage]:
    engine = create_engine(path)
    messages = []
    with Session(engine) as session:
        statement = select(TMessage).where(TMessage.method == method).limit(limit)
        results = session.exec(statement)
        for result in results:
            messages.append(result)
    print(f"load thrift message size: {len(messages)}")
    return messages


def run_send_idl_message(
    host,
    port,
    method,
    messages,
    service,
    proto_factory=TBinaryProtocolFactory,
    trans_factory=TFramedTransportFactory,
):
    client = make_client(
        service,
        host=host,
        port=port,
        timeout=3000,
        proto_factory=proto_factory(),
        trans_factory=trans_factory(),
    )
    idx = 0
    while True:
        idx = (idx + 1) % len(messages)
        message = messages[idx]
        args = message.extract_args(service, method=method)
        try:
            client.__getattr__(method)(*args)
        except Exception as e:
            logging.error("rpc call failed: %s", e)


def run_send_raw_message(host, port, messages):
    idx = 0
    socket = TSocket(host=host, port=port)
    socket.open()
    while True:
        idx = (idx + 1) % len(messages)
        message = messages[idx]
        try:
            socket.write(message.data)
            # socket.close()
        except Exception as e:
            logging.error("rpc call failed: %s", e)


def main():
    hosts = ["0.0.0.0", "0.0.0.0"]
    ports = [6000, 6000]
    pool = ThreadPoolExecutor(1000)
    messages = get_thrift_message("sqlite:///../../thrift-dump/data.db", method="echo", limit=100)
    for idx in range(200):
        pool.submit(run_send_raw_message, hosts[idx % 2], ports[idx % 2], messages)
    pool.shutdown(wait=True)


if __name__ == "__main__":
    main()
