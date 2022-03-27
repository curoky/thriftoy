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

import asyncio
import logging
import random

import thriftpy2
from sqlmodel import Session, create_engine, select
from thriftpy2.contrib.aio.rpc import TAsyncSocket

from thriftoy.ThriftMessage import ThriftMessage

echo_thrift = thriftpy2.load("../echo/echo.thrift", module_name="echo_thrift")


def get_thrift_message(path: str, method: str, limit: int) -> list[ThriftMessage]:
    engine = create_engine(path)
    messages = []
    with Session(engine) as session:
        statement = select(ThriftMessage).where(ThriftMessage.method == method).limit(limit)
        results = session.exec(statement)
        for result in results:
            messages.append(result)
    print(f"load thrift message size: {len(messages)}")
    return messages


async def consumer(messages, host, port):
    socket = TAsyncSocket(host=host, port=port, connect_timeout=10000000, socket_timeout=10000000)
    await socket.open()
    index = 0
    while True:
        index = (index + 1) % len(messages)
        message = messages[index]
        try:
            socket.write(message.data)
            await socket.flush()
            # trans = TAsyncFramedTransportFactory().get_transport(socket)
            # await trans.read(4)
        except Exception as e:
            logging.error("rpc call failed: %s", e)
    socket.close()


async def consumer2(messages, host, port):
    reader, writer = await asyncio.open_connection(host=host, port=port)
    index = 0
    while True:
        index = (index + 1) % len(messages)
        message = messages[index]
        try:
            writer.write(message.data)
            await writer.drain()
            # writer.close()
            # await writer.wait_closed()
            # socket.close()
        except Exception as e:
            logging.error("rpc call failed: %s", e)


async def producer(messages, queue: asyncio.Queue):
    while True:
        try:
            await queue.put(random.choice(messages))
            await asyncio.sleep(0.0001)
        except Exception as e:
            print(e)


async def main():
    hosts = ["0.0.0.0", "0.0.0.0"]
    ports = [6000, 6000]
    tasks = []
    messages = get_thrift_message("sqlite:///../../thrift-dump/data.db", method="echo", limit=100)
    # tasks.append(asyncio.create_task(producer(queue)))
    for index in range(1000):
        tasks.append(
            asyncio.create_task(
                consumer2(
                    messages=messages,
                    host=hosts[index % 2],
                    port=ports[index % 2],
                )
            )
        )

    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    logging.getLogger("asyncio").setLevel(logging.DEBUG)
    asyncio.run(main())
