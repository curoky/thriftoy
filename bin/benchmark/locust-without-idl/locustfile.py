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


import locust
from sqlmodel import Session, create_engine, select

from thriftoy.LocustUser import ThriftWithoutIDLUser
from thriftoy.ThriftMessage import ThriftMessage


def get_thrift_message(path: str) -> list[ThriftMessage]:
    engine = create_engine(path)
    messages = []
    with Session(engine) as session:
        statement = select(ThriftMessage)
        results = session.exec(statement)
        for result in results:
            messages.append(result)
    print(f"load size: {len(messages)}")
    return messages


class MyThriftUser(ThriftWithoutIDLUser):
    wait_time = locust.between(0.009, 0.011)

    method = "echo"
    hosts = ["0.0.0.0", "0.0.0.0"]
    ports = [6000, 6000]
    messages = get_thrift_message("sqlite:///../../thrift-dump/data.db")

    def __init__(self, environment):
        super().__init__(environment)
        self.index = 0

    @locust.task
    def echo(self):
        self.index = (self.index + 1) % len(self.messages)
        message = self.messages[self.index]
        self.send(method=self.method, data=message.data)
