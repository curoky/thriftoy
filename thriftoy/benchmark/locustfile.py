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


import os

import locust

from thriftoy.benchmark.locust_user import ThriftWithoutIDLUser
from thriftoy.common.message import get_message_from_sqlite

dbpath = os.environ.get("DB_PATH")
if dbpath is None:
    raise Exception("DB_PATH is not set")
load_req_size = int(os.environ.get("LOAD_REQ_SIZE", 1))
host = os.environ.get("HOST", "0.0.0.0")
port = os.environ.get("PORT")
if port is None:
    raise Exception("PORT is not set")

print(f"port: {port}")
print(f"host: {host}")
print(f"load_req_size: {load_req_size}")
print(f"dbpath: {dbpath}")

messages = get_message_from_sqlite(
    dbpath,
    limit=load_req_size,
)


class MyThriftUser(ThriftWithoutIDLUser):
    wait_time = locust.between(0.1, 0.1)

    remote_hosts = [host]
    remote_ports = [int(port)]

    def __init__(self, environment):
        super().__init__(environment)
        self.index = 0

    @locust.task
    def recall(self):
        self.index = (self.index + 1) % len(messages)
        message = messages[self.index]
        self.request(message.method, message.data)
