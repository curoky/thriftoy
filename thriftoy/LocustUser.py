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
import random
import socket
import time

import locust
from thriftpy2.protocol import TBinaryProtocolFactory
from thriftpy2.rpc import TSocket, make_client
from thriftpy2.transport import TFramedTransportFactory


class ThriftWithoutIDLUser(locust.User):
    abstract = True
    hosts: list[str]
    ports: list[int]
    timeout = 2000
    socket_family = socket.AF_INET

    def __init__(self, environment):
        super().__init__(environment)
        self.socket = self._init_socket()
        self.socket.open()

    def _init_socket(self):
        idx = random.randint(0, len(self.hosts) - 1)
        return TSocket(host=self.hosts[idx], port=self.ports[idx], socket_family=self.socket_family)

    def send(self, data, method="default"):
        start_perf_counter = time.perf_counter()
        exception = None
        try:
            self.socket.write(data)
            # TFramedTransportFactory().get_transport(self.socket).read(4)
            # socket.close()
        except Exception as e:
            logging.error("write failed: %s", e)
            self.socket = self._init_socket()
            self.socket.open()
            exception = e

        self.environment.events.request.fire(
            request_type="thrift",
            name=method,
            response_time=(time.perf_counter() - start_perf_counter) * 1000,
            response_length=0,
            response=None,
            context=None,
            exception=exception,
        )


class ThriftUser(locust.User):
    abstract = True
    hosts: list[str]
    ports: list[int]
    timeout = 3000

    service = None
    protocol_factory = TBinaryProtocolFactory
    transport_factory = TFramedTransportFactory
    socket_family = socket.AF_INET

    def __init__(self, environment):
        super().__init__(environment)
        idx = random.randint(0, len(self.hosts) - 1)

        self.client = make_client(
            self.service,
            host=self.hosts[idx],
            port=self.ports[idx],
            timeout=self.timeout,
            socket_family=self.socket_family,
            proto_factory=self.protocol_factory(),
            trans_factory=self.transport_factory(),
        )

    def call(self, method, *args, **kwargs):
        start_perf_counter = time.perf_counter()
        exception = None
        res = None
        try:
            res = self.client.__getattr__(method)(*args, **kwargs)
        except Exception as e:
            exception = e
        self.environment.events.request.fire(
            request_type="thrift",
            name=method,
            response_time=(time.perf_counter() - start_perf_counter) * 1000,
            response_length=0,
            response=res,
            context=None,
            exception=exception,
        )
        return res
