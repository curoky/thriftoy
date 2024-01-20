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
import random
import time

import locust
from thriftpy2.protocol import TBinaryProtocolFactory
from thriftpy2.transport import TFramedTransportFactory

from ..client.simple_client import make_simple_client
from ..common.socket import TSimpleSocket


class ThriftWithoutIDLUser(locust.User):
    abstract = True

    remote_hosts: list[str]
    remote_ports: list[int]
    local_bound_hosts: list[str] = []
    timeout = 2000

    def __init__(self, environment):
        super().__init__(environment)
        self.socket = self.create_and_open_socket()

    def create_and_open_socket(self):
        idx = random.randint(0, len(self.remote_hosts) - 1)
        local_host = None
        if len(self.local_bound_hosts) == len(self.remote_hosts):
            local_host = self.local_bound_hosts[idx]
        tsocket = TSimpleSocket(
            remote_host=self.remote_hosts[idx],
            remote_port=self.remote_ports[idx],
            local_host=local_host,
        )
        tsocket.open()
        return tsocket

    def request(self, method, data):
        start_perf_counter = time.perf_counter()
        exception = None
        try:
            self.socket.write(data)
            # TFramedTransportFactory().get_transport(self.socket).read(4)
            # socket.close()
        except Exception as e:
            logging.error("write failed: %s", e)
            self.socket = self.create_and_open_socket()
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

    remote_hosts: list[str]
    remote_ports: list[int]
    timeout = 3000

    service = None
    protocol_factory = TBinaryProtocolFactory
    transport_factory = TFramedTransportFactory

    def __init__(self, environment):
        super().__init__(environment)
        idx = random.randint(0, len(self.remote_hosts) - 1)
        self.client = make_simple_client(
            service=self.service,
            host=self.remote_hosts[idx],
            port=self.remote_ports[idx],
            timeout=self.timeout,
            proto_factory=self.protocol_factory(),
            trans_factory=self.transport_factory(),
        )

    def request(self, method: str, args):
        start_perf_counter = time.perf_counter()
        exception = None
        res = None
        try:
            # res = self.client.__getattr__(method)(*args, **kwargs)
            logging.debug("ThriftUser::request method:%s", method)
            res = self.client.call(method, args)
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
