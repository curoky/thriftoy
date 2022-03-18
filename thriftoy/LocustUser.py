# Copyright (c) -2023 curoky(cccuroky@gmail.com).
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
import time

import locust
import thriftpy2
import thriftpy2.protocol
import thriftpy2.rpc
import thriftpy2.transport

from .ObjectPool import ObjectPool


class ThriftUser(locust.User):
    abstract = True
    ip: str = "0.0.0.0"
    port: int = 8000
    timeout = 2000
    conn_size = 1000

    service = None
    protocol_factory = thriftpy2.protocol.TCyBinaryProtocolFactory
    transport_factory = thriftpy2.transport.TCyFramedTransportFactory

    def __init__(self, environment):
        super().__init__(environment)

        def creator():
            return thriftpy2.rpc.make_client(
                self.service,
                host=self.ip,
                port=self.port,
                timeout=self.timeout,
                proto_factory=self.protocol_factory(),
                trans_factory=self.transport_factory(),
            )

        self.client_pool = ObjectPool(creator, self.conn_size)

    def call(self, method, *args, **kwargs):
        # client = self.client_pool.get()
        client = self.client_pool.creator()
        start_perf_counter = time.perf_counter()
        exception = None
        res = None
        try:
            res = client.__getattr__(method)(*args, **kwargs)
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
        # self.client_pool.put(client)
        return res
