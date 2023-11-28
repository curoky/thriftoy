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
import socket

from thriftpy2.protocol import TBinaryProtocolFactory
from thriftpy2.rpc import TClient, TSocket
from thriftpy2.transport import TFramedTransportFactory


class ThriftClient(TClient):
    def __init__(self, service, iprot, oprot=None):
        super().__init__(service, iprot, oprot)

    def rpc_with_data(self, method: str, data: bytes):
        socket: TSocket = self._oprot.trans._trans._trans
        socket.write(data)
        socket.flush()
        return self._recv(method)


def make_client(
    service,
    host,
    port,
    proto_factory=TBinaryProtocolFactory(),  # noqa: B008
    trans_factory=TFramedTransportFactory(),  # noqa: B008
    socket_family=None,
    timeout=3000,
):
    if socket_family is None:
        socket_family = socket.AF_INET
        if ":" in host:
            socket_family = socket.AF_INET6
    tsocket = TSocket(host, port, socket_family=socket_family, socket_timeout=timeout)

    transport = trans_factory.get_transport(tsocket)
    protocol = proto_factory.get_protocol(transport)
    transport.open()
    return ThriftClient(service, protocol)
