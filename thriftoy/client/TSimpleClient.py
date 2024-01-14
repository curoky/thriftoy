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


from thriftpy2.protocol.binary import TBinaryProtocolFactory
from thriftpy2.rpc import TClient
from thriftpy2.transport.framed import TFramedTransportFactory

from ..common.TMessage import TMessage
from ..common.TSimpleSocket import TSimpleSocket


class TSimpleClient(TClient):
    """
    Simplify `TClient`.
    """

    def __init__(
        self,
        service,
        iprot,
        oprot=None,
    ):
        super().__init__(service, iprot, oprot)

    def send_message(self, message: TMessage):
        return self.call(message.method, message.extract_args(self._service))

    def call(self, method: str, args):
        return TClient.__getattr__(self, method)(*list(vars(args).values()))


def make_simple_client(
    host,
    port,
    service,
    proto_factory=TBinaryProtocolFactory(),  # noqa: B008
    trans_factory=TFramedTransportFactory(),  # noqa: B008
    socket_family=None,
    timeout=3000,
) -> TSimpleClient:
    tsocket = TSimpleSocket(host, port, socket_family=socket_family, socket_timeout=timeout)
    transport = trans_factory.get_transport(tsocket)
    protocol = proto_factory.get_protocol(transport)
    transport.open()
    return TSimpleClient(service, protocol)
