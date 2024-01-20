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
from thriftpy2.rpc import TClient, TSocket
from thriftpy2.thrift import TApplicationException, TMessageType
from thriftpy2.transport.framed import TFramedTransportFactory

from ..common.message import TMessage
from ..common.socket import TSimpleSocket
from ..common.types import TransportType
from ..dump.memory_wrapped_transport import TMemoryWrappedTransportFactory
from ..dump.message_extracted_processor import EmptyThriftStruct


class TUnServicedClient(TClient):
    """
    Enhance `TClient` to perform RPC call using serilized data direactly.
    """

    def __init__(self, iprot, oprot=None):
        super().__init__(None, iprot, oprot)

    def call(self, message: TMessage, recv_message: bool = False) -> TMessage | None:
        socket: TSocket = self._oprot.trans._trans._trans
        socket.write(message.data)
        socket.flush()

        if recv_message:
            return self.recv_message()
        else:
            return None

    def recv_message(self) -> TMessage:
        fname, mtype, rseqid = self._iprot.read_message_begin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(self._iprot)
            self._iprot.read_message_end()
            raise x
        self._iprot.read_struct(EmptyThriftStruct())
        self._iprot.read_message_end()
        data = self._iprot.getvalue()
        self._iprot.clearbuffer()
        return TMessage(method=fname, data=data, seqid=rseqid, type=mtype)


def make_unserivced_client(
    host,
    port,
    proto_factory=TBinaryProtocolFactory(),  # noqa: B008
    trans_factory=TFramedTransportFactory(),  # noqa: B008
    socket_family=None,
    timeout=3000,
) -> TUnServicedClient:
    tsocket = TSimpleSocket(host, port, socket_family=socket_family, socket_timeout=timeout)
    otransport = trans_factory.get_transport(tsocket)
    oprotocol = proto_factory.get_protocol(otransport)
    otransport.open()

    itransport = TMemoryWrappedTransportFactory(TransportType.create(otransport)).get_transport(
        tsocket
    )
    iprotocol = proto_factory.get_protocol(itransport)
    return TUnServicedClient(oprot=oprotocol, iprot=iprotocol)
