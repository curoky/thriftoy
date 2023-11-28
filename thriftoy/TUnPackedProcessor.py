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
from thriftpy2.protocol.binary import TBinaryProtocol
from thriftpy2.rpc import TSocket

from thriftoy import ProtocolType, TransportType

from .ThriftMessage import ThriftMessage
from .ThriftStruct import EmptyThriftStruct
from .TMemoryComplexTransport import TMemoryComplexTransport


class TUnPackedProcessor:
    def __init__(self, transport_type: TransportType, protocol_type: ProtocolType):
        self.transport_type = transport_type
        self.protocol_type = protocol_type

    def unpack_message(self, prot: TBinaryProtocol) -> ThriftMessage:
        method, type, seqid = prot.read_message_begin()
        prot.read_struct(EmptyThriftStruct())
        prot.read_message_end()
        data = prot.trans.getvalue()
        return ThriftMessage(method=method, type=type, seqid=seqid, data=data)

    def process(self, iprot: TBinaryProtocol, oprot: TBinaryProtocol):
        itrans: TMemoryComplexTransport = iprot.trans
        message = self.unpack_message(iprot)
        socket: TSocket = itrans._trans
        assert socket.sock is not None
        message.from_host, message.from_port = socket.sock.getpeername()
        message.listen_host, message.listen_port = socket.sock.getsockname()
        message.transport_type = self.transport_type
        message.protocol_type = self.protocol_type
        self.dump_message(socket, message)

        # NOTICE: if call `itrans.close()`, we should
        # `raise TTransportException(TTransportException.END_OF_FILE)`

    def dump_message(self, socket: TSocket, message: ThriftMessage):
        pass
