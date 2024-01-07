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

from thriftpy2.protocol.binary import TBinaryProtocol
from thriftpy2.rpc import TSocket

from ..common.TMessage import TMessage
from ..common.TTypes import ProtocolType
from .TMemoryWrappedTransport import TMemoryWrappedTransport


class EmptyThriftStruct:
    def __init__(self):
        self.thrift_spec = set()


class TExtractMessageProcessor:
    """
    A TProcessor for unpacking a thrift message without IDL.
    Need to implement the handle_message function.
    """

    def process_in(self, prot: TBinaryProtocol) -> TMessage:
        method, type, seqid = prot.read_message_begin()
        prot.read_struct(EmptyThriftStruct())
        prot.read_message_end()
        data = prot.trans.getvalue()
        return TMessage(method=method, type=type, seqid=seqid, data=data)

    def process(self, iprot: TBinaryProtocol, oprot: TBinaryProtocol):
        itrans: TMemoryWrappedTransport = iprot.trans
        message = self.process_in(iprot)
        socket: TSocket = itrans._trans
        assert socket.sock is not None
        message.from_host, message.from_port = socket.sock.getpeername()
        message.listen_host, message.listen_port = socket.sock.getsockname()
        message.transport_type = itrans.transport_type
        message.protocol_type = ProtocolType.create(iprot)
        self.handle_message(socket, message)

        # NOTICE: if call `itrans.close()`, we should
        # `raise TTransportException(TTransportException.END_OF_FILE)`

    def handle_message(self, socket: TSocket, message: TMessage):
        pass
