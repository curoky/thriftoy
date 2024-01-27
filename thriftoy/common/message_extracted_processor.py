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

import io
import logging
import struct

from thriftpy2.protocol.binary import TBinaryProtocol
from thriftpy2.rpc import TSocket
from thriftpy2.transport.buffered import TBufferedTransport
from thriftpy2.transport.framed import TFramedTransport

from .message import TMessage
from .types import ProtocolType, TransportType


class EmptyThriftStruct:
    def __init__(self):
        self.thrift_spec = set()


class TFramedTransportHook:
    def __init__(self, trans: TFramedTransport) -> None:
        assert isinstance(trans, TFramedTransport)
        self._hooked_trans = trans

    def get_raw_data(self):
        body = self._hooked_trans._rbuf.getvalue()
        return struct.pack("!i", len(body)) + body

    def __getattr__(self, name):
        return getattr(self._hooked_trans, name)


class TBufferedTransportHook:
    def __init__(self, trans: TBufferedTransport) -> None:
        assert isinstance(trans, TBufferedTransport)
        self._hooked_trans = trans
        self._hook_buffer_ = io.BytesIO(b"")

    def get_raw_data(self):
        return self._hook_buffer_.getvalue()

    def read(self, sz: int):
        buf = self._hooked_trans.read(sz)
        self._hook_buffer_.write(buf)
        return buf

    def __getattr__(self, name):
        return getattr(self._hooked_trans, name)


class TMessageExtractedProcessor:
    """
    A TProcessor for unpacking a thrift message without IDL.
    Need to implement the handle_message function.
    """

    def __init__(self, transport_type: TransportType) -> None:
        self.transport_type = transport_type

    def extract_message(self, prot: TBinaryProtocol) -> TMessage:
        origin_trans = prot.trans
        if self.transport_type == TransportType.FRAMED:
            logging.debug("setup TFramedTransportHook for extract message")
            prot.trans = TFramedTransportHook(origin_trans._trans)
            socket: TSocket = origin_trans._trans._trans
        elif self.transport_type == TransportType.BUFFERED:
            logging.debug("setup TBufferedTransportHook for extract message")
            prot.trans = TBufferedTransportHook(origin_trans)
            socket: TSocket = origin_trans._trans
        else:
            raise NotImplementedError(f"Unsupported transport type {self.transport_type}")

        method, type, seqid = prot.read_message_begin()
        prot.read_struct(EmptyThriftStruct())
        prot.read_message_end()
        data = prot.trans.get_raw_data()
        prot.trans = origin_trans
        message = TMessage(method=method, type=type, seqid=seqid, data=data)

        assert socket.sock is not None
        message.from_host, message.from_port = socket.sock.getpeername()
        message.listen_host, message.listen_port = socket.sock.getsockname()
        return message

    def process(self, iprot: TBinaryProtocol, oprot: TBinaryProtocol):
        logging.debug("TMessageExtractedProcessor:: process iprot")
        message = self.extract_message(iprot)
        message.transport_type = self.transport_type
        message.protocol_type = ProtocolType.create(iprot)
        self.handle_message(message, iprot, oprot)

        # NOTICE: if call `itrans.close()`, we should
        # `raise TTransportException(TTransportException.END_OF_FILE)`

    def handle_message(self, message: TMessage, iprot, oprot):
        pass
