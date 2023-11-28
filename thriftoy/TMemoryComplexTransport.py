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

import io
import struct

from thriftpy2.protocol.exc import TProtocolException
from thriftpy2.transport.base import TTransportBase
from thriftpy2.transport.buffered import TBufferedTransport

from . import TransportType


class TMemoryComplexTransport(TBufferedTransport):
    def __init__(
        self,
        trans,
        transport_type: TransportType,
        buf_size: int = TBufferedTransport.DEFAULT_BUFFER,
    ):
        super().__init__(trans, buf_size)
        self.buffer = io.BytesIO(b"")
        self.transport_type = transport_type
        self.frame_size = 0

    def read(self, sz: int):
        if self.frame_size == 0 and self.transport_type == TransportType.FRAMED:
            buf = TTransportBase.read(self, 4)
            (self.frame_size,) = struct.unpack("!i", buf)
            self.buffer.write(buf)
        buf = TTransportBase.read(self, sz)
        self.buffer.write(buf)
        return buf

    def flush(self):
        if self.transport_type == TransportType.FRAMED:
            self._trans.write(struct.pack("!i", len(self._wbuf.getbuffer())))
        TBufferedTransport.flush(self)

    def getvalue(self):
        value = self.buffer.getvalue()
        if self.transport_type == TransportType.FRAMED and len(value) - self.frame_size != 4:
            raise TProtocolException(
                TProtocolException.INVALID_DATA,
                f"{len(value)}(buffer size) - {self.frame_size} (frame size) != 4",
            )
        return value


class TMemoryComplexTransportFactory:
    def __init__(self, transport_type: TransportType) -> None:
        self.transport_type = transport_type

    def get_transport(self, trans):
        return TMemoryComplexTransport(trans, self.transport_type)
