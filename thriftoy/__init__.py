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
from enum import Enum

from thriftpy2.protocol.binary import TBinaryProtocolFactory
from thriftpy2.protocol.compact import TCompactProtocolFactory
from thriftpy2.transport.buffered import TBufferedTransportFactory
from thriftpy2.transport.framed import TFramedTransportFactory


class ProtocolType(str, Enum):
    BINARY = "binary"
    COMPACT = "compact"

    @staticmethod
    def create(factory):
        if isinstance(factory, TBinaryProtocolFactory):
            return ProtocolType.BINARY
        if isinstance(factory, TCompactProtocolFactory):
            return ProtocolType.COMPACT

    def get_factory(self):
        match self.value:
            case ProtocolType.BINARY:
                return TBinaryProtocolFactory()
            case ProtocolType.COMPACT:
                return TCompactProtocolFactory()


class TransportType(str, Enum):
    FRAMED = "framed"
    BUFFERED = "buffered"
    # THEADER = "theader"

    @staticmethod
    def create(factory):
        if isinstance(factory, TFramedTransportFactory):
            return TransportType.FRAMED
        if isinstance(factory, TBufferedTransportFactory):
            return TransportType.BUFFERED

    def get_factory(self):
        match self.value:
            case TransportType.FRAMED:
                return TFramedTransportFactory()
            case TransportType.BUFFERED:
                return TBufferedTransportFactory()
