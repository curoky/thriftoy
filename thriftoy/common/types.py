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

from enum import Enum

from thriftpy2.protocol.binary import TBinaryProtocol, TBinaryProtocolFactory
from thriftpy2.protocol.compact import TCompactProtocol, TCompactProtocolFactory
from thriftpy2.transport.buffered import TBufferedTransportFactory
from thriftpy2.transport.framed import TFramedTransport, TFramedTransportFactory


class ProtocolType(str, Enum):
    BINARY = "binary"
    COMPACT = "compact"

    @staticmethod
    def create(factory_or_proto) -> "ProtocolType":
        if isinstance(factory_or_proto, TBinaryProtocolFactory) or isinstance(
            factory_or_proto, TBinaryProtocol
        ):
            return ProtocolType.BINARY
        if isinstance(factory_or_proto, TCompactProtocolFactory) or isinstance(
            factory_or_proto, TCompactProtocol
        ):
            return ProtocolType.COMPACT
        raise Exception("ProtocolType: unknow factory %s", str(factory_or_proto))

    def get_factory(self) -> TBinaryProtocolFactory | TCompactProtocolFactory:
        match self.value:
            case ProtocolType.BINARY:
                return TBinaryProtocolFactory()
            case ProtocolType.COMPACT:
                return TCompactProtocolFactory()
        raise Exception("ProtocolType: unknow value %s", str(self.value))


class TransportType(str, Enum):
    FRAMED = "framed"
    BUFFERED = "buffered"
    # THEADER = "theader"

    @staticmethod
    def create(factory_or_trans) -> "TransportType":
        if isinstance(factory_or_trans, TFramedTransportFactory) or isinstance(
            factory_or_trans, TFramedTransport
        ):
            return TransportType.FRAMED
        if isinstance(factory_or_trans, TBufferedTransportFactory) or isinstance(
            factory_or_trans, TBinaryProtocol
        ):
            return TransportType.BUFFERED
        raise Exception("TransportType: unknow factory %s", str(factory_or_trans))

    def get_factory(self) -> TFramedTransportFactory | TBufferedTransportFactory:
        match self.value:
            case TransportType.FRAMED:
                return TFramedTransportFactory()
            case TransportType.BUFFERED:
                return TBufferedTransportFactory()
        raise Exception("TransportType: unknow value %s", str(self.value))
