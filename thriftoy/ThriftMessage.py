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

import sqlmodel
from thriftpy2.transport.memory import TMemoryBuffer

from thriftoy import ProtocolType, TransportType


class ThriftMessage(sqlmodel.SQLModel, table=True):
    id: int | None = sqlmodel.Field(default=None, primary_key=True)

    from_host: str | None = ""
    from_port: int | None = 0
    listen_host: str | None = ""
    listen_port: int | None = 0

    method: str
    type: int  # TODO: to enum?
    seqid: int  # TODO: int32 or int64
    protocol_type: ProtocolType = ProtocolType.BINARY
    transport_type: TransportType = TransportType.FRAMED
    data: bytes

    def extract_args(
        self,
        service,
        method: str,
    ):
        # https://github.com/tiangolo/sqlmodel/pull/442
        self.transport_type = TransportType(self.transport_type)
        self.protocol_type = ProtocolType(self.protocol_type)

        trans = self.transport_type.get_factory().get_transport(TMemoryBuffer(value=self.data))
        prot = self.protocol_type.get_factory().get_protocol(trans)
        prot.read_message_begin()
        args = getattr(service, f"{method}_args")()
        args.read(prot)
        prot.read_message_end()
        return args
