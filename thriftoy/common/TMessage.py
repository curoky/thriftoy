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

import sqlmodel
from thriftpy2.transport.memory import TMemoryBuffer

from .TTypes import ProtocolType, TransportType


def extract_method_args(
    data: bytes,
    service,
    method: str,
    transport_type=TransportType.FRAMED,
    protocol_type=ProtocolType.BINARY,
):
    trans = transport_type.get_factory().get_transport(TMemoryBuffer(value=data))
    prot = protocol_type.get_factory().get_protocol(trans)
    prot.read_message_begin()
    args = getattr(service, f"{method}_args")()
    args.read(prot)
    prot.read_message_end()
    return args


class TMessage(sqlmodel.SQLModel, table=True):
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
    ):
        # https://github.com/tiangolo/sqlmodel/pull/442
        self.transport_type = TransportType(self.transport_type)
        self.protocol_type = ProtocolType(self.protocol_type)

        return extract_method_args(
            data=self.data,
            service=service,
            method=self.method,
            transport_type=self.transport_type,
            protocol_type=self.protocol_type,
        )


def get_thrift_message(path: str, limit: int, method: str | None = None) -> list[TMessage]:
    engine = sqlmodel.create_engine(f"sqlite:///{path}")
    messages = []
    with sqlmodel.Session(engine) as session:
        if method:
            statement = sqlmodel.select(TMessage).where(TMessage.method == method).limit(limit)
        else:
            statement = sqlmodel.select(TMessage).limit(limit)
        results = session.exec(statement)
        for result in results:
            messages.append(result)
    return messages
