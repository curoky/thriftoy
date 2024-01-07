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
import logging
import socket

from thriftpy2.transport.socket import TSocket


class TSimpleSocket(TSocket):
    """
    Enhance `TSocket`
        1. auto detect socket family by host.
        2. support binding local host.
    """

    def __init__(
        self,
        remote_host: str,
        remote_port: int,
        socket_timeout=3000,
        connect_timeout=None,
        socket_family=None,
        local_host: str | None = None,
    ):
        if socket_family is None:
            socket_family = socket.AF_INET
        if ":" in remote_host:
            socket_family = socket.AF_INET6
        super().__init__(
            remote_host,
            remote_port,
            None,
            None,
            socket_family,
            socket_timeout,
            connect_timeout,
        )
        self.local_host = local_host

    def _init_sock(self):
        super()._init_sock()
        if self.sock and self.local_host:
            logging.info("ThriftSocket: bind socket on %s", self.local_host)
            self.sock.bind((self.local_host, 0))
