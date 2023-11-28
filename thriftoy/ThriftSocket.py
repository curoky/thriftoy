import logging
import socket

from thriftpy2.transport.socket import TSocket


class ThriftSocket(TSocket):
    def __init__(
        self,
        remote_host=None,
        remote_port=None,
        unix_socket=None,
        sock=None,
        socket_family=socket.AF_INET,
        socket_timeout=3000,
        connect_timeout=None,
        local_host: str | None = None,
    ):
        super().__init__(
            remote_host,
            remote_port,
            unix_socket,
            sock,
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
