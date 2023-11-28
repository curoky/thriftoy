import socket

from thriftpy2.transport.socket import TSocket


class ThriftSocket(TSocket):
    def __init__(
        self,
        host=None,
        port=None,
        unix_socket=None,
        sock=None,
        socket_family=socket.AF_INET,
        socket_timeout=3000,
        connect_timeout=None,
        bound_addr: str | None = None,
    ):
        super().__init__(
            host, port, unix_socket, sock, socket_family, socket_timeout, connect_timeout
        )
        self.bound_addr = bound_addr

    def _init_sock(self):
        self._init_sock()
        if self.sock and self.bound_addr:
            self.sock.bind((self.bound_addr, 0))
