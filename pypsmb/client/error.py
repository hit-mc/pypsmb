class ProtocolError(RuntimeError):
    """
    Error occurred in application-layer protocol communication.
    """
    pass


class UnsupportedProtocolError(ProtocolError):
    """
    Our version of protocol is not supported by the remote host.
    """
    pass
