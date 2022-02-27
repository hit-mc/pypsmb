import logging
import socket
import yaml
from concurrent.futures import ThreadPoolExecutor
import pypsmb.mb as mb
import argparse
import ssl

LOG_FORMAT = '[%(asctime)-15s][%(levelname)s][%(name)s] %(message)s'
logging.basicConfig(format=LOG_FORMAT, level='INFO')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Path to or name of the configuaration file',
                        required=False, default='config.yaml')
    args = parser.parse_args()
    config_filename = args.config
    with open(config_filename, 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    listen = config.get('listen') or {}
    connection = config.get('connection') or {}
    sslconf = config.get('ssl') or None
    host = listen.get('address') or '0.0.0.0'
    port = listen.get('port') or 3880
    max_threads = connection.get('max_threads') or 32
    keep_alive = connection.get('keep_alive') or -1
    if 0 <= keep_alive <= 3:
        raise RuntimeError(
            f'Keep-alive interval is too small ({keep_alive} sec)')

    executor = ThreadPoolExecutor(max_workers=max_threads)
    dispatcher = mb.MessageDispatcher()

    listen_addr = (host, port)
    sock = socket.create_server(listen_addr)

    if sslconf is not None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(sslconf.get('certchain'), sslconf.get('privatekey'))
        sock = context.wrap_socket(sock, server_side=True)
    # if socket.has_dualstack_ipv6():
    #     sock = socket.create_server(listen_addr, family=socket.AF_INET6, dualstack_ipv6=True)
    # else:
    #     sock = socket.create_server(listen_addr)

    with sock:
        print(f'Listening on {host}:{port}...')
        while True:
            try:
                client, addr = sock.accept()
                executor.submit(mb.handle_client,
                                sock=client, addr=addr, dispatcher=dispatcher, keep_alive=keep_alive)
            except ssl.SSLEOFError:
                pass


if __name__ == '__main__':
    main()
