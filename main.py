import socket
import yaml
from concurrent.futures import ThreadPoolExecutor
import mb

if __name__ == '__main__':
    with open('config.yml', 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    listen = config.get('listen') or {}
    connection = config.get('connection') or {}

    host = listen.get('address') or '0.0.0.0'
    port = listen.get('port') or 3880
    max_threads = connection.get('max_threads') or 32
    keep_alive = connection.get('keep_alive') or -1
    if 0 <= keep_alive <= 3:
        raise RuntimeError(f'Keep-alive interval is too small ({keep_alive} sec)')

    executor = ThreadPoolExecutor(max_workers=max_threads)
    dispatcher = mb.MessageDispatcher()

    listen_addr = (host, port)
    sock = socket.create_server(listen_addr)
    # if socket.has_dualstack_ipv6():
    #     sock = socket.create_server(listen_addr, family=socket.AF_INET6, dualstack_ipv6=True)
    # else:
    #     sock = socket.create_server(listen_addr)

    with sock:
        print(f'Listening on {host}:{port}...')
        while True:
            client, addr = sock.accept()
            executor.submit(mb.handle_client,
                            sock=client, addr=addr, dispatcher=dispatcher, keep_alive=keep_alive)
