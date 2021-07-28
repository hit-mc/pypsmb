import logging
import socket
import yaml
from concurrent.futures import ThreadPoolExecutor
from pypsmb import mb

LOG_FORMAT = '[%(asctime)-15s][%(levelname)s][%(name)s] %(message)s'
logging.basicConfig(format=LOG_FORMAT, level='INFO')

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

    sock = socket.create_server((host, port))

    with sock:
        print(f'Listening on {host}:{port}...')
        while True:
            client, addr = sock.accept()
            executor.submit(mb.handle_client,
                            sock=client, addr=addr, dispatcher=dispatcher, keep_alive=keep_alive)
