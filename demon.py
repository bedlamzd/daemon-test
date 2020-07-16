import urllib
import hashlib
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from pathlib import Path
from daemon import pidfile, DaemonContext
import signal
import sys
import argparse
from typing import Optional

# defaults
STORAGE = 'storage'
HOST = 'localhost'
PORT = 8080
UMASK = '0o002'
WORKING_DIR = '/var/lib/demon'
PID_FILE = '/var/run/demon.pid'


def return_file(file_hash: str) -> Optional[bytes]:
    """
    Given file hash find it and return
    :param file_hash: md5 hash
    :return: binary data or None
    """
    if exists_file(file_hash):
        with open(f'{STORAGE}/{file_hash[:2]}/{file_hash}', 'rb') as f:
            file = f.read()
        return file
    print('No File')
    return None


def store_file(file: bytes) -> str:
    """
    Computes md5 hash of file, saves file by its hash
    :param file: binary data
    :return: md5 hash of file
    """
    file_hash = hashlib.md5(file).hexdigest()
    path = Path(f'{STORAGE}/{file_hash[:2]}/{file_hash}')
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        f.write(file)
    print(f'File saved. Hash: {file_hash}')
    return file_hash


def delete_file(file_hash: str) -> bool:
    """
    Given file hash find it and delete
    :param file_hash: md5 hash of file
    :return: True if deleted, else False
    """
    if exists_file(file_hash):
        file = Path(f'{STORAGE}/{file_hash[:2]}/{file_hash}')
        file.unlink()
        print('File deleted.')
        try:
            file.parent.rmdir()
            print('Removed empty dir.')
        except OSError:
            pass
        return True
    return False


def exists_file(file_hash: str) -> bool:
    """
    Checks if file with given hash exists in storage
    :param file_hash: md5 hash of file
    :return: True if exists else False
    """
    return Path(f'{STORAGE}/{file_hash[:2]}/{file_hash}').exists()


class MyServer(BaseHTTPRequestHandler):
    def _parse_query(self) -> dict:
        """
        Parses query in request
        :return: dictionary of parsed params
        """
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)
        params = {key: value[0] if len(value) == 1 else value for key, value in params.items()}
        return params

    def do_GET(self):
        """
        GET request handler. Returns file by hash if exists
        """
        params = self._parse_query()
        if (file_hash := params.get('file_hash', False)) and (file := return_file(file_hash)) is not None:
            self.send_response(200)
            self.send_header('Content-type', 'application')
            self.end_headers()
            self.wfile.write(file)
        else:
            self.send_response(204)
            self.end_headers()

    def do_POST(self):
        """
        POST request handler. Returns json (hopefully) with hash of file in request.
        """
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        file_hash = store_file(body)
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = BytesIO()
        response.write(b'{"Hash": "%s"}' % file_hash.encode('utf-8'))
        self.wfile.write(response.getvalue())

    def do_DELETE(self):
        """
        DELETE request handler. Deletes file by hash if exists.
        """
        params = self._parse_query()
        res = False
        if (file_hash := params.get('file_hash', False)):
            res = delete_file(file_hash)
        if res:
            self.send_response(200)
        else:
            self.send_response(204)
        self.end_headers()


def run(host: str, port: int):
    """
    Start server
    :param host: server host
    :param port: server port
    """
    web_server = HTTPServer((host, port), MyServer)
    print('Server running')

    web_server.serve_forever()

    web_server.server_close()
    print("Server stopped.")


def shutdown(signum, frame):
    sys.exit(0)

# setup argument parser
parser = argparse.ArgumentParser()
parser.add_argument('-wd', '--working_directory', default=WORKING_DIR,
                    help='daemon working directory')
parser.add_argument('--pid', default=PID_FILE, help='specify pid file')
parser.add_argument('--umask', default=UMASK, help='specify umask')
parser.add_argument('-p', '--port', default=PORT, type=int, help='specify server port')

if __name__ == '__main__':
    args = parser.parse_args()
    context = DaemonContext( # setup daemon
        working_directory=args.working_directory,
        umask=int(args.umask, base=8),
        pidfile=pidfile.PIDLockFile(args.pid),
        signal_map={
            signal.SIGTERM: shutdown,
            signal.SIGHUP: shutdown,
        }
    )
    with context:
        run(HOST, args.port)
