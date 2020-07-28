import urllib
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from daemon import pidfile, DaemonContext
import signal
import sys
import argparse
from fileHandler import FileHandler

# defaults
STORAGE = 'storage'
SALT = b''
CHUNKSIZE = 4096
file_handler = FileHandler(storage_path=STORAGE, salt=SALT, chunksize=CHUNKSIZE)
HOST = 'localhost'
PORT = 8080
UMASK = '0o002'
WORKING_DIR = '/var/lib/demon'
PID_FILE = '/var/run/demon.pid'


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
        if (file_hash := params.get('file_hash', False)) and (file := file_handler.get_file(file_hash)):
            self.send_response(200)
            self.send_header('Content-type', 'application')
            self.end_headers()
            while data := file.read(file_handler.chunksize):
                self.wfile.write(data)
        else:
            self.send_response(204)
            self.end_headers()

    def do_POST(self):
        """
        POST request handler. Returns json (hopefully) with hash of file in request.
        """
        content_length = int(self.headers['Content-Length'])
        file_hash = file_handler.save_file(self.rfile, content_length)
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
        if (file_hash := params.get('file_hash', False)) and file_handler.delete_file(file_hash):
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
parser.add_argument('--port', default=PORT, type=int, help='specify server port')
parser.add_argument('--salt', default=SALT, help='specify hash salt')
parser.add_argument('-cs', '--chunksize', default=CHUNKSIZE, type=int, help='specify read chunk size')
parser.add_argument('-s', '--storage', default=STORAGE, help='specify read chunk size')

if __name__ == '__main__':
    args = parser.parse_args()
    file_handler = FileHandler(args.salt, args.chunksize, args.storage)
    context = DaemonContext(  # setup daemon
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
