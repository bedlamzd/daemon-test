import urllib
import hashlib
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from pathlib import Path

STORAGE = 'storage'
HOST = 'localhost'
PORT = 8080


def return_file(file_hash):
    if exists_file(file_hash):
        with open(f'{STORAGE}/{file_hash[:2]}/{file_hash}', 'rb') as f:
            file = f.read()
        return file
    print('No File')
    return None


def store_file(file):
    file_hash = hashlib.md5(file).hexdigest()
    path = Path(f'{STORAGE}/{file_hash[:2]}/{file_hash}')
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        f.write(file)
    print(f'File saved. Hash: {file_hash}')
    return file_hash


def delete_file(file_hash):
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


def exists_file(file_hash):
    return Path(f'{STORAGE}/{file_hash[:2]}/{file_hash}').exists()


class MyServer(BaseHTTPRequestHandler):
    def _parse_query(self):
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)
        params = {key: value[0] if len(value) == 1 else value for key, value in params.items()}
        return params

    def do_GET(self):
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
        params = self._parse_query()
        res = False
        if (file_hash := params.get('file_hash', False)):
            res = delete_file(file_hash)
        if res:
            self.send_response(200)
        else:
            self.send_response(204)
        self.end_headers()


if __name__ == '__main__':
    # TODO: make real daemon out of it
    web_server = HTTPServer((HOST, PORT), MyServer)
    print(web_server.server_address)
    print('Server running')

    try:
        web_server.serve_forever()
    except KeyboardInterrupt:
        pass

    web_server.server_close()
    print("Server stopped.")
