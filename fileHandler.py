from typing import Optional, BinaryIO
from hashlib import md5
import pathlib


class FileHandler:
    def __init__(self, salt: Optional[bytes] = b'',
                 chunksize: Optional[int] = 255,
                 storage_path: Optional[str] = ''):
        self.salt = salt
        self.chunksize = chunksize
        self.storage = storage_path
        self.tmp_counter = -1

    def file_hash(self, file: BinaryIO, limit: Optional[int] = None) -> str:
        file_hash = b''
        if limit is None:
            while data := file.read(self.chunksize):
                file_hash = md5(file_hash + data).digest()
        else:
            while limit > 0:
                data = file.read(self.chunksize if limit // self.chunksize > 0 else limit)
                file_hash = md5(file_hash + data).digest()
                limit -= self.chunksize
        file_hash = file_hash.hex() + md5(self.salt).hexdigest()
        return file_hash

    def valid_hash(self, file_hash: str) -> bool:
        return file_hash.endswith(md5(self.salt).hexdigest())

    def make_path(self, file_hash) -> pathlib.Path:
        return pathlib.Path(f'{self.storage}/{file_hash[:2]}/{file_hash}')

    def make_tmp_path(self):
        self.tmp_counter += 1
        return pathlib.Path(f'{self.storage}/tmp/tmp_file_{self.tmp_counter}')

    def save_file(self, file: BinaryIO, limit: Optional[int] = None) -> str:
        tmp_path = self.make_tmp_path()
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        with open(str(tmp_path), 'wb') as new_file:
            if limit is None:
                while data := file.read(self.chunksize):
                    new_file.write(data)
            else:
                while limit > 0:
                    data = file.read(self.chunksize if limit // self.chunksize > 0 else limit)
                    new_file.write(data)
                    limit -= self.chunksize
        with open(str(tmp_path), 'rb') as file:
            file_hash = self.file_hash(file)
        path = self.make_path(file_hash)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.replace(path)
        return file_hash

    def get_file(self, file_hash: str) -> [BinaryIO, bool]:
        if self.valid_hash(file_hash) and (path := self.make_path(file_hash)).exists():
            return open(str(path), 'rb')
        return False

    def delete_file(self, file_hash: str) -> bool:
        if self.valid_hash(file_hash) and (path := self.make_path(file_hash)).exists():
            path.unlink()
            try:  # delete directory if it's empty
                path.parent.rmdir()
            except OSError:
                pass
            return True
        return False
