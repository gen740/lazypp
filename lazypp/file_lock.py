import fcntl
import os
from pathlib import Path
from typing import IO


class FileLock:
    def __init__(self, lock_file_path: Path):
        self.lock_file_path: Path = lock_file_path
        self.lock_file: IO | None = None

    def acquire(self):
        """Acquire the file lock."""
        if self.lock_file is None:
            os.makedirs(self.lock_file_path.parent, exist_ok=True)
            self.lock_file = open(self.lock_file_path, "w")
        try:
            fcntl.flock(self.lock_file, fcntl.LOCK_EX)
        except Exception as e:
            self.lock_file.close()
            self.lock_file = None
            raise e

    def release(self):
        """Release the file lock."""
        if self.lock_file:
            fcntl.flock(self.lock_file, fcntl.LOCK_UN)
            self.lock_file.close()
            self.lock_file = None
            os.remove(self.lock_file_path)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        _ = exc_type, exc_value, traceback
        self.release()
