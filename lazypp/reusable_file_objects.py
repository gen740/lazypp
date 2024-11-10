import os
import shutil
from pathlib import Path

from xxhash import xxh128

import lazypp
from lazypp.file_lock import FileLock


class ReusableFile:
    """
    This class is suitable for the task that generated the file as an intermediate output,
    but if the file already exists, it will reuse it and reduce the computation time.
    """

    def __init__(
        self,
        *,
        id: str,
        cache_dir: Path | str,
        dependents: list["lazypp.BaseTask"] | None = None,
        copy: bool = False,
        mutable: bool = False,
    ):
        self._id = id
        self._cache_dir = Path(cache_dir) / "reusable_files"
        self._file_lock: FileLock | None = None
        self._copy = copy
        self._mutable = mutable

        if dependents is None:
            dependents = []
        self._depends_tasks = dependents

    @property
    def hash(self) -> str:
        h = xxh128()
        h.update(self._id.encode())
        for task in self._depends_tasks:
            h.update(task.hash.encode())
        return h.hexdigest()

    def __enter__(self) -> Path:
        if os.path.exists(self._cache_dir / self.hash):
            if self._copy:
                shutil.copy(self._cache_dir / self.hash, self.hash)
            else:
                os.link(self._cache_dir / self.hash, self.hash)
            return Path(self.hash)
        else:
            self._file_lock = FileLock(self._cache_dir / (self.hash + ".lock"))
            self._file_lock.acquire()
            if os.path.exists(self._cache_dir / self.hash):
                try:
                    if self._copy:
                        shutil.copy(self._cache_dir / self.hash, self.hash)
                    else:
                        os.link(self._cache_dir / self.hash, self.hash)
                finally:
                    self._file_lock.release()
                    self._file_lock = None
                return Path(self.hash)
            return Path(self.hash)

    def __exit__(self, exc_type, exc_value, traceback):
        _ = exc_type, exc_value, traceback

        if self._file_lock is not None:
            try:
                if not os.path.exists(self.hash):
                    raise FileNotFoundError(f"File {self.hash} not found in workdir.")
                shutil.copy(self.hash, self._cache_dir / self.hash)
            finally:
                self._file_lock.release()
                self._file_lock = None
        else:
            if self._mutable:
                shutil.copy(self.hash, self._cache_dir / self.hash)
