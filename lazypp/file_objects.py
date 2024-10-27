from abc import ABC
import shutil
from hashlib import md5
from pathlib import Path
import pickle
import tarfile
import os


def _is_outside_base(relative_path: Path) -> bool:
    """
    Check if relative path is outside base directory
    """
    depth = 0
    for part in relative_path.parts:
        if part == "..":
            depth -= 1
        elif part != ".":
            depth += 1
        if depth < 0:
            return True
    return False


class BaseEntry(ABC):
    def __init__(
        self,
        path: str | Path,
        *,
        copy: bool = False,
        dest: str | Path | None = None,
    ):
        self._copy = copy
        self._dest_path = Path(dest) if dest is not None else Path(path)
        self._src_path = Path(path)
        self._src_path = self._src_path.resolve()

        if not self._src_path.exists():
            raise FileNotFoundError(f"File not found: {self._src_path}")

        if _is_outside_base(self._dest_path):
            raise ValueError("File is outside base directory")

    @property
    def path(self):
        if self._dest_path:
            return self._dest_path
        return self._src_path

    def __str__(self):
        return str(self.path)

    def _md5_hash(self):
        raise NotImplementedError

    def _copy_to_dest(self, work_dir: Path):
        _ = work_dir
        raise NotImplementedError

    def _cache(self, cache_dir: Path):
        _ = cache_dir
        raise NotImplementedError

    def copy(self, dest: Path):
        raise NotImplementedError


class File(BaseEntry):
    def _md5_hash(self):
        ret = md5()
        with open(self._src_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                ret.update(chunk)
        return ret

    def _copy_to_dest(self, work_dir: Path):
        if self._copy:
            self.copy(work_dir)

    def _cache(self, cache_dir: Path):
        """Cache file to cache directory"""
        dest = cache_dir / self._md5_hash().hexdigest()
        os.makedirs(dest.parent, exist_ok=True)

        shutil.copy(self._dest_path, cache_dir / self._md5_hash().hexdigest())
        self._src_path = dest

        # save self instance to cache directory
        with open(cache_dir / "data", "wb") as f:
            f.write(pickle.dumps(self))

    def copy(self, dest: Path):
        shutil.copy(self._src_path, dest / self.path)


class Directory(BaseEntry):
    def _md5_hash(self):
        ret = md5()
        for root, _, files in os.walk(self._src_path):
            for file in files:
                file_path = os.path.join(root, file)
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        ret.update(chunk)
        return ret

    def _copy_to_dest(self, work_dir: Path):
        if self._copy:
            self.copy(work_dir)

    def _cache(self, cache_dir: Path):
        """Cache directory to cache directory"""
        dest = cache_dir / self._md5_hash().hexdigest()
        os.makedirs(dest.parent, exist_ok=True)

        shutil.copytree(self._dest_path, dest)
        self._src_path = dest

        # save self instance to cache directory
        with open(cache_dir / "data", "wb") as f:
            f.write(pickle.dumps(self))

    def copy(self, dest: Path):
        shutil.copytree(self._src_path, dest / self.path)


def load_from_cache(path: Path) -> BaseEntry:
    with open(path / "data", "rb") as f:
        obj = pickle.load(f)

    return obj
