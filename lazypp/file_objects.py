import os
import pickle
import shutil
from abc import ABC
from hashlib import md5
from pathlib import Path


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
        if dest is not None:
            self._copy = True
        else:
            self._copy = copy

        self._dest_path = Path(dest) if dest is not None else Path(path)
        self._src_path = Path(path).resolve()

        if _is_outside_base(self._dest_path):
            raise ValueError("File is outside base directory")

    @property
    def path(self):
        if self._dest_path:
            return self._dest_path
        return self._src_path

    def __str__(self):
        return f"<{self.__class__.__name__}: {str(self._src_path)} -> {str(self._dest_path)}>"

    def __repr__(self):
        return f"<{self.__class__.__name__}: {str(self._src_path)} -> {str(self._dest_path)}>"

    def _md5_hash(self):
        raise NotImplementedError

    def _copy_to_dest(self, work_dir: Path):
        _ = work_dir
        raise NotImplementedError

    def _cache(self, work_dir: Path, cache_dir: Path):
        _ = work_dir, cache_dir
        raise NotImplementedError

    def copy(self, dest: Path):
        _ = dest
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

    def _cache(self, work_dir: Path, cache_dir: Path):
        """Cache file to cache directory"""
        cach_path = cache_dir / self._md5_hash().hexdigest()
        os.makedirs(cach_path.parent, exist_ok=True)

        shutil.copy(
            work_dir / self._dest_path, cache_dir / self._md5_hash().hexdigest()
        )
        self._src_path = cach_path

        # save self instance to cache directory
        with open(cache_dir / "data", "wb") as f:
            f.write(pickle.dumps(self))

    def copy(self, dest: Path, overwrite: bool = False):
        os.makedirs((dest / self.path).parent, exist_ok=True)
        if os.path.exists(dest / self.path):
            if not overwrite:
                raise FileExistsError(f"{dest / self.path} already exists")
            else:
                os.remove(dest / self.path)
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

    def _cache(self, work_dir: Path, cache_dir: Path):
        """Cache directory to cache directory"""
        cache_path = cache_dir / self._md5_hash().hexdigest()
        os.makedirs(cache_path.parent, exist_ok=True)

        shutil.copytree(work_dir / self._dest_path, cache_path)
        self._src_path = cache_path

        # save self instance to cache directory
        with open(cache_dir / "data", "wb") as f:
            f.write(pickle.dumps(self))

    def copy(self, dest: Path, overwrite: bool = False):
        os.makedirs((dest / self.path).parent, exist_ok=True)
        if os.path.exists(dest / self.path):
            if not overwrite:
                raise FileExistsError(f"{dest / self.path} already exists")
            else:
                shutil.rmtree(dest / self.path)
        shutil.copytree(self._src_path, dest / self.path)
