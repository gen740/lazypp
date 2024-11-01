import os
import pickle
import shutil
from abc import ABC
from pathlib import Path

from xxhash import xxh128


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
    """
    self._src_path: Where the file is located
    self._dest_path: Where the file is copied to
    """

    def __init__(
        self,
        path: str | Path,
        *,
        copy: bool = False,
        dest: str | Path | None = None,
        allow_overwrite: bool = False,
    ):
        self._copy = copy

        if dest is None:
            self._overwrite_if_exists = True
        else:
            self._overwrite_if_exists = False

        if dest is not None and _is_outside_base(Path(dest)):
            raise ValueError("File is outside base directory")
        self._src_path = Path(path).resolve()
        self._dest_path = (
            Path(dest) if dest is not None else Path(self._xxh128_hash().hexdigest())
        )
        self._allow_overwrite = allow_overwrite

    @property
    def path(self):
        return self._dest_path

    def __str__(self):
        return f"<{self.__class__.__name__}: {str(self._src_path)} -> {str(self._dest_path)}>"

    def __repr__(self):
        return f"<{self.__class__.__name__}: {str(self._src_path)} -> {str(self._dest_path)}>"

    def _xxh128_hash(self) -> xxh128:
        raise NotImplementedError

    def _copy_to_dest(self, work_dir: Path):
        _ = work_dir
        raise NotImplementedError

    def _cache(self, work_dir: Path, cache_dir: Path):
        _ = work_dir, cache_dir
        raise NotImplementedError

    def copy(self, dest: Path | str):
        _ = dest
        raise NotImplementedError

    def link(self, dest: Path | str):
        _ = dest
        raise NotImplementedError


class File(BaseEntry):
    def _xxh128_hash(self):
        ret = xxh128()
        with open(self._src_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                ret.update(chunk)
        return ret

    def _copy_to_dest(self, work_dir: Path):
        if self._copy:
            self.copy(work_dir / self._dest_path, self._overwrite_if_exists)
        else:
            self.link(work_dir / self._dest_path, self._overwrite_if_exists)

    def _cache(self, work_dir: Path, cache_dir: Path):
        """Cache file to cache directory"""
        cach_path = cache_dir / self._xxh128_hash().hexdigest()
        os.makedirs(cach_path.parent, exist_ok=True)
        if os.path.islink(work_dir / self._src_path):
            shutil.copy(
                os.readlink(work_dir / self._src_path),
                cache_dir / self._xxh128_hash().hexdigest(),
            )
        else:
            shutil.copy(
                work_dir / self._src_path, cache_dir / self._xxh128_hash().hexdigest()
            )
        self._src_path = cach_path

        # save self instance to cache directory
        with open(cache_dir / "data", "wb") as f:
            f.write(pickle.dumps(self))

    def copy(self, dest: Path | str, overwrite: bool = False):
        os.makedirs(Path(dest).parent, exist_ok=True)
        if os.path.exists(dest):
            if not (overwrite or self._allow_overwrite):
                raise FileExistsError(f"{dest} already exists")
            else:
                os.remove(dest)
        shutil.copy(self._src_path, dest)

    def link(self, dest: Path | str, overwrite: bool = False):
        os.makedirs(Path(dest).parent, exist_ok=True)
        if os.path.exists(dest):
            if not (overwrite or self._allow_overwrite):
                raise FileExistsError(f"{dest} already exists")
            else:
                os.remove(dest)
        os.symlink(self._src_path, dest)


class Directory(BaseEntry):
    def _xxh128_hash(self):
        ret = xxh128()
        for root, _, files in os.walk(self._src_path):
            for file in files:
                file_path = os.path.join(root, file)
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        ret.update(chunk)
        return ret

    def _copy_to_dest(self, work_dir: Path):
        if self._copy:
            self.copy(work_dir / self._dest_path, self._overwrite_if_exists)
        else:
            self.link(work_dir / self._dest_path, self._overwrite_if_exists)

    def _cache(self, work_dir: Path, cache_dir: Path):
        """Cache directory to cache directory"""
        cache_path = cache_dir / self._xxh128_hash().hexdigest()
        os.makedirs(cache_path.parent, exist_ok=True)

        if os.path.islink(work_dir / self._src_path):
            shutil.copytree(os.readlink(work_dir / self._src_path), cache_path)
        else:
            shutil.copytree(work_dir / self._src_path, cache_path)
        self._src_path = cache_path

        # save self instance to cache directory
        with open(cache_dir / "data", "wb") as f:
            f.write(pickle.dumps(self))

    def copy(self, dest: Path | str, overwrite: bool = False):
        os.makedirs(Path(dest).parent, exist_ok=True)
        if os.path.exists(dest):
            if not (overwrite or self._allow_overwrite):
                raise FileExistsError(f"{dest} already exists")
            else:
                if os.path.islink(dest):
                    os.remove(dest)
                else:
                    shutil.rmtree(dest)
        shutil.copytree(self._src_path, dest)

    def link(self, dest: Path | str, overwrite: bool = False):
        os.makedirs(Path(dest).parent, exist_ok=True)
        if os.path.exists(dest):
            if not (overwrite or self._allow_overwrite):
                raise FileExistsError(f"{dest} already exists")
            else:
                if os.path.islink(dest):
                    os.remove(dest)
                else:
                    shutil.rmtree(dest)
        os.symlink(self._src_path, dest)
