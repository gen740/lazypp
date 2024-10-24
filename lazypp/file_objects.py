from pathlib import Path
import shutil


class AbstFileObject:
    def __init__(
        self,
        path: str | Path,
        *,
        dest: str | Path | None = None,
        glob_rule: str | None = None,
    ):
        self._path = Path(path)
        self._dest = Path(dest) if dest is not None else None
        self._glob_rule: None | str = glob_rule
        self._cache_path: None | Path = None

    @property
    def path(self):
        if self._dest:
            return self._dest
        return self._path

    @property
    def source_path(self):
        return self._path

    @property
    def cache_path(self):
        if self._cache_path is None:
            raise RuntimeError("cache_path is not set")
        return self._cache_path

    @cache_path.setter
    def cache_path(self, path: Path):
        self._cache_path = path

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return f"""
AbstFileObject:
    path: {self._path}
    glob_rule: {self._glob_rule}
    cache_path: {self.cache_path}
    """

    def copy(self, dest: str | Path, dirs_exist_ok: bool = False):
        # Copy directory to destination
        shutil.copytree(self.cache_path, dest, dirs_exist_ok=dirs_exist_ok)


class File(AbstFileObject):
    def __str__(self):
        return f"""
File:
    path: {self._path}
    glob_rule: {self._glob_rule}
    cache_path: {self.cache_path}
    """


class Directory(AbstFileObject):
    def __str__(self):
        return f"""
Directory:
    path: {self._path}
    glob_rule: {self._glob_rule}
    cache_path: {self.cache_path}
    """
