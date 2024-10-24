from abc import ABC
from hashlib import md5
from inspect import getsource
from pathlib import Path
from tempfile import TemporaryDirectory
import copy
import hashlib
import os
import shutil

from .file_objects import File, Directory


def _md5_update(md5_obj: "hashlib._Hash", directory: str | Path) -> "hashlib._Hash":
    """
    update hash object with all files in directory
    """
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(32768), b""):
                    md5_obj.update(chunk)
    return md5_obj


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


class TaskBase[INPUT, OUTPUT](ABC):
    CACHE_DIR = Path(__file__).parent.parent / ".cache"

    _is_temporary = False
    _prev_path = os.getcwd()

    @staticmethod
    def change_cache_dir(new_dir: str | Path):
        """
        Change cache directory
        """
        if not os.path.isabs(new_dir):
            print("Cache directory should be absolute path")
            exit(1)
        TaskBase.CACHE_DIR = Path(new_dir)

    def __init__(
        self,
        work_dir: str | Path | None = None,
        input: INPUT = None,
        output: OUTPUT = None,
    ):
        self._work_dir: Path | TemporaryDirectory | None = (
            Path(work_dir) if work_dir else None
        )
        self._hash: str | None = None
        self._input = copy.deepcopy(input)
        self._output = copy.deepcopy(output)
        self._cached_output: OUTPUT | None = None

    def task(self, input: INPUT, output: OUTPUT) -> None:
        _ = (output, input)
        raise NotImplementedError

    def _task_setup(self):
        """
        Create input files
        """
        if not isinstance(self._input, dict):
            raise ValueError("Input should be a dictionary")
        for _, v in self._input.items():
            # Copy files to work_dir
            if isinstance(v, File) or isinstance(v, Directory):
                if not os.path.exists(v.source_path):
                    raise FileNotFoundError(f"{v.source_path} not found")
                if not os.path.isabs(v.path):
                    if _is_outside_base(v.path):
                        raise ValueError("Could not copy file outside base directory")
                    if isinstance(v, File):
                        shutil.copy(v.source_path, self.work_dir / v.path)
                    else:
                        shutil.copytree(v.source_path, self.work_dir / v.path)

            # Copy output files which are dependencies of this task
            if isinstance(v, TaskBase):
                v.output  # call output to run the task
                for _, iv in v.output.items():
                    if isinstance(iv, File) or isinstance(iv, Directory):
                        if not os.path.exists(iv.cache_path):
                            raise FileNotFoundError(f"{iv.source_path} not found")
                        if isinstance(iv, File):
                            shutil.copy(
                                iv.cache_path / iv.source_path, self.work_dir / iv.path
                            )
                        else:
                            shutil.copytree(
                                iv.cache_path / iv.source_path, self.work_dir / iv.path
                            )

    def _check_cache(self) -> bool:
        """
        Check if cache exists
        If corresponding hash directory or output files are not found return False
        """
        if os.path.exists(TaskBase.CACHE_DIR / self.hash):
            if isinstance(self._output, dict):
                for i, _ in self._output.items():
                    if not os.path.exists(TaskBase.CACHE_DIR / self.hash / i):
                        return False
                return True
        return False

    def _load_from_cache(self):
        """
        Load output from cache,

            File, Directory: set cache_path
            str            : read from file

        """
        if not os.path.exists(TaskBase.CACHE_DIR / self.hash):
            raise FileNotFoundError(f"Cache for {self.hash} not found")
        if not isinstance(self._output, dict):
            raise ValueError("Output is not a dictionary")

        for i, v in self._output.items():
            if isinstance(v, File) or isinstance(v, Directory):
                if not os.path.exists(TaskBase.CACHE_DIR / self.hash / i):
                    raise FileNotFoundError(f"Cache for {i} not found")
                v.cache_path = TaskBase.CACHE_DIR / self.hash / i
            elif isinstance(v, str):
                with open(TaskBase.CACHE_DIR / self.hash / i, "r") as f:
                    self._output[i] = f.read()

    @property
    def output(self) -> OUTPUT:
        """
        This attribute actually runs the task
        """
        if self._cached_output is not None:
            return self._cached_output
        outer_prev_path = None
        if TaskBase._is_temporary:
            outer_prev_path = os.getcwd()
            TaskBase._is_temporary = False

        if not os.path.exists(TaskBase.CACHE_DIR):
            os.makedirs(TaskBase.CACHE_DIR)

        # calculate hash
        self.hash

        if self._check_cache():
            print(f"{self.__class__.__name__}: Cache found skipping ({self.hash})")
            self._load_from_cache()
        else:
            print(f"{self.__class__.__name__}: Running task ({self.hash})")
            self._task_setup()

            # run task in work_dir
            prev_path = os.getcwd()

            os.chdir(self.work_dir)
            TaskBase._is_temporary = True
            self.task(self._input, self._output)
            os.chdir(prev_path)
            TaskBase._is_temporary = False

            self._cache_output()

        if outer_prev_path:
            os.chdir(outer_prev_path)
            TaskBase._is_temporary = True

        self._cached_output = self._output

        return self._output

    def _cache_output(self):
        if not isinstance(self._output, dict):
            return

        # remove if exists
        if os.path.exists(TaskBase.CACHE_DIR / self.hash):
            shutil.rmtree(TaskBase.CACHE_DIR / self.hash)

        os.makedirs(TaskBase.CACHE_DIR / self.hash)
        for i, v in self._output.items():
            val: str | File = v
            cache_path = TaskBase.CACHE_DIR / self.hash / i
            if isinstance(val, File) or isinstance(val, Directory):
                os.makedirs(TaskBase.CACHE_DIR / self.hash / i, exist_ok=True)
                if not os.path.exists(self.work_dir / val.path):
                    raise FileNotFoundError(f"{val.path} not found")
                if not os.path.isabs(val.path):
                    shutil.move(self.work_dir / val.path, cache_path)
                val.cache_path = cache_path
            elif isinstance(val, str):
                # save string to cache
                os.makedirs(TaskBase.CACHE_DIR / self.hash, exist_ok=True)
                with open(TaskBase.CACHE_DIR / self.hash / i, "w") as f:
                    f.write(val)

    @property
    def hash(self):
        """
        Calculate hash of the task
        Hash includes source code of the task and input text and file and directory content
        """

        if self._hash is not None:
            return self._hash

        hash = md5(getsource(self.task.__code__).encode())

        # create hash with input
        if isinstance(self._input, dict):
            for i, v in self._input.items():
                hash.update(i.encode())
                if isinstance(v, File):
                    with open(v.source_path, "rb") as f:
                        content = f.read()
                        hash.update(content)

                if isinstance(v, Directory):
                    _md5_update(hash, v.source_path)

                if isinstance(v, str):
                    hash.update(v.encode())

                if isinstance(v, TaskBase):
                    hash.update(v.hash.encode())

        self._hash = hash.hexdigest()
        return self._hash

    @property
    def work_dir(self):
        """
        This creates a temporary directory if work_dir is not set
        if work_dir is set the work_dir would not be deleted
        """
        if self._work_dir is None:
            self._work_dir = TemporaryDirectory()
        if isinstance(self._work_dir, TemporaryDirectory):
            return Path(self._work_dir.name)
        return self._work_dir

    def __getitem__(self, key):
        if not isinstance(self.output, dict):
            raise KeyError("Output is not a dictionary")
        else:
            return self.output[key]
