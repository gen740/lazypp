import asyncio
import json
import os
import pickle
import shutil
from abc import ABC
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from hashlib import md5
from inspect import getsource
from pathlib import Path
from pickle import PicklingError
from tempfile import TemporaryDirectory
from typing import Any, TypeGuard, cast

from rich.console import Console

from .file_objects import BaseEntry, File

console = Console(
    log_path=False,
)


def _pickleable(obj):
    """
    Check if object is pickable
    """
    try:
        pickle.dumps(obj)
        return True
    except PicklingError:
        return False


def _is_valid_input(input: Any) -> TypeGuard[dict[str, Any] | None]:
    """
    Validate input

    key should be string
    value is BaseEntry, BaseTask or pickleable object

    or Map of these

    str
    int
    float
    BaseEntry
    BaseTask

    or Map / Sequence of these

    """
    if input is None:
        return True

    if not isinstance(input, dict):
        return False

    for key, val in input.items():
        if not isinstance(key, str):
            return False

        if not (
            BaseTask in val.__class__.__mro__
            or BaseEntry in val.__class__.__mro__
            or _pickleable(val)
        ):
            return False

    return True


def _is_valid_output(output: Any):
    """
    Validate output

    key should be string

    value should be File, Directory, str or pickleable object
    """
    if output is None:
        return True

    if not isinstance(output, dict):
        return False

    for key, val in output.items():
        if not isinstance(key, str):
            return False

        if not (BaseEntry in val.__class__.__mro__ or _pickleable(val)):
            return False

    return True


class BaseTask[INPUT, OUTPUT](ABC):
    _global_locks = defaultdict(asyncio.Lock)

    def __init__(
        self,
        cache_dir: str | Path,
        input: INPUT,
        worker: ThreadPoolExecutor | None = None,
        work_dir: str | Path | None = None,
        show_input: bool = False,
        show_output: bool = False,
    ):
        self._work_dir: Path | TemporaryDirectory | None = (
            Path(work_dir) if work_dir else None
        )
        self._cached_output: OUTPUT | None = None
        self._worker = worker
        self._cache_dir = Path(cache_dir)
        self._input = input
        self._show_input = show_input
        self._show_output = show_output

        self._hash: str | None = None

    async def task(self, input: INPUT) -> OUTPUT:
        _ = input
        raise NotImplementedError

    def result(self) -> OUTPUT:
        return asyncio.run(self())

    async def __call__(self) -> OUTPUT:
        async with BaseTask._global_locks[self.hash]:
            if self._cached_output is not None:
                return self._cached_output

            if self._check_cache():
                output = self._load_from_cache()
                console.log(
                    f"{self.__class__.__name__}: Cache found skipping",
                )
                if self._show_output:
                    console.log(
                        f"{self.__class__.__name__}: Input",
                        self._input,
                    )
                if self._show_output:
                    console.log(
                        f"{self.__class__.__name__}: Output",
                        output,
                    )
                return output

            await self._setup()

            prev_dir = os.getcwd()
            os.chdir(self.work_dir)

            if self._worker is None:
                console.log(f"{self.__class__.__name__}: Running with")
                if self._show_input:
                    console.log(
                        self._input,
                    )
                output = await self.task(self._input)
            else:

                def run_async_in_thread(async_func, *args, **kwargs):
                    console.log(f"{self.__class__.__name__}: Running with")
                    if self._show_input:
                        console.log(
                            self._input,
                        )
                    return asyncio.run(async_func(*args, **kwargs))

                loop = asyncio.get_event_loop()
                output = loop.run_in_executor(
                    self._worker, run_async_in_thread, self.task, self._input
                )
                output = await output

            if not _is_valid_output(output):
                raise ValueError(
                    "Output should be a dictionary with string keys and have pickleable values"
                )
            self._cache_output(output)
            self._cached_output = output

            os.chdir(prev_dir)

        if self._show_output:
            console.log(
                f"{self.__class__.__name__}: Output",
                output,
            )
        return output

        # return self.output

    @property
    async def output(self) -> OUTPUT:
        """
        return synchronous output
        """
        return await self()

    async def _setup(self):
        """Setup the Task

        This method will copy input files to work_dir
        and dependencies output files to work_dir
        """
        if not _is_valid_input(self._input):
            raise ValueError("Input should be a dictionary with string keys")

        if self._input is None:
            return

        for _, inval in self._input.items():
            if isinstance(inval, BaseEntry):
                inval._copy_to_dest(self.work_dir)

        dependent_tasks = []
        for _, inval in self._input.items():
            if isinstance(inval, BaseTask):
                dependent_tasks.append(inval())
        dependent_tasks_output = await asyncio.gather(*dependent_tasks)

        for output in dependent_tasks_output:
            for _, val in output.items():
                if isinstance(val, BaseEntry):
                    val.copy(self.work_dir)

    def _check_cache(self) -> bool:
        """
        Check if cache exists
        If corresponding hash directory or output files are not found return False
        """
        if os.path.exists(self._cache_dir / self.hash):
            return True
        return False

    def _load_from_cache(self) -> OUTPUT:
        """
        Load output from cache,

            File, Directory: set cache_path
            str            : read from file

        /key/data -> pickled data

        """
        output = {}

        if not os.path.exists(self._cache_dir / self.hash):
            raise FileNotFoundError(f"Cache for {self.hash} not found")

        for dir in os.listdir(self._cache_dir / self.hash):
            output[dir] = pickle.load(
                open(self._cache_dir / self.hash / dir / "data", "rb")
            )
        self._cached_output = cast(OUTPUT, output)
        return self._cached_output

    def _cache_output(self, output):
        if not isinstance(output, dict):
            return

        # remove if exists
        if os.path.exists(self._cache_dir / self.hash):
            shutil.rmtree(self._cache_dir / self.hash)

        os.makedirs(self._cache_dir / self.hash)
        for i, v in output.items():
            val: str | File = v
            cache_path: Path = self._cache_dir / self.hash / i
            if isinstance(val, BaseEntry):
                val._cache(cache_path)
            elif _pickleable(val):
                # save string to cache
                os.makedirs(cache_path, exist_ok=True)
                with open(cache_path / "data", "wb") as f:
                    f.write(pickle.dumps(val))

    @property
    def hash(self):
        """
        Calculate hash of the task
        Hash includes source code of the task and input text and file and directory content
        """
        return md5(self._dump_input().encode()).hexdigest()

    def _dump_input(self, indent: bool | int = False) -> str:
        """
        Dump input to as json string

        """

        if not _is_valid_input(self._input):
            raise ValueError(
                "Input should be a dictionary with string keys and have pickleable values"
            )

        ret_dict: dict[str, Any] = {
            "__lazypp_task_source__": md5(
                getsource(self.task.__code__).encode()
            ).hexdigest(),
        }

        if self._input is None:
            return json.dumps(ret_dict)

        for i, v in self._input.items():
            if BaseTask in v.__class__.__mro__:
                ret_dict[i] = v.hash
            elif BaseEntry in v.__class__.__mro__:
                ret_dict[i] = v._md5_hash().hexdigest()
            else:
                ret_dict[i] = md5(pickle.dumps(v)).hexdigest()

        return json.dumps(ret_dict, indent=indent)

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
