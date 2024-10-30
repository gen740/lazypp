import asyncio
import contextlib
import json
import os
import pickle
import shutil
from abc import ABC
from collections import defaultdict
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from hashlib import md5
from inspect import getsource
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, TypeGuard, cast

from rich.console import Console

import lazypp.dummy_output

from .file_objects import BaseEntry

console = Console(
    log_path=False,
)

_DEBUG = True


def _is_valid_input(input: Any) -> TypeGuard[dict[str, Any] | None]:
    if input is None:
        return True

    if not isinstance(input, dict):
        return False

    for key in input:
        if not isinstance(key, str):
            return False

    return True


def _is_valid_output[OUTPUT](input: OUTPUT | None) -> TypeGuard[OUTPUT]:
    if input is None:
        return True

    if not isinstance(input, dict):
        return False

    for key in input:
        if not isinstance(key, str):
            return False

    return True


class BaseTask[INPUT, OUTPUT](ABC):
    _global_locks = defaultdict(asyncio.Lock)

    def __init__(
        self,
        *,
        cache_dir: str | Path,
        input: INPUT,
        worker: ProcessPoolExecutor | None = None,
        work_dir: str | Path | None = None,
        show_input: bool = False,
        show_output: bool = False,
    ):
        self._work_dir: Path | TemporaryDirectory | None = (
            Path(work_dir) if work_dir else None
        )
        self._worker = worker
        self._cache_dir = Path(cache_dir)
        self._input = input
        self._output: OUTPUT | None = None
        self._show_input = show_input
        self._show_output = show_output
        self._hash: str | None = None

    def _log_input(self):
        if self._show_input:
            console.log(
                f"{self.__class__.__name__}: Input",
                self._input,
            )

    def _log_output(self):
        if self._show_output:
            console.log(
                f"{self.__class__.__name__}: Output",
                self._output,
            )

    def task(self, input: INPUT) -> OUTPUT:
        _ = input
        raise NotImplementedError

    def result(self) -> OUTPUT:
        return asyncio.run(self())

    async def __call__(self) -> OUTPUT:
        async with BaseTask._global_locks[self.hash]:
            if self._output is not None:
                return self._output

            if self._check_cache():
                self._output = self._load_from_cache()
                console.log(
                    f"{self.__class__.__name__}: Cache found skipping",
                )
                self._log_input()
                self._log_output()
                return self._output

            await self._setup()

            if self._worker is None:
                self._output = self._run_task_in_workdir(self.work_dir)
            else:
                loop = asyncio.get_event_loop()
                self._output = await loop.run_in_executor(
                    self._worker, self._run_task_in_workdir, self.work_dir
                )

            self._cache_output()

            if not _is_valid_output(self._output):
                raise ValueError(
                    "Output should be a dictionary with string keys and have pickleable values"
                )

            self._log_output()
            return self._output

    def _run_task_in_workdir(self, work_dir: Path) -> OUTPUT:
        console.log(f"{self.__class__.__name__}: Running in {work_dir}")
        self._log_input()
        with contextlib.chdir(work_dir):
            return self.task(self._input)

    @property
    def output(self) -> OUTPUT:
        """
        return synchronous output
        """
        if self._output is not None:
            return self._output
        return cast(OUTPUT, lazypp.dummy_output.DummyOutput(self))

    async def _setup(self):
        """Setup the Task

        This method will copy input files to work_dir
        and dependencies output files to work_dir
        """
        if not _is_valid_input(self._input):
            raise ValueError("Input should be a dictionary with string keys")

        if self._input is None:
            return

        _call_func_on_specific_class(
            self._input,
            lambda entry: entry._copy_to_dest(self.work_dir),
            BaseEntry,
        )

        dependent_tasks = []
        _call_func_on_specific_class(
            self._input,
            lambda task: dependent_tasks.append(asyncio.create_task(task())),
            BaseTask,
        )
        _call_func_on_specific_class(
            self._input,
            lambda output: dependent_tasks.append(asyncio.create_task(output.task())),
            lazypp.dummy_output.DummyOutput,
        )
        dependent_tasks_output = await asyncio.gather(*dependent_tasks)

        # Restore output

        _call_func_on_specific_class(
            self._input,
            lambda output: output.restore_output(),
            lazypp.dummy_output.DummyOutput,
        )

        for output in dependent_tasks_output:
            _call_func_on_specific_class(
                output,
                lambda entry: entry.copy(self.work_dir),
                BaseEntry,
            )

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
        unpickle "cache_dir/hash/data" file and return the output
        """
        if not os.path.exists(self._cache_dir / self.hash):
            raise FileNotFoundError(f"Cache for {self.hash} not found")
        return cast(
            OUTPUT, pickle.load(open(self._cache_dir / self.hash / "data", "rb"))
        )

    def _cache_output(self):
        if not _is_valid_output(self._output):
            raise ValueError("Output should be a dictionary with string keys")

        # remove if exists
        if os.path.exists(self._cache_dir / self.hash):
            shutil.rmtree(self._cache_dir / self.hash)

        os.makedirs(self._cache_dir / self.hash)
        if _DEBUG:
            console.log(f"{self.__class__.__name__}: Caching output")

        _call_func_on_specific_class(
            self._output,
            lambda entry: entry._cache(self.work_dir, self._cache_dir / self.hash),
            BaseEntry,
        )

        with open(self._cache_dir / self.hash / "data", "wb") as f:
            f.write(pickle.dumps(self._output))

        if _DEBUG:
            console.log(f"{self.__class__.__name__}: Caching output done")

    @property
    def hash(self):
        """
        Calculate hash of the task
        Hash includes source code of the task and input text and file and directory content
        """
        if self._hash is None:
            if _DEBUG:
                console.log(f"{self.__class__.__name__}: Calculating hash")
            self._hash = md5(self._dump_input().encode()).hexdigest()
            if _DEBUG:
                console.log(f"{self.__class__.__name__}: Calculating hash done")
        return self._hash

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

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_worker"] = None  # worker is not picklable
        state["_work_dir"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)


def _call_func_on_specific_class[T](
    obj: Any, func: Callable[[T], Any], t: type[T]
) -> Any:
    visited = set()

    def call_func_inner(inner_obj: Any, parent: Any = None, key: Any = None) -> None:
        if id(inner_obj) in visited:
            return
        visited.add(id(inner_obj))

        if isinstance(inner_obj, t):
            if parent is not None:
                parent[key] = func(inner_obj) or inner_obj
            return

        if isinstance(inner_obj, Mapping):
            for k, v in inner_obj.items():
                call_func_inner(v, inner_obj, k)
        elif isinstance(inner_obj, Sequence) and not isinstance(
            inner_obj, (str, bytes, bytearray)
        ):
            for i, v in enumerate(inner_obj):
                call_func_inner(v, inner_obj, i)

    if isinstance(obj, t):
        return func(obj) or obj

    call_func_inner(obj)
    return obj


def task[INPUT, OUTPUT](
    input: type[INPUT],
    output: type[OUTPUT],
    cache_dir: str | Path,
    worker: ProcessPoolExecutor | None = None,
):
    def decorator(fun: Callable[[INPUT], OUTPUT]) -> type[BaseTask[INPUT, OUTPUT]]:
        class _Task(BaseTask[INPUT, OUTPUT]):
            def __init__(self, input: INPUT):
                super().__init__(
                    cache_dir=cache_dir,
                    worker=worker,
                    input=input,
                )

            def task(self, input: INPUT) -> OUTPUT:
                return fun(input)

        return _Task

    return decorator
