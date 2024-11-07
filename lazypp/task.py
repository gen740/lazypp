import asyncio
import base64
import contextlib
import copy
import json
import multiprocessing
import os
import pickle
import sys
import threading
from abc import ABC
from collections import defaultdict
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import IO, Any, Callable, Literal, TextIO, TypeGuard, cast

from rich import box
from rich.console import Console, Group, RenderableType
from rich.panel import Panel
from rich.pretty import Pretty
from xxhash import xxh128

import lazypp.dummy_output
from lazypp.exceptions import RetryTask

from .file_objects import BaseEntry

console = Console(
    log_path=False,
)

_DEBUG = False


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


class _Tee(TextIO):
    def __init__(self, file_1: IO[str], file_2: IO[str], disable: tuple[bool, bool]):
        self.files = (file_1, file_2)
        self.disable = disable

    def write(self, data):
        bytes_written = 0
        for f, d in zip(self.files, self.disable):
            if not d:
                f.write(data)
                bytes_written = len(data)
        return bytes_written

    def flush(self):
        for f, d in zip(self.files, self.disable):
            if not d:
                f.flush()


class BaseTask[INPUT, OUTPUT](ABC):
    _global_locks = defaultdict(asyncio.Lock)
    _logging_lock: threading.Lock | None = None

    @classmethod
    def _get_logging_lock(cls):
        if cls._logging_lock is None:
            cls._logging_lock = multiprocessing.Manager().Lock()
        return cls._logging_lock

    def __init__(
        self,
        *,
        cache_dir: str | Path,
        input: INPUT,
        worker: ProcessPoolExecutor | None = None,
        work_dir: str | Path | None = None,
        show_input: bool = False,
        show_output: bool = False,
        name: str | None = None,
        capture: Literal["stdout", "stderr", "both", "none"] = "both",
        suppress: Literal["stdout", "stderr", "both", "none"] = "none",
    ):
        self._work_dir: Path | TemporaryDirectory | None = (
            Path(work_dir) if work_dir else None
        )
        self._worker = worker
        self._cache_dir = Path(cache_dir)
        self._input = input
        self._output: OUTPUT | None = None
        self._output_lock = asyncio.Lock()
        self._show_input = show_input
        self._show_output = show_output
        self._hash: str | None = None
        self._upstream_results: list[Any] | None = None
        self._name = name

        if capture in ["stdout", "stderr", "both", "none"]:
            if capture == "stdout":
                self._capture_stdout = True
                self._capture_stderr = False
            elif capture == "stderr":
                self._capture_stdout = False
                self._capture_stderr = True
            elif capture == "both":
                self._capture_stdout = True
                self._capture_stderr = True
            else:
                self._capture_stdout = False
                self._capture_stderr = False
        else:
            raise ValueError(
                "Invalid value for capture. Should be stdout, stderr, both or none"
            )

        if suppress in ["stdout", "stderr", "both", "none"]:
            if suppress == "stdout":
                self._suppress_stdout = True
                self._suppress_stderr = False
            elif suppress == "stderr":
                self._suppress_stdout = False
                self._suppress_stderr = True
            elif suppress == "both":
                self._suppress_stdout = True
                self._suppress_stderr = True
            else:
                self._suppress_stdout = False
                self._suppress_stderr = False
        else:
            raise ValueError(
                "Invalid value for suppress. Should be stdout, stderr, both or none"
            )

    def task(self, input: INPUT) -> OUTPUT:
        _ = input
        raise NotImplementedError

    def result(self) -> OUTPUT:
        return asyncio.run(self())

    def _log_input(self):
        if self._show_input:
            console.log(
                f"{" " * len(self.name)}  Input",
                self._input,
            )

    def _log_output(self):
        if self._show_output:
            console.log(
                f"{" " * len(self.name)}  Output",
                self._output,
            )

    async def _collect_upstream_results(self):
        if self._upstream_results is not None:
            return
        elif self._input is None:
            self._upstream_results = []
            return

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
        self._upstream_results = await asyncio.gather(*dependent_tasks)
        _call_func_on_specific_class(
            self._input,
            lambda output: output.restore_output(),
            lazypp.dummy_output.DummyOutput,
        )

    async def _setup_workdir(self):
        """Setup the Task

        This method will copy input files to work_dir and dependencies output files to work_dir
        This method should be called before running the task
        """
        if self._input is None:
            return
        elif self._upstream_results is None:
            raise ValueError("Upstream results are not collected")

        # Restore output
        _call_func_on_specific_class(
            self._input,
            lambda entry: entry._copy_to_dest(self.work_dir),
            BaseEntry,
        )
        for output in self._upstream_results:
            _call_func_on_specific_class(
                output,
                lambda entry: entry._copy_to_dest(self.work_dir),
                BaseEntry,
            )

    async def __call__(self) -> OUTPUT:
        async with self._output_lock:
            if self._output is not None:
                return self._output

            await self._collect_upstream_results()

            async with BaseTask._global_locks[await self.get_hash()]:
                if self._check_cache():
                    self._output = self._load_from_cache()

                    component: list[RenderableType] = []
                    if self._show_input:
                        component.append(
                            Panel(
                                Group(
                                    Pretty(
                                        self._input,
                                        indent_guides=True,
                                        max_length=50,
                                    ),
                                ),
                                title="Input",
                                title_align="left",
                                border_style="bold color(240)",
                                box=box.SQUARE,
                            )
                        )
                    if self._show_output:
                        component.append(
                            Panel(
                                Group(
                                    Pretty(
                                        self._output,
                                        indent_guides=True,
                                        max_length=50,
                                    ),
                                ),
                                title="Output",
                                title_align="left",
                                border_style="bold color(240)",
                                box=box.SQUARE,
                            )
                        )
                    with BaseTask._get_logging_lock():
                        console.log(
                            Panel(
                                Group(
                                    *component,
                                ),
                                title=f"{self.name}: Cache found",
                                title_align="left",
                                border_style="bold color(240)",
                                box=box.HEAVY,
                            )
                        )

                    return self._output
                retry_count = 0
                while retry_count < 3:
                    try:
                        if self._worker is None:
                            await self._setup_workdir()
                            self._output = self._run_task_in_workdir(self.work_dir)
                        else:
                            await self._setup_workdir()
                            loop = asyncio.get_event_loop()
                            self._output = await loop.run_in_executor(
                                self._worker,
                                self._run_task_in_workdir,
                                self.work_dir,
                            )
                        break
                    except RetryTask as _:
                        retry_count += 1
                        with BaseTask._get_logging_lock():
                            console.log(
                                Panel(
                                    f"[bold red]Retry Count: {retry_count}[/bold red]",
                                    title=f"{self.name}: Retring...",
                                    title_align="left",
                                    border_style="bold red",
                                    box=box.HEAVY,
                                )
                            )
                        if isinstance(self._work_dir, TemporaryDirectory):
                            self._work_dir.cleanup()
                            self._work_dir = None
                        continue
                else:
                    raise RuntimeError("Task failed after 3 retries")

            self._cache_output()
            if not _is_valid_output(self._output):
                raise ValueError(
                    "Output should be a dictionary with string keys and have pickleable values"
                )

            component: list[RenderableType] = []
            if self._show_output:
                component.append(
                    Panel(
                        Group(
                            Pretty(
                                self._output,
                                indent_guides=True,
                                max_length=50,
                            ),
                        ),
                        title="Output",
                        title_align="left",
                        border_style="bold blue",
                        box=box.SQUARE,
                    )
                )
            with BaseTask._get_logging_lock():
                console.log(
                    Panel(
                        Group(
                            *component,
                        ),
                        title=f"{self.name}: Done!",
                        title_align="left",
                        border_style="bold green",
                        box=box.HEAVY,
                    )
                )
            return self._output

    async def get_hash(self) -> str:
        if self._hash is not None:
            return self._hash
        if self._worker is not None:
            loop = asyncio.get_event_loop()
            self._hash = str(
                await loop.run_in_executor(self._worker, self._calculate_hash)
            )
        else:
            self._hash = self._calculate_hash()
        return self._hash

    def _run_task_in_workdir(self, work_dir: Path) -> OUTPUT:
        os.makedirs(self._cache_dir / self.hash, exist_ok=True)

        with contextlib.chdir(work_dir):
            component: list[RenderableType] = []
            component.append("  cwd   : " + str(work_dir))
            if self._capture_stdout:
                component.append(
                    f"  stdout: {self._cache_dir / self.hash / 'stdout.log'}"
                )
            if self._capture_stderr:
                component.append(
                    f"  stderr: {self._cache_dir / self.hash / 'stderr.log'}"
                )
            if self._show_input:
                component.append(
                    Panel(
                        Group(
                            Pretty(
                                self._input,
                                indent_guides=True,
                                max_length=50,
                            ),
                        ),
                        title="Input",
                        title_align="left",
                        border_style="bold blue",
                        box=box.SQUARE,
                    )
                )
            with BaseTask._get_logging_lock():
                console.log(
                    Panel(
                        Group(
                            *component,
                        ),
                        title=f"{self.name}: Running...",
                        title_align="left",
                        border_style="bold blue",
                        box=box.HEAVY,
                    )
                )

            with (
                open(self._cache_dir / self.hash / "stdout.log", "w") as stdout_log,
                open(self._cache_dir / self.hash / "stderr.log", "w") as stderr_log,
                contextlib.redirect_stdout(
                    _Tee(
                        sys.stdout,
                        stdout_log,
                        (self._suppress_stdout, not self._capture_stdout),
                    )
                ),
                contextlib.redirect_stderr(
                    _Tee(
                        sys.stderr,
                        stderr_log,
                        (self._suppress_stderr, not self._capture_stderr),
                    )
                ),
            ):
                return self.task(copy.deepcopy(self._input))

    @property
    def output(self) -> OUTPUT:
        """
        return synchronous output
        """
        if self._output is not None:
            return self._output
        return cast(OUTPUT, lazypp.dummy_output.DummyOutput(self))

    def _check_cache(self) -> bool:
        """
        Check if cache exists
        If corresponding hash directory or output files are not found return False
        """
        if os.path.exists(self._cache_dir / self.hash / "output.pkl"):
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
            OUTPUT, pickle.load(open(self._cache_dir / self.hash / "output.pkl", "rb"))
        )

    def _cache_output(self):
        if not _is_valid_output(self._output):
            raise ValueError("Output should be a dictionary with string keys")

        # remove if exists
        if os.path.exists(self._cache_dir / self.hash / "output.pkl"):
            os.remove(self._cache_dir / self.hash / "output.pkl")

        os.makedirs(self._cache_dir / self.hash, exist_ok=True)
        if _DEBUG:
            console.log(f"{self.__class__.__name__}: Caching output")

        _call_func_on_specific_class(
            self._output,
            lambda entry: entry._cache(self.work_dir, self._cache_dir / self.hash),
            BaseEntry,
        )

        # dump inputs as json
        with open(self._cache_dir / self.hash / "input.json", "w") as f:
            f.write(self._dump_input(indent=4))

        # dump output
        with open(self._cache_dir / self.hash / "output.pkl", "wb") as f:
            f.write(pickle.dumps(self._output))

        if _DEBUG:
            console.log(f"{self.__class__.__name__}: Caching output done")

    @property
    def name(self) -> str:
        if self._name is None:
            self._name = self.__class__.__name__
        return self._name

    def _calculate_hash(self):
        """
        Calculate hash of the task
        Hash includes source code of the task and input text and file and directory content
        """
        if _DEBUG:
            console.log(f"{self.__class__.__name__}: Calculating hash")
        res = f"{xxh128(self._dump_input().encode()).hexdigest()}_{self.name}"
        if _DEBUG:
            console.log(f"{self.__class__.__name__}: Calculating hash done")
        return res

    @property
    def hash(self) -> str:
        if self._hash is None:
            raise ValueError("Hash is not calculated. Run the task to calculate hash")
        return str(self._hash)

    def _dump_input(self, indent: bool | int = False) -> str:
        """
        Dump input to as json string
        """
        if not _is_valid_input(self._input):
            raise ValueError(
                "Input should be a dictionary with string keys and have pickleable values"
            )

        source_code = {
            "co_code": base64.b64encode(self.task.__code__.co_code).decode(),
            "co_consts": self.task.__code__.co_consts,
            "co_varnames": self.task.__code__.co_varnames,
            "co_names": self.task.__code__.co_names,
        }

        if self._input is None:
            return json.dumps({"__lazypp_task_source__": source_code})

        ret = copy.deepcopy(self._input)

        ret["__lazypp_task_source__"] = source_code
        _call_func_on_specific_class(
            ret,
            lambda entry: entry._xxh128_hash().hexdigest(),
            BaseEntry,
        )
        _call_func_on_specific_class(
            ret,
            lambda task: task.hash,
            BaseTask,
        )
        return json.dumps(ret, indent=indent)

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
        state["_output_lock"] = None  # lock is not picklable
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._output_lock = asyncio.Lock()

    def __repr__(self):
        return f"<{self.name}: {self.hash}>"


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
