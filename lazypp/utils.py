import asyncio
import copy
import select
import subprocess
import sys
from collections.abc import Mapping, Sequence
from typing import Any

from lazypp.dummy_output import DummyOutput
from lazypp.task import BaseTask, _call_func_on_specific_class


def run_sh(
    command: list[str],
    env: dict[str, str] | None = None,
):
    # Popen を使って sys.stdout に出力する例
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
    ) as process:
        # 標準出力と標準エラーのリアルタイム処理
        while process.poll() is None:
            readable, _, _ = select.select([process.stdout, process.stderr], [], [])
            for stream in readable:
                line = stream.readline()
                if line:
                    output = sys.stdout if stream is process.stdout else sys.stderr
                    output.write(line)
                    output.flush()


def gather[T](output: T) -> T:
    tasks = []
    visited = set()

    def _gather_task(output: Any):
        if id(output) in visited:
            return
        else:
            visited.add(id(output))
        if isinstance(output, Sequence):
            for item in output:
                _gather_task(item)
        elif isinstance(output, Mapping):
            for item in output.values():
                _gather_task(item)
        elif isinstance(output, DummyOutput):
            tasks.append(output.task())
        elif isinstance(output, BaseTask):
            tasks.append(output())

    _gather_task(output)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*tasks))

    visited.clear()

    ret = copy.deepcopy(output)
    ret = _call_func_on_specific_class(
        ret,
        lambda obj: obj.restore_output(),
        DummyOutput,
    )
    if ret is None:
        raise ValueError("output is None")

    return ret
