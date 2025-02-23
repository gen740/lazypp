import asyncio
import copy
import os
import select
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from typing import Any

import graphviz

from lazypp.dummy_output import DummyOutput
from lazypp.task import BaseTask, _call_func_on_specific_class


def run_sh(
    command: list[str],
    env: dict[str, str] | None = None,
    input_data: str | None = None,  # stdin に送るデータを引数として受け取る
    input_delay: float | None = None,
):
    # Popen を使って sys.stdout に出力する例
    with subprocess.Popen(
        command,
        stdin=subprocess.PIPE,  # stdin を有効にする
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    ) as process:
        if input_data:
            # stdin にデータを書き込む
            assert process.stdin is not None
            if input_delay is not None:
                time.sleep(input_delay)
            process.stdin.write(input_data)
            process.stdin.close()  # 書き込みが終わったら閉じる

        # 標準出力と標準エラーのリアルタイム処理
        while process.poll() is None:
            readable, _, _ = select.select([process.stdout, process.stderr], [], [])
            for stream in readable:
                line = stream.readline()
                if line:
                    output = sys.stdout if stream is process.stdout else sys.stderr
                    output.write(line)
                    output.flush()

        # 終了コードの確認
        return_code = process.wait()
        return return_code


def source(script_path: str):
    command = f"bash -c 'source {script_path} > /dev/null 2>&1 && env'"
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, text=True)
    for line in result.stdout.splitlines():
        key, _, value = line.partition("=")
        if key != "":
            os.environ[key] = value


def _gather_tasks(inputs) -> list[BaseTask]:
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
            tasks.append(output.task)
        elif isinstance(output, BaseTask):
            tasks.append(output)

    _gather_task(inputs)
    return tasks


def gather[T](output: T) -> T:
    tasks = [t() for t in _gather_tasks(output)]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*tasks))

    ret: T = copy.deepcopy(output)
    ret: T = _call_func_on_specific_class(
        ret,
        lambda obj: obj.restore_output(),
        DummyOutput,
    )
    if ret is None:
        raise ValueError("output is None")

    return ret


def visualize(input: Any) -> graphviz.Digraph:
    dot = graphviz.Digraph()

    tasks = _gather_tasks(input)

    def _visualize(task: BaseTask, dot: graphviz.Digraph):
        if task in dot.body:
            return

        if task.status in ["FAILED", "SKIPPED"]:
            dot.node(
                task._visualization_node_name,
                f"{task.name}\nstatus: {task.status}",
                color="red",
                bgcolor="black",
                fillcolor="red",
                style="filled",
                fontcolor="white",
                shape="box",
            )
        elif task.status == "RUNNING":
            dot.node(
                task._visualization_node_name,
                f"{task.name}\nstatus: {task.status}",
                color="green",
                fillcolor="green",
                style="filled",
                fontcolor="black",
                shape="box",
            )
        elif task.status == "COMPLETE":
            dot.node(
                task._visualization_node_name,
                f"{task.name}\nstatus: {task.status}",
                color="blue",
                fillcolor="blue",
                style="filled",
                fontcolor="white",
                shape="box",
            )
        elif task.status == "CACHED":
            dot.node(
                task._visualization_node_name,
                f"{task.name}\nstatus: {task.status}",
                color="black",
                fillcolor="gray",
                style="filled",
                fontcolor="black",
                shape="box",
            )
        else:
            dot.node(
                task._visualization_node_name, f"{task.name}\nstatus: {task.status}"
            )

        # create unique
        unique_dependencies = []

        for dep in task._dependent_tasks:
            if dep.name not in map(lambda x: x.name, unique_dependencies):
                unique_dependencies.append(dep)

        for dep in unique_dependencies:
            dot.edge(dep._visualization_node_name, task._visualization_node_name)
            _visualize(dep, dot)

    for task in tasks:
        _visualize(task, dot)

    return dot
