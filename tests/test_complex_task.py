import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TypedDict

import numpy as np
import pytest

from lazypp import BaseTask, File

cache_dir = Path(tempfile.TemporaryDirectory().name)


sleep_time = 1


class TestBaseTask[INPUT, OUTPUT](BaseTask[INPUT, OUTPUT]):
    def __init__(self, input: INPUT, worker: ProcessPoolExecutor | None):
        super().__init__(
            cache_dir=cache_dir,
            input=input,
            worker=worker,
            show_input=True,
            show_output=True,
        )


class CreateFileInputParameters(TypedDict):
    min: int
    max: int
    delta: int


### Create File Task
class CreateFileInput(TypedDict):
    param: CreateFileInputParameters


class CreateFileOutput(TypedDict):
    files: list[File]
    values: list[int]


class CreateFileTask(TestBaseTask[CreateFileInput, CreateFileOutput]):
    def task(self, input) -> CreateFileOutput:
        time.sleep(sleep_time)
        files = []
        values = []
        for i in range(
            input["param"]["min"], input["param"]["max"], input["param"]["delta"]
        ):
            with open(f"file_{i}.txt", "w") as f:
                f.write(f"value: {i}")
            files.append(File(f"file_{i}.txt"))
            values.append(i)
        return {"files": files, "values": values}


### Summation Task
class SummationInput(TypedDict):
    files: list[File]
    shift: int


class SummationOutput(TypedDict):
    files: list[File]
    values: list[int]


class SummationTask(TestBaseTask[SummationInput, SummationOutput]):
    def task(self, input) -> SummationOutput:
        time.sleep(sleep_time)
        values = []
        files = []
        for i, file in enumerate(input["files"]):
            with open(file.path, "r") as f:
                content = f.read()
                val = int(content.split(":")[1].strip())
            with open(f"{self.hash}_output_{i}.txt", "w") as f:
                f.write(f"value: {val + input["shift"]}")
            values.append(val + input["shift"])

        for i in range(len(input["files"])):
            files.append(File(f"{self.hash}_output_{i}.txt"))
        return {"files": files, "values": values}


### Subtraction Task
class SubtractInput(TypedDict):
    files: list[File]
    subtrahend: int


class SubtractOutput(TypedDict):
    files: list[File]
    values: list[int]


class SubtractTask(TestBaseTask[SubtractInput, SubtractOutput]):
    def task(self, input) -> SubtractOutput:
        time.sleep(sleep_time)
        values = []
        files = []
        for i, file in enumerate(input["files"]):
            with open(file.path, "r") as f:
                content = f.read()
                val = int(content.split(":")[1].strip())
            with open(f"{self.hash}_output_{i}.txt", "w") as f:
                f.write(f"value: {val - input["subtrahend"]}")
            values.append(val - input["subtrahend"])

        for i in range(len(input["files"])):
            files.append(File(f"{self.hash}_output_{i}.txt"))
        return {"files": files, "values": values}


### Multiplier Task
class MultiplyInput(TypedDict):
    files: list[File]
    multiplier: int


class MultiplyOutput(TypedDict):
    files: list[File]
    values: list[int]


class MultiplyTask(TestBaseTask[MultiplyInput, MultiplyOutput]):
    def task(self, input) -> MultiplyOutput:
        time.sleep(sleep_time)
        values = []
        files = []
        for i, file in enumerate(input["files"]):
            with open(file.path, "r") as f:
                content = f.read()
                val = int(content.split(":")[1].strip())
            with open(f"{self.hash}_output_{i}.txt", "w") as f:
                f.write(f"value: {val * input["multiplier"]}")
            values.append(val * input["multiplier"])

        for i in range(len(input["files"])):
            files.append(File(f"{self.hash}_output_{i}.txt"))
        return {"files": files, "values": values}


### Summation All Task
class SummationAllInput(TypedDict):
    files: list[File]


class SummationAllOutput(TypedDict):
    file: File
    value: int


class SummationAllTask(TestBaseTask[SummationAllInput, SummationAllOutput]):
    def task(self, input) -> SummationAllOutput:
        time.sleep(sleep_time)
        value = 0
        for file in input["files"]:
            with open(file.path, "r") as f:
                content = f.read()
                val = int(content.split(":")[1].strip())
            value += val
        with open(f"{self.hash}_output.txt", "w") as f:
            f.write(f"value: {value}")
        return {"file": File(f"{self.hash}_output.txt"), "value": value}


class TaskParameters(TypedDict):
    plus: int
    sub: int
    mul: int


def task(min: int, max: int, delta: int, multiplier: int, params: TaskParameters):
    v1 = np.arange(min, max, delta)
    v2 = multiplier * v1

    added_v1 = v1 + params["plus"]
    added_v2 = v2 + params["plus"]

    sub_v1 = v1 - params["sub"]
    sub_v2 = v2 - params["sub"]

    mul_v1 = v1 * params["mul"]
    mul_v2 = v2 * params["mul"]

    sum_added_v1 = sum(added_v1)
    sum_added_v2 = sum(added_v2)

    sum_sub_v1 = sum(sub_v1)
    sum_sub_v2 = sum(sub_v2)

    sum_mul_v1 = sum(mul_v1)
    sum_mul_v2 = sum(mul_v2)

    sum_all = sum(
        [sum_added_v1, sum_added_v2, sum_sub_v1, sum_sub_v2, sum_mul_v1, sum_mul_v2]
    )

    return {
        "v1": v1,
        "v2": v2,
        "added_v1": added_v1,
        "added_v2": added_v2,
        "sub_v1": sub_v1,
        "sub_v2": sub_v2,
        "mul_v1": mul_v1,
        "mul_v2": mul_v2,
        "sum_added_v1": sum_added_v1,
        "sum_added_v2": sum_added_v2,
        "sum_sub_v1": sum_sub_v1,
        "sum_sub_v2": sum_sub_v2,
        "sum_mul_v1": sum_mul_v1,
        "sum_mul_v2": sum_mul_v2,
        "sum_all": sum_all,
    }


@pytest.mark.parametrize(
    "task_params",
    [
        {
            "max_workers": 1,
            "time_low": 15.0,
            "time_high": 17.0,
            "min": 0,
            "max": 10,
            "delta": 2,
            "multiplier": 2,
            "params": {"plus": 3, "sub": 5, "mul": 3},
        },
        {
            "max_workers": None,
            "time_low": 15.0,
            "time_high": 17.0,
            "min": 3,
            "max": 30,
            "delta": 1,
            "multiplier": 3,
            "params": {"plus": 2, "sub": 6, "mul": 7},
        },
        {
            "max_workers": 4,
            "time_low": 6.0,
            "time_high": 8.0,
            "min": 3,
            "max": 11,
            "delta": 4,
            "multiplier": 7,
            "params": {"plus": 13, "sub": 7, "mul": 17},
        },
        {
            "max_workers": 4,
            "time_low": 0.0,
            "time_high": 0.2,
            "min": 3,
            "max": 11,
            "delta": 4,
            "multiplier": 7,
            "params": {"plus": 13, "sub": 7, "mul": 17},
        },
    ],
)
def test_complex_task(task_params):
    if task_params["max_workers"] is None:
        worker = None
    else:
        worker = ProcessPoolExecutor(max_workers=task_params["max_workers"])

    start = time.time()
    expected_output = task(
        min=task_params["min"],
        max=task_params["max"],
        delta=task_params["delta"],
        multiplier=task_params["multiplier"],
        params=task_params["params"],
    )

    v1_task = CreateFileTask(
        worker=worker,
        input={
            "param": {
                "min": task_params["min"],
                "max": task_params["max"],
                "delta": task_params["delta"],
            }
        },
    )

    v2_task = MultiplyTask(
        worker=worker,
        input={
            "files": v1_task.output["files"],
            "multiplier": task_params["multiplier"],
        },
    )

    added_v1_task = SummationTask(
        worker=worker,
        input={
            "files": v1_task.output["files"],
            "shift": task_params["params"]["plus"],
        },
    )
    added_v2_task = SummationTask(
        worker=worker,
        input={
            "files": v2_task.output["files"],
            "shift": task_params["params"]["plus"],
        },
    )

    sub_v1_task = SubtractTask(
        worker=worker,
        input={
            "files": v1_task.output["files"],
            "subtrahend": task_params["params"]["sub"],
        },
    )
    sub_v2_task = SubtractTask(
        worker=worker,
        input={
            "files": v2_task.output["files"],
            "subtrahend": task_params["params"]["sub"],
        },
    )

    mul_v1_task = MultiplyTask(
        worker=worker,
        input={
            "files": v1_task.output["files"],
            "multiplier": task_params["params"]["mul"],
        },
    )
    mul_v2_task = MultiplyTask(
        worker=worker,
        input={
            "files": v2_task.output["files"],
            "multiplier": task_params["params"]["mul"],
        },
    )

    sum_added_v1_task = SummationAllTask(
        worker=worker, input={"files": added_v1_task.output["files"]}
    )
    sum_added_v2_task = SummationAllTask(
        worker=worker, input={"files": added_v2_task.output["files"]}
    )

    sum_sub_v1_task = SummationAllTask(
        worker=worker, input={"files": sub_v1_task.output["files"]}
    )
    sum_sub_v2_task = SummationAllTask(
        worker=worker, input={"files": sub_v2_task.output["files"]}
    )

    sum_mul_v1_task = SummationAllTask(
        worker=worker, input={"files": mul_v1_task.output["files"]}
    )
    sum_mul_v2_task = SummationAllTask(
        worker=worker, input={"files": mul_v2_task.output["files"]}
    )

    sum_all_task = SummationAllTask(
        worker=worker,
        input={
            "files": [
                sum_added_v1_task.output["file"],
                sum_added_v2_task.output["file"],
                sum_sub_v1_task.output["file"],
                sum_sub_v2_task.output["file"],
                sum_mul_v1_task.output["file"],
                sum_mul_v2_task.output["file"],
            ]
        },
    )

    middle = time.time()

    assert middle - start < 0.1

    sum_all_task.result()
    end = time.time()

    elapsed = end - middle
    assert elapsed < task_params["time_high"]
    assert elapsed > task_params["time_low"]

    assert v1_task.result()["values"] == list(expected_output["v1"])
    assert v2_task.result()["values"] == list(expected_output["v2"])
    assert added_v1_task.result()["values"] == list(expected_output["added_v1"])
    assert added_v2_task.result()["values"] == list(expected_output["added_v2"])
    assert sub_v1_task.result()["values"] == list(expected_output["sub_v1"])
    assert sub_v2_task.result()["values"] == list(expected_output["sub_v2"])
    assert mul_v1_task.result()["values"] == list(expected_output["mul_v1"])
    assert mul_v2_task.result()["values"] == list(expected_output["mul_v2"])
    assert sum_added_v1_task.result()["value"] == expected_output["sum_added_v1"]
    assert sum_added_v2_task.result()["value"] == expected_output["sum_added_v2"]
    assert sum_sub_v1_task.result()["value"] == expected_output["sum_sub_v1"]
    assert sum_sub_v2_task.result()["value"] == expected_output["sum_sub_v2"]
    assert sum_mul_v1_task.result()["value"] == expected_output["sum_mul_v1"]
    assert sum_mul_v2_task.result()["value"] == expected_output["sum_mul_v2"]
    assert sum_all_task.result()["value"] == expected_output["sum_all"]
