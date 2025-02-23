from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TypedDict

from lazypp import BaseTask, File
from lazypp.utils import visualize

cache_dir = Path("cache").resolve()
worker = ProcessPoolExecutor(max_workers=8)


class TestBaseTask[INPUT, OUTPUT](BaseTask[INPUT, OUTPUT]):
    def __init__(self, input: INPUT, name: str | None = None):
        super().__init__(
            name=name,
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
        values = []
        files = []
        for i, file in enumerate(input["files"]):
            with open(file.path, "r") as f:
                content = f.read()
                val = int(content.split(":")[1].strip())
            if val == 4:
                raise RuntimeError("Value is 4")
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


def main():
    task_params = {
        "min": 0,
        "max": 10,
        "delta": 2,
        "multiplier": 2,
        "params": {"plus": 3, "sub": 5, "mul": 3},
    }

    v1_task = CreateFileTask(
        input={
            "param": {
                "min": task_params["min"],
                "max": task_params["max"],
                "delta": task_params["delta"],
            }
        },
        name="v1",
    )

    v2_task = MultiplyTask(
        input={
            "files": v1_task.output["files"],
            "multiplier": task_params["multiplier"],
        },
        name="v2",
    )

    added_v1_task = SummationTask(
        input={
            "files": v1_task.output["files"],
            "shift": task_params["params"]["plus"],
        },
        name="added_v1",
    )
    added_v2_task = SummationTask(
        input={
            "files": v2_task.output["files"],
            "shift": task_params["params"]["plus"],
        },
        name="added_v2",
    )

    sub_v1_task = SubtractTask(
        input={
            "files": v1_task.output["files"],
            "subtrahend": task_params["params"]["sub"],
        },
        name="sub_v1",
    )
    sub_v2_task = SubtractTask(
        input={
            "files": v2_task.output["files"],
            "subtrahend": task_params["params"]["sub"],
        },
        name="sub_v2",
    )

    mul_v1_task = MultiplyTask(
        input={
            "files": v1_task.output["files"],
            "multiplier": task_params["params"]["mul"],
        },
        name="mul_v1",
    )
    mul_v2_task = MultiplyTask(
        input={
            "files": v2_task.output["files"],
            "multiplier": task_params["params"]["mul"],
        },
        name="mul_v2",
    )

    sum_added_v1_task = SummationAllTask(
        input={"files": added_v1_task.output["files"]}, name="sum_added_v1"
    )
    sum_added_v2_task = SummationAllTask(
        input={"files": added_v2_task.output["files"]}, name="sum_added_v2"
    )

    sum_sub_v1_task = SummationAllTask(
        input={"files": sub_v1_task.output["files"]}, name="sum_sub_v1"
    )
    sum_sub_v2_task = SummationAllTask(
        input={"files": sub_v2_task.output["files"]}, name="sum_sub_v2"
    )

    sum_mul_v1_task = SummationAllTask(
        input={"files": mul_v1_task.output["files"]}, name="sum_mul_v1"
    )
    sum_mul_v2_task = SummationAllTask(
        input={"files": mul_v2_task.output["files"]}, name="sum_mul_v2"
    )

    sum_all_task = SummationAllTask(
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
        name="sum_all",
    )

    sum_all_task.result()

    visualize((sum_all_task, None, 4, 3.21)).view()


if __name__ == "__main__":
    main()
