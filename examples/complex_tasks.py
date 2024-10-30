import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TypedDict

from lazypp import BaseTask, File

cache_dir = Path("cache").resolve()
worker = ProcessPoolExecutor(max_workers=4)


class TestBaseTask[INPUT, OUTPUT](BaseTask[INPUT, OUTPUT]):

    def __init__(self, input: INPUT):
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


class FInput(TypedDict):
    param: CreateFileInputParameters


class FOutput(TypedDict):
    output: list[File]


class CreateFileTask(TestBaseTask[FInput, FOutput]):
    def task(self, input):
        time.sleep(1)
        files = []
        for i in range(
            input["param"]["min"], input["param"]["max"], input["param"]["delta"]
        ):
            with open(f"file_{i}.txt", "w") as f:
                f.write(f"file_{i}")
            files.append(File(f"file_{i}.txt"))
        return FOutput({"output": files})


### Multiplier Task
class MultiplierInput(TypedDict):
    files: list[File]
    multiplier: int


class MultiplierOutput(TypedDict):
    files: list[File]


class MultiplierTask(TestBaseTask[MultiplierInput, MultiplierOutput]):
    def task(self, input):
        time.sleep(1)
        for file in input["files"]:
            with open(file.path, "r") as f:
                content = f.read()
            with open(file.path, "w") as f:
                f.write(content * input["multiplier"])
        return MultiplierOutput({"files": input["files"]})


def main():
    create_file_task = CreateFileTask(
        input={"param": {"min": 0, "max": 15, "delta": 3}}
    )

    multiplier_task = MultiplierTask(
        input={"files": create_file_task.output["output"], "multiplier": 2}
    )

    output = multiplier_task.result()

    output["files"][2].copy(Path("output.txt"))


# class DoubleTheInputs(TestBaseTask[]):
#     def task(self, input):
#         time.sleep(1)
#         return FOutput({"output1": input["input1"] * 2, "output2": input["input2"] * 2})
#
#
# class SumInput(TypedDict):
#     double_the_inputs: DoubleTheInputs
#
#
# class SumOutput(TypedDict):
#     sum: float
#
#
# class SumTask(TestBaseTask[SumInput, SumOutput]):
#     def task(self, input):
#         time.sleep(1)
#         return SumOutput(
#             {
#                 "sum": (input["double_the_inputs"].output)["output1"]
#                 + (input["double_the_inputs"].output)["output2"]
#             }
#         )
#
#
# class SubInput(TypedDict):
#     double_the_inputs: DoubleTheInputs
#
#
# class SubOutput(TypedDict):
#     sub: float
#
#
# class SubTask(TestBaseTask[SubInput, SubOutput]):
#     def task(self, input):
#         time.sleep(1)
#         return SubOutput(
#             {
#                 "sub": (input["double_the_inputs"].output)["output1"]
#                 - (input["double_the_inputs"].output)["output2"]
#             }
#         )
#
#
# class MulInput(TypedDict):
#     double_the_inputs: DoubleTheInputs
#
#
# class MulOutput(TypedDict):
#     mul: float
#
#
# class MulTask(TestBaseTask[MulInput, MulOutput]):
#     def task(self, input):
#         time.sleep(1)
#         return MulOutput(
#             {
#                 "mul": (input["double_the_inputs"].output)["output1"]
#                 * (input["double_the_inputs"].output)["output2"]
#             }
#         )
#
#
# class DivInput(TypedDict):
#     double_the_inputs: DoubleTheInputs
#
#
# class DivOutput(TypedDict):
#     div: float
#
#
# class DivTask(TestBaseTask[DivInput, DivOutput]):
#     def task(self, input):
#         time.sleep(1)
#         return DivOutput(
#             {
#                 "div": (input["double_the_inputs"].output)["output1"]
#                 / (input["double_the_inputs"].output)["output2"]
#             }
#         )
#
#
# class SumAllInput(TypedDict):
#     sum: SumTask
#     sub: SubTask
#     mul: MulTask
#     div: DivTask
#
#
# class SumAllOutput(TypedDict):
#     sum_all: float
#
#
# class SumAllTask(TestBaseTask[SumAllInput, SumAllOutput]):
#     def task(self, input):
#         time.sleep(1)
#         return SumAllOutput(
#             {
#                 "sum_all": (input["sum"].output)["sum"]
#                 + (input["sub"].output)["sub"]
#                 + (input["mul"].output)["mul"]
#                 + (input["div"].output)["div"]
#             }
#         )
#
#
# double_the_inputs_task = DoubleTheInputs(input={"input1": 8, "input2": 3})
#
# if __name__ == "__main__":
#     sum_all_task = SumAllTask(
#         input={
#             "sum": SumTask(input={"double_the_inputs": double_the_inputs_task}),
#             "sub": SubTask(input={"double_the_inputs": double_the_inputs_task}),
#             "mul": MulTask(input={"double_the_inputs": double_the_inputs_task}),
#             "div": DivTask(input={"double_the_inputs": double_the_inputs_task}),
#         }
#     )
#
#     print(sum_all_task.result())
if __name__ == "__main__":
    main()
