import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TypedDict

from lazypp import BaseTask


class TestBaseTask[INPUT, OUTPUT](BaseTask[INPUT, OUTPUT]):
    _worker = ThreadPoolExecutor(max_workers=1)

    def __init__(self, input: INPUT):
        super().__init__(
            cache_dir=Path("cache").resolve(),
            input=input,
            worker=TestBaseTask._worker,
            show_input=True,
            show_output=True,
        )


class FInput(TypedDict):
    input1: float
    input2: float


class FOutput(TypedDict):
    output1: float
    output2: float


class DoubleTheInputs(TestBaseTask[FInput, FOutput]):
    async def task(self, input):
        time.sleep(1)
        return FOutput({"output1": input["input1"] * 2, "output2": input["input2"] * 2})


class SumInput(TypedDict):
    double_the_inputs: DoubleTheInputs


class SumOutput(TypedDict):
    sum: float


class SumTask(TestBaseTask[SumInput, SumOutput]):
    async def task(self, input):
        time.sleep(1)
        return SumOutput(
            {
                "sum": (await input["double_the_inputs"]())["output1"]
                + (await input["double_the_inputs"]())["output2"]
            }
        )


class SubInput(TypedDict):
    double_the_inputs: DoubleTheInputs


class SubOutput(TypedDict):
    sub: float


class SubTask(TestBaseTask[SubInput, SubOutput]):
    async def task(self, input):
        time.sleep(1)
        return SubOutput(
            {
                "sub": (await input["double_the_inputs"]())["output1"]
                - (await input["double_the_inputs"]())["output2"]
            }
        )


class MulInput(TypedDict):
    double_the_inputs: DoubleTheInputs


class MulOutput(TypedDict):
    mul: float


class MulTask(TestBaseTask[MulInput, MulOutput]):
    async def task(self, input):
        time.sleep(1)
        return MulOutput(
            {
                "mul": (await input["double_the_inputs"]())["output1"]
                * (await input["double_the_inputs"]())["output2"]
            }
        )


class DivInput(TypedDict):
    double_the_inputs: DoubleTheInputs


class DivOutput(TypedDict):
    div: float


class DivTask(TestBaseTask[DivInput, DivOutput]):
    async def task(self, input):
        time.sleep(1)
        return DivOutput(
            {
                "div": (await input["double_the_inputs"]())["output1"]
                / (await input["double_the_inputs"]())["output2"]
            }
        )


class SumAllInput(TypedDict):
    sum: SumTask
    sub: SubTask
    mul: MulTask
    div: DivTask


class SumAllOutput(TypedDict):
    sum_all: float


class SumAllTask(TestBaseTask[SumAllInput, SumAllOutput]):
    async def task(self, input):
        time.sleep(1)
        return SumAllOutput(
            {
                "sum_all": (await input["sum"]())["sum"]
                + (await input["sub"]())["sub"]
                + (await input["mul"]())["mul"]
                + (await input["div"]())["div"]
            }
        )


double_the_inputs_task = DoubleTheInputs(input={"input1": 8, "input2": 3})

sum_all_task = SumAllTask(
    input={
        "sum": SumTask(input={"double_the_inputs": double_the_inputs_task}),
        "sub": SubTask(input={"double_the_inputs": double_the_inputs_task}),
        "mul": MulTask(input={"double_the_inputs": double_the_inputs_task}),
        "div": DivTask(input={"double_the_inputs": double_the_inputs_task}),
    }
)

print(sum_all_task.result())
