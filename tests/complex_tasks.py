import asyncio
import pickle
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TypedDict

from lazypp import BaseTask


class TestBaseTask[INPUT, OUTPUT](BaseTask[INPUT, OUTPUT]):
    _worker = ProcessPoolExecutor(max_workers=4)

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
    def task(self, input):
        time.sleep(1)
        return FOutput({"output1": input["input1"] * 2, "output2": input["input2"] * 2})


class SumInput(TypedDict):
    double_the_inputs: DoubleTheInputs


class SumOutput(TypedDict):
    sum: float


class SumTask(TestBaseTask[SumInput, SumOutput]):
    def task(self, input):
        time.sleep(1)
        return SumOutput(
            {
                "sum": (input["double_the_inputs"].output)["output1"]
                + (input["double_the_inputs"].output)["output2"]
            }
        )


class SubInput(TypedDict):
    double_the_inputs: DoubleTheInputs


class SubOutput(TypedDict):
    sub: float


class SubTask(TestBaseTask[SubInput, SubOutput]):
    def task(self, input):
        time.sleep(1)
        return SubOutput(
            {
                "sub": (input["double_the_inputs"].output)["output1"]
                - (input["double_the_inputs"].output)["output2"]
            }
        )


class MulInput(TypedDict):
    double_the_inputs: DoubleTheInputs


class MulOutput(TypedDict):
    mul: float


class MulTask(TestBaseTask[MulInput, MulOutput]):
    def task(self, input):
        time.sleep(1)
        return MulOutput(
            {
                "mul": (input["double_the_inputs"].output)["output1"]
                * (input["double_the_inputs"].output)["output2"]
            }
        )


class DivInput(TypedDict):
    double_the_inputs: DoubleTheInputs


class DivOutput(TypedDict):
    div: float


class DivTask(TestBaseTask[DivInput, DivOutput]):
    def task(self, input):
        time.sleep(1)
        return DivOutput(
            {
                "div": (input["double_the_inputs"].output)["output1"]
                / (input["double_the_inputs"].output)["output2"]
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
    def task(self, input):
        time.sleep(1)
        return SumAllOutput(
            {
                "sum_all": (input["sum"].output)["sum"]
                + (input["sub"].output)["sub"]
                + (input["mul"].output)["mul"]
                + (input["div"].output)["div"]
            }
        )


double_the_inputs_task = DoubleTheInputs(input={"input1": 8, "input2": 3})


if __name__ == "__main__":
    sum_all_task = SumAllTask(
        input={
            "sum": SumTask(input={"double_the_inputs": double_the_inputs_task}),
            "sub": SubTask(input={"double_the_inputs": double_the_inputs_task}),
            "mul": MulTask(input={"double_the_inputs": double_the_inputs_task}),
            "div": DivTask(input={"double_the_inputs": double_the_inputs_task}),
        }
    )
    # pickle.dump(sum_all_task, open("sum_all_task.pkl", "wb"))
    # pickle.dump(asyncio.Lock(), open("test.pkl", "wb"))
    print(sum_all_task.result())
