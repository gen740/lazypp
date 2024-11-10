import tempfile
from pathlib import Path
from typing import TypedDict

from lazypp import BaseTask, File

cache_dir = Path(tempfile.TemporaryDirectory().name)


class TestTask1Input(TypedDict):
    a: int
    b: int


class TestTask1Output(TypedDict):
    res: File
    res2: File


class TestTask1(BaseTask[TestTask1Input, TestTask1Output]):
    def task(self, input: TestTask1Input) -> TestTask1Output:
        res = input["a"] + input["b"]
        res2 = input["a"] * input["b"]

        with open("res.txt", "w") as f:
            f.write(str(res))
        with open("res2.txt", "w") as f:
            f.write(str(res2))

        return {
            "res": File(path="res.txt", dest="res.txt"),
            "res2": File(path="res2.txt", dest="res2.txt"),
        }


class TestTask2Input(TypedDict):
    in1: File
    in2: File


class TestTask2Output(TypedDict):
    res: int


class TestTask2(BaseTask[TestTask2Input, TestTask2Output]):
    def task(self, input: TestTask2Input) -> TestTask2Output:
        with open(input["in1"].path, "r") as f:
            a = int(f.read())
        with open(input["in2"].path, "r") as f:
            b = int(f.read())
        return {"res": a + b}


def test_deps():
    task = TestTask1(
        cache_dir=cache_dir,
        input=TestTask1Input(a=1, b=2),
    )
    task2 = TestTask2(
        cache_dir=cache_dir,
        input=TestTask2Input(in1=task.output["res"], in2=task.output["res2"]),
    )

    assert task2.result()["res"] == 5
