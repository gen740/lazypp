import os
from pathlib import Path
from typing import TypedDict

import pytest

from lazypp import BaseTask, Directory, File
from lazypp.task import _is_valid_input


@pytest.mark.parametrize(
    "valid, input",
    [
        (
            True,
            {
                "input1": 1,
                "input2": "2",
                "input3": [1.0, 2.0],
                "input4": ("a", "b"),
                "input5": {"a": 1, "b": 2},
            },
        ),
        (
            True,
            {
                "input": File("tests/data/hello1.txt"),
                "input2": Directory("tests/data/foo1"),
            },
        ),
        (
            True,
            {
                "input": [File("tests/data/hello1.txt"), File("tests/data/hello2.txt")],
                "input2": Directory("data/foo1"),
            },
        ),
        (False, {1: 1}),
        (False, {(1, 2): 1}),
    ],
)
def test_invalid_input(valid: bool, input: dict):
    assert _is_valid_input(input) == valid


def test_basic(tmpdir):
    class TestInput(TypedDict):
        input1: int
        input2: str
        input3: list[float]

    class TestOutput(TypedDict):
        output1: File
        output2: Directory

    class TestTask(BaseTask[TestInput, TestOutput]):
        def task(self, input) -> TestOutput:
            with open("output1.txt", "w") as f:
                f.write(str(input["input1"]))

            os.mkdir("output2")
            with open("output2/output2.txt", "w") as f:
                f.write(input["input2"])

            with open("output2/output3.txt", "w") as f:
                f.write(str(input["input3"]))

            return {"output1": File("output1.txt"), "output2": Directory("output2")}

    task = TestTask(
        input={"input1": 1, "input2": "2", "input3": [1.0, 2.0]},
        cache_dir=tmpdir / "cache",
    )

    output = task.result()

    print(tmpdir)

    output["output1"].copy(Path(tmpdir) / "out" / "output1.txt")
    output["output2"].copy(Path(tmpdir) / "out" / "output2")

    with open(tmpdir / "out/output1.txt") as f:
        assert f.read() == "1"

    with open(tmpdir / "out/output2/output2.txt") as f:
        assert f.read() == "2"

    with open(tmpdir / "out/output2/output3.txt") as f:
        assert f.read() == "[1.0, 2.0]"
