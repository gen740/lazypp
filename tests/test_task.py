from lazypp import BaseTask, File, Directory
from typing import TypedDict
from lazypp.task import _is_valid_input, _is_valid_output
import os
import pytest


# def test_dump_task_input(tmpdir):
#     class TestTaskBase[INPUT, OUTPUT](BaseTask[INPUT, OUTPUT]):
#         def __init__(self, input: INPUT, output: OUTPUT):
#             super().__init__(
#                 cache_dir=tmpdir, work_dir=None, input=input, output=output
#             )
#
#     class TestTaskInput(TypedDict):
#         input1: int
#         input2: str
#         input3: list[float]
#
#     class Task(TestTaskBase[TestTaskInput, None]):
#         def task(self, input, output):
#             pass
#
#     task = Task(
#         input={"input1": 1, "input2": "2", "input3": [1.0, 2.0, 3.0]},
#         output=None,
#     )
#
#     assert task.hash == "49a7bff5a6b2c1769ba7d29e20c1c6a2"


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
        (True, {"input": File("data/hello1.txt"), "input2": Directory("data/foo1")}),
        (
            True,
            {
                "input": [File("data/hello1.txt"), File("data/hello2.txt")],
                "input2": Directory("data/foo1"),
            },
        ),
        (False, {1: 1}),
        (False, {(1, 2): 1}),
    ],
)
def test_invalid_input(valid: bool, input: dict):
    assert _is_valid_input(input) == valid


# @pytest.mark.parametrize(
#     "valid, input",
#     [
#         (
#             True,
#             {
#                 "input1": 1,
#                 "input2": "2",
#                 "input3": [1.0, 2.0],
#                 "input4": ("a", "b"),
#                 "input5": {"a": 1, "b": 2},
#             },
#         ),
#         (True, {"input": File("test.txt"), "input2": Directory("foo")}),
#         (
#             True,
#             {
#                 "input": [File("test1.txt"), File("test2.txt")],
#                 "input2": Directory("foo"),
#             },
#         ),
#         (False, {1: 1}),
#         (False, {(1, 2): 1}),
#     ],
# )
# def test_invalid_output(valid: bool, input: dict):
#     assert _is_valid_output(input) == valid


def test_task(tmpdir):
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

    task = TestTask(cache_dir=tmpdir)

    print(task(input={"input1": 1, "input2": "2", "input3": [1.0, 2.0]}))
