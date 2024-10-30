import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TypedDict

import lazypp
from lazypp import BaseTask, Directory, File

WORKER = ProcessPoolExecutor(max_workers=4)


class TestBaseTask[INPUT, OUTPUT](BaseTask[INPUT, OUTPUT]):
    _worker = ProcessPoolExecutor(max_workers=4)

    def __init__(self, input: INPUT):
        super().__init__(
            cache_dir=Path("cache").resolve(),
            input=input,
            # worker=TestBaseTask._worker,
        )


class FInput(TypedDict):
    n: str
    delta: str


class FOutput(TypedDict):
    files: Directory


class CreateFiles(TestBaseTask[FInput, FOutput]):
    def task(self, input):
        n = int(input["n"])
        delta = int(input["delta"])

        if not os.path.exists("files"):
            os.mkdir("files")

        content = 0
        for i in range(0, n):
            with open(f"files/file_{i}.txt", "w") as f:
                f.write(str(content))
            content += delta

        return FOutput({"files": Directory(path="files")})


# @lazypp.task(FInput, FOutput, cache_dir=Path("cache").resolve(), worker=WORKER)
# def CreateFiles(input):
#     n = int(input["n"])
#     delta = int(input["delta"])
#
#     if not os.path.exists("files"):
#         os.mkdir("files")
#
#     content = 0
#     for i in range(0, n):
#         with open(f"files/file_{i}.txt", "w") as f:
#             f.write(str(content))
#         content += delta
#
#     return FOutput({"files": Directory(path="files")})


class SumInput(TypedDict):
    files: CreateFiles


class SumOutput(TypedDict):
    sum: str
    result_file: File


class SumFiles(TestBaseTask[SumInput, SumOutput]):
    def task(self, input):
        files = os.listdir(input["files"].output["files"].path)
        total = 0
        for file in files:
            with open(f"{input['files'].output["files"].path}/{file}", "r") as f:
                total += int(f.read())
        sum = str(total)

        with open("result.txt", "w") as f:
            f.write(sum)

        return SumOutput({"sum": sum, "result_file": File(path="result.txt")})


if __name__ == "__main__":
    sum_files_task = SumFiles(
        input={
            "files": CreateFiles(
                input={"n": "28", "delta": "10"},
            )
        },
    )

    print(sum_files_task.result())
