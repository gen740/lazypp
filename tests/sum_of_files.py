import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TypedDict

from lazypp import BaseTask, Directory, File


class TestBaseTask[INPUT, OUTPUT](BaseTask[INPUT, OUTPUT]):
    def __init__(self, input: INPUT):
        super().__init__(
            cache_dir=Path("cache").resolve(),
            input=input,
            worker=ThreadPoolExecutor(max_workers=4),
        )


class FInput(TypedDict):
    n: str
    delta: str


class FOutput(TypedDict):
    files: Directory


class CreateFiles(TestBaseTask[FInput, FOutput]):
    async def task(self, input):
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


class SumInput(TypedDict):
    files: CreateFiles


class SumOutput(TypedDict):
    sum: str
    result_file: File


class SumFiles(TestBaseTask[SumInput, SumOutput]):
    async def task(self, input):
        files = os.listdir((await input["files"]())["files"].path)
        total = 0
        for file in files:
            with open(f"{(await input['files']())["files"].path}/{file}", "r") as f:
                total += int(f.read())
        sum = str(total)

        with open("result.txt", "w") as f:
            f.write(sum)

        return SumOutput({"sum": sum, "result_file": File(path="result.txt")})


sum_files_task = SumFiles(
    input={
        "files": CreateFiles(
            input={"n": "28", "delta": "10"},
        )
    },
)

print(sum_files_task.result())

sum_files_task.result()["result_file"].copy(Path("out"))
