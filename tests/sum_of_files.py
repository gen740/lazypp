from typing import TypedDict
from lazypp import BaseTask, Directory, File
from pathlib import Path
import os
import asyncio


class FInput(TypedDict):
    n: str
    delta: str


class FOutput(TypedDict):
    files: Directory


class CreateFiles(BaseTask[FInput, FOutput]):
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


create_files_task = CreateFiles(
    input={"n": "28", "delta": "10"},
    cache_dir=Path("cache").resolve(),
)


class SumInput(TypedDict):
    files: CreateFiles


class SumOutput(TypedDict):
    sum: str
    result_file: File


class SumFiles(BaseTask[SumInput, SumOutput]):
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
    input={"files": create_files_task},
    cache_dir=Path("cache").resolve(),
)

# sum_files_task.output["result_file"].copy("out", dirs_exist_ok=True)

print(asyncio.run(sum_files_task()))
