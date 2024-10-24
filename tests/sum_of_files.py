from typing import TypedDict
from lazypp import TaskBase, Directory, File
from pathlib import Path
import os


TaskBase.change_cache_dir(str(Path(__file__).parent / "cache"))


class FInput(TypedDict):
    n: str
    delta: str


class FOutput(TypedDict):
    files: Directory


class CreateFiles(TaskBase[FInput, FOutput]):
    def task(self, input, output):
        n = int(input["n"])
        delta = int(input["delta"])
        os.mkdir(output["files"].path)

        content = 0
        for i in range(0, n):
            with open(f"{output['files'].path}/file_{i}.txt", "w") as f:
                f.write(str(content))
            content += delta


create_files_task = CreateFiles(
    input={"n": "27", "delta": "10"},
    output={"files": Directory(path="files")},
)


class SumInput(TypedDict):
    files: CreateFiles


class SumOutput(TypedDict):
    sum: str
    result_file: File


class SumFiles(TaskBase[SumInput, SumOutput]):
    def task(self, input, output):
        files = os.listdir(input["files"].output["files"].path)
        total = 0
        for file in files:
            with open(f"{input['files'].output["files"].path}/{file}", "r") as f:
                total += int(f.read())
        output["sum"] = str(total)

        with open(output["result_file"].path, "w") as f:
            f.write(output["sum"])


sum_files_task = SumFiles(
    input={"files": create_files_task},
    output={"sum": "1", "result_file": File(path="result.txt")},
)

sum_files_task.output["result_file"].copy("out", dirs_exist_ok=True)

print(sum_files_task.output)
