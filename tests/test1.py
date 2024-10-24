from typing import TypedDict
from lazypp import TaskBase, File
from pathlib import Path


TaskBase.change_cache_dir(str(Path(__file__).parent / "cache"))


class Fin(TypedDict):
    prefix: File


class Fout(TypedDict):
    hello_world: File
    outtext: str


class CreateFile(TaskBase[Fin, Fout]):
    def task(self, input, output):
        with open(input["prefix"].path, "r") as f:
            content = f.read()

        with open(output["hello_world"].path, "w") as f:
            f.write(f"{content}Hello, World!")

        output["outtext"] = "Chaged output"


ctask = CreateFile(
    input={"prefix": File(path="prefix")},
    output={"hello_world": File(path="hello"), "outtext": "Default output"},
)


class Fin2(TypedDict):
    task1: CreateFile


class Fout2(TypedDict):
    out: str


class ReadFileTask(TaskBase[Fin2, Fout2]):
    def task(self, input, output):
        print(self.work_dir)

        with open(input["task1"]["hello_world"].dest_path, "r") as f:
            content = f.read()

        output["out"] = content


rtask = ReadFileTask(input={"task1": ctask}, output={"out": "out"})

out = rtask.output

print(out)
