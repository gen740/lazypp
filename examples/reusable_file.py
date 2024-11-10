import time
from pathlib import Path
from typing import TypedDict

from lazypp import BaseTask, ReusableFile
from lazypp.utils import gather

cache_dir = Path("cache").resolve()


class SomeBusyTaskInput(TypedDict):
    reusable_file: ReusableFile
    adder: int


class SomeBusyTaskOutput(TypedDict):
    res: int


class SomeBusyTask(BaseTask[SomeBusyTaskInput, SomeBusyTaskOutput]):
    def task(self, input: SomeBusyTaskInput) -> SomeBusyTaskOutput:
        with input["reusable_file"] as file:
            if not file.exists():
                time.sleep(1)  # some heavy computation
                with open(file, "w") as f:
                    f.write("3")
                    res = 3 + input["adder"]
            else:
                with open(file, "r") as f:
                    res = int(f.read()) + input["adder"]

        return {"res": res}


if __name__ == "__main__":
    some_file = ReusableFile(
        id="some_file.txt",
        cache_dir=cache_dir,
    )

    tasks: list[int] = []

    for i in range(30):
        tasks.append(
            SomeBusyTask(
                cache_dir=cache_dir,
                input=SomeBusyTaskInput(reusable_file=some_file, adder=i),
            ).output["res"]
        )
    print(gather(tasks))
