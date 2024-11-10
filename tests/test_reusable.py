import tempfile
import time
from pathlib import Path
from typing import TypedDict

import pytest

from lazypp import BaseTask, ReusableFile
from lazypp.utils import gather

cache_dir = Path(tempfile.TemporaryDirectory().name)


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


@pytest.mark.parametrize("num_repeat", [1, 3, 5, 10, 20, 100])
def test_reusable_file(num_repeat):
    some_file = ReusableFile(
        id=f"some_file_{num_repeat}.txt",
        cache_dir=cache_dir,
    )

    tasks: list[int] = []

    for i in range(num_repeat):
        tasks.append(
            SomeBusyTask(
                cache_dir=cache_dir,
                input=SomeBusyTaskInput(reusable_file=some_file, adder=i),
            ).output["res"]
        )
    start = time.time()
    assert gather(tasks) == [i + 3 for i in range(num_repeat)]
    end = time.time()

    assert end - start < 2
