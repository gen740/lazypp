import asyncio
from pathlib import Path
from typing import TypedDict

from lazypp import BaseTask


class TestBaseTask[INPUT, OUTPUT](BaseTask[INPUT, OUTPUT]):
    def __init__(self, input: INPUT):
        super().__init__(
            cache_dir=Path("cache").resolve(),
            input=input,
        )


class Fin(TypedDict):
    your_name: str


class Fout(TypedDict):
    output: str


class Hello(TestBaseTask[Fin, Fout]):
    async def task(self, input: Fin) -> Fout:
        await asyncio.sleep(3)  # Some long running task
        return {"output": f"Hello, {input['your_name']}"}


ctask = Hello(
    input={"your_name": "John"},
)

print(ctask.result())
