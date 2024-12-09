# Lazypp

Lazy execution parallel pipeline.

## Installation

To install Lazypp, simply run:

```bash
pip install lazypp
```

## Usage

1. First, create the input and output `TypedDict`.
2. Inherit from `BaseTask` and override the `task` member function.

Here's an example:

```python
from pathlib import Path
from typing import TypedDict
import time

from lazypp import BaseTask

# Define a base class for your tasks
class TestBaseTask[INPUT, OUTPUT](BaseTask[INPUT, OUTPUT]):
    def __init__(self, input: INPUT):
        super().__init__(
            cache_dir=Path("cache").resolve(),
            input=input,
        )

# Define input and output types
class Fin(TypedDict):
    your_name: str

class Fout(TypedDict):
    output: str

# Define a specific task that says hello
class Hello(TestBaseTask[Fin, Fout]):
    def task(self, input: Fin) -> Fout:
        time.sleep(3)  # Simulating a long-running task
        return {"output": f"Hello, {input['your_name']}"}

# Create and execute the task
hello_task = Hello(
    input={"your_name": "John"},
)

print(hello_task.result())
```

## Features

### Cached Output

The output is automatically cached in the `cache_dir` according to the input and the code of the `task` member function. As a result, running the task a second time is much faster than the first execution.

### Parallel Execution

Tasks can be nested, and Lazypp will automatically parallelize these tasks to save time and optimize performance.
