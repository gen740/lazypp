import lazypp.task


class DummyOutput:
    def __init__(self, task: "lazypp.task.BaseTask"):
        self.task = task
        self._keys = []

    def __getitem__(self, key):
        self._keys.append(key)
        return self

    def restore_output(self):
        ret = self.task.output
        for key in self._keys:
            ret = ret[key]
        return ret

    def __getattr__(self, _):
        raise RuntimeError("You should run task first")
