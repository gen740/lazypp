import threading

from concurrent.futures import ThreadPoolExecutor


class Worker:
    def __init__(self, n_workers: int = 1):
        self._n_workers = n_workers
        self._workers = []
        self._tasks = []
        self._lock = threading.Lock()

    def post(self, task):
        self._tasks.append(task)
