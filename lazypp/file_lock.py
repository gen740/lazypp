import fcntl


class FileLock:
    def __init__(self, lock_file_path):
        self.lock_file_path = lock_file_path
        self.lock_file = None

    def __enter__(self):
        self.lock_file = open(self.lock_file_path, "w")
        try:
            fcntl.flock(self.lock_file, fcntl.LOCK_EX)
        except Exception as e:
            self.lock_file.close()
            raise e
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        _ = exc_type, exc_value, traceback
        if self.lock_file:
            fcntl.flock(self.lock_file, fcntl.LOCK_UN)
            self.lock_file.close()
