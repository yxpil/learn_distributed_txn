# -*- coding: utf-8 -*-
"""
节点内的本地存储层：文件锁 + 原子 JSON 落盘。
"单库本地事务" = 加文件锁 -> 读 JSON -> 就地修改 -> 临时文件原子替换 -> 解锁。
每个分片一个 JSON 文件，行锁粒度即分片文件锁（演示层面足够）。
"""
import json
import os
import tempfile
import time


class FileLockError(Exception):
    pass


class FileLock:
    """跨进程互斥锁：用原子创建目录实现 O_EXCL。带超时与过期锁回收。"""

    def __init__(self, target_path, timeout=10.0, poll=0.02, stale_after=30.0):
        self.lock_dir = target_path + ".lock"
        self.timeout = timeout
        self.poll = poll
        self.stale_after = stale_after

    def acquire(self):
        deadline = time.time() + self.timeout
        while True:
            try:
                os.mkdir(self.lock_dir)
                return True
            except FileExistsError:
                try:
                    if time.time() - os.path.getmtime(self.lock_dir) > self.stale_after:
                        self.release()  # 回收陈旧锁
                        continue
                except OSError:
                    pass
                if time.time() > deadline:
                    raise FileLockError(f"acquire lock timeout: {self.lock_dir}")
                time.sleep(self.poll)
            except (PermissionError, OSError):
                if time.time() > deadline:
                    raise FileLockError(f"acquire lock error: {self.lock_dir}")
                time.sleep(self.poll)

    def release(self):
        try:
            os.rmdir(self.lock_dir)
        except OSError:
            pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
        return False


class JsonStore:
    """一个 JSON 文件即一个"数据库分片"。"""

    def __init__(self, path, initial=None):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            self.atomic_write(initial if initial is not None else {})

    def read(self):
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def atomic_write(self, data):
        d = os.path.dirname(self.path)
        fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)  # 原子替换，避免读到半截文件
        except BaseException:
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise

    def local_tx(self, mutate_fn, lock_timeout=10.0):
        """
        单库本地事务：mutate_fn(data: dict) -> result，整体在锁内读写。
        mutate_fn 抛出异常则本次修改全部不落盘。
        """
        with FileLock(self.path, timeout=lock_timeout):
            data = self.read()
            result = mutate_fn(data)
            self.atomic_write(data)
            return result
