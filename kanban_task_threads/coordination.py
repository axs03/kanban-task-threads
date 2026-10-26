"""Host-filesystem coordination for full-list forum tag replacements.

Separate board leases do not protect one forum shared by those boards. flock
releases on process exit; the lock file is deliberately never unlinked.
"""

import os
import stat
import time
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def forum_lock(path, timeout=10.0, *, parent=None):
    """A cross-process lock for full-list forum tag replacement.

    ``parent`` optionally pins the identity ``(st_dev, st_ino)`` of the lock
    file's containing directory (captured at build time). If the directory is
    renamed and recreated — splitting the flock across two inodes and yielding
    two simultaneous holders — the pin detects the replacement and refuses the
    lock. The shared state root must be owned and non-writable by untrusted
    principals; path checks alone are not a sandbox.
    """
    import fcntl

    path = Path(path).absolute()
    if path.resolve() != path:
        raise RuntimeError("unsafe forum coordination path")
    path.parent.mkdir(parents=True, exist_ok=True)
    parent_info = path.parent.stat()
    if not stat.S_ISDIR(parent_info.st_mode):
        raise RuntimeError("unsafe forum coordination directory")
    expected_parent = (
        tuple(parent) if parent is not None else (parent_info.st_dev, parent_info.st_ino)
    )
    if (parent_info.st_dev, parent_info.st_ino) != expected_parent:
        raise RuntimeError("forum coordination directory replaced")
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if info.st_nlink != 1 or not stat.S_ISREG(info.st_mode):
            raise RuntimeError("unsafe forum coordination file")
        # Verify the parent again before locking: no swap between open and lock.
        if _dir_identity(path.parent) != expected_parent:
            raise RuntimeError("forum coordination directory replaced")
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("forum setup busy; retry next pass") from None
                time.sleep(0.025)
        # Re-verify parent and lock-file identity after acquiring the lock.
        if _dir_identity(path.parent) != expected_parent:
            raise RuntimeError("forum coordination directory replaced")
        current = path.stat()
        if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
            raise RuntimeError("forum coordination file identity changed")
        yield
    finally:
        os.close(fd)


def _dir_identity(path):
    current = path.stat()
    return current.st_dev, current.st_ino
