from contextlib import contextmanager
import errno
import os
from pathlib import Path


class MediaBusyError(RuntimeError):
    pass


@contextmanager
def media_lock(root: Path):
    # Keep the lock file in place. Unlinking it can let two processes lock
    # different file objects while both write to the same media directory.
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    with (root / '.hgxz-sync.lock').open('a+b') as lock_file:
        lock_file.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise
            raise MediaBusyError('Media directory is busy with another HGXZ process') from exc
        yield
