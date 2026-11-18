from threading import Event
from time import monotonic, sleep
import subprocess


class SyncCancelled(Exception):
    pass


def check_cancelled(stop: Event | None) -> None:
    if stop is not None and stop.is_set():
        raise SyncCancelled


def wait_or_cancel(stop: Event | None, seconds: float) -> None:
    if stop is None:
        sleep(seconds)
    elif stop.wait(seconds):
        raise SyncCancelled



def run_process(args: list[str], *, stop: Event | None = None,
                timeout: float | None = None, check: bool = False,
                text: bool = False) -> subprocess.CompletedProcess:
    if stop is None:
        return subprocess.run(args, capture_output=True, timeout=timeout, check=check, text=text)

    check_cancelled(stop)
    deadline = None if timeout is None else monotonic() + timeout
    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=text) as proc:
        try:
            while True:
                check_cancelled(stop)
                delay = 0.1
                if deadline is not None:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(args, timeout)
                    delay = min(delay, remaining)
                try:
                    stdout, stderr = proc.communicate(timeout=delay)
                    break
                except subprocess.TimeoutExpired:
                    continue
        except BaseException:
            proc.kill()
            proc.communicate()
            raise
    check_cancelled(stop)
    result = subprocess.CompletedProcess(args, proc.returncode, stdout, stderr)
    if check:
        result.check_returncode()
    return result
