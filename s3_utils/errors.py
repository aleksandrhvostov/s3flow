from __future__ import annotations
import logging
import functools
from typing import Type, Callable, Any

class S3UtilsError(Exception): pass
class S3CopyError(S3UtilsError): pass
class S3DeleteError(S3UtilsError): pass
class S3DownloadError(S3UtilsError): pass

def setup_logging(level: int = logging.INFO, logfile: str | None = None) -> None:
    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):  # avoid duplicate handlers
        root.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    root.addHandler(stream)
    if logfile:
        fh = logging.FileHandler(logfile)
        fh.setFormatter(fmt)
        root.addHandler(fh)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

def log_and_reraise(exception_cls: Type[Exception] = S3UtilsError):
    def deco(func: Callable[..., Any]):
        @functools.wraps(func)
        def wrapper(*a, **kw):
            try:
                return func(*a, **kw)
            except Exception as e:
                logging.getLogger(func.__module__).error("%s failed: %s", func.__name__, e)
                raise exception_cls(f"{func.__name__} failed: {e}") from e
        return wrapper
    return deco
