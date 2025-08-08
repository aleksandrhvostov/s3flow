import logging
import functools

class S3UtilsError(Exception):
    pass

def setup_logging(logfile=None, level=logging.INFO):
    logging.basicConfig(filename=logfile, level=level, format='%(asctime)s [%(levelname)s] %(message)s')

def log_and_reraise(func):
    @functools.wraps(func)
    def wrapper(*a, **kw):
        try:
            return func(*a, **kw)
        except Exception as e:
            logging.error(f"{func.__name__} failed: {e}")
            raise S3UtilsError(f"{func.__name__} failed: {e}")
    return wrapper
