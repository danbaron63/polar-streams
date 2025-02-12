import logging
from functools import wraps


def staticlog(level=logging.DEBUG):
    def dec(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logging.getLogger(func.__qualname__).log(
                level, f"{func.__name__}: args({args}), kwargs({kwargs})"
            )
            return func(*args, **kwargs)

        return wrapper

    return dec


def log(level=logging.DEBUG):
    def dec(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            logging.getLogger(func.__qualname__).log(
                level, f"{func.__name__}: args({args}), kwargs({kwargs})"
            )
            return func(self, *args, **kwargs)

        return wrapper

    return dec
