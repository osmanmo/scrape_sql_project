import getpass
import logging
import os

from dynaconf import Validator

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


def strip_cast(value):
    if value is not None and isinstance(value, str):
        return value.strip()

    return value


def log_empty(name, value):
    if not value:
        logger.warning(f"{name} variable is not set.")

    return True


def not_empty(value):
    return value not in [None, ""]


class BasicValidator(Validator):
    def __init__(self, name, **kwargs):
        kwargs = {
            "cast": strip_cast,
            "condition": lambda v: log_empty(name, v),
            **kwargs,
        }
        if "must_exist" not in kwargs and "default" not in kwargs:
            kwargs["default"] = None

        super().__init__(name, **kwargs)


validators = [

    BasicValidator("DB_USER", must_exist=True, condition=not_empty),
    BasicValidator("DB_PASSWORD"),
    BasicValidator("DB_HOST", must_exist=True, condition=not_empty),
    BasicValidator("DB_FLAVOR", must_exist=True, condition=not_empty),
    BasicValidator("DB_NAME", must_exist=True, condition=not_empty),
    BasicValidator("DB_PORT", must_exist=True, condition=not_empty),

]
