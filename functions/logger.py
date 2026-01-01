# functions/logger.py

import logging
import json
import sys
from datetime import datetime

from functions.constants import LOGGING_ENABLED, LOG_LEVEL


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log: dict = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            log.update(extra_fields)

        return json.dumps(log, default=str)


def get_logger(name: str = "app") -> logging.Logger:
    logger = logging.getLogger(name)

    logger.setLevel(LOG_LEVEL)

    if not LOGGING_ENABLED:
        logger.disabled = True
        return logger

    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    logger.addHandler(handler)
    logger.propagate = False

    return logger
