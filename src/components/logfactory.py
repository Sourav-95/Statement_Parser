# logger_config.py
import logging
import os
from datetime import datetime
from threading import Lock
from constants import ConstantRetriever  # Your project constant location

DEFAULT_LOG_LEVEL = logging.INFO
LOGGER_REGISTRY = {}
LOG_FILE_PATH = None
FILE_HANDLER = None
LOCK = Lock()

def set_global_log_level(level):
    global DEFAULT_LOG_LEVEL
    DEFAULT_LOG_LEVEL = level

def get_logger(name=__name__, log_level=None, log_to_file=True):
    global LOG_FILE_PATH, FILE_HANDLER

    if name in LOGGER_REGISTRY:
        return LOGGER_REGISTRY[name]

    logger = logging.getLogger(name)
    logger.setLevel(log_level or DEFAULT_LOG_LEVEL)

    if logger.hasHandlers():
        return logger

    formatter = logging.Formatter(
        "%(asctime)s - [%(filename)s:%(lineno)d] %(funcName)s() - %(levelname)s: %(message)s"
    )

    # Console handler (shared by all)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_to_file:
        with LOCK:
            if FILE_HANDLER is None:
                LOG_BASE = ConstantRetriever.SCRIPT_BASE
                log_dir = os.path.join(LOG_BASE, 'logs')
                os.makedirs(log_dir, exist_ok=True)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                LOG_FILE_PATH = os.path.join(log_dir, f"log_{timestamp}.log")

                # Delay file creation: only open when actual log is written
                FILE_HANDLER = logging.FileHandler(LOG_FILE_PATH, delay=True)
                FILE_HANDLER.setFormatter(formatter)

        logger.addHandler(FILE_HANDLER)

    LOGGER_REGISTRY[name] = logger
    return logger
