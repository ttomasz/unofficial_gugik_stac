import logging
import sys

logging_level = logging.INFO
stdoutHandler = logging.StreamHandler(stream=sys.stdout)
fmt = logging.Formatter(
    "%(asctime)s [%(name)s][%(levelname)s] - %(message)s",
)
stdoutHandler.setFormatter(fmt)
stdoutHandler.setLevel(logging_level)
logger = logging.getLogger("ggkstac")
logger.setLevel(logging_level)
logger.addHandler(stdoutHandler)

def set_logging_level(level: str) -> None:
    global logging_level, stdoutHandler, logger
    logging_level = logging.getLevelNamesMapping()[level]
    stdoutHandler.setLevel(logging_level)
    logger.setLevel(logging_level)
