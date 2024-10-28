import logging
import sys

logging_level = logging.INFO
stdoutHandler = logging.StreamHandler(stream=sys.stdout)
fmt = logging.Formatter(
    "%(name)s: %(asctime)s | %(levelname)s | %(filename)s:%(lineno)s | %(process)d >>> %(message)s",
)
stdoutHandler.setFormatter(fmt)
stdoutHandler.setLevel(logging_level)
logger = logging.getLogger("ggkstac")
logger.setLevel(logging_level)
logger.addHandler(stdoutHandler)
