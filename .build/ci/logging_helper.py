#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
We want to add a little functionality on top of built-in logging; colorization, some new log levels, and logging
to a file built-in as well as some other conveniences.
"""

import logging
import threading
from enum import Enum
from logging import Logger
from logging.handlers import RotatingFileHandler


PROGRESS_LEVEL_NUM = 25
SECTION_LEVEL_NUM = 26
logging.addLevelName(PROGRESS_LEVEL_NUM, 'PROGRESS')
logging.addLevelName(SECTION_LEVEL_NUM, 'SECTION')


class LogLevel(Enum):
    """
    Matches logging.<VALUE> int levels; wrapped in enum here for convenience
    """
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    NOTSET = 0


class CustomLogger(Logger):
    # Some decorations to match the paradigm used in some other .sh files
    def progress(self, message: str, *args, **kws) -> None:
        if self.isEnabledFor(PROGRESS_LEVEL_NUM):
            self._log(PROGRESS_LEVEL_NUM, message, args, **kws)

    def separator(self, *args, **kws) -> None:
        if self.isEnabledFor(logging.DEBUG) and self.isEnabledFor(SECTION_LEVEL_NUM):
            self._log(SECTION_LEVEL_NUM, '-----------------------------------------------------------------------------', args, **kws)

    def header(self, message: str, *args, **kws) -> None:
        if self.isEnabledFor(logging.DEBUG) and self.isEnabledFor(SECTION_LEVEL_NUM):
            self._log(SECTION_LEVEL_NUM, f'----[{message}]----', args, **kws)


logging.setLoggerClass(CustomLogger)
LOG_FORMAT_STRING = '%(asctime)s - [tid:%(threadid)s] - [%(levelname)s]::%(message)s'


def build_logger(name: str, verbose: bool) -> CustomLogger:
    logger = CustomLogger(name)
    logger.setLevel(logging.INFO)
    logger.addFilter(ThreadContextFilter())

    stdout_handler = logging.StreamHandler()
    file_handler = RotatingFileHandler(f'{name}.log')

    formatter = CustomFormatter()
    stdout_handler.setFormatter(formatter)
    # Don't want color escape characters in our file logging, so we just use the string rather than the whole formatter
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT_STRING))

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

    if verbose:
        logger.setLevel(logging.DEBUG)

    # Prevent root logger propagation from duplicating results
    logger.propagate = False
    return logger


def set_loglevel(logger: logging.Logger, level: LogLevel) -> None:
    if logger.handlers:
        for handler in logger.handlers:
            handler.setLevel(level.value)


def mute_logging(logger: logging.Logger) -> None:
    if logger.handlers:
        for handler in logger.handlers:
            handler.setLevel(logging.CRITICAL + 1)


# Since we'll thread, let's point out threadid in our format
class ThreadContextFilter(logging.Filter):
    def filter(self, record):
        record.threadid = threading.get_ident()
        return True


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;21m"
    blue = "\x1b[34;21m"
    green = "\x1b[32;21m"
    yellow = "\x1b[33;21m"
    red = "\x1b[31;21m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    purple = "\x1b[35m"

    FORMATS = {
        PROGRESS_LEVEL_NUM: blue + LOG_FORMAT_STRING + reset,
        SECTION_LEVEL_NUM: green + LOG_FORMAT_STRING + reset,
        logging.DEBUG: purple + LOG_FORMAT_STRING + reset,
        logging.INFO: grey + LOG_FORMAT_STRING + reset,
        logging.WARNING: yellow + LOG_FORMAT_STRING + reset,
        logging.ERROR: red + LOG_FORMAT_STRING + reset,
        logging.CRITICAL: bold_red + LOG_FORMAT_STRING + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno, self.format)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
