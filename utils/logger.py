"""Logging setup for comparator tool."""

import logging
import os
import sys


def setup_logger(config: dict) -> logging.Logger:
    """Configure and return the root logger based on config."""
    log_config = config.get("logging", {})
    level = getattr(logging, log_config.get("level", "INFO").upper(), logging.INFO)
    log_file = log_config.get("file", "")

    logger = logging.getLogger("comparator")
    logger.setLevel(level)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler -- stderr, not stdout.
    #
    # stdout carries the command's payload (the table or the JSON document);
    # diagnostics go to stderr. With the log on stdout, `-f json | jq` failed
    # because the log lines are interleaved with the JSON and the stream is
    # no longer a valid document. This is also just the usual convention:
    # something you pipe is data, something you read is status.
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(level)
    console.setFormatter(fmt)
    logger.addHandler(console)

    # File handler
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
