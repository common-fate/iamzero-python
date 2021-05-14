import logging

ROOT_LOGGER_NAME = "iamzero"
LOG_FORMAT = (
    "[%(levelname)s][%(asctime)s #%(process)d.%(threadName)s]"
    " %(name)s:%(lineno)s \t%(message)s"
)


def configure_root_logger(name, log_level=logging.DEBUG):
    """
    configure iamzero logger
    """
    logger = logging.getLogger(name)

    # Don't propagate messages to upper loggers
    logger.propagate = False
    logger.handlers = []

    formatter = logging.Formatter(LOG_FORMAT)

    # Configure the stderr handler
    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)

    try:
        logger.setLevel(log_level)
    except ValueError:
        logger.error("Unknown log_level %r, default log level is CRITICAL", log_level)
        logger.setLevel(logging.CRITICAL)

    return logger
