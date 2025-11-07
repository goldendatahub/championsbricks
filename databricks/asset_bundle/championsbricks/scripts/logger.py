import logging

def setup_logger(name: str) -> logging.Logger:

    # Deactivate log4j logging
    logging.getLogger("py4j").setLevel(logging.ERROR) # Disable log4j logs

    # Set up custom logging
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)  # You can later change this via config

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(name)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# Usage example
if __name__ == "__main__":
    logger = setup_logger("example_logger")
    logger.info("This is an info message.")
    logger.error("This is an error message.")
    logger.debug("This is a debug message.")
    logger.warning("This is a warning message.")