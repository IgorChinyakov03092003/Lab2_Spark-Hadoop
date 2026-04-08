import logging
import os

def get_logger(name="SparkLab"):
    log_dir = "/report/logs"
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')

        # Файловый хэндлер
        fh = logging.FileHandler(f"{log_dir}/{name}.log")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        # Консольный хэндлер
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger
