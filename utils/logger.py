"""
logger.py – Logs padronizados com prefixo de nó e nível.
"""

import logging
import sys


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """Creates and configures a logger."""
    logger = logging.getLogger(name)

    # Impedir duplicação de handlers se o logger já existir
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
            datefmt="%H:%M:%S",
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(level)
    return logger