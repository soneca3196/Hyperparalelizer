"""
logger.py – Logs padronizados com prefixo de nó e nível.

Por padrão os loggers ficam em INFO (sem o ruído de DEBUG: registro de
handlers, "fragmento já local", etc.). Use `set_level(logging.DEBUG)`
(tipicamente via a flag --debug do CLI) para religar o detalhamento
completo quando precisar depurar algo.
"""

import logging
import sys
from typing import Dict, Optional

_DEFAULT_LEVEL = logging.INFO
_LOGGERS: Dict[str, logging.Logger] = {}


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """Cria um logger"""
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

    logger.setLevel(level if level is not None else _DEFAULT_LEVEL)
    _LOGGERS[name] = logger
    return logger


def set_level(level: int) -> None:
    """Ajusta o nível de todos os loggers"""
    global _DEFAULT_LEVEL
    _DEFAULT_LEVEL = level
    for logger in _LOGGERS.values():
        logger.setLevel(level)