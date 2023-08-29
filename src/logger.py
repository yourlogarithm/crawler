import logging
from pyngleton import singleton


@singleton
class Logger:
    def __init__(self, level: str = 'INFO'):
        self._inner = logging.getLogger('crawler')
        self._inner.setLevel(level)

    def __getattr__(self, item):
        return getattr(self._inner, item)
