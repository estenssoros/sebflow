import logging


class LoggingMixin(object):
    def __init__(self, context=None):
        self._set_context(context)

    @property
    def logger(self):
        return self.log

    @property
    def log(self):
        try:
            return self._log
        except AttributeError:
            self._log = logging.root.getChild(
                self.__class__.__module__ + '.' + self.__class__.__name__
            )
            return self._log

    def _set_context(self, context):
        if context is not None:
            set_context(self.log, context)


def set_context(logger, value):
    _logger = logger
    while _logger:
        for handler in _logger.handlers:
            try:
                handler.set_context(value)
            except AttributeError:
                pass
        if _logger.propagate is True:
            _logger = _logger.patent
        else:
            _logger = None
