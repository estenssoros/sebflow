# -*- coding: utf-8 -*-

from sebflow.exceptions import SebflowException
from sebflow import configuration as conf
from importlib import import_module

from sebflow.utils.log.logging_mixin import LoggingMixin

api_auth = None

log = LoggingMixin().log


def load_auth():
    auth_backend = 'sebflow.api.auth.backend.default'
    try:
        auth_backend = conf.get("api", "auth_backend")
    except conf.SebflowConfigException:
        pass

    try:
        global api_auth
        api_auth = import_module(auth_backend)
    except ImportError as err:
        log.critical(
            "Cannot import %s for API authentication due to: %s",
            auth_backend, err
        )
        raise SebflowException(err)
