# -*- coding: utf-8 -*-
from importlib import import_module

from sebflow import configuration as conf
from sebflow.exceptions import SebflowException
from sebflow.utils.log.logging_mixin import LoggingMixin
from sebflow.models import DAG

def load_login():
    log = LoggingMixin().log

    auth_backend = 'airflow.default_login'
    try:
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            auth_backend = conf.get('webserver', 'auth_backend')
    except conf.SebflowConfigException:
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            log.warning(
                "auth_backend not found in webserver config reverting to "
                "*deprecated*  behavior of importing airflow_login")
            auth_backend = "airflow_login"

    try:
        global login
        login = import_module(auth_backend)
    except ImportError as err:
        log.critical(
            "Cannot import authentication module %s. "
            "Please correct your authentication backend or disable authentication: %s",
            auth_backend, err
        )
        if conf.getboolean('webserver', 'AUTHENTICATE'):
            raise SebflowException("Failed to import authentication backend")
