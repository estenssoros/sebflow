import logging
import os
import sys
from logging.config import dictConfig

import sebflow.configuration as conf

log = logging.getLogger(__name__)

LOG_FORMAT = conf.get('core', 'log_format')
BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')
FILENAME_TEMPLATE = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log'
PROCESSOR_LOG_FOLDER = conf.get('scheduler', 'CHILD_PROCESS_LOG_DIRECTORY')
PROCESSOR_FILENAME_TEMPLATE = '{{ filename }}.log'
LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'sebflow': {
            'format': LOG_FORMAT,
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'sebflow',
            'stream': 'ext://sys.stdout'
        },
    },
    'root': {
        'handlers': ['console'],
        'level': LOG_LEVEL,
    }
}


def prepare_classpath():
    config_path = os.path.join(conf.get('core', 'sebflow_home'), 'config')
    config_path = os.path.expanduser(config_path)

    if config_path not in sys.path:
        sys.path.append(config_path)


def configure_logging():
    assert (isinstance(DEFAULT_LOGGING_CONFIG, dict))
    prepare_classpath()
    try:
        # Try to init logging
        dictConfig(DEFAULT_LOGGING_CONFIG)
    except ValueError as e:
        log.warning('Unable to load the config, contains a configuration error.')
        # When there is an error in the config, escalate the exception
        # otherwise Airflow would silently fall back on the default config
        raise e

    return DEFAULT_LOGGING_CONFIG
