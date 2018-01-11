import errno
import logging
import os
import shutil

from six import iteritems

from sebflow.exceptions import SebflowConfigException
from six.moves import configparser

ConfigParser = configparser.SafeConfigParser

logger = logging.getLogger(__name__)


def expand_env_var(env_var):
    '''
    make sure we're dealing with absolute paths
    '''
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


class SebflowConfigParser(ConfigParser):
    def __init__(self, *args, **kwargs):
        ConfigParser.__init__(self, *args, **kwargs)

    def get(self, section, key, **kwargs):
        section = str(section).lower()
        key = str(key).lower()

        if self.has_option(section, key):
            return expand_env_var(ConfigParser.get(self, section, key, **kwargs))
        else:
            raise SebflowConfigException("section/key [{section}/{key}] not found in config".format(**locals()))

    def getboolean(self, section, key):
        val = str(self.get(section, key)).lower().strip()
        if '#' in val:
            val = val.split('#')[0].strip()
        if val.lower() in ('t', 'true', '1'):
            return True
        elif val.lower() in ('f', 'false', '0'):
            return False
        else:
            raise SebflowConfigException(
                'The value for configuration option "{}:{}" is not a boolean (received "{}").'.format(section, key, val))

    def getint(self, section, key):
        return int(self.get(section, key))

    def getfloat(self, section, key):
        return float(self.get(section, key))

    def read(self, filename):
        assert(os.path.exists(filename))
        ConfigParser.read(self, filename)

    def getsection(self, section):
        """
        Returns the section as a dict. Values are converted to int, float, bool
        as required.
        :param section: section from the config
        :return: dict
        """
        if section in self._sections:
            _section = self._sections[section]
            for key, val in iteritems(self._sections[section]):
                try:
                    val = int(val)
                except ValueError:
                    try:
                        val = float(val)
                    except ValueError:
                        if val.lower() in ('t', 'true'):
                            val = True
                        elif val.lower() in ('f', 'false'):
                            val = False
                _section[key] = val
            return _section

        return None


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise SebflowConfigException('Had trouble creating a directory')


if 'SEBFLOW_HOME' not in os.environ:
    SEBFLOW_HOME = expand_env_var('~/sebflow')
else:
    SEBFLOW_HOME = expand_env_var(os.environ['SEBFLOW_HOME'])

mkdir_p(SEBFLOW_HOME)

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), 'config_files', 'sebflow.cfg')

if not os.path.exists(os.path.join(SEBFLOW_HOME, 'sebflow.cfg')):
    logger.warning('Could not find sebflow.cfg file. Creating one at %s' % os.path.join(SEBFLOW_HOME, 'sebflow.cfg'))
    shutil.copy(DEFAULT_CONFIG, SEBFLOW_HOME)

conf = SebflowConfigParser()
conf.read(os.path.join(SEBFLOW_HOME, 'sebflow.cfg'))


def get(section, key, **kwargs):
    return conf.get(section, key, **kwargs)


def getboolean(section, key):
    return conf.getboolean(section, key)


def getfloat(section, key):
    return conf.getfloat(section, key)


def getint(section, key):
    return conf.getint(section, key)


def getsection(section):
    return conf.getsection(section)
