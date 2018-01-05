import copy
import errno
import os
import shlex
import StringIO
import subprocess
import sys
from collections import OrderedDict

from sebflow.exceptions import SebflowConfigException
from sebflow.utils.log.logging_mixin import LoggingMixin

from six.moves import configparser

log = LoggingMixin().log

ConfigParser = configparser.SafeConfigParser

def generate_fernet_key():
    try:
        from cryptography.fernet import Fernet
    except ImportError:
        pass
    try:
        key = Fernet.generate_key().decode()
    except NameError:
        key = "cryptography_not_found_storing_passwords_in_plain_text"
    return key

# start_path = os.path.dirname(__file__)
start_path='C:\Users\sestenssoro\sebflow\sebflow'
_templates_dir = os.path.join(start_path, 'config_templates')
with open(os.path.join(_templates_dir, 'default_sebflow.cfg')) as f:
    DEFAULT_CONFIG = f.read()


def expand_env_var(env_var):
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


def run_command(command):
    process = subprocess.Popen(
        shlex.split(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True)
    output, stderr = [stream.decode(sys.getdefaultencoding(), 'ignore')
                      for stream in process.communicate()]

    if process.returncode != 0:
        raise SebflowConfigException(
            "Cannot execute {}. Error code is: {}. Output: {}, Stderr: {}"
            .format(command, process.returncode, output, stderr)
        )

    return output


class SebflowConfigParser(ConfigParser):
    def __init__(self, *args, **kwargs):
        ConfigParser.__init__(self, *args, **kwargs)
        self.read_string(parameterized_config(DEFAULT_CONFIG))
        self.is_validated = False

    def read_string(self, string):
        self.readfp(StringIO.StringIO(string))

    def _validate(self):
        if (
                self.get("core", "executor") != 'SequentialExecutor' and
                "sqlite" in self.get('core', 'sql_alchemy_conn')):
            raise SebflowConfigException(
                "error: cannot use sqlite with the {}".format(
                    self.get('core', 'executor')))

        elif (
            self.getboolean("webserver", "authenticate") and
            self.get("webserver", "owner_mode") not in ['user', 'ldapgroup']
        ):
            raise SebflowConfigException(
                "error: owner_mode option should be either "
                "'user' or 'ldapgroup' when filtering by owner is set")

        elif (
            self.getboolean("webserver", "authenticate") and
            self.get("webserver", "owner_mode").lower() == 'ldapgroup' and
            self.get("webserver", "auth_backend") != (
                'sebflow.contrib.auth.backends.ldap_auth')
        ):
            raise SebflowConfigException(
                "error: attempt at using ldapgroup "
                "filtering without using the Ldap backend")

        self.is_validated = True

    def _get_env_var_option(self, section, key):
        env_var = 'SEBFLOW__{S}__{K}'.format(S=section.upper(), K=key.upper())
        if env_var in os.environ:
            return expand_env_var(os.environ[env_var])

    def _get_cmd_option(self, section, key):
        fallback_key = key + '_cmd'
        # if this is a valid command key...
        if (section, key) in SebflowConfigParser.as_command_stdout:
            # if the original key is present, return it no matter what
            if self.has_option(section, key):
                return ConfigParser.get(self, section, key)
            # otherwise, execute the fallback key
            elif self.has_option(section, fallback_key):
                command = self.get(section, fallback_key)
                return run_command(command)

    def get(self, section, key, **kwargs):
        section = str(section).lower()
        key = str(key).lower()

        # first check environment variables
        option = self._get_env_var_option(section, key)
        if option is not None:
            return option

        # ...then the config file
        if self.has_option(section, key):
            return expand_env_var(
                ConfigParser.get(self, section, key, **kwargs))

        # ...then commands
        option = self._get_cmd_option(section, key)
        if option:
            return option

        else:
            log.warning(
                "section/key [{section}/{key}] not found in config".format(**locals())
            )

            raise SebflowConfigException(
                "section/key [{section}/{key}] not found "
                "in config".format(**locals()))

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
                'The value for configuration option "{}:{}" is not a '
                'boolean (received "{}").'.format(section, key, val))

    def getint(self, section, key):
        return int(self.get(section, key))

    def getfloat(self, section, key):
        return float(self.get(section, key))

    def read(self, filenames):
        ConfigParser.read(self, filenames)
        self._validate()

    def getsection(self, section):
        if section in self._sections:
            return self._sections[section]

        return None

    def as_dict(self, display_source=False, display_sensitive=False):
        cfg = copy.deepcopy(self._sections)

        # remove __name__ (affects Python 2 only)
        for options in cfg.values():
            options.pop('__name__', None)

        # add source
        if display_source:
            for section in cfg:
                for k, v in cfg[section].items():
                    cfg[section][k] = (v, 'sebflow config')

        # add env vars and overwrite because they have priority
        for ev in [ev for ev in os.environ if ev.startswith('SEBFLOW__')]:
            try:
                _, section, key = ev.split('__')
                opt = self._get_env_var_option(section, key)
            except ValueError:
                opt = None
            if opt:
                if (
                        not display_sensitive
                        and ev != 'SEBFLOW__CORE__UNIT_TEST_MODE'):
                    opt = '< hidden >'
                if display_source:
                    opt = (opt, 'env var')
                cfg.setdefault(section.lower(), OrderedDict()).update(
                    {key.lower(): opt})

        # add bash commands
        for (section, key) in SebflowConfigParser.as_command_stdout:
            opt = self._get_cmd_option(section, key)
            if opt:
                if not display_sensitive:
                    opt = '< hidden >'
                if display_source:
                    opt = (opt, 'bash cmd')
                cfg.setdefault(section, OrderedDict()).update({key: opt})

        return cfg


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

if 'SEBFLOW_CONFIG' not in os.environ:
    if os.path.isfile(expand_env_var('~/sebflow.cfg')):
        SEBFLOW_CONFIG = expand_env_var('~/sebflow.cfg')
    else:
        SEBFLOW_CONFIG = SEBFLOW_HOME + '/sebflow.cfg'
else:
    SEBFLOW_CONFIG = expand_env_var(os.environ['SEBFLOW_CONFIG'])

TEST_DAGS_FOLDER = os.path.join(SEBFLOW_HOME, 'dags')


def parameterized_config(template):
    all_vars = {k: v for d in [globals(), locals()] for k, v in d.items()}
    return template.format(**all_vars)

if not os.path.isfile(SEBFLOW_CONFIG):
    FERNET_KEY = generate_fernet_key()
else:
    FERNET_KEY = ''

TEMPLATE_START = ('# ----------------------- TEMPLATE BEGINS HERE -----------------------')

if not os.path.isfile(SEBFLOW_CONFIG):
    log.info(
        'Creating new Sebflow config file in: %s',
        SEBFLOW_CONFIG
    )
    with open(SEBFLOW_CONFIG, 'w') as f:
        cfg = parameterized_config(DEFAULT_CONFIG)
        f.write(cfg.split(TEMPLATE_START)[-1].strip())

log.info("Reading the config from %s", SEBFLOW_CONFIG)

conf = SebflowConfigParser()
conf.read(SEBFLOW_CONFIG)


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


def has_option(section, key):
    return conf.has_option(section, key)


def remove_option(section, option):
    return conf.remove_option(section, option)


def as_dict(display_source=False, display_sensitive=False):
    return conf.as_dict(
        display_source=display_source, display_sensitive=display_sensitive)
as_dict.__doc__ = conf.as_dict.__doc__


def set(section, option, value):  # noqa
    return conf.set(section, option, value)
