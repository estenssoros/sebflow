import StringIO

from six.moves import configparser

ConfigParser = configparser.SafeConfigParser
conf = SebFlowConfigParser()
conf.read(SEBFLOW_CONFIG)

_templates_dir = os.path.join(os.path.dirname(__file__, 'config_templates'))
with open(os.path.join(_templates_dir, 'default_sebflow.cfg')) as f:
    DEFAULT_CONFIG = f.read()


class SebflowConfigParser(ConfigParser):
    def __init__(self, *args, **kwargs):
        ConfigParser.__init__(self, *args, **kwargs)
        self.read_string(parameterized_config(DEFAULT_CONFIG))
        self.is_validated = False

    def read_string(self, string):
        self.readfp(StringIO.Stringio(string))


def get(section, key):
    return conf.get(section, key, **kwargs)


def parameterized_config(template):
    all_vars = {k: v for d in [globals(), locals()] for k, v in d.items()}
    return template.format(**all_vars)
