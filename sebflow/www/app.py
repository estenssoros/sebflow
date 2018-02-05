from flask import Flask
from flask_cache import Cache
from flask_wtf.csrf import CSRFProtect

import sebflow
from sebflow import configuration
from sebflow.www.blueprints import routes
from sebflow.logging_config import configure_logging

csrf = CSRFProtect()


def create_app(config=None, testing=False):
    app = Flask(__name__)
    app.secret_key = configuration.get('webserver', 'SECRET_KEY')
    app.config['LOGIN_DISABLED'] = not configuration.getboolean('webserver', 'AUTHENTICATE')

    csrf.init_app(app)

    app.config['TESTING'] = testing

    sebflow.load_login()
    sebflow.login.login_manager.init_app(app)

    from sebflow import api

    api.load_auth()
    api.api_auth.init_app(app)

    cache = Cache(app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

    app.register_blueprint(routes)

    configure_logging()

    return app


def cached_app(config=None):
    global app
    if not app:
        app = create_app(config)
    return app
