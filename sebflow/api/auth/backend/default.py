# -*- coding: utf-8 -*-


from functools import wraps

client_auth = None


def init_app(app):
    pass


def requires_authentication(function):
    @wraps(function)
    def decorated(*args, **kwargs):
        return function(*args, **kwargs)

    return decorated
