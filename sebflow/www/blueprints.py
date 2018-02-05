# -*- coding: utf-8 -*-

import markdown
from flask import Blueprint, Markup, redirect, url_for

routes = Blueprint('routes', __name__)


@routes.route('/')
def index():
    return redirect(url_for('admin.index'))


@routes.route('/health')
def health():
    """ We can add an array of tests here to check the server's health """
    content = Markup(markdown.markdown("The server is healthy!"))
    return content
