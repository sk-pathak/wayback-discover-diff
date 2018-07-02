import os
from flask import (Flask, request)
import yaml
import urllib3
from .discover import Discover


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
    )

    with open(os.environ['WAYBACK_DISCOVER_DIFF_CONF'], 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
    simhash_size = cfg['simhash']['size']

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure  the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    http = urllib3.PoolManager()

    @app.route('/simhash')
    def simhash():
        return Discover.simhash(request)

    @app.route('/calculate-simhash')
    def request_url():
        return Discover.request_url(simhash_size, request, http)

    return app