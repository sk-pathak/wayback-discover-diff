"""application.py -- top-level web application for wayback-discover-diff.
"""
import logging.config
import os
from celery import Celery
from flask_cors import CORS
from redis import StrictRedis, BlockingConnectionPool
from wayback_discover_diff import stats
from wayback_discover_diff.util import load_config
from wayback_discover_diff.discover import Discover

# Init config
CFG = load_config()

# Init logging
logconf = CFG.get('logging')
if logconf:
    logging.config.dictConfig(logconf)

# Init statsd client
stats_conf = CFG.get('statsd')
if isinstance(stats_conf, dict):
    stats.configure(**stats_conf)

# Init Celery app
CELERY = Celery(**CFG['celery'])
CELERY.register_task(Discover(CFG))

# Init Flask app
from . import web
APP = web.get_app(CFG)

# Initialize CORS support
cors = CFG.get('cors')
if cors:
    CORS(APP, origins=cors)

# Initialize Celery and Redis
APP.celery = CELERY
APP.redis = StrictRedis(
    connection_pool=BlockingConnectionPool.from_url(
        **CFG.get('redis')
        )
    )

# ensure  the instance folder exists
try:
    os.makedirs(APP.instance_path)
except OSError:
    pass
