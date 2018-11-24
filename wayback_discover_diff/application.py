"""application.py -- top-level web application for wayback-discover-diff.
"""
import logging
import os
from celery import Celery
from flask_cors import CORS
from redis import StrictRedis
from wayback_discover_diff.util import load_config
from wayback_discover_diff.discover import Discover

# Init config
CFG = load_config()

# Init logging
logfile = CFG['logfile']['name']
loglevel = CFG['logfile']['level']
logging.getLogger(__name__).setLevel(loglevel)
logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    handlers=[logging.FileHandler(logfile),
                              logging.StreamHandler()]
                    )

# Init Celery app
CELERY = Celery('wayback-discover-diff', broker=CFG['celery_broker'],
                backend=CFG['celery_backend'])
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
APP.redis_db = StrictRedis.from_url(CFG['redis_uri'], decode_responses=True)

# ensure  the instance folder exists
try:
    os.makedirs(APP.instance_path)
except OSError:
    pass
