import os

from celery import (Celery, states)
from celery.result import AsyncResult
from flask import (Flask, request, jsonify, json)
import yaml

from wayback_discover_diff.discover import Discover

# create and configure the app
APP = Flask(__name__, instance_relative_config=True)
APP.config.from_mapping(
    SECRET_KEY='dev',
)
APP.config.update(CELERYD_HIJACK_ROOT_LOGGER=False)
with open(os.environ['WAYBACK_DISCOVER_DIFF_CONF'], 'r') as ymlfile:
    CFG = yaml.load(ymlfile)

APP.config.update(
    CELERY_BROKER_URL='redis://' + str(CFG['redis']['host']) + ':' + str(CFG['redis']['port']),
    CELERY_RESULT_BACKEND='redis://' + str(CFG['redis']['host']) + ':' + str(CFG['redis']['port'])
)

APP.config.from_pyfile('config.py', silent=True)

# NOTE it is a known issue that with the following we instantiate 2 Discovery
# objects and create 2 Redis connections. There is certainly a way to have
# only one. I couldn't find a way to run `app.discovery.simhash` in another way.
APP.discover = Discover(CFG)

# ensure  the instance folder exists
try:
    os.makedirs(APP.instance_path)
except OSError:
    pass

# Initialize Celery and register Discover task.
CELERY = Celery(APP.name, broker=APP.config['CELERY_BROKER_URL'])
CELERY.conf.update(APP.config)
CELERY.register_task(Discover(CFG))


@APP.route('/simhash')
def simhash():
    url = request.args.get('url')
    timestamp = request.args.get('timestamp')
    if not timestamp:
        year = request.args.get('year')
        return APP.discover.year_simhash(url, year)
    return APP.discover.timestamp_simhash(url, timestamp)


@APP.route('/calculate-simhash')
def request_url():
    url = request.args.get('url')
    year = request.args.get('year')
    return jsonify({'status': 'started',
                    'job_id': str(CELERY.tasks['Discover'].apply_async(args=[url, year]))})


@APP.route('/job')
def job_status():
    job_id = request.args.get('job_id')
    task = AsyncResult(job_id, app=CELERY)
    if task.state == states.PENDING:
        # job did not finish yet
        response = {
            'status': task.state,
            'job_id': task.id,
            'info': task.info.get('info', 1)
        }
    else:
        task_info = json.loads(task.info)
        if task_info.get('status', 0) == 'error':
            # something went wrong in the background job
            response = {
                'info': task_info.get('info', 1),
                'job_id': task.id,
                'status': task_info.get('status', 0)
            }
        else:
            response = {
                'status': task.state,
                'job_id': task.id,
                'duration': task_info.get('duration', 1)
            }
    return jsonify(response)


if __name__ == '__main__':
    APP.run()
