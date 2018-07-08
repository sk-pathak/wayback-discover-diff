import os
from flask import (Flask, request, jsonify)
import yaml
from wayback_discover_diff.discover import Discover
from celery import Celery
from celery.result import AsyncResult

# create and configure the app
app = Flask(__name__, instance_relative_config=True)
app.config.from_mapping(
    SECRET_KEY='dev',
)

with open(os.environ['WAYBACK_DISCOVER_DIFF_CONF'], 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

app.config.update(
    CELERY_BROKER_URL='redis://'+str(cfg['redis']['host'])+':'+str(cfg['redis']['port']),
    CELERY_RESULT_BACKEND='redis://'+str(cfg['redis']['host'])+':'+str(cfg['redis']['port'])
)

app.config.from_pyfile('config.py', silent=True)

# NOTE it is a known issue that with the following we instantiate 2 Discovery
# objects and create 2 Redis connections. There is certainly a way to have
# only one. I couldn't find a way to run `app.discovery.simhash` in another way.
app.discover = Discover(cfg)

# ensure  the instance folder exists
try:
    os.makedirs(app.instance_path)
except OSError:
    pass

# Initialize Celery and register Discover task.
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
celery.register_task(Discover(cfg))


@app.route('/simhash')
def simhash():
    url = request.args.get('url')
    timestamp = request.args.get('timestamp')
    return app.discover.simhash(url, timestamp)


@app.route('/calculate-simhash')
def request_url():
    url = request.args.get('url')
    year = request.args.get('year')
    return jsonify({'status': 'started', 'job_id': str(celery.tasks['Discover'].apply_async(args=[url, year]))})


@app.route('/job')
def job_status():
    job_id = request.args.get('job_id')
    task = AsyncResult(job_id, app=celery)
    if task.state == 'PENDING':
        # job did not start yet
        response = {
            'status': task.state,
            'job_id': task.id
            # TODO for some reason this doesn't work with my changes,
            # I need to do this different? I didn't have the time to check
            #'info': task.info.get('info', 1),
        }
    elif task.state != 'FAILURE':
        response = {
            'status': task.state,
            'job_id': task.id
            # TODO fix
            #'duration': task.info.get('duration', 1)
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True)
# return app
