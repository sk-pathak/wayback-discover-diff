import pkg_resources
from celery import (Celery, states)
from celery.result import AsyncResult
from celery.exceptions import CeleryError
from flask import (Flask, request, jsonify, json)
from .util import year_simhash, timestamp_simhash

APP = Flask(__name__, instance_relative_config=True)


def get_app(config):
    """Utility method to set APP configuration. Its used by application.py.
    """
    APP.config.from_mapping(
        SECRET_KEY='dev',
    )
    APP.config.update(CELERYD_HIJACK_ROOT_LOGGER=False)
    APP.config.update(config)
    return APP

@APP.route('/')
def root():
    """Return info on the current package version.
    """
    version = pkg_resources.require("wayback-discover-diff")[0].version
    return "wayback-discover-diff service version: %s" % version


@APP.route('/simhash')
def simhash():
    """Return simhash data for specific URL and year (optional),
    page is also optional.
    """
    url = request.args.get('url')
    if not url:
        return jsonify({'status': 'error', 'info': 'url param is required.'})

    timestamp = request.args.get('timestamp')
    if not timestamp:
        year = request.args.get('year')
        if not year:
            return jsonify({'status': 'error', 'info': 'year param is required.'})
        page = request.args.get('page', type=int)
        if page and page <= 0:
            return jsonify({'status': 'error', 'info': 'pager param should be > 0.'})

        snapshots = APP.config.get('snapshots')
        snapshots_per_page = snapshots.get('snapshots_per_page')
        results = year_simhash(APP.redis_db, url, year, page, snapshots_per_page)
        if not results:
            results = {'simhash': 'None'}
        return jsonify(results)
    else:
        # self._log.info('requesting redis db entry for %s %s', url, timestamp)
        results = timestamp_simhash(APP.redis_db, url, timestamp)
        if not results:
            results = {'simhash': 'None'}
        return jsonify(results)


@APP.route('/calculate-simhash')
def request_url():
    try:
        url = request.args.get('url')
        if not url:
            return jsonify({'status': 'error', 'info': 'url param is required.'})
        year = request.args.get('year')
        if not year:
            return jsonify({'status': 'error', 'info': 'year param is required.'})
        res = APP.celery.tasks['Discover'].apply_async(args=[url, year])
        return jsonify({'status': 'started', 'job_id': res.id})
    except CeleryError as exc:
        return jsonify({'status': 'error', 'info': 'Cannot start calculation.'})


@APP.route('/job')
def job_status():
    try:
        job_id = request.args.get('job_id')
        if not job_id:
            return jsonify({'status': 'error', 'info': 'job_id param is required.'})
        task = AsyncResult(job_id, app=APP.celery)
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
    except AttributeError:
        return jsonify({'status': 'error', 'info': 'Cannot get status.'})
