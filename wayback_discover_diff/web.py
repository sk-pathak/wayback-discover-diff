import logging
import pkg_resources
from time import time
from celery import states
from celery.result import AsyncResult
from celery.exceptions import CeleryError
from flask import Flask, request
from redis.exceptions import RedisError
from .stats import statsd_incr
from .util import (year_simhash, timestamp_simhash, url_is_valid,
                   compress_captures)

APP = Flask(__name__, instance_relative_config=True)
APP._logger = logging.getLogger('wayback_discover_diff.web')

def get_app(config):
    """Utility method to set APP configuration. Its used by application.py.
    """
    APP.config.from_mapping(
        SECRET_KEY='wayback machine simhash service',
    )
    APP.config.update(CELERYD_HIJACK_ROOT_LOGGER=False)
    APP.config.update(config)
    return APP


def get_active_task(url, year):
    """Check for current simhash processing tasks for targe url & year
    """
    try:
        pending = APP.celery.control.inspect().active()
        if pending:
            for task in list(pending.values())[0]:
                if task['args'] == "['%s', '%s']" % (url, year):
                    return task
        return None
    except RedisError:
        # Redis connection timeout is quite common in production Celery.
        return None


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
    try:
        statsd_incr('get-simhash-year-request')
        url = request.args.get('url')
        if not url:
            return {'status': 'error', 'info': 'url param is required.'}
        if not url_is_valid(url):
            return {'status': 'error', 'info': 'invalid url format.'}
        timestamp = request.args.get('timestamp')
        if not timestamp:
            year = request.args.get('year', type=int)
            if not year:
                return {'status': 'error', 'info': 'year param is required.'}
            page = request.args.get('page', type=int)
            snapshots_per_page = APP.config.get('snapshots', {}).get('number_per_page')
            results_tuple = year_simhash(APP.redis_db, url, year, page,
                                         snapshots_per_page)
            # check if year_simhash produced an error response and return it
            if isinstance(results_tuple, dict):
                return results_tuple
            task = get_active_task(url, year)

            output = dict(captures=results_tuple[0],
                          total_captures=results_tuple[1],
                          status='PENDING' if task else 'COMPLETE')
            if request.args.get('compress') in ['true', '1']:
                (captures, hashes) = compress_captures(output['captures'])
                output['captures'] = captures
                output['hashes'] = hashes
            return output

        results = timestamp_simhash(APP.redis_db, url, timestamp)
        # check if timestamp_simhash produced an error response and return it
        if isinstance(results, dict):
            return results
        task = get_active_task(url, timestamp[:4])
        if task:
            return {'status': 'PENDING', 'captures': results}
        return {'status': 'COMPLETE', 'captures': results}
    except ValueError as exc:
        APP._logger.warning('Cannot get simhash of %s, (%s)', url, str(exc))
        return {'status': 'error', 'info': 'year param must be numeric.'}


@APP.route('/calculate-simhash')
def request_url():
    """Start simhash calculation for URL & year.
    Validate parameters url & timestamp before starting Celery task.
    """
    try:
        statsd_incr('calculate-simhash-year-request')
        url = request.args.get('url')
        if not url:
            return {'status': 'error', 'info': 'url param is required.'}
        if not url_is_valid(url):
            return {'status': 'error', 'info': 'invalid url format.'}
        year = request.args.get('year', type=int)
        if not year:
            return {'status': 'error', 'info': 'year param is required.'}
        # see if there is an active job for this request
        task = get_active_task(url, year)
        if task:
            return {'status': 'PENDING', 'job_id': task['id']}
        res = APP.celery.tasks['Discover'].apply_async(
            args=[url, year],
            kwargs=dict(created=time())
            )
        return {'status': 'started', 'job_id': res.id}
    except CeleryError as exc:
        APP._logger.warning('Cannot calculate simhash of %s, %s (%s)', url,
                            year, str(exc))
        return {'status': 'error', 'info': 'Cannot start calculation.'}
    except ValueError as exc:
        APP._logger.warning('Cannot calculate simhash of %s, no year (%s)',
                            url, str(exc))
        return {'status': 'error', 'info': 'year param must be numeric.'}


@APP.route('/job')
def job_status():
    """Return job status.
    """
    try:
        statsd_incr('status-request')
        job_id = request.args.get('job_id')
        if not job_id:
            return {'status': 'error', 'info': 'job_id param is required.'}
        task = AsyncResult(job_id, app=APP.celery)
        if task.state == states.PENDING:
            if task.info:
                info = task.info.get('info', 1)
            else:
                info = None
            # job did not finish yet
            return {'status': task.state, 'job_id': task.id, 'info': info}

        if task.info and task.info.get('status', 0) == 'error':
            # something went wrong in the background job
            return {'info': task.info.get('info', 1), 'job_id': task.id,
                    'status': task.info.get('status', 0)}
        if task.info:
            duration = task.info.get('duration', 1)
        else:
            duration = 1
        return {'status': task.state, 'job_id': task.id, 'duration': duration}
    except (CeleryError, AttributeError) as exc:
        APP._logger.error('Cannot get job status of %s, (%s)', job_id, str(exc))
        return {'status': 'error', 'info': 'Cannot get status.'}
