import pkg_resources
from celery import states
from celery.result import AsyncResult
from celery.exceptions import CeleryError
from flask import (Flask, request, jsonify)
from redis.exceptions import RedisError
from .util import (year_simhash, timestamp_simhash, url_is_valid,
                   compress_captures)

APP = Flask(__name__, instance_relative_config=True)


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
        tasks = list(pending.values())[0]
        for task in tasks:
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
        url = request.args.get('url')
        if not url:
            return jsonify({'status': 'error', 'info': 'url param is required.'})
        assert url_is_valid(url)

        timestamp = request.args.get('timestamp')
        if not timestamp:
            year = request.args.get('year')
            if not year:
                return jsonify({'status': 'error', 'info': 'year param is required.'})
            # validate that year is integer
            int(year)
            page = request.args.get('page', type=int)
            if page and page <= 0:
                return jsonify({'status': 'error', 'info': 'pager param should be > 0.'})

            snapshots = APP.config.get('snapshots')
            snapshots_per_page = snapshots.get('number_per_page')
            results_tuple = year_simhash(APP.redis_db, url, year, page, snapshots_per_page)
            # check if year_simhash produced an error response and return it
            if isinstance(results_tuple, dict):
                return jsonify(results_tuple)
            task = get_active_task(url, year)

            output = dict(captures=results_tuple[0],
                          total_captures=results_tuple[1],
                          status='PENDING' if task else 'COMPLETE')
            if request.args.get('compress'):
                (captures, hashes) = compress_captures(output['captures'])
                output['captures'] = captures
                output['hashes'] = hashes
            return jsonify(output)
        else:
            results = timestamp_simhash(APP.redis_db, url, timestamp)
            # check if timestamp_simhash produced an error response and return it
            if isinstance(results, dict):
                return jsonify(results)
            task = get_active_task(url, timestamp[:4])
            if task:
                return jsonify({'status': 'PENDING', 'captures': results})
            return jsonify({'status': 'COMPLETE', 'captures': results})
    except ValueError:
        return jsonify({'status': 'error', 'info': 'year param must be numeric.'})
    except AssertionError as exc:
        return jsonify({'status': 'error', 'info': 'invalid url format.'})


@APP.route('/calculate-simhash')
def request_url():
    """Validate parameters url & timestamp before starting Celery task.
    """
    try:
        url = request.args.get('url')
        if not url:
            return jsonify({'status': 'error', 'info': 'url param is required.'})
        assert url_is_valid(url)
        year = request.args.get('year')
        if not year:
            return jsonify({'status': 'error', 'info': 'year param is required.'})
        # validate that year is integer
        int(year)
        # see if there is an active job for this request
        task = get_active_task(url, year)
        if task:
            return jsonify({'status': 'PENDING', 'job_id': task['id']})
        res = APP.celery.tasks['Discover'].apply_async(args=[url, year])
        return jsonify({'status': 'started', 'job_id': res.id})
    except CeleryError as exc:
        return jsonify({'status': 'error', 'info': 'Cannot start calculation.'})
    except ValueError as exc:
        return jsonify({'status': 'error', 'info': 'year param must be numeric.'})
    except AssertionError as exc:
        return jsonify({'status': 'error', 'info': 'invalid url format.'})

@APP.route('/job')
def job_status():
    try:
        job_id = request.args.get('job_id')
        if not job_id:
            return jsonify({'status': 'error', 'info': 'job_id param is required.'})
        task = AsyncResult(job_id, app=APP.celery)
        if task.state == states.PENDING:
            # job did not finish yet
            response = {'status': task.state,
                        'job_id': task.id,
                        'info': task.info.get('info', 1)}
        else:
            if task.info.get('status', 0) == 'error':
                # something went wrong in the background job
                response = {'info': task.info.get('info', 1),
                            'job_id': task.id,
                            'status': task.info.get('status', 0)}
            else:
                response = {'status': task.state,
                            'job_id': task.id,
                            'duration': task.info.get('duration', 1)}
        return jsonify(response)
    except (CeleryError, AttributeError):
        return jsonify({'status': 'error', 'info': 'Cannot get status.'})
