import json
from werkzeug.test import Client
from werkzeug.wrappers import BaseResponse, BaseRequest

from wayback_discover_diff.web import get_app

CONFIG = {}     # TODO

APP = get_app(CONFIG)
# Initialize Celery and register Discover task.
# celery = Celery(__name__, broker='redis://'+str(cfg['redis']['host'])+':'+str(cfg['redis']['port']))
# celery.conf.update(
#     CELERY_BROKER_URL='redis://'+str(cfg['redis']['host'])+':'+str(cfg['redis']['port']),
#     CELERY_RESULT_BACKEND='redis://'+str(cfg['redis']['host'])+':'+str(cfg['redis']['port'])
# )
# celery.register_task(app)


def test_no_url():
    client = Client(APP, response_wrapper=BaseResponse)
    resp = client.get('/simhash?timestamp=20141115130953')
    assert resp.status_code == 200
    assert json.loads(resp.data) == dict(status='error', info='URL is required.')

"""
def test_no_timestamp():
    url = 'iskme.org'
    timestamp = None
    result = app.timestamp_simhash(url, timestamp)
    assert json.dumps({'error': 'Timestamp is required.'}) == result


def test_no_entry():
    url = 'nonexistingdomain.org'
    timestamp = '20180000000000'
    result = app.timestamp_simhash(url, timestamp)
    assert json.dumps({'simhash': 'None'}) == result


# def test_start_task():
#     url = 'iskme.org'
#     year = '2018'
#     job_id = celery.tasks['Discover'].apply(args=[url, year])
#     assert job_id is not None


def test_task_no_url():
    url = None
    year = '2018'
    job = celery.tasks['Discover'].apply(args=[url, year])
    assert job.get() == json.dumps({'status':'error', 'info': 'URL is required.'})


def test_task_no_year():
    url = 'nonexistingdomain.org'
    year = None
    job = celery.tasks['Discover'].apply(args=[url, year])
    assert job.get() == json.dumps({'status':'error', 'info': 'Year is required.'})


def test_task_no_snapshots():
    url = 'nonexistingdomain.org'
    year = '2018'
    job = celery.tasks['Discover'].apply(args=[url, year])
    assert job.get() == json.dumps({'status':'error', 'info': 'no snapshots found for this year and url combination'})

def test_success_calc_simhash():
    url = 'iskme.org'
    year = '2018'
    job = celery.tasks['Discover'].apply(args=[url, year])
    task_info = json.loads(job.info)
    assert task_info.get('duration', -1) != -1
"""
