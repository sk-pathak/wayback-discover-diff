"""Test web endpoints.
"""
import json
import pytest
from werkzeug.test import Client
from werkzeug.wrappers import Response
from test_util import StubRedis

from wayback_discover_diff.web import get_app


@pytest.fixture
def app():
    cfg = dict(redis_uri='redis://localhost/9',
        snapshots=dict(snapshots_per_page=100)
        )
    web_app = get_app(cfg)
    web_app.redis = StubRedis()
    return web_app

# TODO we must mock Celery task
# Initialize Celery and register Discover task.
# celery = Celery(__name__, broker='redis://'+str(cfg['redis']['host'])+':'+str(cfg['redis']['port']))
# celery.conf.update(
#     CELERY_BROKER_URL='redis://'+str(cfg['redis']['host'])+':'+str(cfg['redis']['port']),
#     CELERY_RESULT_BACKEND='redis://'+str(cfg['redis']['host'])+':'+str(cfg['redis']['port'])
# )
# celery.register_task(app)


def test_simhash_parameters(app):
    client = Client(app, response_wrapper=Response)
    resp = client.get('/simhash?timestamp=20141115130953')
    assert resp.status_code == 200
    data = json.loads(resp.data.decode('utf-8'))
    assert data == dict(status='error', info='url param is required.')

    resp = client.get('/simhash?url=example.com')
    assert resp.status_code == 200
    data = json.loads(resp.data.decode('utf-8'))
    assert data == dict(status='error', info='year param is required.')

    resp = client.get('/simhash?url=invalid&timestamp=20141115130953')
    assert resp.status_code == 200
    data = json.loads(resp.data.decode('utf-8'))
    assert data == dict(status='error', info='invalid url format.')

    # StubRedis already has simhash data for 20140202131837 and example.com
    resp = client.get('/simhash?url=example.com&timestamp=20140202131837')
    data = json.loads(resp.data.decode('utf-8'))
    assert data.get('simhash') == 'og2jGKWHsy4='


def test_no_entry(app):
    client = Client(app, response_wrapper=Response)
    resp = client.get('/simhash?timestamp=20180000000000&url=nonexistingdomain.org')
    assert resp.status_code == 200
    data = json.loads(resp.data.decode('utf-8'))
    assert data == {'message': 'CAPTURE_NOT_FOUND', 'status': 'error'}

# TODO must mock this
# def test_start_task():
#     url = 'iskme.org'
#     year = '2018'
#     job_id = celery.tasks['Discover'].apply(args=[url, year])
#     assert job_id is not None


def test_simhash_task_parameters(app):
    client = Client(app, response_wrapper=Response)
    resp = client.get('/calculate-simhash?year=2018')
    assert resp.status_code == 200
    data = json.loads(resp.data.decode('utf-8'))
    assert data == dict(status='error', info='url param is required.')

    resp = client.get('/calculate-simhash?url=example.com&year=XY')
    assert resp.status_code == 200
    data = json.loads(resp.data.decode('utf-8'))
    assert data == dict(status='error', info='year param is required.')

    resp = client.get('/calculate-simhash?url=nonexistingdomain.org')
    assert resp.status_code == 200
    data = json.loads(resp.data.decode('utf-8'))
    assert data == dict(status='error', info='year param is required.')

    resp = client.get('/calculate-simhash?url=nonexistingdomain.org&year=-')
    assert resp.status_code == 200
    data = json.loads(resp.data.decode('utf-8'))
    assert data == dict(status='error', info='year param is required.')

    resp = client.get('/calculate-simhash?url=foo&year=2000')
    assert resp.status_code == 200
    data = json.loads(resp.data.decode('utf-8'))
    assert data == dict(status='error', info='invalid url format.')


def test_task_no_snapshots(app):
    client = Client(app, response_wrapper=Response)
    resp = client.get('/simhash?url=nonexistingdomain.org&year=1999')
    data = json.loads(resp.data.decode('utf-8'))
    assert data == {'message': 'NO_CAPTURES', 'status': 'error'}


# TODO must mock this
# def test_success_calc_simhash():
#     url = 'iskme.org'
#     year = '2018'
#     job = celery.tasks['Discover'].apply(args=[url, year])
#     task_info = json.loads(job.info)
#     assert task_info.get('duration', -1) != -1


def test_root(app):
    client = Client(app, response_wrapper=Response)
    resp = client.get('/')
    data = resp.data.decode('utf-8')
    assert data.startswith("wayback-discover-diff")


def test_job_params(app):
    client = Client(app, response_wrapper=Response)
    resp = client.get('/job')
    data = json.loads(resp.data.decode('utf-8'))
    assert data == dict(status='error', info='job_id param is required.')
