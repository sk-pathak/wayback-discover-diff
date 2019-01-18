import pytest

from wayback_discover_diff.util import (url_is_valid, year_simhash,
                                        timestamp_simhash)


SAMPLE_REDIS_CONTENT = {
    'com,example)/': {
        '20141021062411': 'o52rOf0Hi2o=',
        '20140202131837': 'og2jGKWHsy4=',
        '20140824062257': 'o52jPP0Hg2o=',
        '20160824062257': 'o52jPP0Hg2o='
        },
    'com,other)/': {
        '2014': '-1'
    },
    'org,nonexistingdomain)/': {
        '1999': '-1'
    },
}


class StubRedis(dict):
    """Mock Redis connection for unit tests.
    """
    def __init__(self, *args, **kwargs):
        self.update(SAMPLE_REDIS_CONTENT)

    def hset(self, key, hkey, hval):
        e = self.get(key)
        if e is None:
            self[key] = e = {}
        else:
            assert isinstance(e, dict)
        e[hkey] = hval

    def hget(self, key, hkey):
        e = self.get(key)
        if e is None: return None
        assert isinstance(e, dict)
        return e.get(hkey)

    def hkeys(self, key):
        e = self.get(key)
        if e is None: return {}
        assert isinstance(e, dict)
        return self.get(key).keys()

    def hmget(self, key, hkeys):
        e = self.get(key)
        if e is None: return None
        assert isinstance(e, dict)
        out = {}
        for hkey in hkeys:
            out[hkey] = e.get(hkey)
        return out


@pytest.mark.parametrize('url,result', [
    ('http://example.com/', True),
    ('other', False)
    ])
def test_url_is_valid(url, result):
    assert url_is_valid(url) == result


@pytest.mark.parametrize('url,timestamp,simhash', [
    ('http://example.com', '20141021062411', 'o52rOf0Hi2o='),
    ('http://example.com', '2014102', None),
    ('http://other.com', '20141021062411', None),
    ])
def test_timestamp_simhash(url, timestamp, simhash):
    redis_db = StubRedis()
    res = timestamp_simhash(redis_db, url, timestamp)
    if len(res.keys()) == 1:
        assert res == {'simhash': simhash}
    elif url == 'http://other.com':
        assert res == {'status': 'error', 'message': 'NO_CAPTURES'}
    else:
        assert res == {'status': 'error', 'message': 'CAPTURE_NOT_FOUND'}


@pytest.mark.parametrize('url,year,count', [
    ('http://example.com', '2014', 3),
    ('http://example.com', '2016', 1),
    ('http://example.com', '2017', None),
    ('http://example.com', '', None),
    ('http://other.com', '2014', None),
    ])
def test_year_simhash(url, year, count):
    redis_db = StubRedis()
    res = year_simhash(redis_db, url, year)
    # check if year_simhash produced an error response
    if isinstance(res,dict):
        if year == '2014':
            assert res == {'status': 'error', 'message': 'NO_CAPTURES'}
        else:
            assert res == {'status': 'error', 'message': 'NOT_CAPTURED'}
    if count:
        assert len(res[0]) == count
