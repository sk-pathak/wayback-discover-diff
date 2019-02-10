from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import string
from datetime import datetime
import cProfile
import base64
from itertools import groupby
import xxhash
from celery import Task
import urllib3
from urllib3.exceptions import HTTPError
from redis import StrictRedis, BlockingConnectionPool
from redis.exceptions import RedisError
from simhash import Simhash
from surt import surt
from bs4 import BeautifulSoup

# https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
urllib3.disable_warnings()


TRANSLATOR = str.maketrans(string.punctuation, ' '*len(string.punctuation))

def extract_html_features(html):
    """Process HTML document and get key features as text. Steps:
    kill all script and style elements
    get lowercase text
    remove all punctuation
    break into lines and remove leading and trailing space on each
    break multi-headlines into a line each
    drop blank lines
    return a dict with features and their weights
    """
    soup = BeautifulSoup(html, features='lxml')
    for script in soup(["script", "style"]):
        script.decompose()
    text = soup.get_text().lower()
    text = text.translate(TRANSLATOR)
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    text = text.split()
    # this frees up memory https://stackoverflow.com/questions/11284643/python-high-memory-usage-with-beautifulsoup
    soup.decompose()
    return {k: sum(1 for _ in g) for k, g in groupby(sorted(text))}

# This custom hash function generated ALWAYS the same simhash size despite
# changing simhash size setting. Using the default, we get the right simhashes.
# 
# def hash_function(x):
#     """Custom FAST hash function used to generate simhash.
#     """
#     return int(xxhash.xxh64(x).hexdigest(), 16)


def calculate_simhash(features_dict, simhash_size):
    """Calculate simhash for features in a dict. `features_dict` contains data
    like {'text': weight}
    """
    return Simhash(features_dict, simhash_size).value


class Discover(Task):
    """Custom Celery Task class.
    http://docs.celeryproject.org/en/latest/userguide/tasks.html#custom-task-classes
    """
    name = 'Discover'
    task_id = None

    def __init__(self, cfg):
        self.simhash_size = cfg['simhash']['size']
        self.simhash_expire = cfg['simhash']['expire_after']

        headers = None
        cdx_auth_token = cfg.get('cdx_auth_token')
        if cdx_auth_token:
            headers = dict(cookie='cdx_auth_token=%s' % cdx_auth_token)

        self.http = urllib3.HTTPConnectionPool('web.archive.org', maxsize=50,
                                               retries=urllib3.Retry(3, redirect=2),
                                               headers=headers)
        self.redis_db = StrictRedis(
            connection_pool=BlockingConnectionPool.from_url(
                cfg['redis_uri'], max_connections=50,
                timeout=cfg.get('redis_timeout', 10),
                decode_responses=True
                )
            )
        self.tpool = ThreadPoolExecutor(max_workers=cfg['threads'])
        self.snapshots_number = cfg['snapshots']['number_per_year']
        # Initialize logger
        self._log = logging.getLogger(__name__)

    def download_capture(self, ts):
        """Download capture from WBM and update job status.
        Return capture body (probably HTML text)
        """
        try:
            self._log.info('fetching capture %s %s', ts, self.url)
            resp = self.http.request('GET', '/web/%sid_/%s' % (ts, self.url))
            return resp.data.decode('utf-8', 'ignore')
        except HTTPError as exc:
            self._log.error('cannot fetch capture %s %s (%s)', ts, self.url, exc)

        except RedisError as exc:
            self._log.error('cannot update job status (%s)', exc)

    def start_profiling(self, snapshot, index):
        cProfile.runctx('self.get_calc_save(snapshot, index)',
                        globals=globals(), locals=locals(), filename='profile.prof')

    def get_calc_save(self, timestamp, digest):
        """if capture with diplicate digest has already calculate simhash,
        return that and avoid redownloading and processing. Else, download
        capture, extract HTML features, calculate simhash and save to Redis.

        The result is a simhash number if capture is duplicate or new. If there
        is any problem (.e.g not found or cannot be calculated), return None
        """
        if digest in self.seen:
            self._log.info("already seen %s %s" % (digest, self.seen[digest]))
            return self.seen[digest]

        response_data = self.download_capture(timestamp)
        if response_data:
            data = extract_html_features(response_data)
            if data:
                self._log.info("calculating simhash")
                return calculate_simhash(data, self.simhash_size)

    def run(self, url, year):
        """Run Celery Task.
        """
        self.job_id = self.request.id
        self.url = url
        time_started = datetime.now()
        self._log.info('calculate simhash started')
        if not self.url:
            self._log.error('did not give url parameter')
            return {'status': 'error', 'info': 'URL is required.'}
        if not year:
            self._log.error('did not give year parameter')
            return {'status': 'error', 'info': 'Year is required.'}

        try:
            self._log.info('fetching CDX of %s for year %s', self.url, year)
            self.update_state(state='PENDING',
                              meta={'info': 'Fetching %s captures for year %s' % (
                                    self.url, year)})
            # Collapse captures by timestamp to get 1 capture per hour.
            # Its necessary to reduce the huge number of captures some websites
            # (e.g. twitter.com has 167k captures for 2018. Get only 2xx captures.
            cdx_url = '/cdx/search/cdx?url=%s&from=%s&to=%s&fl=timestamp,digest&collapse=timestamp:10&statuscode=200' % (
                self.url, year, year)
            if self.snapshots_number != -1:
                cdx_url += '&limit=%d' % self.snapshots_number
            response = self.http.request('GET', cdx_url)
            self._log.info('finished fetching timestamps of %s for year %s',
                           self.url, year)
            assert response.status == 200
            assert response.data
            captures_txt = response.data.decode('utf-8')
            if not captures_txt:
                self._log.error('no captures found for this year and url combination')
                self.redis_db.hset(surt(self.url), year, -1)
                self.redis_db.expire(surt(self.url), self.simhash_expire)
                return {'status': 'error',
                        'info': 'no captures found for this year and url combination'}
        except (AssertionError, ValueError, HTTPError) as exc:
            self._log.error('invalid CDX query response %s (%s)', cdx_url, exc)
            return {'status': 'error', 'info': str(exc)}
        except RedisError as exc:
            self._log.error('error connecting with Redis for url %s year %s (%s)',
                            url, year, exc)
            return {'status': 'error', 'info': str(exc)}
        captures = captures_txt.strip().split("\n")
        total = len(captures)
        self.seen = dict()
        futures_to_url = {}
        # calculate simhashes in parallel
        for cap in captures:
            (ts, digest) = cap.split(' ')
            future = self.tpool.submit(self.get_calc_save, ts, digest)
            futures_to_url[future] = (ts, digest)
        final_results = {}
        i = 0
        for future in as_completed(futures_to_url):
            cap = futures_to_url[future]
            simhash = future.result()
            if simhash:
                if cap[1] not in self.seen:
                    self.seen[cap[1]] = simhash
                final_results[cap[0]] = simhash
            if i % 10 == 0:
                self.update_state(
                    task_id=self.job_id, state='PENDING',
                    meta={'info': '%d out of %d captures have been processed.' % (i, total)}
                )
            i += 1
        if final_results:
            # batch write results to Redis
            try:
                urlkey = surt(self.url)
                pipe = self.redis_db.pipeline()
                for ts, simhash in final_results.items():
                    if simhash:
                        simhash_value = simhash.value
                        pipe.hset(urlkey, ts,
                                  base64.b64encode(simhash_value.to_bytes((simhash_value.bit_length() + 7) // 8)))
                pipe.expire(urlkey, self.simhash_expire)
                pipe.execute()
                pipe.reset()
            except RedisError as exc:
                self._log.error('cannot write simhashes to Redis for URL %s (%s)',
                                self.url, exc)

        time_ended = datetime.now()
        self._log.info('calculate simhash ended with duration: %d',
                       (time_ended - time_started).seconds)
        return {'duration': str((time_ended - time_started).seconds)}

    def save_to_redis(self, ts, data):
        try:
            urlkey = surt(self.url)
            self._log.info('save simhash to Redis for timestamp %s urlkey %s',
                           ts, urlkey)
            self.redis_db.hset(urlkey, ts,
                               base64.b64encode(str(data).encode('ascii')))
        except RedisError as exc:
            self._log.error('cannot save simhash to Redis (%s)', exc)
