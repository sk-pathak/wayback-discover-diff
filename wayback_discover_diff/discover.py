import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import string
from datetime import datetime
import cProfile
import struct
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
        script.extract()
    text = soup.get_text().lower()
    text = text.translate(TRANSLATOR)
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    text = text.split()
    return {k: sum(1 for _ in g) for k, g in groupby(sorted(text))}


def hash_function(x):
    """Custom FAST hash function used to generate simhash.
    """
    return int(xxhash.xxh64(x).hexdigest(), 16)


def calculate_simhash(features_dict, simhash_size):
    """Calculate simhash for features in a dict. `features_dict` contains data
    like {'text': weight}
    """
    return Simhash(features_dict, simhash_size, hashfunc=hash_function).value


class Discover(Task):
    """Custom Celery Task class.
    http://docs.celeryproject.org/en/latest/userguide/tasks.html#custom-task-classes
    """
    name = 'Discover'
    task_id = None

    def __init__(self, cfg):
        self.simhash_size = cfg['simhash']['size']
        self.simhash_expire = cfg['simhash']['expire_after']
        self.http = urllib3.HTTPConnectionPool('web.archive.org', maxsize=100,
                                               retries=urllib3.Retry(3, redirect=1))
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

    def download_snapshot(self, snapshot, i):
        """Download capture from WBM and update job status.
        """
        try:
            self._log.info('fetching snapshot %d out of %d', i+1, self.total)
            if i % 10 == 0:
                self.update_state(task_id=self.job_id, state='PENDING',
                                  meta={'info': '%d out of %d captures have been processed.' % (i, self.total)})
            resp = self.http.request('GET', '/web/%sid_/%s' % (snapshot, self.url))
            return resp.data.decode('utf-8', 'ignore')
        except HTTPError as exc:
            self._log.error('cannot fetch capture %s %s (%s)', snapshot,
                            self.url, exc)

        except RedisError as exc:
            self._log.error('cannot update job status (%s)', exc)

    def start_profiling(self, snapshot, index):
        cProfile.runctx('self.get_calc_save(snapshot, index)',
                        globals=globals(), locals=locals(), filename='profile.prof')

    def get_calc_save(self, snapshot, index):
        """Download capture, extract HTML features, calculate simhash and save
        to Redis.
        """
        response_data = self.download_snapshot(snapshot, index)
        if response_data:
            data = extract_html_features(response_data)
            simhash = calculate_simhash(data, self.simhash_size)
            self.digest_dict[self.non_dup[snapshot]] = simhash
            self.save_to_redis(snapshot, simhash, index)

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

        self.digest_dict = {}

        try:
            self._log.info('fetching timestamps of %s for year %s', self.url, year)
            self.update_state(state='PENDING',
                              meta={'info': 'Fetching %s timestamps for year %s' % (
                                    self.url, year)})
            # Collapse captures by timestamp to get 1 capture per hour.
            # Its necessary to reduce the huge number of captures some websites
            # (e.g. twitter.com has 167k captures for 2018.
            cdx_url = '/cdx/search/cdx?url=%s&from=%s&to=%s&fl=timestamp,digest&collapse=timestamp:10&output=json' % (
                self.url, year, year)
            if self.snapshots_number != -1:
                cdx_url += '&limit=%d' % self.snapshots_number
            response = self.http.request('GET', cdx_url)
            self._log.info('finished fetching timestamps of %s for year %s',
                           self.url, year)
            snapshots = json.loads(response.data.decode('utf-8'))
            if not snapshots:
                self._log.error('no snapshots found for this year and url combination')
                self.redis_db.hset(surt(self.url), year, -1)
                self.redis_db.expire(surt(self.url), self.simhash_expire)
                return {'status': 'error',
                        'info': 'no snapshots found for this year and url combination'}
        except HttpError as exc:
            self._log.error('did not get a valid CDX server response for %s (%s)',
                            cdx_url, exc)
        except ValueError as exc:
            self._log.error('invalid CDX query response %s (%s)', cdx_url, exc)
            return {'status': 'error', 'info': exc}
        except RedisError as exc:
            self._log.error('error connecting with Redis for url %s year %s (%s)',
                            url, year, exc)
            return {'status': 'error', 'info': exc}
        snapshots.pop(0)
        self.total = len(snapshots)

        self.non_dup = {}
        self.dup = {}
        for elem in snapshots:
            if elem[1] in self.non_dup.values():
                self.dup[elem[0]] = elem[1]
            else:
                self.non_dup[elem[0]] = elem[1]

            # Start the load operations and mark each future with its URL
            # future_to_url = {self.tpool.submit(self.start_profiling,
            #                                    snapshot, index):
            future_to_url = {self.tpool.submit(self.get_calc_save,
                                               snapshot, index):
                             snapshot for index, snapshot in enumerate(self.non_dup)}
            for future in as_completed(future_to_url):
                future.result()
            for elem in self.dup:
                try:
                    self.save_to_redis(elem, self.digest_dict[self.dup[elem]], elem)
                except KeyError:
                    self._log.info('Failed to fetch snapshot: %s', elem)
            self.redis_db.expire(surt(self.url), self.simhash_expire)
        time_ended = datetime.now()
        self._log.info('calculate simhash ended with duration: %d',
                       (time_ended - time_started).seconds)
        return {'duration': str((time_ended - time_started).seconds)}

    def save_to_redis(self, snapshot, data, identifier):
        try:
            self._log.info('saving to redis simhash for snapshot %s out of %d',
                           str(identifier), self.total)
            self.redis_db.hset(surt(self.url), snapshot,
                               base64.b64encode(struct.pack('L', data)))
        except RedisError as exc:
            self._log.error('cannot save simhash to Redis (%s)', exc)
