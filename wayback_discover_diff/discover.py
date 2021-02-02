from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import logging
import string
from datetime import datetime
import cProfile
import base64
from itertools import groupby
from celery import Task
import urllib3
from urllib3.exceptions import HTTPError
from redis import StrictRedis, BlockingConnectionPool
from redis.exceptions import RedisError
from simhash import Simhash
from surt import surt
from selectolax.parser import HTMLParser
from werkzeug.urls import url_fix

from .stats import statsd_incr

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
    try:
        tree = HTMLParser(html)
        tree.strip_tags(['script', 'style'])
        text = tree.root.text(separator=' ')
        if not text:
            return {}
    except UnicodeDecodeError:
        return {}
    text = text.lower().translate(TRANSLATOR)
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return {k: sum(1 for _ in g) for k, g in groupby(sorted(text.split()))}

# This custom hash function generated ALWAYS the same simhash size despite
# changing simhash size setting. Using the default, we get the right simhashes.
#
# import xxhash
#
# def hash_function(x):
#     """Custom FAST hash function used to generate simhash.
#     """
#     return int(xxhash.xxh64(x).hexdigest(), 16)


def custom_hash_function(x):
    return int.from_bytes(hashlib.blake2b(x).digest(), byteorder='big')


def calculate_simhash(features_dict, simhash_size, hashfunc=None):
    """Calculate simhash for features in a dict. `features_dict` contains data
    like {'text': weight}
    """
    return Simhash(features_dict, simhash_size, hashfunc=hashfunc).value


def pack_simhash_to_bytes(simhash, simhash_size=None):
    # simhash_value = simhash.value
    if simhash_size is None:
        size_in_bytes = (simhash.bit_length() + 7) // 8
    else:
        size_in_bytes = simhash_size // 8
    return simhash.to_bytes(size_in_bytes, byteorder='little')


class Discover(Task):
    """Custom Celery Task class.
    http://docs.celeryproject.org/en/latest/userguide/tasks.html#custom-task-classes
    """
    name = 'Discover'
    task_id = None
    # If a simhash calculation for a URL & year does more than
    # `max_download_errors`, stop it to avoid pointless requests. The captures
    # are not text/html or there is a problem with the WBM.
    max_download_errors = 10
    max_capture_download = 1000000

    def __init__(self, cfg):
        self.simhash_size = cfg['simhash']['size']
        self.simhash_expire = cfg['simhash']['expire_after']
        if self.simhash_size > 512:
            raise Exception('do not support simhash longer than 512')

        headers = {'User-Agent': 'wayback-discover-diff',
                   'Accept-Encoding': 'gzip,deflate',
                   'Connection': 'keep-alive'}
        cdx_auth_token = cfg.get('cdx_auth_token')
        if cdx_auth_token:
            headers['cookie'] = 'cdx_auth_token=%s' % cdx_auth_token

        self.http = urllib3.HTTPConnectionPool('web.archive.org', maxsize=50,
                                               retries=4, headers=headers)
        self.redis_db = StrictRedis(
            connection_pool=BlockingConnectionPool.from_url(
                cfg['redis_uri'], max_connections=50,
                timeout=cfg.get('redis_timeout', 10),
                decode_responses=True
                )
            )
        self.tpool = ThreadPoolExecutor(max_workers=cfg['threads'])
        self.snapshots_number = cfg['snapshots']['number_per_year']
        self.download_errors = 0
        # Initialize logger
        self._log = logging.getLogger('wayback_discover_diff.worker')

    def download_capture(self, ts):
        """Download capture data from the WBM and update job status. Return
        data only when its text or html. If not, increment download_errors
        which will stop the task after 10 errors. Fetch data up to a limit
        to avoid getting too much (which is unnecessary) and have a consistent
        operation time.
        """
        try:
            statsd_incr('download-capture')
            self._log.info('fetching capture %s %s', ts, self.url)
            res = self.http.request('GET', '/web/%sid_/%s' % (ts, self.url),
                                    preload_content=False)
            data = res.read(self.max_capture_download)
            ctype = res.headers.get('content-type')
            res.release_conn()
            if ctype:
                ctype = ctype.lower()
                if "text" in ctype or "html" in ctype:
                    return data
        except HTTPError as exc:
            self._log.warning('cannot fetch capture %s %s (%s)', ts, self.url,
                              str(exc))
        self.download_errors += 1
        return None

    def start_profiling(self, snapshot, index):
        cProfile.runctx('self.get_calc(snapshot, index)',
                        globals=globals(), locals=locals(),
                        filename='profile.prof')

    def get_calc(self, timestamp, digest):
        """if a capture with an equal digest has been already processed,
        return cached simhash and avoid redownloading and processing. Else,
        download capture, extract HTML features and calculate simhash.
        If there are already too many download failures, return None without
        any processing to avoid pointless requests.
        Return None if any problem occurs (e.g. HTTP error or cannot calculate)
        """
        if digest in self.seen:
            self._log.info("already seen %s %s" % (digest, self.seen[digest]))
            return self.seen[digest]

        if self.download_errors >= self.max_download_errors:
            self._log.error('Too many download errors, exiting')
            return None

        response_data = self.download_capture(timestamp)
        if response_data:
            data = extract_html_features(response_data)
            if data:
                statsd_incr('calculate-simhash')
                self._log.info("calculating simhash")
                return calculate_simhash(data, self.simhash_size, hashfunc=custom_hash_function)
        return None

    def run(self, url, year, created):
        """Run Celery Task.
        """
        self.job_id = self.request.id
        self.url = url_fix(url)
        time_started = datetime.now()
        self._log.info('Start calculating simhashes.')
        self.download_errors = 0
        if not self.url:
            self._log.error('did not give url parameter')
            return {'status': 'error', 'info': 'URL is required.'}
        if not year:
            self._log.error('did not give year parameter')
            return {'status': 'error', 'info': 'Year is required.'}
        urlkey = surt(self.url)
        try:
            self._log.info('fetching CDX of %s for year %s', self.url, year)
            self.update_state(state='PENDING',
                              meta={'info': 'Fetching %s captures for year %s' % (
                                    self.url, year)})
            # Collapse captures by timestamp to get 3 captures per day (max).
            # TODO increase that in the future when we can handle more captures.
            # Its necessary to reduce the huge number of captures some websites
            # (e.g. twitter.com has 167k captures for 2018. Get only 2xx captures.
            fields = {'url': self.url, 'from': year, 'to': year,
                      'statuscode': 200, 'fl': 'timestamp,digest',
                      'collapse': 'timestamp:9'}
            if self.snapshots_number != -1:
                fields['limit'] = self.snapshots_number
            response = self.http.request('GET', '/web/timemap', fields=fields)
            self._log.info('finished fetching timestamps of %s for year %s',
                           self.url, year)
            if response.status == 200:
                if not response.data:
                    self._log.info('no captures found for %s %s', self.url, year)
                    self.redis_db.hset(urlkey, year, -1)
                    self.redis_db.expire(urlkey, self.simhash_expire)
                    return {'status': 'error',
                            'info': 'no captures found for this year and url combination'}
                captures_txt = response.data.decode('utf-8')
        except (ValueError, HTTPError) as exc:
            self._log.error('invalid CDX query response (%s)', exc)
            return {'status': 'error', 'info': str(exc)}
        except RedisError as exc:
            self._log.error('error connecting with Redis for url %s year %s (%s)',
                            url, year, str(exc))
            return {'status': 'error', 'info': str(exc)}
        captures = captures_txt.strip().split("\n")
        total = len(captures)
        self.seen = dict()
        futures_to_url = {}
        # calculate simhashes in parallel
        for cap in captures:
            if self.download_errors >= self.max_download_errors:
                self._log.error('Too many download errors, exiting, %s %s',
                                url, year)
                return None
            (ts, digest) = cap.split(' ')
            future = self.tpool.submit(self.get_calc, ts, digest)
            futures_to_url[future] = (ts, digest)
        final_results = {}
        i = 0
        for future in as_completed(futures_to_url):
            cap = futures_to_url[future]
            simhash = future.result()
            if simhash:
                if cap[1] not in self.seen:
                    self.seen[cap[1]] = simhash
                # This encoding is necessary to store simhash data in Redis.
                final_results[cap[0]] = base64.b64encode(
                    pack_simhash_to_bytes(simhash, self.simhash_size)
                    )
            if i % 10 == 0:
                self.update_state(
                    task_id=self.job_id, state='PENDING',
                    meta={'info': 'Processed %d out of %d captures.' % (i, total)}
                )
            i += 1
        self._log.info('%d final results for %s and year %s.',
                       len(final_results), self.url, year)
        if final_results:
            try:
                self.redis_db.hmset(urlkey, final_results)
                self.redis_db.expire(urlkey, self.simhash_expire)
            except RedisError as exc:
                self._log.error('cannot write simhashes to Redis for URL %s (%s)',
                                self.url, str(exc))

        duration = (datetime.now() - time_started).seconds
        self._log.info('Simhash calculation finished in %.2fsec.', duration)
        return {'duration': str(duration)}
