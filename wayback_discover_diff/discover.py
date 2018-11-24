import json
import concurrent.futures
import logging
from datetime import datetime
import cProfile
import struct
import base64
from itertools import groupby
import xxhash
from celery import Task
import urllib3
from redis import StrictRedis
from simhash import Simhash
from surt import surt
from bs4 import BeautifulSoup

# https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
urllib3.disable_warnings()


def extract_html_features(html):
    """Process HTML document and get key features as text. Steps:
    kill all script and style elements
    get lowercase text
    break into lines and remove leading and trailing space on each
    break multi-headlines into a line each
    drop blank lines
    return a dict with features and their weights
    """
    soup = BeautifulSoup(html, features='lxml')
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text().lower()
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
        self.http = urllib3.PoolManager(retries=urllib3.Retry(3, redirect=1))
        self.redis_db = StrictRedis.from_url(cfg['redis_uri'], decode_responses=True)
        self.thread_number = cfg['threads']
        self.snapshots_number = cfg['snapshots']['number_per_year']
        # Initialize logger
        self._log = logging.getLogger(__name__)

    def download_snapshot(self, snapshot, i):
        self._log.info('fetching snapshot %d out of %d', i+1, self.total)
        if i % 10 == 0:
            self.update_state(task_id=self.job_id, state='PENDING',
                              meta={'info': str(i) + ' captures have been processed'})
        response = self.http.request('GET', 'http://web.archive.org/web/' + snapshot + 'id_/' + self.url)
        self._log.info('calculating simhash for snapshot %d out of %d', i, self.total)
        return response.data.decode('utf-8', 'ignore')

    def start_profiling(self, snapshot, index):
        cProfile.runctx('self.get_calc_save(snapshot, index)',
                        globals=globals(), locals=locals(), filename='profile.prof')

    def get_calc_save(self, snapshot, index):
        response_data = self.download_snapshot(snapshot, index)
        data = extract_html_features(response_data)
        simhash = calculate_simhash(data, self.simhash_size)
        self.digest_dict[self.non_dup[snapshot]] = simhash
        self.save_to_redis(snapshot, simhash, index)

    def run(self, url, year):
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
                                meta={'info': 'Fetching timestamps of '
                                            + self.url + ' for year ' + year})
            wayback_url = 'http://web.archive.org/cdx/search/cdx?url=' + self.url + \
                            '&' + 'from=' + year + '&to=' + year + '&fl=timestamp,digest&output=json'
            if self.snapshots_number != -1:
                wayback_url += '&limit=' + str(self.snapshots_number)
            response = self.http.request('GET', wayback_url)
            self._log.info('finished fetching timestamps of %s for year %s', self.url, year)
            snapshots = json.loads(response.data.decode('utf-8'))

            if not snapshots:
                self._log.error('no snapshots found for this year and url combination')
                return {'status': 'error',
                        'info': 'no snapshots found for this year and url combination'}
            snapshots.pop(0)
            self.total = len(snapshots)
            self.job_id = self.request.id

            self.non_dup = {}
            self.dup = {}
            for elem in snapshots:
                if elem[1] in self.non_dup.values():
                    self.dup[elem[0]] = elem[1]
                else:
                    self.non_dup[elem[0]] = elem[1]
            with concurrent.futures.ThreadPoolExecutor(max_workers=
                                                       self.thread_number) as executor:
                # Start the load operations and mark each future with its URL
                # future_to_url = {executor.submit(self.start_profiling,
                #                                  snapshot, index):
                future_to_url = {executor.submit(self.get_calc_save,
                                                    snapshot, index):
                                        snapshot for index, snapshot in enumerate(self.non_dup)}
                for future in concurrent.futures.as_completed(future_to_url):
                    try:
                        future.result()
                    except Exception as exc:
                        self._log.error(exc)
                for elem in self.dup:
                    try:
                        self.save_to_redis(elem, self.digest_dict[self.dup[elem]], elem)
                    except KeyError:
                        self._log.info('Failed to fetch snapshot: %s', elem)
                self.redis_db.expire(surt(self.url), self.simhash_expire)
        except Exception as exc:
            self._log.error(exc)
            return {'status': 'error', 'info': exc}
        time_ended = datetime.now()
        self._log.info('calculate simhash ended with duration: %d',
                        (time_ended - time_started).seconds)
        return {'duration': str((time_ended - time_started).seconds)}

    def save_to_redis(self, snapshot, data, identifier):
        if isinstance(identifier, int):
            self._log.info('saving to redis simhash for snapshot %d out of %d', identifier, self.total)
        else:
            self._log.info('saving to redis simhash for snapshot %s', identifier)
        self.redis_db.hset(surt(self.url), snapshot, base64.b64encode(struct.pack('L', data)))
