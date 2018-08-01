import json
import concurrent.futures
import logging
import datetime
import cProfile
from itertools import groupby
from bs4 import BeautifulSoup
import xxhash
from celery.utils.log import get_task_logger
from celery import Task
import urllib3
import redis
from simhash import Simhash

# https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
urllib3.disable_warnings()


class Discover(Task):
    """Custom Celery Task class.
    http://docs.celeryproject.org/en/latest/userguide/tasks.html#custom-task-classes
    """
    name = 'Discover'
    task_id = None

    def __init__(self, cfg):
        self.simhash_size = cfg['simhash']['size']
        self.http = urllib3.PoolManager(retries=urllib3.Retry(3, redirect=1))
        redis_host = cfg['redis']['host']
        redis_port = cfg['redis']['port']
        redis_db = cfg['redis']['db']
        self.logfile = cfg['logfile']
        self.thread_number = cfg['threads']
        self.snapshots_number = cfg['snapshots']['number_per_year']
        self.redis_db = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

        # Initialize logger
        self._log = logging.getLogger(__name__)
        logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s',
                            filename=self.logfile, level=logging.INFO)
        # Initialize Task logger
        self._task_log = get_task_logger(__name__)

    def timestamp_simhash(self, url, timestamp):
        if not url:
            self._log.error('did not give url parameter')
            return json.dumps({'error': 'URL is required.'})
        if not timestamp:
            self._log.error('did not give timestamp parameter')
            return json.dumps({'error': 'Timestamp is required.'})
        self._log.info('requesting redis db entry for %s %s', url, timestamp)
        results = self.redis_db.hget(url, timestamp)
        if results:
            results = results.decode('utf-8')
            self._log.info('found entry %s', results)
            return json.dumps({'simhash': results})
        self._log.info('entry not found')
        return json.dumps({'simhash': 'None'})

    def year_simhash(self, url, year):
        if not url:
            self._log.error('did not give url parameter')
            return json.dumps({'error': 'URL is required.'})
        if not year:
            self._log.error('did not give year parameter')
            return json.dumps({'error': 'Year is required.'})
        self._log.info('requesting redis db entry for %s %s', url, year)
        results = self.redis_db.hkeys(url)
        if results:
            available_simhashes = []
            for timestamp in results:
                timestamp = timestamp.decode('UTF-8')
                timestamp_year = timestamp[:4]
                if timestamp_year == str(year):
                    simhash = self.redis_db.hget(url, timestamp).decode('utf-8')
                    available_simhashes.append({'timestamp': timestamp, 'simhash': simhash})
                self._log.info('found entry %s', timestamp)
            return json.dumps(available_simhashes)
        self._log.info('No simhases for this URL and Year')
        return json.dumps({'simhash': 'None'})

    def download_snapshot(self, snapshot, url, i, total):
        self._task_log.info('fetching snapshot %d out of %d', i, total)
        # rdb.set_trace()
        # self.update_state(task_id= self.request.id, state='PENDING',
        # meta={'info': ' captures have been processed'})
        response = self.http.request('GET', 'http://web.archive.org/web/' + snapshot[0] + '/' + url)
        self._task_log.info('calculating simhash for snapshot %d out of %d', i, total)
        return response

    def start_simhash_import(self, snapshot,
                             url, i, total):
        cProfile.runctx('self.get_calc_save(snapshot, url, i, total)',
                        globals=globals(), locals=locals(), filename='profile.prof')

    def get_calc_save(self, snapshot, url, i, total):
        data = self.download_snapshot(snapshot, url, i, total)
        data = self.calc_features(data)
        simhash = self.calculate_simhash(data)
        self.save_to_redis(url, snapshot, simhash, total)

    def calc_features(self, response):
        soup = BeautifulSoup(response.data.decode('utf-8', 'ignore'))

        # kill all script and style elements
        for script in soup(["script", "style"]):
            script.extract()  # rip it out

        # get text
        text = soup.get_text()
        # turn all characters to lowercase
        text = text.lower()
        # break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        # break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)

        text = text.split()

        text = {k: sum(1 for _ in g) for k, g in groupby(sorted(text))}

        return text

    def calculate_simhash(self, text):
        temp_simhash = Simhash(text, self.simhash_size, hashfunc=hash_function).value
        self._task_log.info(temp_simhash)
        return temp_simhash

    def run(self, url, year):
        # rdb.set_trace()
        time_started = datetime.datetime.now()
        self._task_log.info('calculate simhash started')
        if not url:
            self._task_log.error('did not give url parameter')
            result = {'status': 'error', 'info': 'URL is required.'}
        elif not year:
            self._task_log.error('did not give year parameter')
            result = {'status': 'error', 'info': 'Year is required.'}
        else:
            try:
                self._task_log.info('fetching timestamps of %s for year %s', url, year)
                self.update_state(state='PENDING',
                                  meta={'info': 'Fetching timestamps of '
                                                + url + ' for year ' + year})
                wayback_url = 'http://web.archive.org/cdx/search/cdx?url=' + url + \
                              '&' + 'from=' + year + '&to=' + year + '&fl=timestamp&output=json'
                if self.snapshots_number != -1:
                    wayback_url += '&limit=' + str(self.snapshots_number)
                response = self.http.request('GET', wayback_url)
                self._task_log.info('finished fetching timestamps of %s for year %s', url, year)
                snapshots = json.loads(response.data.decode('utf-8'))

                if snapshots:
                    self._task_log.error('no snapshots found for this year and url combination')
                    result = {'status': 'error',
                              'info': 'no snapshots found for this year and url combination'}
                    return json.dumps(result, sort_keys=True)
                snapshots.pop(0)
                total = len(snapshots)
                with concurrent.futures.ThreadPoolExecutor(max_workers=
                                                           self.thread_number) as executor:
                    # Start the load operations and mark each future with its URL
                    # TODO: Fix snapshot number
                    future_to_url = {executor.submit(self.start_simhash_import,
                                                     snapshot, url, 1, total):
                                         snapshot for snapshot in snapshots}
                    for future in concurrent.futures.as_completed(future_to_url):
                        try:
                            future.result()
                        except Exception as exc:
                            self._task_log.error(exc)
            except Exception as exc:
                self._task_log.error(exc.args[0])
                result = {'status': 'error', 'info': exc.args[0]}
                return json.dumps(result, sort_keys=True)
            time_ended = datetime.datetime.now()
            result = {'duration': str((time_ended - time_started).seconds)}
            self._task_log.info('calculate simhash ended with duration: %d',
                                (time_ended - time_started).seconds)
            return json.dumps(result, sort_keys=True)
        return json.dumps(result, sort_keys=True)

    def save_to_redis(self, url, snapshot, data, total):
        # TODO: Fix snapshot number
        self._task_log.info('saving to redis simhash for snapshot %d out of %d', 1, total)
        self.redis_db.hset(url, snapshot[0], data)


def hash_function(x):
    return int(xxhash.xxh64(x).hexdigest(), 16)
