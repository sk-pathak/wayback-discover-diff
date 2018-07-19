import json
from simhash import Simhash
import redis
import urllib3
import datetime
from celery import Task
import logging
from celery.utils.log import get_task_logger

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
        self.redis_db = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

        #Initialize logger
        self._log = logging.getLogger(__name__)
        logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s',
                            filename=self.logfile, level=logging.INFO)
        #Initialize Task logger
        self._task_log = get_task_logger(__name__)

    def simhash(self, url, timestamp):
        if not url:
            self._log.error('did not give url parameter')
            return json.dumps({'error': 'URL is required.'})
        elif not timestamp:
            self._log.error('did not give timestamp parameter')
            return json.dumps({'error': 'Timestamp is required.'})
        else:
            self._log.info('requesting redis db entry for %s %s', url, timestamp)
            results = self.redis_db.hget(url, timestamp)
            if results:
                results = results.decode('utf-8')
                self._log.info('found entry %s', results)
                return json.dumps({'simhash': results})
            else:
                self._log.info('entry not found')
                return json.dumps({'simhash': 'None'})

    def run(self, url, year):
        time_started = datetime.datetime.now()
        self._task_log.info('calculate simhash started')
        if not url:
            self._task_log.error('did not give url parameter')
            result = {'status':'error', 'info': 'URL is required.'}
        elif not year:
            self._task_log.error('did not give year parameter')
            result = {'status':'error', 'info': 'Year is required.'}
        else:
            try:
                self._task_log.info('fetching timestamps of %s for year %s', url, year)
                self.update_state(state='PENDING',
                                  meta={'info': 'Fetching timestamps of ' + url + ' for year ' + year})
                r = self.http.request('GET', 'https://web.archive.org/cdx/search/cdx?url=' + url + '&'
                                          'from=' + year + '&to=' + year + '&fl=timestamp&output=json&output=json&limit=3')
                self._task_log.info('finished fetching timestamps of %s for year %s', url, year)
                snapshots = json.loads(r.data.decode('utf-8'))
                if len(snapshots) == 0:
                    self._task_log.error('no snapshots found for this year and url combination')
                    result = {'status':'error', 'info': 'no snapshots found for this year and url combination'}
                    return json.dumps(result, sort_keys=True)
                snapshots.pop(0)
                total = len(snapshots)
                for i, snapshot in enumerate(snapshots):
                    self._task_log.info('fetching snapshot %d out of %d', i, total)
                    self.update_state(state='PENDING',
                                      meta={'info': str(i) + ' out of ' + str(total) + ' captures have been processed',
                                            })

                    r = self.http.request('GET', 'https://web.archive.org/web/' + snapshot[0] + '/' + url)
                    self._task_log.info('calculating simhash for snapshot %d out of %d', i, total)
                    temp_simhash = Simhash(r.data.decode('utf-8'), self.simhash_size).value
                    self._task_log.info('saving to redis simhash for snapshot %d out of %d', i, total)
                    self.redis_db.hset(url, snapshot[0], temp_simhash)
            except Exception as e:
                self._task_log.error(e.args[0])
                result = {'status':'error', 'info': e.args[0]}
                return json.dumps(result, sort_keys=True)
            time_ended = datetime.datetime.now()
            result = {'duration': str((time_ended - time_started).seconds)}
            self._task_log.info('calculate simhash ended with duration: %d', (time_ended - time_started).seconds)
            return json.dumps(result, sort_keys=True)
        return json.dumps(result, sort_keys=True)

