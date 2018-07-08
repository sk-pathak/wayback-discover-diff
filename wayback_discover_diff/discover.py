from flask import (flash, jsonify)
import json
from simhash import Simhash
import redis
import urllib3
import datetime
from celery import Task

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
        # TODO Do we also want to set the max number of redirects?
        self.http = urllib3.PoolManager(retries=urllib3.Retry(3))
        redis_host = cfg['redis']['host']
        redis_port = cfg['redis']['port']
        redis_db = cfg['redis']['db']
        self.redis_db = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

    def simhash(self, url, timestamp):
        if not url:
            return json.dumps({'error': 'URL is required.'})
        elif not timestamp:
            return json.dumps({'error': 'Timestamp is required.'})
        else:
            results = self.redis_db.get(url + timestamp)
            if results:
                return json.dumps({'simhash': results.decode('utf-8')})
            else:
                return json.dumps({'simhash': 'None'})

    def run(self, url, year):
        time_started = datetime.datetime.now()
        if not url:
            result = 'URL is required.'
        elif not year:
            result = 'Year is required.'
        else:
            # TODO this must be inside the try/catch and HTTP exceptions must
            # be handled.
            r = self.http.request('GET', 'https://web.archive.org/cdx/search/cdx?url=' + url + '&'
                                          'from=' + year + '&to=' + year + '&fl=timestamp&output=json&output=json&limit=30')
            try:
                snapshots = json.loads(r.data.decode('utf-8'))
                total = len(snapshots)
                if total == 0:
                    raise ValueError
                snapshots.pop(0)
                for i, snapshot in enumerate(snapshots):
                    self.update_state(state='PENDING',
                                      meta={'job_id': self.request.id,
                                            'info': str(i) + ' out of ' + str(total) + ' captures have been processed',
                                            })

                    r = self.http.request('GET', 'https://web.archive.org/web/' + snapshot[0] + '/' + url)
                    temp_simhash = Simhash(r.data.decode('utf-8'), self.simhash_size).value
                    self.redis_db.set(url + snapshot[0], temp_simhash)
            except ValueError as e:
                return json.dumps({'Message': 'Failed to fetch snapshots, please try again.'})
            time_ended = datetime.datetime.now()
            result = {'job_id': str(self.request.id), 'duration': str((time_ended - time_started).seconds)}
            return result
        return json.dumps(result)
