from flask import (flash, jsonify)
import json
from simhash import Simhash
import redis
import urllib3


class Discover(object):

    @staticmethod
    def simhash(url, timestamp):
        error = None
        if not url:
            error = 'URL is required.'
        elif not timestamp:
            error = 'Timestamp is required.'
        else:
            redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
            results = redis_db.get(url + timestamp)
            if results is not None:
                simhash_result = {'simhash': results.decode('utf-8')}
            else:
                simhash_result = {'simhash': 'None'}
        flash(error)
        simhash_result = json.dumps(simhash_result)
        return simhash_result

    @staticmethod
    def request_url(simhash_size, url, year):
        error = None
        if not url:
            simhashes = 'URL is required.'
        elif not year:
            simhashes = 'Year is required.'
        else:
            http = urllib3.PoolManager()
            r = http.request('GET', 'https://web.archive.org/cdx/search/cdx?url=' + url + '&'
                                                                                          'from=' + year + '&to=' + year + '&fl=timestamp&output=json&output=json&limit=3')
            try:
                snapshots = json.loads(r.data.decode('utf-8'))
                if len(snapshots) == 0:
                    raise ValueError
                snapshots.pop(0)
                simhashes = []
                redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
                for snapshot in snapshots:
                    r = http.request('GET', 'https://web.archive.org/web/' + snapshot[0] + '/' + url)
                    temp_simhash = Simhash(r.data.decode('utf-8'), simhash_size).value
                    redis_db.set(url + snapshot[0], temp_simhash)
                    simhashes.append(temp_simhash)
            except (ValueError) as e:
                return json.dumps({'Message': 'Failed to fetch snapshots, please try again.'})

        return jsonify(simhashes)
