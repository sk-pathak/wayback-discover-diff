from flask import (flash, jsonify)
import urllib3
import json
from simhash import Simhash
import redis

class Discover(object):

    @staticmethod
    def simhash(request):
        url = request.args.get('url')
        timestamp = request.args.get('timestamp')
        error = None
        if not url:
            error = 'URL is required.'
        elif not timestamp:
            error = 'Timestamp is required.'
        else:
            http = urllib3.PoolManager()
            r = http.request('GET', 'https://web.archive.org/web/' + timestamp + '/' + url)
            if r.status == 200:
                simhash_result = {}
                simhash_result['simhash'] = Simhash(r.data.decode('utf-8')).value
                simhash_result = json.dumps(simhash_result)
            else:
                simhash_result = 'None'
        flash(error)
        return simhash_result

    @staticmethod
    def request_url(request):
        url = request.args.get('url')
        year = request.args.get('year')
        error = None
        if not url:
            error = 'URL is required.'
        elif not year:
            error = 'Year is required.'
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
                # create a connection to the localhost Redis server instance, by
                # default it runs on port 6379
                redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
                for snapshot in snapshots:
                    r = http.request('GET', 'https://web.archive.org/web/' + snapshot[0] + '/' + url)
                    temp_simhash = Simhash(r.data.decode('utf-8')).value
                    redis_db.set(url+snapshot[0], temp_simhash)
                    simhashes.append(temp_simhash)
            except (ValueError) as e:
                return json.dumps({'Message': 'Failed to fetch snapshots, please try again.'})
        flash(error)

        return jsonify(simhashes)
