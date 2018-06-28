import os
from flask import (Flask, flash, jsonify, request)
import urllib3
from simhash import Simhash

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure  the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/simhash')
    def simhash():
        url = request.args.get('url')
        timestamp = request.args.get('timestamp')
        error = None
        if not url:
            error = 'URL is required.'
        elif not timestamp:
            error = 'Timestamp is required.'
        else:
            http = urllib3.PoolManager()
            r = http.request('GET', 'https://web.archive.org/web/'+timestamp+'/'+url)
            if r.status == 200:
                simhash_result = {}
                simhash_result['simhash'] = Simhash(r.data.decode('utf-8')).value
            else:
                simhash_result = 'None'
        flash(error)

        return jsonify(simhash_result)

    @app.route('/request')
    def requestURL():
        url = request.args.get('url')
        year = request.args.get('year')
        error = None
        if not url:
            error = 'URL is required.'
        elif not year:
            error = 'Year is required.'
        else:
            http = urllib3.PoolManager()
            print('https://web.archive.org/cdx/search/cdx?url='+url+
                  '&from='+year+'&to='+year+'&fl=timestamp&output=json&output=json&limit=3')
            r = http.request('GET', 'https://web.archive.org/cdx/search/cdx?url='+url+'}&'
                                                                                      'from='+year+'&to='+year+'&fl=timestamp&output=json&output=json&limit=3')
            snapshots = jsonify(r.data.decode('utf-8'))
            snapshots.pop(0)
            simhashes = []
            for snapshot in snapshots:
                print(f'https://web.archive.org/web/{snapshot[0]}/{url}')
                r = http.request('GET', 'https://web.archive.org/web/'+snapshot[0]+'/'+url)
                simhashes.append(Simhash(r.data.decode('utf-8')).value)
        flash(error)

        return jsonify(simhashes)

    return app