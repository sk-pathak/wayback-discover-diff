from simhash import Simhash
from flask import (
    Blueprint, flash, jsonify, request
)
import urllib3


bp = Blueprint('simhash', __name__, url_prefix='/simhash')


@bp.route('/', methods=('GET', 'POST'))
def register():
    url = request.args.get('url')
    timestamp = request.args.get('timestamp')
    error = None
    if not url:
        error = 'URL is required.'
    elif not timestamp:
        error = 'Timestamp is required.'
    else:
        http = urllib3.PoolManager()
        r = http.request('GET', f'https://web.archive.org/web/{timestamp}/{url}')
        if r.status == 200:
            simhash_result = {}
            simhash_result['simhash'] = Simhash(r.data.decode('utf-8')).value
        else:
            simhash_result = 'None'
    flash(error)

    return jsonify(simhash_result)
