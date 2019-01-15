"""SPN Utility methods.
"""
import logging
from math import ceil
import os
import re
from urllib.parse import urlparse
import yaml
from redis.exceptions import RedisError
from surt import surt
import tldextract


def load_config():
    """Load conf file defined by ENV var WAYBACK_DISCOVER_DIFF_CONF.
    If not available load ./conf.yaml
    """
    config = {}
    try:
        cfg_file = os.environ.get('WAYBACK_DISCOVER_DIFF_CONF')
        if not cfg_file:
            cfg_file = os.getcwd() + '/conf.yml'
            logging.warning('using default configuration from %s', cfg_file)
        with open(cfg_file, 'rt') as cfg:
            config = yaml.load(cfg)
            logging.debug('config=%s', config)
    except IOError:
        logging.error('Error loading configuration', exc_info=1)
    return config


def timestamp_simhash(redis_db, url, timestamp):
    """Get stored simhash data from Redis for URL and timestamp
    """
    try:
        if url and timestamp:
            results = redis_db.hget(surt(url), timestamp)
            if results:
                return {'simhash': results}
            results = redis_db.hget(surt(url), timestamp[:4])
            if results:
                return {'status': 'error', 'message': 'NO_CAPTURES'}
            return {'status': 'error', 'message': 'CAPTURE_NOT_FOUND'}
    except RedisError as exc:
        logging.error('error loading simhash data for url %s timestamp %s (%s)',
                      url, timestamp, exc)


def year_simhash(redis_db, url, year, page=None, snapshots_per_page=None):
    """Get stored simhash data for url, year and page (optional).
    """
    try:
        if url and year:
            results = redis_db.hkeys(surt(url))
            if results:
                timestamps_to_fetch = []
                for timestamp in results:
                    if timestamp == str(year):
                        return {'status': 'error', 'message': 'NO_CAPTURES'}
                    if timestamp[:4] == str(year):
                        timestamps_to_fetch.append(timestamp)
                if timestamps_to_fetch:
                    return handle_results(redis_db, timestamps_to_fetch, url,
                                          snapshots_per_page, page)
            return {'status': 'error', 'message': 'NOT_CAPTURED'}
    except RedisError as exc:
        logging.error('error loading simhash data for url %s year %s page %d (%s)',
                      url, year, page, exc)


def handle_results(redis_db, timestamps_to_fetch, url, snapshots_per_page,
                   page=None):
    """Utility method used by `year_simhash`
    """
    available_simhashes = [["total number of captures", len(timestamps_to_fetch)]]
    if page:
        number_of_pages = ceil(len(timestamps_to_fetch) / snapshots_per_page)
        if page > number_of_pages:
            page = number_of_pages
        if number_of_pages > 0:
            timestamps_to_fetch = \
                timestamps_to_fetch[(page - 1) * snapshots_per_page:(page * snapshots_per_page)]
        else:
            number_of_pages = 1
    try:
        results = redis_db.hmget(surt(url), timestamps_to_fetch)
        for i, simhash in enumerate(results):
            available_simhashes.append([str(timestamps_to_fetch[i]), simhash])
        if page:
            available_simhashes.insert(0, ["pages", number_of_pages])
        return available_simhashes
    except RedisError as exc:
        logging.error('cannot handle results for url %s page %d (%s)',
                      url, page, exc)


EMAIL_RE = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")


def url_is_valid(url):
    try:
        if not url:
            return False
        if EMAIL_RE.match(url):
            return False
        ext = tldextract.extract(url)
        return ext.domain != '' and ext.suffix != ''
    except (ValueError, AttributeError):
        return False
