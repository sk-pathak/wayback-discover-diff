"""Statsd methods to record statistics.
"""
import logging
import socket
from contextlib import contextmanager
from timeit import default_timer as time
import statsd


STATSD_CLIENT = statsd.StatsClient('localhost', 8125)


def configure(host, port):
    """Confiugure StatsD client.
    """
    global STATSD_CLIENT
    _logger = logging.getLogger(__name__)
    hostname = socket.getfqdn().split(".")[0]
    prefix = 'wb.changes.%s.' % hostname
    try:
        STATSD_CLIENT = statsd.StatsClient(host=host, port=port, prefix=prefix)
        _logger.info('configured statsd client %s %s %s', host, port, prefix)
    except socket.error as exc:
        _logger.error('cannot connect to statsd server %s %s %s (%s)', host,
                      port, prefix, str(exc))


def statsd_incr(metric, count=1):
    """Utility method to increment statsd metric.
    """
    STATSD_CLIENT.incr(metric, count)


def statsd_timing(metric, dt_sec):
    """Utility method to record statsd timing metric. Input is in sec (usually
    the difference between two times), we must convert to millisec.
    """
    STATSD_CLIENT.timing(metric, int(dt_sec * 1000))


@contextmanager
def timing(metric):
    t0 = time()
    try:
        yield self
    finally:
        STATSD_CLIENT.timing(metric, int((time() - t0) * 1000))
