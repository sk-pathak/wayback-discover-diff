import sys
from setuptools import setup, find_packages


if sys.version_info < (3, 10):
    raise RuntimeError("Python version is {}. Requires 3.10 or greater."
                       "".format(sys.version_info))


setup(
    name='wayback-discover-diff',
    version='0.1.9.6',
    description='Calculate wayback machine captures simhash',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        'Werkzeug<3',
        'Flask>=2.1.3,<2.2.0',
        'simhash>=2.1.2',
        'urllib3==1.26.16',
        'PyYAML>=6',
        # required for Celery
        'celery==5.4.0',
        'kombu>=5.3.4,<6.0',
        'redis==4.6.0',

        'hiredis',
        'flask-cors',
        'selectolax>=0.3.21',
        'statsd',
        'surt'
        ],
    tests_require=[
        'pytest',
        'mock'
        ],
    )
