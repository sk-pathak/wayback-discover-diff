import sys
from setuptools import setup, find_packages


if sys.version_info < (3, 8):
    raise RuntimeError("Python version is {}. Requires 3.8 or greater."
                       "".format(sys.version_info))


setup(
    name='wayback-discover-diff',
    version='0.1.9.3',
    description='Calculate wayback machine captures simhash',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        'Flask>=2.2.2',
        'simhash>=2.1.2',
        'urllib3>=1.25.9',
        'PyYAML>=5.4.1,<6.0',
        # required for Celery
        'celery==5.2.7',
        'kombu>=5.2.3,<6.0',
        'redis==4.4.2',

        'hiredis',
        'flask-cors',
        'selectolax>=0.3.11',
        'statsd',
        'surt'
        ],
    tests_require=[
        'pytest',
        'mock'
        ],
    )
