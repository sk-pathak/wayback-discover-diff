import sys
from setuptools import setup, find_packages


if sys.version_info < (3, 6):
    raise RuntimeError("Python version is {}. Requires 3.6 or greater."
                       "".format(sys.version_info))


setup(
    name='wayback-discover-diff',
    version='0.1.8.0',
    description='Calculate wayback machine captures simhash',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        'Flask>=2.0.2',
        'simhash>=2.1.1',
        'urllib3>=1.25.6',
        'PyYAML>=5.3.1,<6',
        # required for Celery
        'celery==5.2.3',
        'kombu>=5.2.3,<6.0',
        'redis==4.1.0',

        'hiredis',
        'flask-cors',
        'selectolax',
        'statsd',
        'surt'
        ],
    tests_require=[
        'pytest',
        'mock'
        ],
    )
