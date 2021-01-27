import sys
from setuptools import setup, find_packages


if sys.version_info < (3, 6):
    raise RuntimeError("Python version is {}. Requires 3.6 or greater."
                       "".format(sys.version_info))


setup(
    name='wayback-discover-diff',
    version='0.1.7.2',
    description='Calculate wayback machine captures simhash',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        'Flask>=1.1.2',
        'simhash',
        'urllib3>=1.25.6',
        'PyYAML>=5.1',
        # required for Celery
        'celery[redis]==4.4.6',
        'kombu==4.6.11',
        'redis>=3.4.1',

        'flask-cors',
        'selectolax',
        'surt'
        ],
    tests_require=[
        'pytest',
        'mock',
        'pylint'
        ],
    )
