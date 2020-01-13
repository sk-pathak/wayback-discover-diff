from setuptools import setup, find_packages

setup(
    name='wayback-discover-diff',
    version='0.1.6.16',
    description='Calculate wayback machine captures simhash',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        'Flask>=1.1.1',
        'simhash',
        'urllib3>=1.25.6',
        'PyYAML>=5.1',
        # required for Celery 4.4.0
        'celery[redis]==4.4.0',
        'kombu>=4.6.7,<4.7',
        'redis>=3.3.0',

        'lxml',
        'beautifulsoup4>=4.8.2',
        'flask-cors',
        'surt'
        ],
    tests_require=[
        'pytest',
        'mock',
        'pylint'
        ],
    )
