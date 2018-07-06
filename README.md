# wayback-discover-diff

A Python 3.4 application running a web service that accepts HTTP GET requests and returns JSON:

- /request?url=http://{URL}&year={YEAR}

Run background task to calculate simhash for all captures of target URL in the specified year. **Right now this command only calculates the simhash for the first three captures of the year because it takes forever to download all the snapshots of a year.** 

  Return JSON {"simhash"}.

- /simhash?url=http://{URL}&timestamp={timestamp}
  
  Returns JSON {“simhash”: “XXXX”} if capture simhash can me calculated or None if it fails.
  
## Installing

Using conda or another Python environment management system, select Python 3.4 to create a virtualenv and activate it:
```Shell
python -m venv venv
. venv/bin/activate
```

Install and update using pip:
```Shell
python setup.py install
```

## Run
In order to run this server you should run two components:

### Flask
Inside the root project folder run

```
export FLASK_APP=wayback_discover_diff
export FLASK_ENV=development
export WAYBACK_DISCOVER_DIFF_CONF=wayback_discover_diff/conf.yml
flask run
```

### Celery
Inside the wayback_discover_diff folder located in the project's root folder run 

```
export WAYBACK_DISCOVER_DIFF_CONF=conf.yml
celery worker -A wayback_discover_diff.celery -l debug
```

Open http://127.0.0.1:5000 in a browser.
