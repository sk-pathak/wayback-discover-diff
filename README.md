# wayback-discover-diff

A Python 3 application running a web service that accepts HTTP GET requests and returns JSON:

- /request?url=http://{URL}&year={YEAR}

Run background task to calculate simhash for all captures of target URL in the specified year. **Right now this command only calculates the simhash for the first three captures of the year because it takes forever to download all the snapshots of a year.** 

  Return JSON {"simhash"}.

- /simhash?url=http://{URL}&timestamp={timestamp}
  
  Returns JSON {“simhash”: “XXXX”} if capture simhash can me calculated or None if it fails.
  
## Installing

Install and update using pip:
```Shell
pip install -U Flask
```

Using conda or another Python environment management system, select Python 3.6 to create a virtualenv and activate it:
```Shell
python -m venv venv
. venv/bin/activate
```

## Run
```
export FLASK_APP=flaskr
export FLASK_ENV=development
flask run
```

Open http://127.0.0.1:5000 in a browser.
