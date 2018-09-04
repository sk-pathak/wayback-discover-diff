# wayback-discover-diff

A Python 3.4 application running a web service that accepts HTTP GET requests and returns JSON:

- /calculate-simhash?url={URL}&year={YEAR}

  Run background task to calculate simhash for all captures of target URL in the specified year.

  Return JSON {“status”: “started”, “job_id”: “XXYYZZ (uuid)”}

- /simhash?url={URL}&timestamp={timestamp}
  
  Returns JSON {“simhash”: “XXXX”} if capture simhash has already been calculated or None if it fails.
  
- /simhash?url={URL}&year={YEAR}
  
  Which returns all the timestamps for which a simhash value exists in the DB for that specific URL and year with the following       format : [["SIMHASH_VALUE", TIMESTAMP_VALUE"]]

  
- /job?job_id=<job_Id>
  
  Returns JSON {“status”: “pending”, “job_Id”: “XXYYZZ”, “info”: “X out of Y captures have been processed”} the status of the job matching that specific job id
  
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
Copy the conf.yml.example file to the same directory, removing the .example extension

```
cd wayback_discover_diff
cp conf.yml.example conf.yml
```
## Run
In order to run this server you should run :
```
bash run_gunicorn.sh
```

Open http://127.0.0.1:4000 in a browser.

## Tests
In order to run the tests call the script:
```
bash run_tests.sh
```
