# wayback-discover-diff

A Python 3.4 application running a web service that accepts HTTP GET requests and returns JSON:

- /calculate-simhash?url={URL}&year={YEAR}

  Checks if there is a task to calculate simhash for all captures of target URL in the specified year already running.
  If there isn't, it creates it.
  
  Return JSON {“status”: “started”, “job_id”: “XXYYZZ (uuid)”}
 
  **OR**
  
  If there is a task already running it returns its job_id.
  
  Return JSON {“status”: “PENDING”, “job_id”: “XXYYZZ (uuid)”}
 
- /simhash?url={URL}&timestamp={timestamp}
  
  Returns JSON {“simhash”: “XXXX”} if that capture's simhash value has already been calculated
  
  **OR**
  
  Returns JSON {"message": "NO_CAPTURES", "status": "error"} if the WBM has no captures for this year and URL combination.
  
  **OR**
  
  Returns JSON { "message": "CAPTURE_NOT_FOUND", "status": "error" } if the timestamp does not exist.

  
- /simhash?url={URL}&year={YEAR}
  
  Which returns all the timestamps for which a simhash value exists in the DB for that specific URL and year with the following       format : [["TIMESTAMP_VALUE", "SIMHASH_VALUE"]]

  Returns JSON { captures	[…], job_status	"COMPLETE" } if there are simhash values in the DB and that job is completed.

  **OR**

  Returns JSON { captures	[…], job_status	"PENDING" } if there are simhash values in the DB but that job is still pending.

  **OR**

  Returns JSON {'status': 'error', 'message': 'NOT_CAPTURED'} if that URL and year combination hasn't been hashed yet.

  **OR**

  Returns JSON {'status': 'error', 'message': 'NO_CAPTURES'} if the WBM doesn't have snapshots for that year and URL.

  - /simhash?url={URL}&year={YEAR}&page={PAGE_NUMBER}
  
  Which is the same as the request above but, depending on the page size that is set in the conf.yml file, the results are paginated. The response has the following format : [["pages","NUMBER_OF_PAGES"],["TIMESTAMP_VALUE", "SIMHASH_VALUE"]]
  
  **The SIMHASH_VALUE is base64 encoded**
  
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
