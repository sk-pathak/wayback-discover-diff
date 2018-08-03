#!/usr/bin/env bash
#Run Redis server
redis-server &

# Initialize options for gunicorn
OPTS=(
  --env FLASK_APP=wayback_discover_diff
  --env FLASK_ENV=development
  --env WAYBACK_DISCOVER_DIFF_CONF=wayback_discover_diff/conf.yml
  --workers 1
  -b 127.0.0.1:4000
  --reload
)

#Run gunicorn
gunicorn "${OPTS[@]}" wayback_discover_diff:APP &

#Run celery worker
cd wayback_discover_diff
WAYBACK_DISCOVER_DIFF_CONF=conf.yml celery worker -A wayback_discover_diff.CELERY -l debug