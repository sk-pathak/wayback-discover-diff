#!/usr/bin/env bash
# Initialize options for gunicorn
OPTS=(
  --env FLASK_APP=wayback_discover_diff
  --env FLASK_ENV=development
  --env WAYBACK_DISCOVER_DIFF_CONF=wayback_discover_diff/conf.yml
  --workers 2
  -b 0.0.0.0:8096
  --reload
)

#Run gunicorn
gunicorn "${OPTS[@]}" wayback_discover_diff.application:APP
