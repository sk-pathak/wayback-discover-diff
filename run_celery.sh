#!/usr/bin/env bash
WAYBACK_DISCOVER_DIFF_CONF=wayback_discover_diff/conf.yml celery -A wayback_discover_diff.application.CELERY worker -l debug
