#!/usr/bin/env bash
#Run Redis server
redis-server &

export WAYBACK_DISCOVER_DIFF_CONF=wayback_discover_diff/conf.yml

#Run tests
pytest