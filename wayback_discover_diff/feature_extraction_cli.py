#!/usr/bin/env python
"""Extract and print features from target capture.
Utility script useful to experiment and evaluate feature extraction.
"""
import pprint
import sys
import urllib3
from wayback_discover_diff.discover import extract_html_features

url = sys.argv[1]

http = urllib3.PoolManager()
res = http.request('GET', url)
data = res.data.decode('utf-8')

features = extract_html_features(data)
pprint.pprint(features)
