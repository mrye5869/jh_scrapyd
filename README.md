Jh Scrapyd
==========

A preemptive scrapyd cluster built using Redis queues

* Free software: MIT license (including the work distributed under the Apache 2.0 licen[WHEEL](..%2F..%2Fscrapy-cluster%2Fpyppeteer_frame%2Fvenv%2FLib%2Fsite-packages%2Fjh_scrapyd-0.0.1.dist-info%2FWHEEL)se)
* Documentation: https://scrapyd.readthedocs.org/en/latest/

## Installation

scrapyd >= 1.4.3

Install with `pip` from PyPI:

```
pip install jh_scrapyd
```

## Configuration
editor scrapyd.conf
```
[scrapyd]
jobs_to_keep  = 20000
# Finished task queue
jobstorage    = scrapyd.jobstorage.SqliteJobStorage

# Scrapy main application
application   = jh_scrapyd.app.application

# Queue system, queue related management
spiderqueue   = jh_scrapyd.spiderqueue.JsonRedisPriorityQueue

# Web page management root
webroot       = jh_scrapyd.website.Root
[services]
# Cancel queue task
cancel.json   = jh_scrapyd.webservice.JhCancel

[jh_scrapyd]
# Preemptive cluster debugging mode
is_debug = 1

# Add Redis to configure celarclear
host = 127.0.0.1
password = 
port = 6379
db = 0

# Is there a unified queue (mainly used for different projects to share queue priority)
is_unified_queue = 1

# Queue prefix
queue_prefix = jh_scrapyd

# Number of pending tasks on the page, -1 Display all
tab_pending_count = 100
```