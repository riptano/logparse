#!/usr/bin/env python
import cassandra
import sys
log = cassandra.SystemLog(sys.argv[1])
log.solr_index('http://localhost:8983/solr/logparse.systemlog/update')
