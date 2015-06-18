#!/usr/bin/env python
import sys

from systemlog import SystemLog
from cassandra_store import CassandraStore

log = SystemLog(sys.argv[1])
cassandra = CassandraStore()

for line in log.lines:
    line.update(line.pop('message_fields', {}))
    cassandra.insert_generic('systemlog', line)
