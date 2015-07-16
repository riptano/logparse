#!/usr/bin/env python
import sys
import socket
import fileinput

import systemlog
from cassandra_store import CassandraStore

cassandra = CassandraStore()

hostname = socket.gethostname()
log = fileinput.input()
for event in systemlog.parse_log(log):
    event['host'] = hostname
    event['log_file'] = fileinput.filename()
    event['log_line'] = fileinput.lineno()
    cassandra.insert_generic('systemlog', event)
