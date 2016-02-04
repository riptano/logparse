#!/usr/bin/env python
import sys
import fileinput
from collections import defaultdict
from getopt import getopt, GetoptError

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import systemlog

try:
    (options, arguments) = getopt(sys.argv[1:], 'i:s:etu',
                                    ['interval', 'scale', 'events', 'events-only', 'show-unknown'])
except GetoptError, error:
    sys.stderr.write('%s\n' % str(error))
    sys.exit(1)

# dirty, but I'm not rewriting fileinput.input
sys.argv = [sys.argv[0]]

interval = 3600
scale = 10
allevents = False
eventsonly = False
unknowns = False
for opt, arg in options:
    if opt in ('-i', '--interval'):
        interval = int(arg)
    if opt in ('-s', '--scale'):
        scale = float(arg)
    if opt in ('-e', '--events'):
        allevents = True
    if opt in ('-t', '--events-only'):
        eventsonly = True
    if opt in ('-u', '--show-unknown'):
        unknowns = True

if arguments:
    log = fileinput.input(arguments[0])
else:
    log = fileinput.input()

stages = {}
data = defaultdict(lambda: defaultdict(int))
enum = 0
for event in systemlog.parse_log(log):
    stage = event['thread_name'] + ' ' + event['source_file']
    if stage[0:3].isupper() or event['thread_name'] == 'main': # skip rmi, handshaking, streams, etc
        continue
    if event['event_type'] == 'unknown' and not unknowns:
        continue
    if event['event_type'] == 'messages_dropped':
        stage = event['message_type'] + ' dropped'
    elif event['event_type'] == 'begin_flush':
        stage = 'flushed bytes (serialized)'
    elif allevents:
        if not eventsonly:
            stage = ' '.join((event['event_category'], event['event_type'], stage))
        else:
            stage = event['event_category'] + ' ' +event['event_type'] 

    if stage not in stages:
        stages[stage] = enum
        enum += 1
    ts = int(event['date'].strftime('%s')) / interval
    if event['event_type'] == 'pause':
        data[ts][stage] += event['duration'] / 1000
    elif event['event_type'] == 'messages_dropped':
        if 'internal_timeout' in event:
            data[ts][stage] += event['internal_timeout'] + event['cross_node_timeout']
        else:
            data[ts][stage] += event['messages_dropped']
    elif event['event_type'] == 'begin_flush' and event['serialized_bytes'] != None:
        data[ts][stage] += (event['serialized_bytes'] / 1024**2 / scale)
    elif event['event_type'] in ('incremental_compaction', 'large_partition'):
        data[ts][stage] += event['partition_size'] / 1024**2 / scale
    elif event['event_type'] == 'begin_compaction':
        data[ts][stage] += len(event['input_sstables'])
    elif event['event_type'] == 'end_compaction':
        data[ts][stage] += event['output_bytes'] / 1024**2 / scale
    else:
        data[ts][stage] += 1

fig, ax = plt.subplots()
colors = cm.rainbow(np.linspace(0, 1, len(stages)))
for ts, info in data.iteritems():
    for stage in info.keys():
        ax.scatter(ts, stages[stage], s=info[stage]*scale, c=colors[stages[stage]], alpha=0.5)

plt.yticks(stages.values(), stages.keys())

ax.set_xlabel('Time (%s second buckets)' % interval, fontsize=20)
if not eventsonly:
    ax.set_ylabel('Stage', fontsize=20)
else:
    ax.set_ylabel('Event', fontsize=20)

ax.grid(True)

plt.plot()
try:
    fig.tight_layout()
except ValueError: # too many things on the Y axis
    fig.subplots_adjust(bottom = 0)
    fig.subplots_adjust(top = 1)
    fig.subplots_adjust(right = 1)
    fig.subplots_adjust(left = 0)
plt.subplots_adjust(left=0.21)
plt.show()
