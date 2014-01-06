#!/usr/bin/python
import sys
import logparse
import pandas as pd
import matplotlib.pyplot as plt

log = logparse.SystemLogParser(sys.argv[1:])
gc = pd.DataFrame(log.sessions[0]['garbage_collections'])
gc.plot(x='date', y='duration')
plt.show()
