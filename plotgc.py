#!/usr/bin/python
import sys
import cassandra
import pandas as pd
import matplotlib.pyplot as plt

log = cassandra.SystemLog()
gc = pd.DataFrame(log.sessions[0]['garbage_collections'])
gc.plot(x='date', y='duration')
plt.show()
