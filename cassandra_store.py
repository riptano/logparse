import json
import uuid
import collections

from copy import deepcopy
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.util import uuid_from_time
from cassandra.concurrent import execute_concurrent_with_args

import collections

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def genericize(columns, parameters):
    def date_handler(obj):
        return obj.isoformat() if hasattr(obj, 'isoformat') else obj

    parameters = flatten(deepcopy(parameters))

    if 'id' not in parameters:
        if 'date' in parameters:
            parameters['id'] = uuid_from_time(parameters['date'])
        else:
            parameters['id'] = uuid.uuid4()

    generic = {'b_': {}, 'd_': {}, 'i_': {}, 'f_': {}, 's_': {}, 'l_': {}}
    for key, value in parameters.items():
        if key not in columns:
            if type(value) is bool:
                prefix = 'b_'
            elif type(value) is datetime:
                prefix = 'd_'
            elif type(value) is int:
                prefix = 'i_'
            elif type(value) is float:
                prefix = 'f_'
            elif isinstance(value, (str, unicode)):
                prefix = 's_'
            elif type(value) is list:
                prefix = 'l_'
                value = '\n'.join([str(x) for x in value])
            elif value is not None:
                prefix = 's_'
                value = json.dumps(value, default=date_handler)
            if value is not None:
                generic[prefix][prefix + key] = value
            del parameters[key]
    for key, value in generic.items():
        if key in columns:
            parameters[key] = value
    for name in columns:
        if name not in parameters:
            parameters[name] = None
    return parameters

class CassandraStore:
    def __init__(self, contacts=None, keyspace='logparse'):
        self.cluster = Cluster(contacts)
        self.session = self.cluster.connect()
        self.session.row_factory = dict_factory
        self.session.set_keyspace(keyspace)
        self.keyspace = keyspace
        self.prepare_statements()

    def prepare_statements(self):
        self.insert_statements = {}
        self.columns = {}
        for table in self.cluster.metadata.keyspaces[self.keyspace].tables:
            self.columns[table] = [column for column in self.cluster.metadata.keyspaces[self.keyspace].tables[table].columns]
            statement = 'insert into {} ({}) values ({});'.format(
                table,
                ', '.join(self.columns[table]),
                ', '.join([':' + column for column in self.columns[table]])
            )
            self.insert_statements[table] = self.session.prepare(statement)

    def insert(self, table, parameters, async=False):
        def log_error(ex):
            print 'ERROR: %s when inserting %s: %s' % (ex, table, parameters)

        parameters = genericize(self.columns[table], parameters)
        try:
            if async:
                future = self.session.execute_async(self.insert_statements[table], parameters)
                future.add_errback(log_error)
            else:
                self.session.execute(self.insert_statements[table], parameters)
        except Exception, ex:
            log_error(ex)

    def slurp(self, table, stream, concurrency=1000):
        generic_stream = (genericize(self.columns[table], parameters) for parameters in stream)
        for success, result in execute_concurrent_with_args(self.session, self.insert_statements[table], generic_stream, concurrency=concurrency, results_generator=True):
            if not success:
                yield result
