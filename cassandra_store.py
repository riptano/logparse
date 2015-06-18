import json

from copy import deepcopy
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


class CassandraStore:
    def __init__(self, contact_points=None, keyspace='logparse', async=True):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect()
        self.session.row_factory = dict_factory
        self.session.set_keyspace(keyspace)
        self.keyspace = keyspace
        self.prepare_statements()
        self.async = async

    def prepare_statements(self):
        self.insert_statements = {}
        self.columns = {}
        for table in self.cluster.metadata.keyspaces[self.keyspace].tables:
            self.columns[table] = [column for column in self.cluster.metadata.keyspaces[self.keyspace].tables[table].columns]
            statement= 'insert into {} ({}) values ({});'.format(
                table,
                ', '.join(self.columns[table]),
                ', '.join([':' + column for column in self.columns[table]])
            )
            self.insert_statements[table] = self.session.prepare(statement)

    def insert_generic(self, table, parameters):
        def log_error(exc):
            print 'ERROR: %s when inserting %s: %s' % (exc, table, parameters)

        def date_handler(obj):
            return obj.isoformat() if hasattr(obj, 'isoformat') else obj

        generic = {'b_': {}, 'd_': {}, 'i_': {}, 'f_': {}, 's_': {}}
        for key, value in parameters.items():
            if key not in self.columns[table]:
                if type(value) is bool:
                    prefix = 'b_'
                elif type(value) is datetime:
                    prefix = 'd_'
                elif type(value) is int:
                    prefix = 'i_'
                elif type(value) is float:
                    prefix = 'f_'
                elif type(value) is str or type(value) is unicode:
                    prefix = 's_'
                elif value is not None:
                    prefix = 's_'
                    value = json.dumps(value, default=date_handler)
                if value is not None:
                    generic[prefix][prefix + key] = value
                del parameters[key]
        for key, value in generic.items():
            if key in self.columns[table]:
                parameters[key] = value
        for name in self.columns[table]:
            if name not in parameters:
                parameters[name] = None
        try:
            if self.async:
                future = self.session.execute_async(self.insert_statements[table], parameters)
                future.add_errback(log_error)
            else:
                self.session.execute(self.insert_statements[table], parameters)
        except Exception, ex:
            log_error(ex)