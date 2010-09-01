# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from . import AvroTester
from time import time
from random import randint
from avro.ipc import AvroRemoteException
import struct

def i64(i):
    return struct.pack('>q', i)

def timestamp():
    return long(time() * 1e6)

def new_column(suffix, stamp=None, ttl=0):
    ts = isinstance(stamp, (long,int)) and stamp or timestamp()
    column = dict()
    column['name'] = 'name-%s' % suffix
    column['value'] = 'value-%s' % suffix
    column['clock'] = {'timestamp': ts}
    column['ttl'] = ttl
    return column

def assert_columns_match(colA, colB):
    assert colA['name'] == colB['name'], \
            "column name mismatch: %s != %s" % (colA['name'], colB['name'])
    assert colA['value'] == colB['value'], \
            "column value mismatch: %s != %s" % (colA['value'], colB['value'])

def assert_cosc(thing, with_supercolumn=False):
    containing = with_supercolumn and 'super_column' or 'column'
    assert isinstance(thing, dict), "Expected dict, got %s" % type(thing)
    assert thing.has_key(containing) and thing[containing].has_key('name'), \
            "Invalid or missing \"%s\" member" % containing

class TestRpcOperations(AvroTester):
    def test_insert_super(self):
        "setting and getting a super column"
        self.__set_keyspace('Keyspace1')

        params = dict()
        params['key'] = 'key1'
        params['column_parent'] = dict()
        params['column_parent']['column_family'] = 'Super1'
        params['column_parent']['super_column'] = 'sc1'
        params['column'] = dict()
        params['column']['name'] = i64(1)
        params['column']['value'] = 'v1'
        params['column']['clock'] = { 'timestamp' : 0 }
        params['consistency_level'] = 'ONE'
        self.client.request('insert', params)

        read_params = dict()
        read_params['key'] = params['key']
        read_params['column_path'] = dict()
        read_params['column_path']['column_family'] = 'Super1'
        read_params['column_path']['super_column'] = params['column_parent']['super_column']
        read_params['column_path']['column'] = params['column']['name']
        read_params['consistency_level'] = 'ONE'

        cosc = self.client.request('get', read_params)

        assert_cosc(cosc)
        assert_columns_match(cosc['column'], params['column'])

    def test_describe_keyspaces(self):
        "retrieving a list of all keyspaces"
        keyspaces = self.client.request('describe_keyspaces', {})
        assert 'Keyspace1' in keyspaces, "Keyspace1 not in " + keyspaces

    def test_describe_keyspace(self):
        "retrieving a keyspace metadata"
        ks1 = self.client.request('describe_keyspace',
                {'keyspace': "Keyspace1"})
        assert ks1['replication_factor'] == 1
        cf0 = ks1['cf_defs'][0]
        assert cf0['comparator_type'] == "org.apache.cassandra.db.marshal.BytesType"

    def test_describe_cluster_name(self):
        "retrieving the cluster name"
        name = self.client.request('describe_cluster_name', {})
        assert 'Test' in name, "'Test' not in '" + name + "'"

    def test_describe_version(self):
        "getting the remote api version string"
        vers = self.client.request('describe_version', {})
        assert isinstance(vers, (str,unicode)), "api version is not a string"
        segs = vers.split('.')
        assert len(segs) == 3 and len([i for i in segs if i.isdigit()]) == 3, \
               "incorrect api version format: " + vers

    def test_describe_partitioner(self):
        "getting the partitioner"
        part = "org.apache.cassandra.dht.CollatingOrderPreservingPartitioner"
        result = self.client.request('describe_partitioner', {})
        assert result == part, "got %s, expected %s" % (result, part)
       
    def __get(self, key, cf, super_name, col_name, consistency_level='ONE'):
        """
        Given arguments for the key, column family, super column name,
        column name, and consistency level, returns a dictionary 
        representing a ColumnOrSuperColumn record.

        Raises an AvroRemoteException if the column is not found.
        """
        params = dict()
        params['key'] = key
        params['column_path'] = dict()
        params['column_path']['column_family'] = cf
        params['column_path']['column'] = col_name
        params['consistency_level'] = consistency_level

        if (super_name):
            params['super_column'] = super_name

        return self.client.request('get', params)

    def __set_keyspace(self, keyspace_name):
        self.client.request('set_keyspace', {'keyspace': keyspace_name})

# vi:ai sw=4 ts=4 tw=0 et
