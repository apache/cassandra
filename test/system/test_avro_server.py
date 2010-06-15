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

def assert_columns_match(colA, colB):
    assert colA['name'] == colB['name'], \
            "column name mismatch: %s != %s" % (colA['name'], colB['name'])
    assert colA['value'] == colB['value'], \
            "column value mismatch: %s != %s" % (colA['value'], colB['value'])

def assert_cosc(thing, with_supercolumn=False):
    containing = with_supercolumn and 'super_column' or 'column'
    assert isinstance(thing, dict), "Expected dict, got %s" % type(thing)
    assert thing.has_key(containing) and thing[containing].has_key('name'), \
            "Invalid or missing \"%s\"" % containing

class TestRpcOperations(AvroTester):
    def test_insert_simple(self):       # Also tests get
        "setting and getting a simple column"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

        params = dict()
        params['key'] = 'key1'
        params['column_parent'] = {'column_family': 'Standard1'}
        params['column'] = dict()
        params['column']['name'] = 'c1'
        params['column']['value'] = 'v1'
        params['column']['clock'] = { 'timestamp' : 0 }
        params['consistency_level'] = 'ONE'
        self.client.request('insert', params)

        read_params = dict()
        read_params['key'] = params['key']
        read_params['column_path'] = dict()
        read_params['column_path']['column_family'] = 'Standard1'
        read_params['column_path']['column'] = params['column']['name']
        read_params['consistency_level'] = 'ONE'

        cosc = self.client.request('get', read_params)

        assert_cosc(cosc)
        assert_columns_match(cosc['column'], params['column'])

    def test_insert_super(self):
        "setting and getting a super column"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

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

    def test_remove_simple(self):
        "removing a simple column"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

        params = dict()
        params['key'] = 'key1'
        params['column_parent'] = {'column_family': 'Standard1'}
        params['column'] = dict()
        params['column']['name'] = 'c1'
        params['column']['value'] = 'v1'
        params['column']['clock'] = { 'timestamp' : 0 }
        params['consistency_level'] = 'ONE'
        self.client.request('insert', params)

        read_params = dict()
        read_params['key'] = params['key']
        read_params['column_path'] = dict()
        read_params['column_path']['column_family'] = 'Standard1'
        read_params['column_path']['column'] = params['column']['name']
        read_params['consistency_level'] = 'ONE'

        cosc = self.client.request('get', read_params)

        assert_cosc(cosc)

        remove_params = read_params
        remove_params['clock'] = {'timestamp': 1}

        self.client.request('remove', remove_params)

        try: cosc = self.client.request('get', read_params)
        except AvroRemoteException, err: pass
        else: assert False, "Expected exception, returned %s instead" % cosc

    def test_describe_keyspaces(self):
        "retrieving a list of all keyspaces"
        keyspaces = self.client.request('describe_keyspaces', {})
        assert 'Keyspace1' in keyspaces, "Keyspace1 not in " + keyspaces

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

# vi:ai sw=4 ts=4 tw=0 et
