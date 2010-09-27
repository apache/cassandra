
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
import avro_utils
from time import time
import struct
from avro.ipc import AvroRemoteException

def new_column(suffix, stamp=None, ttl=0):
    ts = isinstance(stamp, (long,int)) and stamp or timestamp()
    column = dict()
    column['name'] = 'name-%s' % suffix
    column['value'] = 'value-%s' % suffix
    column['timestamp'] = ts
    column['ttl'] = ttl
    return column

def _create_multi_key_column():
    mutations = list()

    for i in range(10):
        mutation = {'column_or_supercolumn': {'column': new_column(i)}}
        mutations.append(mutation)

    mutation_params = dict()
    mutation_params['mutation_map'] = list()
    for i in range(3):
        entry = {'key': 'k'+str(i), 'mutations': {'Standard1': mutations}}
        mutation_params['mutation_map'].append(entry)
    mutation_params['consistency_level'] = 'ONE'
    return mutation_params

def _read_multi_key_column_count():
    count_params = dict()
    count_params['keys'] = ['k0', 'k1', 'k2']
    count_params['column_parent'] = {'column_family': 'Standard1'}
    sr = {'start': '', 'finish': '', 'reversed': False, 'count': 1000}
    count_params['predicate'] = {'slice_range': sr}
    count_params['consistency_level'] = 'ONE'
    return count_params

def timestamp():
    return long(time() * 1e6)

def i64(i):
    return struct.pack('<d', i)

class TestStandardOperations(AvroTester):
    """
    Operations on Standard column families
    """
    def test_insert_simple(self):       # Also tests get
        "setting and getting a simple column"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

        params = dict()
        params['key'] = 'key1'
        params['column_parent'] = {'column_family': 'Standard1'}
        params['column'] = new_column(1)
        params['consistency_level'] = 'ONE'
        self.client.request('insert', params)

        read_params = dict()
        read_params['key'] = params['key']
        read_params['column_path'] = dict()
        read_params['column_path']['column_family'] = 'Standard1'
        read_params['column_path']['column'] = params['column']['name']
        read_params['consistency_level'] = 'ONE'

        cosc = self.client.request('get', read_params)

        avro_utils.assert_cosc(cosc)
        avro_utils.assert_columns_match(cosc['column'], params['column'])

    def test_insert_unicode(self):
        "send some unicode to the server"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})
        params = dict();
        params['key'] = 'key1'
        params['column_parent'] = {'column_family': 'Standard1'}
        params['column'] = dict()
        params['column']['name'] = struct.pack('bbbbbbbbbb',36,-62,-94,-30,-126,-84,-16,-92,-83,-94)
        params['column']['value'] = struct.pack('bbbbbbbbbb',36,-62,-94,-30,-126,-84,-16,-92,-83,-94)
        params['column']['timestamp'] = timestamp()
        params['column']['ttl'] = None
        params['consistency_level'] = 'ONE'
        self.client.request('insert', params)
        
        read_params = dict();
        read_params['key'] = params['key']
        read_params['column_path'] = dict()
        read_params['column_path']['column_family'] = 'Standard1'
        read_params['column_path']['column'] = params['column']['name']
        read_params['consistency_level'] = 'ONE'
        cosc = self.client.request('get', read_params)

        avro_utils.assert_cosc(cosc)
        avro_utils.assert_columns_match(cosc['column'], params['column'])
        
    def test_remove_simple(self):
        "removing a simple column"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

        params = dict()
        params['key'] = 'key1'
        params['column_parent'] = {'column_family': 'Standard1'}
        params['column'] = new_column(1)
        params['consistency_level'] = 'ONE'
        self.client.request('insert', params)

        read_params = dict()
        read_params['key'] = params['key']
        read_params['column_path'] = dict()
        read_params['column_path']['column_family'] = 'Standard1'
        read_params['column_path']['column'] = params['column']['name']
        read_params['consistency_level'] = 'ONE'

        cosc = self.client.request('get', read_params)

        avro_utils.assert_cosc(cosc)

        remove_params = read_params
        remove_params['timestamp'] = timestamp()

        self.client.request('remove', remove_params)

        avro_utils.assert_raises(AvroRemoteException,
                self.client.request, 'get', read_params)

    def test_batch_mutate(self):
        "batching addition/removal mutations"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

        mutations = list()
       
        # New column mutations
        for i in range(3):
            cosc = {'column': new_column(i)}
            mutation = {'column_or_supercolumn': cosc}
            mutations.append(mutation)

        map_entry = {'key': 'key1', 'mutations': {'Standard1': mutations}}

        params = dict()
        params['mutation_map'] = [map_entry]
        params['consistency_level'] = 'ONE'

        self.client.request('batch_mutate', params)

        # Verify that new columns were added
        for i in range(3):
            column = new_column(i)
            cosc = self.__get('key1', 'Standard1', None, column['name'])
            avro_utils.assert_cosc(cosc)
            avro_utils.assert_columns_match(cosc['column'], column)

        # Add one more column; remove one column
        extra_column = new_column(3); remove_column = new_column(0)
        mutations = [{'column_or_supercolumn': {'column': extra_column}}]
        deletion = dict()
        deletion['timestamp'] = timestamp()
        deletion['predicate'] = {'column_names': [remove_column['name']]}
        mutations.append({'deletion': deletion})

        map_entry = {'key': 'key1', 'mutations': {'Standard1': mutations}}

        params = dict()
        params['mutation_map'] = [map_entry]
        params['consistency_level'] = 'ONE'

        self.client.request('batch_mutate', params)

        # Ensure successful column removal
        avro_utils.assert_raises(AvroRemoteException,
                self.__get, 'key1', 'Standard1', None, remove_column['name'])

        # Ensure successful column addition
        cosc = self.__get('key1', 'Standard1', None, extra_column['name'])
        avro_utils.assert_cosc(cosc)
        avro_utils.assert_columns_match(cosc['column'], extra_column)

    def test_get_slice_simple(self):
        "performing a slice of simple columns"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

        columns = list(); mutations = list()

        for i in range(6):
            columns.append(new_column(i))

        for column in columns:
            mutation = {'column_or_supercolumn': {'column': column}}
            mutations.append(mutation)

        mutation_params = dict()
        map_entry = {'key': 'key1', 'mutations': {'Standard1': mutations}}
        mutation_params['mutation_map'] = [map_entry]
        mutation_params['consistency_level'] = 'ONE'

        self.client.request('batch_mutate', mutation_params)

        # Slicing on list of column names
        slice_params= dict()
        slice_params['key'] = 'key1'
        slice_params['column_parent'] = {'column_family': 'Standard1'}
        slice_params['predicate'] = {'column_names': list()}
        slice_params['predicate']['column_names'].append(columns[0]['name'])
        slice_params['predicate']['column_names'].append(columns[4]['name'])
        slice_params['consistency_level'] = 'ONE'

        coscs = self.client.request('get_slice', slice_params)

        for cosc in coscs: avro_utils.assert_cosc(cosc)
        avro_utils.assert_columns_match(coscs[0]['column'], columns[0])
        avro_utils.assert_columns_match(coscs[1]['column'], columns[4])

        # Slicing on a range of column names
        slice_range = dict()
        slice_range['start'] = columns[2]['name']
        slice_range['finish'] = columns[5]['name']
        slice_range['reversed'] = False
        slice_range['count'] = 1000
        slice_params['predicate'] = {'slice_range': slice_range}

        coscs = self.client.request('get_slice', slice_params)

        for cosc in coscs: avro_utils.assert_cosc(cosc)
        assert len(coscs) == 4, "expected 4 results, got %d" % len(coscs)
        avro_utils.assert_columns_match(coscs[0]['column'], columns[2])
        avro_utils.assert_columns_match(coscs[3]['column'], columns[5])

    def test_multiget_slice_simple(self):
        "performing a slice of simple columns, multiple keys"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

        columns = list(); mutation_params = dict()

        for i in range(12):
            columns.append(new_column(i))

        # key1, first 6 columns
        mutations_one = list()
        for column in columns[:6]:
            mutation = {'column_or_supercolumn': {'column': column}}
            mutations_one.append(mutation)

        map_entry = {'key': 'key1', 'mutations': {'Standard1': mutations_one}}
        mutation_params['mutation_map'] = [map_entry]

        # key2, last 6 columns
        mutations_two = list()
        for column in columns[6:]:
            mutation = {'column_or_supercolumn': {'column': column}}
            mutations_two.append(mutation)

        map_entry = {'key': 'key2', 'mutations': {'Standard1': mutations_two}}
        mutation_params['mutation_map'].append(map_entry)

        mutation_params['consistency_level'] = 'ONE'

        self.client.request('batch_mutate', mutation_params)

        # Slice all 6 columns on both keys
        slice_params= dict()
        slice_params['keys'] = ['key1', 'key2']
        slice_params['column_parent'] = {'column_family': 'Standard1'}
        sr = {'start': '', 'finish': '', 'reversed': False, 'count': 1000}
        slice_params['predicate'] = {'slice_range': sr}
        slice_params['consistency_level'] = 'ONE'

        coscs_map = self.client.request('multiget_slice', slice_params)
        for entry in coscs_map:
            assert(entry['key'] in ['key1', 'key2']), \
                    "expected one of [key1, key2]; got %s" % entry['key']
            assert(len(entry['columns']) == 6), \
                    "expected 6 results, got %d" % len(entry['columns'])

    def test_get_count(self):
        "counting columns"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

        mutations = list()

        for i in range(10):
            mutation = {'column_or_supercolumn': {'column': new_column(i)}}
            mutations.append(mutation)

        mutation_params = dict()
        map_entry = {'key': 'key1', 'mutations': {'Standard1': mutations}}
        mutation_params['mutation_map'] = [map_entry]
        mutation_params['consistency_level'] = 'ONE'

        self.client.request('batch_mutate', mutation_params)

        count_params = dict()
        count_params['key'] = 'key1'
        count_params['column_parent'] = {'column_family': 'Standard1'}
        sr = {'start': '', 'finish': '', 'reversed': False, 'count': 1000}
        count_params['predicate'] = {'slice_range': sr}
        count_params['consistency_level'] = 'ONE'

        num_columns = self.client.request('get_count', count_params)
        assert(num_columns == 10), "expected 10 results, got %d" % num_columns

    def test_multiget_count(self):
        "obtaining the column count for multiple rows"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

        self.client.request('batch_mutate', _create_multi_key_column())

        counts = self.client.request('multiget_count', _read_multi_key_column_count())
        for e in counts:
            assert(e['count'] == 10), \
                "expected 10 results for %s, got %d" % (e['key'], e['count'])

    def test_truncate(self):
        "truncate a column family"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

        self.client.request('batch_mutate', _create_multi_key_column())

        # truncate Standard1
        self.client.request('truncate',{'column_family':'Standard1'})

        counts = self.client.request('multiget_count', _read_multi_key_column_count())
        for e in counts:
            assert(e['count'] == 0)

    def test_index_slice(self):
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})
        cp = dict(column_family='Indexed1')
        self.client.request('insert', dict(key='key1', column_parent=cp, column=dict(name='birthdate', value=i64(1), timestamp=0), consistency_level='ONE'))
        self.client.request('insert', dict(key='key2', column_parent=cp, column=dict(name='birthdate', value=i64(2), timestamp=0), consistency_level='ONE'))
        self.client.request('insert', dict(key='key2', column_parent=cp, column=dict(name='b', value=i64(2), timestamp=0), consistency_level='ONE'))
        self.client.request('insert', dict(key='key3', column_parent=cp, column=dict(name='birthdate', value=i64(3), timestamp=0), consistency_level='ONE'))
        self.client.request('insert', dict(key='key3', column_parent=cp, column=dict(name='b', value=i64(3), timestamp=0), consistency_level='ONE'))

        # simple query on one index expression
        sp = dict(slice_range=dict(start='', finish='', reversed=False, count=1000))
        clause = dict(expressions=[dict(column_name='birthdate', op='EQ', value=i64(1))], start_key='', count=100)
        result = self.client.request('get_indexed_slices', dict(column_parent=cp, index_clause=clause, column_predicate=sp, consistency_level='ONE'))
        assert len(result) == 1, result
        assert result[0]['key'] == 'key1'
        assert len(result[0]['columns']) == 1, result[0]['columns']

        # solo unindexed expression is invalid
        clause = dict(expressions=[dict(column_name='b', op='EQ', value=i64(1))], start_key='', count=100)
        avro_utils.assert_raises(AvroRemoteException,
            self.client.request, 'get_indexed_slices', dict(column_parent=cp, index_clause=clause, column_predicate=sp, consistency_level='ONE'))

        # but unindexed expression added to indexed one is ok
        clause = dict(expressions=[dict(column_name='b', op='EQ', value=i64(3)),
                                   dict(column_name='birthdate', op='EQ', value=i64(3))],
                                   start_key='', count=100)
        result = self.client.request('get_indexed_slices', dict(column_parent=cp, index_clause=clause, column_predicate=sp, consistency_level='ONE'))
        assert len(result) == 1, result
        assert result[0]['key'] == 'key3'
        assert len(result[0]['columns']) == 2, result[0]['columns']

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

