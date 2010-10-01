
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
from avro.ipc import AvroRemoteException
import avro_utils
import time
    
def _make_write_params(key, cf, sc, c, v, ts=0, cl='ONE'):
    params = dict()
    params['key'] = key
    params['column_parent'] = dict()
    params['column_parent']['column_family'] = cf
    params['column_parent']['super_column'] = sc
    params['column'] = dict()
    params['column']['name'] = c
    params['column']['value'] = v
    params['column']['timestamp'] = ts
    params['consistency_level'] = cl
    return params
    
def _make_read_params(key, cf, sc, c, cl):
    params = dict()
    params['key'] = key
    column_path = dict()
    column_path['column_family'] = cf
    column_path['super_column'] = sc
    column_path['column'] = c
    params['column_path'] = column_path
    params['consistency_level'] = cl
    return params

def _super_col(name, columns):
    return {'name': name, 'columns': columns}

def Mutation(**kwargs):
    return kwargs
    
def SlicePredicate(**kwargs):
    return kwargs

def SliceRange(start='', finish='', reversed=False, count=10):
    return {'start': start, 'finish': finish, 'reversed':reversed, 'count': count}

def ColumnParent(*args, **kwargs):
    cp = {}
    if args and len(args) > 0:
        cp['column_family'] = args[0]
    if args and len(args) > 1:
        cp['super_column'] = args[1]
    for k,v in kwargs.items():
        cp[k] = v
    return cp

def Deletion(*args, **kwargs):
    cp = {}
    if args and len(args) > 0:
        cp['timestamp'] = args[0]
    for k,v in kwargs.items():
        cp[k] = v
    return cp
   
def ColumnPath(*args, **kwargs):
    cp = {}
    if args and len(args) > 0:
        cp['column_family'] = args[0]
    for k,v in kwargs.items():
        cp[k] = v
    return cp

def Column(name, value, timestamp, ttl=None):
    return {'name':name, 'value':value, 'timestamp': timestamp, 'ttl': ttl}

def _i64(i):
    return avro_utils.i64(i)

def waitfor(secs, fn, *args, **kwargs):
    start = time.time()
    success = False
    last_exception = None
    while not success and time.time() < start + secs:
        try:
            fn(*args, **kwargs)
            success = True
        except KeyboardInterrupt:
            raise
        except Exception, e:
            last_exception = e
            pass
    if not success and last_exception:
        raise last_exception

ZERO_WAIT = 5
    
_SUPER_COLUMNS = [_super_col('sc1', [Column(avro_utils.i64(4), 'value4', 0)]), 
                  _super_col('sc2', [Column(avro_utils.i64(5), 'value5', 0), 
                                     Column(avro_utils.i64(6), 'value6', 0)])]
    
class TestSuperOperations(AvroTester):

    def _set_keyspace(self, keyspace):
        self.client.request('set_keyspace', {'keyspace': keyspace})
        
    """
    Operations on Super column families
    """
    def test_super_insert(self):
        "simple super column insert"
        self._set_keyspace('Keyspace1')
        self._insert_super()
        self._verify_super()
        
    def test_slice_super(self):
        "tests simple insert and get_slice"
        self._set_keyspace('Keyspace1')
        self._insert_super()
        p = {'slice_range': {'start': '', 'finish': '', 'reversed': False, 'count': 10}}
        parent = {'column_family': 'Super1', 'super_column': 'sc1'}
        cosc = self.client.request('get_slice', {'key': 'key1', 'column_parent': parent, 'predicate': p, 'consistency_level': 'ONE'})
        avro_utils.assert_cosc(cosc[0])
    
    def test_missing_super(self):
        "verifies that inserting doesn't yield false positives."
        self._set_keyspace('Keyspace1')
        avro_utils.assert_raises(AvroRemoteException,
                self.client.request,
                'get',
                _make_read_params('key1', 'Super1', 'sc1', avro_utils.i64(1), 'ONE'))
        self._insert_super()
        avro_utils.assert_raises(AvroRemoteException,
                self.client.request,
                'get',
                _make_read_params('key1', 'Super1', 'sc1', avro_utils.i64(1), 'ONE'))
        
    def test_super_get(self):
        "read back a super column"
        self._set_keyspace('Keyspace1')
        self._insert_super()
        result = self.client.request('get', _make_read_params('key1', 'Super1', 'sc2', None, 'ONE'))['super_column']
        assert result == _SUPER_COLUMNS[1], result
    
    def test_super_subcolumn_limit(self):
        "test get_slice honors subcolumn reversal and limit"
        self._set_keyspace('Keyspace1')
        self._insert_super()
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1))
        column_parent = ColumnParent('Super1', 'sc2')
        slice = [result['column'] for result in self.client.request('get_slice', {'key': 'key1', 'column_parent': column_parent, 'predicate': p, 'consistency_level': 'ONE'})]
        assert slice == [Column(_i64(5), 'value5', 0)], slice
        p = SlicePredicate(slice_range=SliceRange('', '', True, 1))
        slice = [result['column'] for result in self.client.request('get_slice', {'key': 'key1', 'column_parent': column_parent, 'predicate': p, 'consistency_level': 'ONE'})]
        assert slice == [Column(_i64(6), 'value6', 0)], slice
     
    def test_time_uuid(self):
        "test operation on timeuuid subcolumns in super columns"
        import uuid
        L = []
        self._set_keyspace('Keyspace2')
        # 100 isn't enough to fail reliably if the comparator is borked
        for i in xrange(500):
            L.append(uuid.uuid1())
            self.client.request('insert', {'key': 'key1', 'column_parent': ColumnParent('Super4', 'sc1'), 'column': Column(L[-1].bytes, 'value%s' % i, i), 'consistency_level': 'ONE'})
        slice = self._big_slice('key1', ColumnParent('Super4', 'sc1'))
        assert len(slice) == 500, len(slice)
        for i in xrange(500):
            u = slice[i]['column']
            assert u['value'] == 'value%s' % i
            assert u['name'] == L[i].bytes

        p = SlicePredicate(slice_range=SliceRange('', '', True, 1))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result['column'] for result in self.client.request('get_slice', {'key': 'key1', 'column_parent': column_parent, 'predicate': p, 'consistency_level': 'ONE'})]
        assert slice == [Column(L[-1].bytes, 'value499', 499)], slice

        p = SlicePredicate(slice_range=SliceRange('', L[2].bytes, False, 1000))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result['column'] for result in self.client.request('get_slice', {'key': 'key1', 'column_parent': column_parent, 'predicate': p, 'consistency_level': 'ONE'})]
        assert slice == [Column(L[0].bytes, 'value0', 0),
                         Column(L[1].bytes, 'value1', 1),
                         Column(L[2].bytes, 'value2', 2)], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, '', True, 1000))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result['column'] for result in self.client.request('get_slice', {'key': 'key1', 'column_parent': column_parent, 'predicate': p, 'consistency_level': 'ONE'})]
        assert slice == [Column(L[2].bytes, 'value2', 2),
                         Column(L[1].bytes, 'value1', 1),
                         Column(L[0].bytes, 'value0', 0)], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, '', False, 1))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result['column'] for result in self.client.request('get_slice', {'key': 'key1', 'column_parent': column_parent, 'predicate': p, 'consistency_level': 'ONE'})]
        assert slice == [Column(L[2].bytes, 'value2', 2)], slice
    
    def test_batch_mutate_remove_super_columns_with_standard_under(self):
        "batch mutate with deletions in super columns"
        self._set_keyspace('Keyspace1')
        column_families = ['Super1', 'Super2']
        keys = ['key_%d' % i for i in range(11,21)]
        self._insert_super()

        mutations = []
        for sc in _SUPER_COLUMNS:
            names = []
            for c in sc['columns']:
                names.append(c['name'])
            mutations.append(Mutation(deletion=Deletion(20, super_column=c['name'], predicate=SlicePredicate(column_names=names))))

        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = [{'key': key, 'mutations': mutation_map} for key in keys]

        def _assert_no_columnpath(key, column_path):
            self._assert_no_columnpath(key, column_path)
            
        self.client.request('batch_mutate', {'mutation_map': keyed_mutations, 'consistency_level': 'ZERO'})
        for column_family in column_families:
            for sc in _SUPER_COLUMNS:
                for c in sc['columns']:
                    for key in keys:
                        waitfor(ZERO_WAIT, _assert_no_columnpath, key, ColumnPath(column_family, super_column=sc['name'], column=c['name']))
                        
        
    # internal helper functions.
    
    def _insert_super(self, key='key1'):
        self.client.request('insert', _make_write_params(key, 'Super1', 'sc1', avro_utils.i64(4), 'value4', 0, 'ONE'))
        self.client.request('insert', _make_write_params(key, 'Super1', 'sc2', avro_utils.i64(5), 'value5', 0, 'ONE'))
        self.client.request('insert', _make_write_params(key, 'Super1', 'sc2', avro_utils.i64(6), 'value6', 0, 'ONE'))
    
    def _big_slice(self, key, column_parent):
        p = {'slice_range': {'start': '', 'finish': '', 'reversed': False, 'count': 1000}}
        return self.client.request('get_slice',  {'key': key, 'column_parent': column_parent, 'predicate': p, 'consistency_level': 'ONE'})
        
    def _verify_super(self, supercf='Super1', key='key1'):
        col = self.client.request('get', _make_read_params(key, supercf, 'sc1', avro_utils.i64(4), 'ONE'))['column']
        avro_utils.assert_columns_match(col, {'name': avro_utils.i64(4), 'value': 'value4', 'timestamp': 0})
        slice = [result['super_column'] for result in self._big_slice(key, {'column_family': supercf})]
        assert slice == _SUPER_COLUMNS, _SUPER_COLUMNS
    
    def _assert_no_columnpath(self, key, column_path):
        try:
            self.client.request('get', {'key': key, 'column_path': column_path, 'consistency_level': 'ONE'})
            assert False, ('columnpath %s existed in %s when it should not' % (column_path, key))
        except AvroRemoteException:
            assert True, 'column did not exist'

        
