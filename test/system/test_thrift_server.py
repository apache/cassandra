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

# to run a single test, run from trunk/:
# PYTHONPATH=test nosetests --tests=system.test_thrift_server:TestMutations.test_empty_range

import os, sys, time, struct, uuid

from . import root, ThriftTester
from . import thrift_client as client

from thrift.Thrift import TApplicationException
from ttypes import *
from constants import VERSION


def _i64(n):
    return struct.pack('>q', n) # big endian = network order

_SIMPLE_COLUMNS = [Column('c1', 'value1', 0),
                   Column('c2', 'value2', 0)]
_SUPER_COLUMNS = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 0),
                                                   Column(_i64(6), 'value6', 0)])]

def _assert_column(column_family, key, column, value, ts = 0):
    try:
        assert client.get(key, ColumnPath(column_family, column=column), ConsistencyLevel.ONE).column == Column(column, value, ts)
    except NotFoundException:
        raise Exception('expected %s:%s:%s:%s, but was not present' % (column_family, key, column, value) )

def _assert_columnpath_exists(key, column_path):
    try:
        assert client.get(key, column_path, ConsistencyLevel.ONE)
    except NotFoundException:
        raise Exception('expected %s with %s but was not present.' % (key, column_path) )

def _assert_no_columnpath(key, column_path):
    try:
        client.get(key, column_path, ConsistencyLevel.ONE)
        assert False, ('columnpath %s existed in %s when it should not' % (column_path, key))
    except NotFoundException:
        assert True, 'column did not exist'

def _insert_simple(block=True):
   return _insert_multi(['key1'])

def _insert_batch(block):
   return _insert_multi_batch(['key1'], block)

def _insert_multi(keys):
    CL = ConsistencyLevel.ONE
    for key in keys:
        client.insert(key, ColumnParent('Standard1'), Column('c1', 'value1', 0), CL)
        client.insert(key, ColumnParent('Standard1'), Column('c2', 'value2', 0), CL)

def _insert_multi_batch(keys, block):
    cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS],
             'Standard2': [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]}
    for key in keys:
        client.batch_mutate({key: cfmap}, ConsistencyLevel.ONE)

def _big_slice(key, column_parent):
    p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
    return client.get_slice(key, column_parent, p, ConsistencyLevel.ONE)

def _big_multislice(keys, column_parent):
    p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
    return client.multiget_slice(keys, column_parent, p, ConsistencyLevel.ONE)

def _verify_batch():
    _verify_simple()
    L = [result.column
         for result in _big_slice('key1', ColumnParent('Standard2'))]
    assert L == _SIMPLE_COLUMNS, L

def _verify_simple():
    assert client.get('key1', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE).column == Column('c1', 'value1', 0)
    L = [result.column
         for result in _big_slice('key1', ColumnParent('Standard1'))]
    assert L == _SIMPLE_COLUMNS, L

def _insert_super(key='key1'):
    client.insert(key, ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', 0), ConsistencyLevel.ONE)
    client.insert(key, ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 0), ConsistencyLevel.ONE)
    client.insert(key, ColumnParent('Super1', 'sc2'), Column(_i64(6), 'value6', 0), ConsistencyLevel.ONE)
    time.sleep(0.1)

def _insert_range():
    client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', 0), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Standard1'), Column('c2', 'value2', 0), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Standard1'), Column('c3', 'value3', 0), ConsistencyLevel.ONE)
    time.sleep(0.1)

def _insert_counter_range():
    client.add('key1', ColumnParent('Counter1'), CounterColumn('c1', 1), ConsistencyLevel.ONE)
    client.add('key1', ColumnParent('Counter1'), CounterColumn('c2', 2), ConsistencyLevel.ONE)
    client.add('key1', ColumnParent('Counter1'), CounterColumn('c3', 3), ConsistencyLevel.ONE)
    time.sleep(0.1)

def _verify_range():
    p = SlicePredicate(slice_range=SliceRange('c1', 'c2', False, 1000))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].column.name == 'c1'
    assert result[1].column.name == 'c2'

    p = SlicePredicate(slice_range=SliceRange('c3', 'c2', True, 1000))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].column.name == 'c3'
    assert result[1].column.name == 'c2'

    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 1000))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 3, result
    
    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 2))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2, result

def _verify_counter_range():
    p = SlicePredicate(slice_range=SliceRange('c1', 'c2', False, 1000))
    result = client.get_slice('key1', ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].counter_column.name == 'c1'
    assert result[1].counter_column.name == 'c2'

    p = SlicePredicate(slice_range=SliceRange('c3', 'c2', True, 1000))
    result = client.get_slice('key1', ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].counter_column.name == 'c3'
    assert result[1].counter_column.name == 'c2'

    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 1000))
    result = client.get_slice('key1', ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 3, result

    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 2))
    result = client.get_slice('key1', ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2, result

def _set_keyspace(keyspace):
    client.set_keyspace(keyspace)

def _insert_super_range():
    client.insert('key1', ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', 0), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 0), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(6), 'value6', 0), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Super1', 'sc3'), Column(_i64(7), 'value7', 0), ConsistencyLevel.ONE)
    time.sleep(0.1)

def _insert_counter_super_range():
    client.add('key1', ColumnParent('SuperCounter1', 'sc1'), CounterColumn(_i64(4), 4), ConsistencyLevel.ONE)
    client.add('key1', ColumnParent('SuperCounter1', 'sc2'), CounterColumn(_i64(5), 5), ConsistencyLevel.ONE)
    client.add('key1', ColumnParent('SuperCounter1', 'sc2'), CounterColumn(_i64(6), 6), ConsistencyLevel.ONE)
    client.add('key1', ColumnParent('SuperCounter1', 'sc3'), CounterColumn(_i64(7), 7), ConsistencyLevel.ONE)
    time.sleep(0.1)

def _verify_super_range():
    p = SlicePredicate(slice_range=SliceRange('sc2', 'sc3', False, 2))
    result = client.get_slice('key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].super_column.name == 'sc2'
    assert result[1].super_column.name == 'sc3'

    p = SlicePredicate(slice_range=SliceRange('sc3', 'sc2', True, 2))
    result = client.get_slice('key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].super_column.name == 'sc3'
    assert result[1].super_column.name == 'sc2'

def _verify_counter_super_range():
    p = SlicePredicate(slice_range=SliceRange('sc2', 'sc3', False, 2))
    result = client.get_slice('key1', ColumnParent('SuperCounter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].counter_super_column.name == 'sc2'
    assert result[1].counter_super_column.name == 'sc3'

    p = SlicePredicate(slice_range=SliceRange('sc3', 'sc2', True, 2))
    result = client.get_slice('key1', ColumnParent('SuperCounter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].counter_super_column.name == 'sc3'
    assert result[1].counter_super_column.name == 'sc2'

def _verify_super(supercf='Super1', key='key1'):
    assert client.get(key, ColumnPath(supercf, 'sc1', _i64(4)), ConsistencyLevel.ONE).column == Column(_i64(4), 'value4', 0)
    slice = [result.super_column
             for result in _big_slice(key, ColumnParent('Super1'))]
    assert slice == _SUPER_COLUMNS, slice

def _expect_exception(fn, type_):
    try:
        r = fn()
    except type_, t:
        return t
    else:
        raise Exception('expected %s; got %s' % (type_.__name__, r))
    
def _expect_missing(fn):
    _expect_exception(fn, NotFoundException)

def get_range_slice(client, parent, predicate, start, end, count, cl):
    kr = KeyRange(start, end, count=count)
    return client.get_range_slices(parent, predicate, kr, cl)
    

class TestMutations(ThriftTester):
    def test_insert(self):
        _set_keyspace('Keyspace1')
        _insert_simple(False)
        time.sleep(0.1)
        _verify_simple()

    def test_empty_slice(self):
        _set_keyspace('Keyspace1')
        assert _big_slice('key1', ColumnParent('Standard2')) == []
        assert _big_slice('key1', ColumnParent('Super1')) == []

    def test_missing_super(self):
        _set_keyspace('Keyspace1')
        _expect_missing(lambda: client.get('key1', ColumnPath('Super1', 'sc1', _i64(1)), ConsistencyLevel.ONE))
        _insert_super()
        _expect_missing(lambda: client.get('key1', ColumnPath('Super1', 'sc1', _i64(1)), ConsistencyLevel.ONE))

    def test_count(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        _insert_super()
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
        assert client.get_count('key1', ColumnParent('Standard2'), p, ConsistencyLevel.ONE) == 0
        assert client.get_count('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 2
        assert client.get_count('key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE) == 2
        assert client.get_count('key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE) == 2

        # Let's make that a little more interesting
        client.insert('key1', ColumnParent('Standard1'), Column('c3', 'value3', 0), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('c4', 'value4', 0), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('c5', 'value5', 0), ConsistencyLevel.ONE)

        p = SlicePredicate(slice_range=SliceRange('c2', 'c4', False, 1000)) 
        assert client.get_count('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 3

    def test_count_paging(self):
        _set_keyspace('Keyspace1')
        _insert_simple()

        # Exercise paging
        column_parent = ColumnParent('Standard1')
        super_column_parent = ColumnParent('Super1', 'sc3')
        # Paging for small columns starts at 1024 columns
        columns_to_insert = [Column('c%d' % (i,), 'value%d' % (i,), 0) for i in xrange(3, 1026)]
        cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(c)) for c in columns_to_insert]}
        client.batch_mutate({'key1' : cfmap }, ConsistencyLevel.ONE)

        p = SlicePredicate(slice_range=SliceRange('', '', False, 2000))
        assert client.get_count('key1', column_parent, p, ConsistencyLevel.ONE) == 1025

        # Ensure that the count limit isn't clobbered
        p = SlicePredicate(slice_range=SliceRange('', '', False, 10))
        assert client.get_count('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 10

    def test_insert_blocking(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        _verify_simple()

    def test_super_insert(self):
        _set_keyspace('Keyspace1')
        _insert_super()
        _verify_super()

    def test_super_get(self):
        _set_keyspace('Keyspace1')
        _insert_super()
        result = client.get('key1', ColumnPath('Super1', 'sc2'), ConsistencyLevel.ONE).super_column
        assert result == _SUPER_COLUMNS[1], result

    def test_super_subcolumn_limit(self):
        _set_keyspace('Keyspace1')
        _insert_super()
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1))
        column_parent = ColumnParent('Super1', 'sc2')
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(_i64(5), 'value5', 0)], slice
        p = SlicePredicate(slice_range=SliceRange('', '', True, 1))
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(_i64(6), 'value6', 0)], slice
        
    def test_long_order(self):
        _set_keyspace('Keyspace1')
        def long_xrange(start, stop, step):
            i = start
            while i < stop:
                yield i
                i += step
        L = []
        for i in long_xrange(0, 104294967296, 429496729):
            name = _i64(i)
            client.insert('key1', ColumnParent('StandardLong1'), Column(name, 'v', 0), ConsistencyLevel.ONE)
            L.append(name)
        slice = [result.column.name for result in _big_slice('key1', ColumnParent('StandardLong1'))]
        assert slice == L, slice
        
    def test_integer_order(self):
        _set_keyspace('Keyspace1')
        def long_xrange(start, stop, step):
            i = start
            while i >= stop:
                yield i
                i -= step
        L = []
        for i in long_xrange(104294967296, 0, 429496729):
            name = _i64(i)
            client.insert('key1', ColumnParent('StandardInteger1'), Column(name, 'v', 0), ConsistencyLevel.ONE)
            L.append(name)
        slice = [result.column.name for result in _big_slice('key1', ColumnParent('StandardInteger1'))]
        L.sort()
        assert slice == L, slice

    def test_time_uuid(self):
        import uuid
        L = []
        _set_keyspace('Keyspace2')
        # 100 isn't enough to fail reliably if the comparator is borked
        for i in xrange(500):
            L.append(uuid.uuid1())
            client.insert('key1', ColumnParent('Super4', 'sc1'), Column(L[-1].bytes, 'value%s' % i, i), ConsistencyLevel.ONE)
        slice = _big_slice('key1', ColumnParent('Super4', 'sc1'))
        assert len(slice) == 500, len(slice)
        for i in xrange(500):
            u = slice[i].column
            assert u.value == 'value%s' % i
            assert u.name == L[i].bytes

        p = SlicePredicate(slice_range=SliceRange('', '', True, 1))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[-1].bytes, 'value499', 499)], slice

        p = SlicePredicate(slice_range=SliceRange('', L[2].bytes, False, 1000))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[0].bytes, 'value0', 0),
                         Column(L[1].bytes, 'value1', 1),
                         Column(L[2].bytes, 'value2', 2)], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, '', True, 1000))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[2].bytes, 'value2', 2),
                         Column(L[1].bytes, 'value1', 1),
                         Column(L[0].bytes, 'value0', 0)], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, '', False, 1))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[2].bytes, 'value2', 2)], slice
        
    def test_long_remove(self):
        column_parent = ColumnParent('StandardLong1')
        sp = SlicePredicate(slice_range=SliceRange('', '', False, 1))
        _set_keyspace('Keyspace1')
        for i in xrange(10):
            parent = ColumnParent('StandardLong1')

            client.insert('key1', parent, Column(_i64(i), 'value1', 10 * i), ConsistencyLevel.ONE)
            client.remove('key1', ColumnPath('StandardLong1'), 10 * i + 1, ConsistencyLevel.ONE)
            slice = client.get_slice('key1', column_parent, sp, ConsistencyLevel.ONE)
            assert slice == [], slice
            # resurrect
            client.insert('key1', parent, Column(_i64(i), 'value2', 10 * i + 2), ConsistencyLevel.ONE)
            slice = [result.column
                     for result in client.get_slice('key1', column_parent, sp, ConsistencyLevel.ONE)]
            assert slice == [Column(_i64(i), 'value2', 10 * i + 2)], (slice, i)
        
    def test_integer_remove(self):
        column_parent = ColumnParent('StandardInteger1')
        sp = SlicePredicate(slice_range=SliceRange('', '', False, 1))
        _set_keyspace('Keyspace1')
        for i in xrange(10):
            parent = ColumnParent('StandardInteger1')

            client.insert('key1', parent, Column(_i64(i), 'value1', 10 * i), ConsistencyLevel.ONE)
            client.remove('key1', ColumnPath('StandardInteger1'), 10 * i + 1, ConsistencyLevel.ONE)
            slice = client.get_slice('key1', column_parent, sp, ConsistencyLevel.ONE)
            assert slice == [], slice
            # resurrect
            client.insert('key1', parent, Column(_i64(i), 'value2', 10 * i + 2), ConsistencyLevel.ONE)
            slice = [result.column
                     for result in client.get_slice('key1', column_parent, sp, ConsistencyLevel.ONE)]
            assert slice == [Column(_i64(i), 'value2', 10 * i + 2)], (slice, i)

    def test_batch_insert(self):
        _set_keyspace('Keyspace1')
        _insert_batch(False)
        time.sleep(0.1)
        _verify_batch()

    def test_batch_insert_blocking(self):
        _set_keyspace('Keyspace1')
        _insert_batch(True)
        _verify_batch()
        
    def test_batch_mutate_standard_columns(self):
        _set_keyspace('Keyspace1')
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in  range(27,32)] 
        mutations = [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)
        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for key in keys:
               _assert_column(column_family, key, 'c1', 'value1')

    def test_batch_mutate_standard_columns_blocking(self):
        _set_keyspace('Keyspace1')
        
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in  range(38,46)]
         
        mutations = [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)
        keyed_mutations = dict((key, mutation_map) for key in keys)
        
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for key in keys:
                _assert_column(column_family, key, 'c1', 'value1')

    def test_batch_mutate_remove_standard_columns(self):
        _set_keyspace('Keyspace1')
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in range(11,21)]
        _insert_multi(keys)

        mutations = [Mutation(deletion=Deletion(20, predicate=SlicePredicate(column_names=[c.name]))) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for c in _SIMPLE_COLUMNS:
                for key in keys:
                    _assert_no_columnpath(key, ColumnPath(column_family, column=c.name))

    def test_batch_mutate_remove_standard_row(self):
        _set_keyspace('Keyspace1')
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in range(11,21)]
        _insert_multi(keys)

        mutations = [Mutation(deletion=Deletion(20))]
        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for c in _SIMPLE_COLUMNS:
                for key in keys:
                    _assert_no_columnpath(key, ColumnPath(column_family, column=c.name))

    def test_batch_mutate_remove_super_columns_with_standard_under(self):
        _set_keyspace('Keyspace1')
        column_families = ['Super1', 'Super2']
        keys = ['key_%d' % i for i in range(11,21)]
        _insert_super()

        mutations = []
        for sc in _SUPER_COLUMNS:
            names = []
            for c in sc.columns:
                names.append(c.name)
            mutations.append(Mutation(deletion=Deletion(20, super_column=c.name, predicate=SlicePredicate(column_names=names))))

        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)
        for column_family in column_families:
            for sc in _SUPER_COLUMNS:
                for c in sc.columns:
                    for key in keys:
                        _assert_no_columnpath(key, ColumnPath(column_family, super_column=sc.name, column=c.name))

    def test_batch_mutate_remove_super_columns_with_none_given_underneath(self):
        _set_keyspace('Keyspace1')
        
        keys = ['key_%d' % i for i in range(17,21)]

        for key in keys:
            _insert_super(key)

        mutations = []

        for sc in _SUPER_COLUMNS:
            mutations.append(Mutation(deletion=Deletion(20,
                                                        super_column=sc.name)))

        mutation_map = {'Super1': mutations}

        keyed_mutations = dict((key, mutation_map) for key in keys)

        # Sanity check
        for sc in _SUPER_COLUMNS:
            for key in keys:
                _assert_columnpath_exists(key, ColumnPath('Super1', super_column=sc.name))

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for sc in _SUPER_COLUMNS:
            for c in sc.columns:
                for key in keys:
                    _assert_no_columnpath(key, ColumnPath('Super1', super_column=sc.name))
    
    def test_batch_mutate_remove_super_columns_entire_row(self):
        _set_keyspace('Keyspace1')
        
        keys = ['key_%d' % i for i in range(17,21)]

        for key in keys:
            _insert_super(key)

        mutations = []

        mutations.append(Mutation(deletion=Deletion(20)))

        mutation_map = {'Super1': mutations}

        keyed_mutations = dict((key, mutation_map) for key in keys)

        # Sanity check
        for sc in _SUPER_COLUMNS:
            for key in keys:
                _assert_columnpath_exists(key, ColumnPath('Super1', super_column=sc.name))

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for sc in _SUPER_COLUMNS:
          for key in keys:
            _assert_no_columnpath(key, ColumnPath('Super1', super_column=sc.name))

    def test_batch_mutate_insertions_and_deletions(self):
        _set_keyspace('Keyspace1')
        
        first_insert = SuperColumn("sc1",
                                   columns=[Column(_i64(20), 'value20', 3),
                                            Column(_i64(21), 'value21', 3)])
        second_insert = SuperColumn("sc1",
                                    columns=[Column(_i64(20), 'value20', 3),
                                             Column(_i64(21), 'value21', 3)])
        first_deletion = {'super_column': "sc1",
                          'predicate': SlicePredicate(column_names=[_i64(22), _i64(23)])}
        second_deletion = {'super_column': "sc2",
                           'predicate': SlicePredicate(column_names=[_i64(22), _i64(23)])}

        keys = ['key_30', 'key_31']
        for key in keys:
            sc = SuperColumn('sc1',[Column(_i64(22), 'value22', 0),
                                    Column(_i64(23), 'value23', 0)])
            cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=sc))]}
            client.batch_mutate({key: cfmap}, ConsistencyLevel.ONE)

            sc2 = SuperColumn('sc2', [Column(_i64(22), 'value22', 0),
                                      Column(_i64(23), 'value23', 0)])
            cfmap2 = {'Super2': [Mutation(ColumnOrSuperColumn(super_column=sc2))]}
            client.batch_mutate({key: cfmap2}, ConsistencyLevel.ONE)

        cfmap3 = {
            'Super1' : [Mutation(ColumnOrSuperColumn(super_column=first_insert)),
                        Mutation(deletion=Deletion(3, **first_deletion))],
        
            'Super2' : [Mutation(deletion=Deletion(2, **second_deletion)),
                        Mutation(ColumnOrSuperColumn(super_column=second_insert))]
            }

        keyed_mutations = dict((key, cfmap3) for key in keys)
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for key in keys:
            for c in [_i64(22), _i64(23)]:
                _assert_no_columnpath(key, ColumnPath('Super1', super_column='sc1', column=c))
                _assert_no_columnpath(key, ColumnPath('Super2', super_column='sc2', column=c))

            for c in [_i64(20), _i64(21)]:
                _assert_columnpath_exists(key, ColumnPath('Super1', super_column='sc1', column=c))
                _assert_columnpath_exists(key, ColumnPath('Super2', super_column='sc1', column=c))

    def test_bad_system_calls(self):
        def duplicate_index_names():
            _set_keyspace('Keyspace1')
            cd1 = ColumnDef('foo', 'BytesType', IndexType.KEYS, 'i')
            cd2 = ColumnDef('bar', 'BytesType', IndexType.KEYS, 'i')
            cf = CfDef('Keyspace1', 'BadCF', column_metadata=[cd1, cd2])
            client.system_add_column_family(cf)
        _expect_exception(duplicate_index_names, InvalidRequestException)

    def test_bad_batch_calls(self):
        # mutate_does_not_accept_cosc_and_deletion_in_same_mutation
        def too_full():
            _set_keyspace('Keyspace1')
            col = ColumnOrSuperColumn(column=Column("foo", 'bar', 0))
            dele = Deletion(2, predicate=SlicePredicate(column_names=['baz']))
            client.batch_mutate({'key_34': {'Standard1': [Mutation(col, dele)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(too_full, InvalidRequestException)

        # test_batch_mutate_does_not_yet_accept_slice_ranges
        def send_range():
            _set_keyspace('Keyspace1')
            sp = SlicePredicate(slice_range=SliceRange(start='0', finish="", count=10))
            d = Deletion(2, predicate=sp)
            client.batch_mutate({'key_35': {'Standard1':[Mutation(deletion=d)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(send_range, InvalidRequestException)

        # test_batch_mutate_does_not_accept_cosc_on_undefined_cf:
        def bad_cf():
            _set_keyspace('Keyspace1')
            col = ColumnOrSuperColumn(column=Column("foo", 'bar', 0))
            client.batch_mutate({'key_36': {'Undefined': [Mutation(col)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(bad_cf, InvalidRequestException)

        # test_batch_mutate_does_not_accept_deletion_on_undefined_cf
        def bad_cf():
            _set_keyspace('Keyspace1')
            d = Deletion(2, predicate=SlicePredicate(column_names=['baz']))
            client.batch_mutate({'key_37': {'Undefined':[Mutation(deletion=d)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(bad_cf, InvalidRequestException)

        # a column value that does not match the declared validator
        def send_string_instead_of_long():
            _set_keyspace('Keyspace1')
            col = ColumnOrSuperColumn(column=Column('birthdate', 'bar', 0))
            client.batch_mutate({'key_38': {'Indexed1': [Mutation(col)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(send_string_instead_of_long, InvalidRequestException)

    def test_column_name_lengths(self):
        _set_keyspace('Keyspace1')
        _expect_exception(lambda: client.insert('key1', ColumnParent('Standard1'), Column('', 'value', 0), ConsistencyLevel.ONE), InvalidRequestException)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*1, 'value', 0), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*127, 'value', 0), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*128, 'value', 0), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*129, 'value', 0), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*255, 'value', 0), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*256, 'value', 0), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*257, 'value', 0), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*(2**16 - 1), 'value', 0), ConsistencyLevel.ONE)
        _expect_exception(lambda: client.insert('key1', ColumnParent('Standard1'), Column('x'*(2**16), 'value', 0), ConsistencyLevel.ONE), InvalidRequestException)

    def test_bad_calls(self):
        _set_keyspace('Keyspace1')
        # missing arguments
        _expect_exception(lambda: client.insert(None, None, None, None), TApplicationException)
        # supercolumn in a non-super CF
        _expect_exception(lambda: client.insert('key1', ColumnParent('Standard1', 'x'), Column('y', 'value', 0), ConsistencyLevel.ONE), InvalidRequestException)
        # no supercolumn in a super CF
        _expect_exception(lambda: client.insert('key1', ColumnParent('Super1'), Column('y', 'value', 0), ConsistencyLevel.ONE), InvalidRequestException)
        # column but no supercolumn in remove
        _expect_exception(lambda: client.remove('key1', ColumnPath('Super1', column='x'), 0, ConsistencyLevel.ONE), InvalidRequestException)
        # super column in non-super CF
        _expect_exception(lambda: client.remove('key1', ColumnPath('Standard1', 'y', 'x'), 0, ConsistencyLevel.ONE), InvalidRequestException)
        # key too long
        _expect_exception(lambda: client.get('x' * 2**16, ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE), InvalidRequestException)
        # empty key
        _expect_exception(lambda: client.get('', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE), InvalidRequestException)
        cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c)) for c in _SUPER_COLUMNS],
                 'Super2': [Mutation(ColumnOrSuperColumn(super_column=c)) for c in _SUPER_COLUMNS]}
        _expect_exception(lambda: client.batch_mutate({'': cfmap}, ConsistencyLevel.ONE), InvalidRequestException)
        # empty column name
        _expect_exception(lambda: client.get('key1', ColumnPath('Standard1', column=''), ConsistencyLevel.ONE), InvalidRequestException)
        # get doesn't specify column name
        _expect_exception(lambda: client.get('key1', ColumnPath('Standard1'), ConsistencyLevel.ONE), InvalidRequestException)
        # supercolumn in a non-super CF
        _expect_exception(lambda: client.get('key1', ColumnPath('Standard1', 'x', 'y'), ConsistencyLevel.ONE), InvalidRequestException)
        # get doesn't specify supercolumn name
        _expect_exception(lambda: client.get('key1', ColumnPath('Super1'), ConsistencyLevel.ONE), InvalidRequestException)
        # invalid CF
        _expect_exception(lambda: get_range_slice(client, ColumnParent('S'), SlicePredicate(column_names=['', '']), '', '', 5, ConsistencyLevel.ONE), InvalidRequestException)
        # 'x' is not a valid Long
        _expect_exception(lambda: client.insert('key1', ColumnParent('Super1', 'sc1'), Column('x', 'value', 0), ConsistencyLevel.ONE), InvalidRequestException)
        # start is not a valid Long
        p = SlicePredicate(slice_range=SliceRange('x', '', False, 1))
        column_parent = ColumnParent('StandardLong1')
        _expect_exception(lambda: client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish
        p = SlicePredicate(slice_range=SliceRange(_i64(10), _i64(0), False, 1))
        column_parent = ColumnParent('StandardLong1')
        _expect_exception(lambda: client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start is not a valid Long, supercolumn version
        p = SlicePredicate(slice_range=SliceRange('x', '', False, 1))
        column_parent = ColumnParent('Super1', 'sc1')
        _expect_exception(lambda: client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish, supercolumn version
        p = SlicePredicate(slice_range=SliceRange(_i64(10), _i64(0), False, 1))
        column_parent = ColumnParent('Super1', 'sc1')
        _expect_exception(lambda: client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish, key version
        _expect_exception(lambda: get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['']), 'z', 'a', 1, ConsistencyLevel.ONE), InvalidRequestException)
        # ttl must be positive
        column = Column('cttl1', 'value1', 0, 0)
        _expect_exception(lambda: client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # don't allow super_column in Deletion for standard ColumnFamily
        deletion = Deletion(1, 'supercolumn', None)
        mutation = Mutation(deletion=deletion)
        mutations = {'key' : {'Standard1' : [mutation]}}
        _expect_exception(lambda: client.batch_mutate(mutations, ConsistencyLevel.QUORUM),
                          InvalidRequestException)
        # 'x' is not a valid long
        deletion = Deletion(1, 'x', None)
        mutation = Mutation(deletion=deletion)
        mutations = {'key' : {'Super5' : [mutation]}}
        _expect_exception(lambda: client.batch_mutate(mutations, ConsistencyLevel.QUORUM), InvalidRequestException)
        # counters don't support ANY
        _expect_exception(lambda: client.add('key1', ColumnParent('Counter1', 'x'), CounterColumn('y', 1), ConsistencyLevel.ANY), InvalidRequestException)

    def test_batch_insert_super(self):
         _set_keyspace('Keyspace1')
         cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c))
                             for c in _SUPER_COLUMNS],
                  'Super2': [Mutation(ColumnOrSuperColumn(super_column=c))
                             for c in _SUPER_COLUMNS]}
         client.batch_mutate({'key1': cfmap}, ConsistencyLevel.ONE)
         _verify_super('Super1')
         _verify_super('Super2')

    def test_batch_insert_super_blocking(self):
         _set_keyspace('Keyspace1')
         cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c)) 
                             for c in _SUPER_COLUMNS],
                  'Super2': [Mutation(ColumnOrSuperColumn(super_column=c))
                             for c in _SUPER_COLUMNS]}
         client.batch_mutate({'key1': cfmap}, ConsistencyLevel.ONE)
         _verify_super('Super1')
         _verify_super('Super2')

    def test_cf_remove_column(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        client.remove('key1', ColumnPath('Standard1', column='c1'), 1, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('key1', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE))
        assert client.get('key1', ColumnPath('Standard1', column='c2'), ConsistencyLevel.ONE).column \
            == Column('c2', 'value2', 0)
        assert _big_slice('key1', ColumnParent('Standard1')) \
            == [ColumnOrSuperColumn(column=Column('c2', 'value2', 0))]

        # New insert, make sure it shows up post-remove:
        client.insert('key1', ColumnParent('Standard1'), Column('c3', 'value3', 0), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice('key1', ColumnParent('Standard1'))]
        assert columns == [Column('c2', 'value2', 0), Column('c3', 'value3', 0)], columns

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed
        client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', 0), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice('key1', ColumnParent('Standard1'))]
        assert columns == [Column('c2', 'value2', 0), Column('c3', 'value3', 0)], columns
        # Next, w/ a newer timestamp; it should come back:
        client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', 2), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice('key1', ColumnParent('Standard1'))]
        assert columns == [Column('c1', 'value1', 2), Column('c2', 'value2', 0), Column('c3', 'value3', 0)], columns


    def test_cf_remove(self):
        _set_keyspace('Keyspace1')
        
        _insert_simple()
        _insert_super()

        # Remove the key1:Standard1 cf; verify super is unaffected
        client.remove('key1', ColumnPath('Standard1'), 3, ConsistencyLevel.ONE)
        assert _big_slice('key1', ColumnParent('Standard1')) == []
        _verify_super()

        # Test resurrection.  First, re-insert a value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', 0), ConsistencyLevel.ONE)
        assert _big_slice('key1', ColumnParent('Standard1')) == []
        # Next, w/ a newer timestamp; it should come back:
        client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', 4), ConsistencyLevel.ONE)
        result = _big_slice('key1', ColumnParent('Standard1'))
        assert result == [ColumnOrSuperColumn(column=Column('c1', 'value1', 4))], result

        # check removing the entire super cf, too.
        client.remove('key1', ColumnPath('Super1'), 3, ConsistencyLevel.ONE)
        assert _big_slice('key1', ColumnParent('Super1')) == []
        assert _big_slice('key1', ColumnParent('Super1', 'sc1')) == []


    def test_super_cf_remove_and_range_slice(self):
        _set_keyspace('Keyspace1')

        client.insert('key3', ColumnParent('Super1', 'sc1'), Column(_i64(1), 'v1', 0), ConsistencyLevel.ONE)
        client.remove('key3', ColumnPath('Super1', 'sc1'), 5, ConsistencyLevel.ONE)

        rows = {}
        for row in get_range_slice(client, ColumnParent('Super1'), SlicePredicate(slice_range=SliceRange('', '', False, 1000)), '', '', 1000, ConsistencyLevel.ONE):
            scs = [cosc.super_column for cosc in row.columns]
            rows[row.key] = scs
        assert rows == {'key3': []}, rows

    def test_super_cf_remove_column(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove('key1', ColumnPath('Super1', 'sc2', _i64(5)), 5, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('key1', ColumnPath('Super1', 'sc2', _i64(5)), ConsistencyLevel.ONE))
        super_columns = [result.super_column for result in _big_slice('key1', ColumnParent('Super1'))]
        assert super_columns == [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
                                 SuperColumn(name='sc2', columns=[Column(_i64(6), 'value6', 0)])]
        _verify_simple()

        # New insert, make sure it shows up post-remove:
        client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(7), 'value7', 0), ConsistencyLevel.ONE)
        super_columns_expected = [SuperColumn(name='sc1', 
                                              columns=[Column(_i64(4), 'value4', 0)]),
                                  SuperColumn(name='sc2', 
                                              columns=[Column(_i64(6), 'value6', 0), Column(_i64(7), 'value7', 0)])]

        super_columns = [result.super_column for result in _big_slice('key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, actual

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 0), ConsistencyLevel.ONE)

        super_columns = [result.super_column for result in _big_slice('key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Next, w/ a newer timestamp; it should come back
        client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 6), ConsistencyLevel.ONE)
        super_columns = [result.super_column for result in _big_slice('key1', ColumnParent('Super1'))]
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]), 
                                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 6), 
                                                                   Column(_i64(6), 'value6', 0), 
                                                                   Column(_i64(7), 'value7', 0)])]
        assert super_columns == super_columns_expected, super_columns

        # shouldn't be able to specify a column w/o a super column for remove
        cp = ColumnPath(column_family='Super1', column='sc2')
        e = _expect_exception(lambda: client.remove('key1', cp, 5, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("column cannot be specified without") >= 0

    def test_super_cf_remove_supercolumn(self):
        _set_keyspace('Keyspace1')
        
        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove('key1', ColumnPath('Super1', 'sc2'), 5, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('key1', ColumnPath('Super1', 'sc2', _i64(5)), ConsistencyLevel.ONE))
        super_columns = _big_slice('key1', ColumnParent('Super1', 'sc2'))
        assert super_columns == [], super_columns
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)])]
        super_columns = [result.super_column
                         for result in _big_slice('key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns
        _verify_simple()

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 1), ConsistencyLevel.ONE)
        super_columns = [result.super_column
                         for result in _big_slice('key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Next, w/ a newer timestamp; it should come back
        client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 6), ConsistencyLevel.ONE)
        super_columns = [result.super_column
                         for result in _big_slice('key1', ColumnParent('Super1'))]
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
                                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 6)])]
        assert super_columns == super_columns_expected, super_columns

        # check slicing at the subcolumn level too
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
        columns = [result.column
                   for result in client.get_slice('key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE)]
        assert columns == [Column(_i64(5), 'value5', 6)], columns


    def test_super_cf_resurrect_subcolumn(self):
        _set_keyspace('Keyspace1')
        key = 'vijay'
        client.insert(key, ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', 0), ConsistencyLevel.ONE)

        client.remove(key, ColumnPath('Super1', 'sc1'), 1, ConsistencyLevel.ONE)

        client.insert(key, ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', 2), ConsistencyLevel.ONE)

        result = client.get(key, ColumnPath('Super1', 'sc1'), ConsistencyLevel.ONE)
        assert result.super_column.columns is not None, result.super_column


    def test_empty_range(self):
        _set_keyspace('Keyspace1')
        assert get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE) == []
        _insert_simple()
        assert get_range_slice(client, ColumnParent('Super1'), SlicePredicate(column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE) == []

    def test_range_with_remove(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        assert get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), 'key1', '', 1000, ConsistencyLevel.ONE)[0].key == 'key1'

        client.remove('key1', ColumnPath('Standard1', column='c1'), 1, ConsistencyLevel.ONE)
        client.remove('key1', ColumnPath('Standard1', column='c2'), 1, ConsistencyLevel.ONE)
        actual = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c2']), '', '', 1000, ConsistencyLevel.ONE)
        assert actual == [KeySlice(columns=[], key='key1')], actual

    def test_range_with_remove_cf(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        assert get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), 'key1', '', 1000, ConsistencyLevel.ONE)[0].key == 'key1'

        client.remove('key1', ColumnPath('Standard1'), 1, ConsistencyLevel.ONE)
        actual = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE)
        assert actual == [KeySlice(columns=[], key='key1')], actual

    def test_range_collation(self):
        _set_keyspace('Keyspace1')
        for key in ['-a', '-b', 'a', 'b'] + [str(i) for i in xrange(100)]:
            client.insert(key, ColumnParent('Standard1'), Column(key, 'v', 0), ConsistencyLevel.ONE)

        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '', '', 1000, ConsistencyLevel.ONE)
        # note the collated ordering rather than ascii
        L = ['0', '1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2', '20', '21', '22', '23', '24', '25', '26', '27','28', '29', '3', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '4', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '5', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '6', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '7', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '8', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '9', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', 'a', '-a', 'b', '-b']
        assert len(slices) == len(L)
        for key, ks in zip(L, slices):
            assert key == ks.key

    def test_range_partial(self):
        _set_keyspace('Keyspace1')
        
        for key in ['-a', '-b', 'a', 'b'] + [str(i) for i in xrange(100)]:
            client.insert(key, ColumnParent('Standard1'), Column(key, 'v', 0), ConsistencyLevel.ONE)

        def check_slices_against_keys(keyList, sliceList):
            assert len(keyList) == len(sliceList)
            for key, ks in zip(keyList, sliceList):
                assert key == ks.key
        
        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), 'a', '', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['a', '-a', 'b', '-b'], slices)
        
        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '', '15', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['0', '1', '10', '11', '12', '13', '14', '15'], slices)

        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '50', '51', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['50', '51'], slices)
        
        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '1', '', 10, ConsistencyLevel.ONE)
        check_slices_against_keys(['1', '10', '11', '12', '13', '14', '15', '16', '17', '18'], slices)

    def test_get_slice_range(self):
        _set_keyspace('Keyspace1')
        _insert_range()
        _verify_range()
        
    def test_get_slice_super_range(self):
        _set_keyspace('Keyspace1')
        _insert_super_range()
        _verify_super_range()

    def test_get_range_slices_tokens(self):
        _set_keyspace('Keyspace2')
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert(key, ColumnParent('Super3', 'sc1'), Column(cname, 'v-' + cname, 0), ConsistencyLevel.ONE)

        cp = ColumnParent('Super3', 'sc1')
        predicate = SlicePredicate(column_names=['col1', 'col3'])
        range = KeyRange(start_token='55', end_token='55', count=100)
        result = client.get_range_slices(cp, predicate, range, ConsistencyLevel.ONE)
        assert len(result) == 5
        assert result[0].columns[0].column.name == 'col1'
        assert result[0].columns[1].column.name == 'col3'

    def test_get_range_slice_super(self):
        _set_keyspace('Keyspace2')
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert(key, ColumnParent('Super3', 'sc1'), Column(cname, 'v-' + cname, 0), ConsistencyLevel.ONE)

        cp = ColumnParent('Super3', 'sc1')
        result = get_range_slice(client, cp, SlicePredicate(column_names=['col1', 'col3']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3
        assert result[0].columns[0].column.name == 'col1'
        assert result[0].columns[1].column.name == 'col3'

        cp = ColumnParent('Super3')
        result = get_range_slice(client, cp, SlicePredicate(column_names=['sc1']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3
        assert list(set(row.columns[0].super_column.name for row in result))[0] == 'sc1'
        
    def test_get_range_slice(self):
        _set_keyspace('Keyspace1')
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert(key, ColumnParent('Standard1'), Column(cname, 'v-' + cname, 0), ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')

        # test empty slice
        result = get_range_slice(client, cp, SlicePredicate(column_names=['col1', 'col3']), 'key6', '', 1, ConsistencyLevel.ONE)
        assert len(result) == 0

        # test empty columns
        result = get_range_slice(client, cp, SlicePredicate(column_names=['a']), 'key2', '', 1, ConsistencyLevel.ONE)
        assert len(result) == 1
        assert len(result[0].columns) == 0

        # test column_names predicate
        result = get_range_slice(client, cp, SlicePredicate(column_names=['col1', 'col3']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3, result
        assert result[0].columns[0].column.name == 'col1'
        assert result[0].columns[1].column.name == 'col3'

        # row limiting via count.
        result = get_range_slice(client, cp, SlicePredicate(column_names=['col1', 'col3']), 'key2', 'key4', 1, ConsistencyLevel.ONE)
        assert len(result) == 1

        # test column slice predicate
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start='col2', finish='col4', reversed=False, count=5)), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].key == 'key1'
        assert result[1].key == 'key2'
        assert len(result[0].columns) == 3
        assert result[0].columns[0].column.name == 'col2'
        assert result[0].columns[2].column.name == 'col4'

        # col limiting via count
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start='col2', finish='col4', reversed=False, count=2)), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 2

        # and reversed 
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start='col4', finish='col2', reversed=True, count=5)), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert result[0].columns[0].column.name == 'col4'
        assert result[0].columns[2].column.name == 'col2'

        # row limiting via count
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start='col2', finish='col4', reversed=False, count=5)), 'key1', 'key2', 1, ConsistencyLevel.ONE)
        assert len(result) == 1

        # removed data
        client.remove('key1', ColumnPath('Standard1', column='col1'), 1, ConsistencyLevel.ONE)
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange('', '')), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert len(result) == 2, result
        assert result[0].columns[0].column.name == 'col2', result[0].columns[0].column.name
        assert result[1].columns[0].column.name == 'col1'
        
    
    def test_wrapped_range_slices(self):
        _set_keyspace('Keyspace1')

        def copp_token(key):
            # I cheated and generated this from Java
            return {'a': '00530000000100000001', 
                    'b': '00540000000100000001', 
                    'c': '00550000000100000001',
                    'd': '00560000000100000001', 
                    'e': '00580000000100000001'}[key]

        for key in ['a', 'b', 'c', 'd', 'e']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert(key, ColumnParent('Standard1'), Column(cname, 'v-' + cname, 0), ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')

        result = client.get_range_slices(cp, SlicePredicate(column_names=['col1', 'col3']), KeyRange(start_token=copp_token('e'), end_token=copp_token('e')), ConsistencyLevel.ONE)
        assert [row.key for row in result] == ['a', 'b', 'c', 'd', 'e',], [row.key for row in result]

        result = client.get_range_slices(cp, SlicePredicate(column_names=['col1', 'col3']), KeyRange(start_token=copp_token('c'), end_token=copp_token('c')), ConsistencyLevel.ONE)
        assert [row.key for row in result] == ['d', 'e', 'a', 'b', 'c',], [row.key for row in result]
        

    def test_get_slice_by_names(self):
        _set_keyspace('Keyspace1')
        _insert_range()
        p = SlicePredicate(column_names=['c1', 'c2'])
        result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) 
        assert len(result) == 2
        assert result[0].column.name == 'c1'
        assert result[1].column.name == 'c2'

        _insert_super()
        p = SlicePredicate(column_names=[_i64(4)])
        result = client.get_slice('key1', ColumnParent('Super1', 'sc1'), p, ConsistencyLevel.ONE) 
        assert len(result) == 1
        assert result[0].column.name == _i64(4)

    def test_multiget_slice(self):
        """Insert multiple keys and retrieve them using the multiget_slice interface"""

        _set_keyspace('Keyspace1')
        # Generate a list of 10 keys and insert them
        num_keys = 10
        keys = ['key'+str(i) for i in range(1, num_keys+1)]
        _insert_multi(keys)

        # Retrieve all 10 key slices
        rows = _big_multislice(keys, ColumnParent('Standard1'))
        keys1 = rows.keys().sort()
        keys2 = keys.sort()

        columns = [ColumnOrSuperColumn(c) for c in _SIMPLE_COLUMNS]
        # Validate if the returned rows have the keys requested and if the ColumnOrSuperColumn is what was inserted
        for key in keys:
            assert rows.has_key(key) == True
            assert columns == rows[key]

    def test_multi_count(self):
        """Insert multiple keys and count them using the multiget interface"""
        _set_keyspace('Keyspace1')

        # Generate a list of 10 keys countaining 1 to 10 columns and insert them
        num_keys = 10
        for i in range(1, num_keys+1):
          key = 'key'+str(i)
          for j in range(1, i+1):
            client.insert(key, ColumnParent('Standard1'), Column('c'+str(j), 'value'+str(j), 0), ConsistencyLevel.ONE)

        # Count columns in all 10 keys
        keys = ['key'+str(i) for i in range(1, num_keys+1)]
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
        counts = client.multiget_count(keys, ColumnParent('Standard1'), p, ConsistencyLevel.ONE)

        # Check the returned counts
        for i in range(1, num_keys+1):
          key = 'key'+str(i)
          assert counts[key] == i

    def test_batch_mutate_super_deletion(self):
        _set_keyspace('Keyspace1')
        _insert_super('test')
        d = Deletion(1, predicate=SlicePredicate(column_names=['sc1']))
        cfmap = {'Super1': [Mutation(deletion=d)]}
        client.batch_mutate({'test': cfmap}, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('key1', ColumnPath('Super1', 'sc1'), ConsistencyLevel.ONE))

    def test_super_reinsert(self):
        _set_keyspace('Keyspace1')
        for x in xrange(3):
            client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(x), 'value', 1), ConsistencyLevel.ONE)

        client.remove('key1', ColumnPath('Super1'), 2, ConsistencyLevel.ONE)

        for x in xrange(3):
            client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(x + 3), 'value', 3), ConsistencyLevel.ONE)

        for n in xrange(1, 4):
            p =  SlicePredicate(slice_range=SliceRange('', '', False, n))
            slice = client.get_slice('key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE)
            assert len(slice) == n, "expected %s results; found %s" % (n, slice)

    def test_describe_keyspace(self):
        kspaces = client.describe_keyspaces()
        assert len(kspaces) == 3, kspaces # ['Keyspace2', 'Keyspace1', 'system']

        sysks = client.describe_keyspace("system")
        assert sysks in kspaces

        ks1 = client.describe_keyspace("Keyspace1")
        assert ks1.strategy_options['replication_factor'] == '1', ks1.strategy_options
        for cf in ks1.cf_defs:
            if cf.name == "Standard1":
                cf0 = cf
                break;
        assert cf0.comparator_type == "org.apache.cassandra.db.marshal.BytesType"

    def test_describe(self):
        server_version = client.describe_version()
        assert server_version == VERSION, (server_version, VERSION)
        assert client.describe_cluster_name() == 'Test Cluster'

    def test_describe_ring(self):
        assert list(client.describe_ring('Keyspace1'))[0].endpoints == ['127.0.0.1']

    def test_describe_partitioner(self):
        # Make sure this just reads back the values from the config.
        assert client.describe_partitioner() == "org.apache.cassandra.dht.CollatingOrderPreservingPartitioner"

    def test_describe_snitch(self):
        assert client.describe_snitch() == "org.apache.cassandra.locator.SimpleSnitch"

    def test_invalid_ks_names(self):
        def invalid_keyspace():
            client.system_add_keyspace(KsDef('in-valid', 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor':'1'}, cf_defs=[]))
        _expect_exception(invalid_keyspace, InvalidRequestException)

    def test_invalid_strategy_class(self):
        def add_invalid_keyspace():
            client.system_add_keyspace(KsDef('ValidKs', 'InvalidStrategyClass', {}, cf_defs=[]))
        exc = _expect_exception(add_invalid_keyspace, InvalidRequestException)
        s = str(exc)
        assert s.find("InvalidStrategyClass") > -1, s
        assert s.find("Unable to find replication strategy") > -1, s

        def update_invalid_keyspace():
            client.system_add_keyspace(KsDef('ValidKsForUpdate', 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor':'1'}, cf_defs=[]))
            client.system_update_keyspace(KsDef('ValidKsForUpdate', 'InvalidStrategyClass', {}, cf_defs=[]))

        exc = _expect_exception(update_invalid_keyspace, InvalidRequestException)
        s = str(exc)
        assert s.find("InvalidStrategyClass") > -1, s
        assert s.find("Unable to find replication strategy") > -1, s

    def test_invalid_cf_names(self):
        def invalid_cf():
            _set_keyspace('Keyspace1')
            newcf = CfDef('Keyspace1', 'in-valid')
            client.system_add_column_family(newcf)
        _expect_exception(invalid_cf, InvalidRequestException)
        
        def invalid_cf_inside_new_ks():
            cf = CfDef('ValidKsName_invalid_cf', 'in-valid')
            _set_keyspace('system')
            client.system_add_keyspace(KsDef('ValidKsName_invalid_cf', 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor': '1'}, cf_defs=[cf]))
        _expect_exception(invalid_cf_inside_new_ks, InvalidRequestException)
    
    def test_system_cf_recreate(self):
        "ensures that keyspaces and column familes can be dropped and recreated in short order"
        for x in range(2):
            
            keyspace = 'test_cf_recreate'
            cf_name = 'recreate_cf'
            
            # create
            newcf = CfDef(keyspace, cf_name)
            newks = KsDef(keyspace, 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor':'1'}, cf_defs=[newcf])
            client.system_add_keyspace(newks)
            _set_keyspace(keyspace)
            
            # insert
            client.insert('key0', ColumnParent(cf_name), Column('colA', 'colA-value', 0), ConsistencyLevel.ONE)
            col1 = client.get_slice('key0', ColumnParent(cf_name), SlicePredicate(slice_range=SliceRange('', '', False, 100)), ConsistencyLevel.ONE)[0].column
            assert col1.name == 'colA' and col1.value == 'colA-value'
                    
            # drop
            client.system_drop_column_family(cf_name) 
            
            # recreate
            client.system_add_column_family(newcf)
            
            # query
            cosc_list = client.get_slice('key0', ColumnParent(cf_name), SlicePredicate(slice_range=SliceRange('', '', False, 100)), ConsistencyLevel.ONE)
            # this was failing prior to CASSANDRA-1477.
            assert len(cosc_list) == 0 , 'cosc length test failed'
            
            client.system_drop_keyspace(keyspace)
    
    def test_system_keyspace_operations(self):
        # create.  note large RF, this is OK
        keyspace = KsDef('CreateKeyspace', 
                         'org.apache.cassandra.locator.SimpleStrategy', 
                         {'replication_factor': '10'},
                         cf_defs=[CfDef('CreateKeyspace', 'CreateKsCf')])
        client.system_add_keyspace(keyspace)
        newks = client.describe_keyspace('CreateKeyspace')
        assert 'CreateKsCf' in [x.name for x in newks.cf_defs]
        
        _set_keyspace('CreateKeyspace')
        
        # modify valid
        modified_keyspace = KsDef('CreateKeyspace', 
                                  'org.apache.cassandra.locator.OldNetworkTopologyStrategy', 
                                  {'replication_factor': '1'},
                                  cf_defs=[])
        client.system_update_keyspace(modified_keyspace)
        modks = client.describe_keyspace('CreateKeyspace')
        assert modks.strategy_class == modified_keyspace.strategy_class
        assert modks.strategy_options == modified_keyspace.strategy_options
        
        # drop
        client.system_drop_keyspace('CreateKeyspace')
        def get_second_ks():
            client.describe_keyspace('CreateKeyspace')
        _expect_exception(get_second_ks, NotFoundException)
        
    def test_create_then_drop_ks(self):
        keyspace = KsDef('AddThenDrop', 
                strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                strategy_options={'replication_factor':'1'},
                cf_defs=[])
        def test_existence():
            client.describe_keyspace(keyspace.name)
        _expect_exception(test_existence, NotFoundException)
        client.set_keyspace('system')
        client.system_add_keyspace(keyspace)
        test_existence()
        client.system_drop_keyspace(keyspace.name)
  
    def test_column_validators(self):
        # columndef validation for regular CF
        ks = 'Keyspace1'
        _set_keyspace(ks)
        cd = ColumnDef('col', 'LongType', None, None)
        cf = CfDef('Keyspace1', 'ValidatorColumnFamily', column_metadata=[cd])
        client.system_add_column_family(cf)
        ks_def = client.describe_keyspace(ks)
        assert 'ValidatorColumnFamily' in [x.name for x in ks_def.cf_defs]

        cp = ColumnParent('ValidatorColumnFamily')
        col0 = Column('col', _i64(42), 0)
        col1 = Column('col', "ceci n'est pas 64bit", 0)
        client.insert('key0', cp, col0, ConsistencyLevel.ONE)
        e = _expect_exception(lambda: client.insert('key1', cp, col1, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("failed validation") >= 0

        # columndef validation for super CF
        scf = CfDef('Keyspace1', 'ValidatorSuperColumnFamily', column_type='Super', column_metadata=[cd])
        client.system_add_column_family(scf)
        ks_def = client.describe_keyspace(ks)
        assert 'ValidatorSuperColumnFamily' in [x.name for x in ks_def.cf_defs]

        scp = ColumnParent('ValidatorSuperColumnFamily','sc1')
        client.insert('key0', scp, col0, ConsistencyLevel.ONE)
        e = _expect_exception(lambda: client.insert('key1', scp, col1, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("failed validation") >= 0

        # columndef and cfdef default validation
        cf = CfDef('Keyspace1', 'DefaultValidatorColumnFamily', column_metadata=[cd], default_validation_class='UTF8Type')
        client.system_add_column_family(cf)
        ks_def = client.describe_keyspace(ks)
        assert 'DefaultValidatorColumnFamily' in [x.name for x in ks_def.cf_defs]

        dcp = ColumnParent('DefaultValidatorColumnFamily')
        # inserting a longtype into column 'col' is valid at the columndef level
        client.insert('key0', dcp, col0, ConsistencyLevel.ONE)
        # inserting a UTF8type into column 'col' fails at the columndef level
        e = _expect_exception(lambda: client.insert('key1', dcp, col1, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("failed validation") >= 0
        
        # insert a longtype into column 'fcol' should fail at the cfdef level
        col2 = Column('fcol', _i64(4224), 0)
        e = _expect_exception(lambda: client.insert('key1', dcp, col2, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("failed validation") >= 0
        # insert a UTF8type into column 'fcol' is valid at the cfdef level
        col3 = Column('fcol', "Stringin' it up in the Stringtel Stringifornia", 0)
        client.insert('key0', dcp, col3, ConsistencyLevel.ONE)

    def test_system_column_family_operations(self):
        _set_keyspace('Keyspace1')
        # create
        cd = ColumnDef('ValidationColumn', 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'NewColumnFamily', column_metadata=[cd])
        client.system_add_column_family(newcf)
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'NewColumnFamily' in [x.name for x in ks1.cf_defs]
        cfid = [x.id for x in ks1.cf_defs if x.name=='NewColumnFamily'][0]
        assert cfid > 1000
        
        # modify invalid
        modified_cf = CfDef('Keyspace1', 'NewColumnFamily', column_metadata=[cd])
        modified_cf.id = cfid
        def fail_invalid_field():
            modified_cf.comparator_type = 'LongType'
            client.system_update_column_family(modified_cf)
        _expect_exception(fail_invalid_field, InvalidRequestException)
        
        # modify valid
        modified_cf.comparator_type = 'BytesType' # revert back to old value.
        modified_cf.row_cache_size = 25
        modified_cf.gc_grace_seconds = 1
        client.system_update_column_family(modified_cf)
        ks1 = client.describe_keyspace('Keyspace1')
        server_cf = [x for x in ks1.cf_defs if x.name=='NewColumnFamily'][0]
        assert server_cf
        assert server_cf.row_cache_size == 25
        assert server_cf.gc_grace_seconds == 1
        
        # drop
        client.system_drop_column_family('NewColumnFamily')
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'NewColumnFamily' not in [x.name for x in ks1.cf_defs]
        assert 'Standard1' in [x.name for x in ks1.cf_defs]

        # Make a LongType CF and add a validator
        newcf = CfDef('Keyspace1', 'NewLongColumnFamily', comparator_type='LongType')
        client.system_add_column_family(newcf)

        three = _i64(3)
        cd = ColumnDef(three, 'LongType', None, None)
        ks1 = client.describe_keyspace('Keyspace1')
        modified_cf = [x for x in ks1.cf_defs if x.name=='NewLongColumnFamily'][0]
        modified_cf.column_metadata = [cd]
        client.system_update_column_family(modified_cf)

        ks1 = client.describe_keyspace('Keyspace1')
        server_cf = [x for x in ks1.cf_defs if x.name=='NewLongColumnFamily'][0]
        assert server_cf.column_metadata[0].name == _i64(3), server_cf.column_metadata

    def test_dynamic_indexes_creation_deletion(self):
        _set_keyspace('Keyspace1')
        cfdef = CfDef('Keyspace1', 'BlankCF')
        client.system_add_column_family(cfdef)

        ks1 = client.describe_keyspace('Keyspace1')
        cfid = [x.id for x in ks1.cf_defs if x.name=='BlankCF'][0]
        modified_cd = ColumnDef('birthdate', 'BytesType', IndexType.KEYS, None)
        modified_cf = CfDef('Keyspace1', 'BlankCF', column_metadata=[modified_cd])
        modified_cf.id = cfid
        client.system_update_column_family(modified_cf)

        # Add a second indexed CF ...
        birthdate_coldef = ColumnDef('birthdate', 'BytesType', IndexType.KEYS, None)
        age_coldef = ColumnDef('age', 'BytesType', IndexType.KEYS, 'age_index')
        cfdef = CfDef('Keyspace1', 'BlankCF2', column_metadata=[birthdate_coldef, age_coldef])
        client.system_add_column_family(cfdef)
 
        # ... and update it to have a third index
        ks1 = client.describe_keyspace('Keyspace1')
        cfdef = [x for x in ks1.cf_defs if x.name=='BlankCF2'][0]
        name_coldef = ColumnDef('name', 'BytesType', IndexType.KEYS, 'name_index')
        cfdef.column_metadata.append(name_coldef)
        client.system_update_column_family(cfdef)
       
        # Now drop the indexes
        ks1 = client.describe_keyspace('Keyspace1')
        cfdef = [x for x in ks1.cf_defs if x.name=='BlankCF2'][0]
        birthdate_coldef = ColumnDef('birthdate', 'BytesType', None, None)
        age_coldef = ColumnDef('age', 'BytesType', None, None)
        name_coldef = ColumnDef('name', 'BytesType', None, None)
        cfdef.column_metadata = [birthdate_coldef, age_coldef, name_coldef]
        client.system_update_column_family(cfdef)

        ks1 = client.describe_keyspace('Keyspace1')
        cfdef = [x for x in ks1.cf_defs if x.name=='BlankCF'][0]
        birthdate_coldef = ColumnDef('birthdate', 'BytesType', None, None)
        cfdef.column_metadata = [birthdate_coldef]
        client.system_update_column_family(cfdef)
        
        client.system_drop_column_family('BlankCF')
        client.system_drop_column_family('BlankCF2')

    def test_dynamic_indexes_with_system_update_cf(self):
        _set_keyspace('Keyspace1')
        cd = ColumnDef('birthdate', 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'ToBeIndexed', default_validation_class='LongType', column_metadata=[cd])
        client.system_add_column_family(newcf)

        client.insert('key1', ColumnParent('ToBeIndexed'), Column('birthdate', _i64(1), 0), ConsistencyLevel.ONE)
        client.insert('key2', ColumnParent('ToBeIndexed'), Column('birthdate', _i64(2), 0), ConsistencyLevel.ONE)
        client.insert('key2', ColumnParent('ToBeIndexed'), Column('b', _i64(2), 0), ConsistencyLevel.ONE)
        client.insert('key3', ColumnParent('ToBeIndexed'), Column('birthdate', _i64(3), 0), ConsistencyLevel.ONE)
        client.insert('key3', ColumnParent('ToBeIndexed'), Column('b', _i64(3), 0), ConsistencyLevel.ONE)

        # Should fail without index
        cp = ColumnParent('ToBeIndexed')
        sp = SlicePredicate(slice_range=SliceRange('', ''))
        clause = IndexClause([IndexExpression('birthdate', IndexOperator.EQ, _i64(1))], '')
        _expect_exception(lambda: client.get_indexed_slices(cp, clause, sp, ConsistencyLevel.ONE), InvalidRequestException)

        # add an index on 'birthdate'
        ks1 = client.describe_keyspace('Keyspace1')
        cfid = [x.id for x in ks1.cf_defs if x.name=='ToBeIndexed'][0]
        modified_cd = ColumnDef('birthdate', 'BytesType', IndexType.KEYS, 'bd_index')
        modified_cf = CfDef('Keyspace1', 'ToBeIndexed', column_metadata=[modified_cd])
        modified_cf.id = cfid
        client.system_update_column_family(modified_cf)
        
        ks1 = client.describe_keyspace('Keyspace1')
        server_cf = [x for x in ks1.cf_defs if x.name=='ToBeIndexed'][0]
        assert server_cf
        assert server_cf.column_metadata[0].index_type == modified_cd.index_type
        assert server_cf.column_metadata[0].index_name == modified_cd.index_name
        
        # sleep a bit to give time for the index to build.
        time.sleep(0.5)
        
        # simple query on one index expression
        cp = ColumnParent('ToBeIndexed')
        sp = SlicePredicate(slice_range=SliceRange('', ''))
        clause = IndexClause([IndexExpression('birthdate', IndexOperator.EQ, _i64(1))], '')
        result = client.get_indexed_slices(cp, clause, sp, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == 'key1'
        assert len(result[0].columns) == 1, result[0].columns

    def test_system_super_column_family_operations(self):
        _set_keyspace('Keyspace1')
        
        # create
        cd = ColumnDef('ValidationColumn', 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'NewSuperColumnFamily', 'Super', column_metadata=[cd])
        client.system_add_column_family(newcf)
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'NewSuperColumnFamily' in [x.name for x in ks1.cf_defs]
        
        # drop
        client.system_drop_column_family('NewSuperColumnFamily')
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'NewSuperColumnFamily' not in [x.name for x in ks1.cf_defs]
        assert 'Standard1' in [x.name for x in ks1.cf_defs]

    def test_insert_ttl(self):
        """ Test simple insertion of a column with ttl """
        _set_keyspace('Keyspace1')
        column = Column('cttl1', 'value1', 0, 5)
        client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        assert client.get('key1', ColumnPath('Standard1', column='cttl1'), ConsistencyLevel.ONE).column == column

    def test_simple_expiration(self):
        """ Test that column ttled do expires """
        _set_keyspace('Keyspace1')
        column = Column('cttl3', 'value1', 0, 2)
        client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        time.sleep(1)
        c = client.get('key1', ColumnPath('Standard1', column='cttl3'), ConsistencyLevel.ONE).column
        assert c == column
        assert client.get('key1', ColumnPath('Standard1', column='cttl3'), ConsistencyLevel.ONE).column == column
        time.sleep(2)
        _expect_missing(lambda: client.get('key1', ColumnPath('Standard1', column='cttl3'), ConsistencyLevel.ONE))
    
    def test_simple_expiration_batch_mutate(self):
        """ Test that column ttled do expires using batch_mutate """
        _set_keyspace('Keyspace1')
        column = Column('cttl4', 'value1', 0, 2)
        cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(column))]}
        client.batch_mutate({'key1': cfmap}, ConsistencyLevel.ONE)
        time.sleep(1)
        c = client.get('key1', ColumnPath('Standard1', column='cttl4'), ConsistencyLevel.ONE).column
        assert c == column
        assert client.get('key1', ColumnPath('Standard1', column='cttl4'), ConsistencyLevel.ONE).column == column
        time.sleep(2)
        _expect_missing(lambda: client.get('key1', ColumnPath('Standard1', column='cttl3'), ConsistencyLevel.ONE))

    def test_update_expiring(self):
        """ Test that updating a column with ttl override the ttl """
        _set_keyspace('Keyspace1')
        column1 = Column('cttl4', 'value1', 0, 1)
        client.insert('key1', ColumnParent('Standard1'), column1, ConsistencyLevel.ONE)
        column2 = Column('cttl4', 'value1', 1)
        client.insert('key1', ColumnParent('Standard1'), column2, ConsistencyLevel.ONE)
        time.sleep(1.5)
        assert client.get('key1', ColumnPath('Standard1', column='cttl4'), ConsistencyLevel.ONE).column == column2

    def test_remove_expiring(self):
        """ Test removing a column with ttl """
        _set_keyspace('Keyspace1')
        column = Column('cttl5', 'value1', 0, 10)
        client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        client.remove('key1', ColumnPath('Standard1', column='cttl5'), 1, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('key1', ColumnPath('Standard1', column='ctt5'), ConsistencyLevel.ONE))
    
    def test_describe_ring_on_invalid_keyspace(self):
        def req():
            client.describe_ring('system')
        _expect_exception(req, InvalidRequestException)

    def test_incr_decr_standard_add(self):
        _set_keyspace('Keyspace1')

        d1 = 12
        d2 = -21
        d3 = 35
        # insert positive and negative values and check the counts
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d2), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv2 = client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == (d1+d2)

        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d3), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv3 = client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv3.counter_column.value == (d1+d2+d3)

    def test_incr_decr_super_add(self):
        _set_keyspace('Keyspace1')

        d1 = -234
        d2 = 52345
        d3 = 3123

        client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'),  CounterColumn('c1', d1), ConsistencyLevel.ONE)
        client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'),  CounterColumn('c2', d2), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = client.get('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1'), ConsistencyLevel.ONE)
        assert rv1.counter_super_column.columns[0].value == d1
        assert rv1.counter_super_column.columns[1].value == d2

        client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'),  CounterColumn('c1', d2), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv2 = client.get('key1', ColumnPath('SuperCounter1', 'sc1', 'c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == (d1+d2)

        client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'),  CounterColumn('c1', d3), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv3 = client.get('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        assert rv3.counter_column.value == (d1+d2+d3)

    def test_incr_standard_remove(self):
        _set_keyspace('Keyspace1')

        d1 = 124

        # insert value and check it exists
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv1 = client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        client.remove_counter('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath('key1', ColumnPath(column_family='Counter1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv2 = client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        client.remove_counter('key1', ColumnPath(column_family='Counter1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath('key1', ColumnPath(column_family='Counter1', column='c1'))

    def test_incr_super_remove(self):
        _set_keyspace('Keyspace1')

        d1 = 52345

        # insert value and check it exists
        client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'), CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv1 = client.get('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        client.remove_counter('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'), CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv2 = client.get('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        client.remove_counter('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'))

    def test_incr_decr_standard_remove(self):
        _set_keyspace('Keyspace1')

        d1 = 124

        # insert value and check it exists
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv1 = client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        client.remove_counter('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath('key1', ColumnPath(column_family='Counter1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv2 = client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        client.remove_counter('key1', ColumnPath(column_family='Counter1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath('key1', ColumnPath(column_family='Counter1', column='c1'))

    def test_incr_decr_super_remove(self):
        _set_keyspace('Keyspace1')

        d1 = 52345

        # insert value and check it exists
        client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'), CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv1 = client.get('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        client.remove_counter('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'), CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv2 = client.get('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        client.remove_counter('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'))
        
    def test_incr_decr_standard_batch_add(self):
        _set_keyspace('Keyspace1')

        d1 = 12
        d2 = -21
        update_map = {'key1': {'Counter1': [
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d1))),
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d2))),
            ]}}
        
        # insert positive and negative values and check the counts
        client.batch_mutate(update_map, ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1+d2

    def test_incr_decr_standard_batch_remove(self):
        _set_keyspace('Keyspace1')

        d1 = 12
        d2 = -21

        # insert positive and negative values and check the counts
        update_map = {'key1': {'Counter1': [
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d1))),
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d2))),
            ]}}
        client.batch_mutate(update_map, ConsistencyLevel.ONE)
        time.sleep(5)
        rv1 = client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1+d2

        # remove the previous column and check that it is gone
        update_map = {'key1': {'Counter1': [
            Mutation(deletion=Deletion(predicate=SlicePredicate(column_names=['c1']))),
            ]}}
        client.batch_mutate(update_map, ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath('key1', ColumnPath(column_family='Counter1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        update_map = {'key1': {'Counter1': [
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d1))),
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d2))),
            ]}}
        client.batch_mutate(update_map, ConsistencyLevel.ONE)
        time.sleep(5)
        rv2 = client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1+d2

        update_map = {'key1': {'Counter1': [
            Mutation(deletion=Deletion()),
            ]}}
        client.batch_mutate(update_map, ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath('key1', ColumnPath(column_family='Counter1', column='c1'))
        
    def test_incr_decr_standard_slice(self):
        _set_keyspace('Keyspace1')

        d1 = 12
        d2 = -21
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1), ConsistencyLevel.ONE)
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c2', d1), ConsistencyLevel.ONE)
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c3', d1), ConsistencyLevel.ONE)
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c3', d2), ConsistencyLevel.ONE)
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c4', d1), ConsistencyLevel.ONE)
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c5', d1), ConsistencyLevel.ONE)

        time.sleep(0.1)
        # insert positive and negative values and check the counts
        counters = client.get_slice('key1', ColumnParent('Counter1'), SlicePredicate(['c3', 'c4']), ConsistencyLevel.ONE)
        
        assert counters[0].counter_column.value == d1+d2
        assert counters[1].counter_column.value == d1
         
    def test_incr_decr_standard_muliget_slice(self):
        _set_keyspace('Keyspace1')

        d1 = 12
        d2 = -21
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c2', d1), ConsistencyLevel.ONE)
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c3', d1), ConsistencyLevel.ONE)
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c3', d2), ConsistencyLevel.ONE)
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c4', d1), ConsistencyLevel.ONE)
        client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c5', d1), ConsistencyLevel.ONE)

        client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c2', d1), ConsistencyLevel.ONE)
        client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c3', d1), ConsistencyLevel.ONE)
        client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c3', d2), ConsistencyLevel.ONE)
        client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c4', d1), ConsistencyLevel.ONE)
        client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c5', d1), ConsistencyLevel.ONE)


        time.sleep(0.1)
        # insert positive and negative values and check the counts
        counters = client.multiget_slice(['key1', 'key2'], ColumnParent('Counter1'), SlicePredicate(['c3', 'c4']), ConsistencyLevel.ONE)
        
        assert counters['key1'][0].counter_column.value == d1+d2
        assert counters['key1'][1].counter_column.value == d1   
        assert counters['key2'][0].counter_column.value == d1+d2
        assert counters['key2'][1].counter_column.value == d1

    def test_counter_get_slice_range(self):
        _set_keyspace('Keyspace1')
        _insert_counter_range()
        _verify_counter_range()

    def test_counter_get_slice_super_range(self):
        _set_keyspace('Keyspace1')
        _insert_counter_super_range()
        _verify_counter_super_range()

    def test_index_scan(self):
        _set_keyspace('Keyspace1')
        client.insert('key1', ColumnParent('Indexed1'), Column('birthdate', _i64(1), 0), ConsistencyLevel.ONE)
        client.insert('key2', ColumnParent('Indexed1'), Column('birthdate', _i64(2), 0), ConsistencyLevel.ONE)
        client.insert('key2', ColumnParent('Indexed1'), Column('b', _i64(2), 0), ConsistencyLevel.ONE)
        client.insert('key3', ColumnParent('Indexed1'), Column('birthdate', _i64(3), 0), ConsistencyLevel.ONE)
        client.insert('key3', ColumnParent('Indexed1'), Column('b', _i64(3), 0), ConsistencyLevel.ONE)

        # simple query on one index expression
        cp = ColumnParent('Indexed1')
        sp = SlicePredicate(slice_range=SliceRange('', ''))
        clause = IndexClause([IndexExpression('birthdate', IndexOperator.EQ, _i64(1))], '')
        result = client.get_indexed_slices(cp, clause, sp, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == 'key1'
        assert len(result[0].columns) == 1, result[0].columns

        # solo unindexed expression is invalid
        clause = IndexClause([IndexExpression('b', IndexOperator.EQ, _i64(1))], '')
        _expect_exception(lambda: client.get_indexed_slices(cp, clause, sp, ConsistencyLevel.ONE), InvalidRequestException)

        # but unindexed expression added to indexed one is ok
        clause = IndexClause([IndexExpression('b', IndexOperator.EQ, _i64(3)),
                              IndexExpression('birthdate', IndexOperator.EQ, _i64(3))],
                             '')
        result = client.get_indexed_slices(cp, clause, sp, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == 'key3'
        assert len(result[0].columns) == 2, result[0].columns
        
    def test_index_scan_uuid_names(self):
        _set_keyspace('Keyspace1')
        sp = SlicePredicate(slice_range=SliceRange('', ''))
        cp = ColumnParent('Indexed3') # timeuuid name, utf8 values
        u = uuid.UUID('00000000-0000-1000-0000-000000000000').bytes
        u2 = uuid.UUID('00000000-0000-1000-0000-000000000001').bytes
        client.insert('key1', ColumnParent('Indexed3'), Column(u, 'a', 0), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Indexed3'), Column(u2, 'b', 0), ConsistencyLevel.ONE)
        # name comparator + data validator of incompatible types -- see CASSANDRA-2347
        clause = IndexClause([IndexExpression(u, IndexOperator.EQ, 'a'),
                              IndexExpression(u2, IndexOperator.EQ, 'b')], '')
        result = client.get_indexed_slices(cp, clause, sp, ConsistencyLevel.ONE)
        assert len(result) == 1, result

        cp = ColumnParent('Indexed2') # timeuuid name, long values

        # name must be valid (TimeUUID)
        clause = IndexClause([IndexExpression('foo', IndexOperator.EQ, uuid.UUID('00000000-0000-1000-0000-000000000000').bytes)], '')
        _expect_exception(lambda: client.get_indexed_slices(cp, clause, sp, ConsistencyLevel.ONE), InvalidRequestException)
        
        # value must be valid (TimeUUID)
        clause = IndexClause([IndexExpression(uuid.UUID('00000000-0000-1000-0000-000000000000').bytes, IndexOperator.EQ, "foo")], '')
        _expect_exception(lambda: client.get_indexed_slices(cp, clause, sp, ConsistencyLevel.ONE), InvalidRequestException)
        
    def test_index_scan_expiring(self):
        """ Test that column ttled expires from KEYS index"""
        _set_keyspace('Keyspace1')
        client.insert('key1', ColumnParent('Indexed1'), Column('birthdate', _i64(1), 0, 1), ConsistencyLevel.ONE)
        cp = ColumnParent('Indexed1')
        sp = SlicePredicate(slice_range=SliceRange('', ''))
        clause = IndexClause([IndexExpression('birthdate', IndexOperator.EQ, _i64(1))], '')
        # query before expiration
        result = client.get_indexed_slices(cp, clause, sp, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        # wait for expiration and requery
        time.sleep(2)
        result = client.get_indexed_slices(cp, clause, sp, ConsistencyLevel.ONE)
        assert len(result) == 0, result
     
    def test_column_not_found_quorum(self): 
        _set_keyspace('Keyspace1')
        key = 'doesntexist'
        column_path = ColumnPath(column_family="Standard1", column="idontexist")
        try:
            client.get(key, column_path, ConsistencyLevel.QUORUM)
            assert False, ('columnpath %s existed in %s when it should not' % (column_path, key))
        except NotFoundException:
            assert True, 'column did not exist'

    def test_get_range_slice_after_deletion(self):
        _set_keyspace('Keyspace2')
        key = 'key1'
        # three supercoluns, each with "col1" subcolumn
        for i in range(1,4):
            client.insert(key, ColumnParent('Super3', 'sc%d' % i), Column('col1', 'val1', 0), ConsistencyLevel.ONE)

        cp = ColumnParent('Super3')
        predicate = SlicePredicate(slice_range=SliceRange('sc1', 'sc3', False, count=1))
        k_range = KeyRange(start_key=key, end_key=key, count=1)

        # validate count=1 restricts to 1 supercolumn
        result = client.get_range_slices(cp, predicate, k_range, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 1

        # remove sc1; add back subcolumn to override tombstone
        client.remove(key, ColumnPath('Super3', 'sc1'), 1, ConsistencyLevel.ONE)
        result = client.get_range_slices(cp, predicate, k_range, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 1
        client.insert(key, ColumnParent('Super3', 'sc1'), Column('col1', 'val1', 2), ConsistencyLevel.ONE)
        result = client.get_range_slices(cp, predicate, k_range, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 1, result[0].columns
        assert result[0].columns[0].super_column.name == 'sc1'


class TestTruncate(ThriftTester):
    def test_truncate(self):
        _set_keyspace('Keyspace1')
        
        _insert_simple()
        _insert_super()

        # truncate Standard1
        client.truncate('Standard1')
        assert _big_slice('key1', ColumnParent('Standard1')) == []

        # truncate Super1
        client.truncate('Super1')
        assert _big_slice('key1', ColumnParent('Super1')) == []
        assert _big_slice('key1', ColumnParent('Super1', 'sc1')) == []
