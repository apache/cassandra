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
# PYTHONPATH=test nosetests --tests=system.test_server:TestMutations.test_empty_range

import os, sys, time, struct

from . import client, root, CassandraTester

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

def _assert_column(keyspace, column_family, key, column, value, ts = 0):
    try:
        assert client.get(keyspace, key, ColumnPath(column_family, column=column), ConsistencyLevel.ONE).column == Column(column, value, ts)
    except NotFoundException:
        raise Exception('expected %s:%s:%s:%s:%s, but was not present' % (keyspace, column_family, key, column, value) )

def _assert_columnpath_exists(keyspace, key, column_path):
    try:
        assert client.get(keyspace, key, column_path, ConsistencyLevel.ONE)
    except NotFoundException:
        raise Exception('expected %s:%s with %s but was not present.' % (keyspace, key, column_path) )

def _assert_no_columnpath(keyspace, key, column_path):
    try:
        client.get(keyspace, key, column_path, ConsistencyLevel.ONE)
        assert False, ('columnpath %s existed in %s:%s when it should not' % (column_path, keyspace, key))
    except NotFoundException:
        assert True, 'column did not exist'

def _insert_simple(block=True):
   return _insert_multi(['key1'], block)

def _insert_batch(block):
   return _insert_multi_batch(['key1'], block)

def _insert_multi(keys, block=True):
    if block:
        consistencyLevel = ConsistencyLevel.ONE
    else:
        consistencyLevel = ConsistencyLevel.ZERO

    for key in keys:
        client.insert('Keyspace1', key, ColumnPath('Standard1', column='c1'), 'value1', 0, consistencyLevel)
        client.insert('Keyspace1', key, ColumnPath('Standard1', column='c2'), 'value2', 0, consistencyLevel)

def _insert_multi_batch(keys, block):
    cfmap = {'Standard1': [ColumnOrSuperColumn(c) for c in _SIMPLE_COLUMNS],
             'Standard2': [ColumnOrSuperColumn(c) for c in _SIMPLE_COLUMNS]}
    if block:
        consistencyLevel = ConsistencyLevel.ONE
    else:
        consistencyLevel = ConsistencyLevel.ZERO

    for key in keys:
        client.batch_insert('Keyspace1', key, cfmap, consistencyLevel)

def _big_slice(keyspace, key, column_parent):
    p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
    return client.get_slice(keyspace, key, column_parent, p, ConsistencyLevel.ONE)

def _big_multislice(keyspace, keys, column_parent):
    p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
    return client.multiget_slice(keyspace, keys, column_parent, p, ConsistencyLevel.ONE)

def _verify_batch():
    _verify_simple()
    L = [result.column
         for result in _big_slice('Keyspace1', 'key1', ColumnParent('Standard2'))]
    assert L == _SIMPLE_COLUMNS, L

def _verify_simple():
    assert client.get('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE).column == Column('c1', 'value1', 0)
    L = [result.column
         for result in _big_slice('Keyspace1', 'key1', ColumnParent('Standard1'))]
    assert L == _SIMPLE_COLUMNS, L

def _insert_super(key='key1'):
    client.insert('Keyspace1', key, ColumnPath('Super1', 'sc1', _i64(4)), 'value4', 0, ConsistencyLevel.ZERO)
    client.insert('Keyspace1', key, ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 0, ConsistencyLevel.ZERO)
    client.insert('Keyspace1', key, ColumnPath('Super1', 'sc2', _i64(6)), 'value6', 0, ConsistencyLevel.ZERO)
    time.sleep(0.1)

def _insert_range():
    client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 0, ConsistencyLevel.ONE)
    client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c2'), 'value2', 0, ConsistencyLevel.ONE)
    client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c3'), 'value3', 0, ConsistencyLevel.ONE)
    time.sleep(0.1)

def _verify_range():
    p = SlicePredicate(slice_range=SliceRange('c1', 'c2', False, 1000))
    result = client.get_slice('Keyspace1','key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].column.name == 'c1'
    assert result[1].column.name == 'c2'

    p = SlicePredicate(slice_range=SliceRange('c3', 'c2', True, 1000))
    result = client.get_slice('Keyspace1','key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].column.name == 'c3'
    assert result[1].column.name == 'c2'

    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 1000))
    result = client.get_slice('Keyspace1','key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 3, result
    
    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 2))
    result = client.get_slice('Keyspace1','key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2, result

def _insert_super_range():
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc1', _i64(4)), 'value4', 0, False)
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 0, False)
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(6)), 'value6', 0, False)
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc3', _i64(7)), 'value7', 0, False)
    time.sleep(0.1)

def _verify_super_range():
    p = SlicePredicate(slice_range=SliceRange('sc2', 'sc3', False, 2))
    result = client.get_slice('Keyspace1', 'key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].super_column.name == 'sc2'
    assert result[1].super_column.name == 'sc3'

    p = SlicePredicate(slice_range=SliceRange('sc3', 'sc2', True, 2))
    result = client.get_slice('Keyspace1', 'key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].super_column.name == 'sc3'
    assert result[1].super_column.name == 'sc2'

def _verify_super(supercf='Super1', key='key1'):
    assert client.get('Keyspace1', key, ColumnPath(supercf, 'sc1', _i64(4)), ConsistencyLevel.ONE).column == Column(_i64(4), 'value4', 0)
    slice = [result.super_column
             for result in _big_slice('Keyspace1', key, ColumnParent('Super1'))]
    assert slice == _SUPER_COLUMNS, slice

def _expect_exception(fn, type_):
    try:
        r = fn()
    except type_:
        pass
    else:
        raise Exception('expected %s; got %s' % (type_.__name__, r))
    
def _expect_missing(fn):
    _expect_exception(fn, NotFoundException)

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


class TestMutations(CassandraTester):
    def test_insert(self):
        _insert_simple(False)
        time.sleep(0.1)
        _verify_simple()

    def test_empty_slice(self):
        assert _big_slice('Keyspace1', 'key1', ColumnParent('Standard2')) == []
        assert _big_slice('Keyspace1', 'key1', ColumnParent('Super1')) == []

    def test_missing_super(self):
        _expect_missing(lambda: client.get('Keyspace1', 'key1', ColumnPath('Super1', 'sc1', _i64(1)), ConsistencyLevel.ONE))
        _insert_super()
        _expect_missing(lambda: client.get('Keyspace1', 'key1', ColumnPath('Super1', 'sc1', _i64(1)), ConsistencyLevel.ONE))

    def test_count(self):
        _insert_simple()
        _insert_super()
        assert client.get_count('Keyspace1', 'key1', ColumnParent('Standard2'), ConsistencyLevel.ONE) == 0
        assert client.get_count('Keyspace1', 'key1', ColumnParent('Standard1'), ConsistencyLevel.ONE) == 2
        assert client.get_count('Keyspace1', 'key1', ColumnParent('Super1', 'sc2'), ConsistencyLevel.ONE) == 2
        assert client.get_count('Keyspace1', 'key1', ColumnParent('Super1'), ConsistencyLevel.ONE) == 2

    def test_insert_blocking(self):
        _insert_simple()
        _verify_simple()

    def test_super_insert(self):
        _insert_super()
        _verify_super()

    def test_super_reinsert(self):
        for x in xrange(3):
            client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(x)), 'value', 1, ConsistencyLevel.ONE)

        client.remove('Keyspace1', 'key1', ColumnPath('Super1'), 2, ConsistencyLevel.ONE)

        for x in xrange(3):
            client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(x + 3)), 'value', 3, ConsistencyLevel.ONE)

        for n in xrange(1, 4):
            p =  SlicePredicate(slice_range=SliceRange('', '', False, n))
            slice = client.get_slice('Keyspace1', 'key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE)
            assert len(slice) == n, "expected %s results; found %s" % (n, slice)

    def test_super_get(self):
        _insert_super()
        result = client.get('Keyspace1', 'key1', ColumnPath('Super1', 'sc2'), ConsistencyLevel.ONE).super_column
        assert result == _SUPER_COLUMNS[1], result

    def test_super_subcolumn_limit(self):
        _insert_super()
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1))
        column_parent = ColumnParent('Super1', 'sc2')
        slice = [result.column
                 for result in client.get_slice('Keyspace1', 'key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(_i64(5), 'value5', 0)], slice
        p = SlicePredicate(slice_range=SliceRange('', '', True, 1))
        slice = [result.column
                 for result in client.get_slice('Keyspace1', 'key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(_i64(6), 'value6', 0)], slice
        
    def test_long_order(self):
        def long_xrange(start, stop, step):
            i = start
            while i < stop:
                yield i
                i += step
        L = []
        for i in long_xrange(0, 104294967296, 429496729):
            name = _i64(i)
            client.insert('Keyspace1', 'key1', ColumnPath('StandardLong1', column=name), 'v', 0, ConsistencyLevel.ONE)
            L.append(name)
        slice = [result.column.name for result in _big_slice('Keyspace1', 'key1', ColumnParent('StandardLong1'))]
        assert slice == L, slice
        
    def test_time_uuid(self):
        import uuid
        L = []
        # 100 isn't enough to fail reliably if the comparator is borked
        for i in xrange(500):
            L.append(uuid.uuid1())
            client.insert('Keyspace2', 'key1', ColumnPath('Super4', 'sc1', L[-1].bytes), 'value%s' % i, i, ConsistencyLevel.ONE)
        slice = _big_slice('Keyspace2', 'key1', ColumnParent('Super4', 'sc1'))
        assert len(slice) == 500, len(slice)
        for i in xrange(500):
            u = slice[i].column
            assert u.value == 'value%s' % i
            assert u.name == L[i].bytes

        p = SlicePredicate(slice_range=SliceRange('', '', True, 1))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('Keyspace2', 'key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[-1].bytes, 'value499', 499)], slice

        p = SlicePredicate(slice_range=SliceRange('', L[2].bytes, False, 1000))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('Keyspace2', 'key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[0].bytes, 'value0', 0),
                         Column(L[1].bytes, 'value1', 1),
                         Column(L[2].bytes, 'value2', 2)], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, '', True, 1000))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('Keyspace2', 'key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[2].bytes, 'value2', 2),
                         Column(L[1].bytes, 'value1', 1),
                         Column(L[0].bytes, 'value0', 0)], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, '', False, 1))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('Keyspace2', 'key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[2].bytes, 'value2', 2)], slice
        
    def test_long_remove(self):
        column_parent = ColumnParent('StandardLong1')
        sp = SlicePredicate(slice_range=SliceRange('', '', False, 1))

        for i in xrange(10):
            path = ColumnPath('StandardLong1', column=_i64(i))

            client.insert('Keyspace1', 'key1', path, 'value1', 10 * i, ConsistencyLevel.ONE)
            client.remove('Keyspace1', 'key1', ColumnPath('StandardLong1'), 10 * i + 1, ConsistencyLevel.ONE)
            slice = client.get_slice('Keyspace1', 'key1', column_parent, sp, ConsistencyLevel.ONE)
            assert slice == [], slice
            # resurrect
            client.insert('Keyspace1', 'key1', path, 'value2', 10 * i + 2, ConsistencyLevel.ONE)
            slice = [result.column
                     for result in client.get_slice('Keyspace1', 'key1', column_parent, sp, ConsistencyLevel.ONE)]
            assert slice == [Column(_i64(i), 'value2', 10 * i + 2)], (slice, i)
        
    def test_batch_insert(self):
        _insert_batch(False)
        time.sleep(0.1)
        _verify_batch()

    def test_batch_insert_blocking(self):
        _insert_batch(True)
        _verify_batch()
        
    def test_batch_mutate_standard_columns(self):
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in  range(27,32)] 
        mutations = [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)
        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate('Keyspace1', keyed_mutations, ConsistencyLevel.ZERO)
        time.sleep(0.1)

        for column_family in column_families:
            for key in keys:
                _assert_column('Keyspace1', column_family, key, 'c1', 'value1')

    def test_batch_mutate_standard_columns_blocking(self):
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in  range(38,46)] 
        mutations = [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)
        keyed_mutations = dict((key, mutation_map) for key in keys)
        
        client.batch_mutate('Keyspace1', keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for key in keys:
                _assert_column('Keyspace1', column_family, key, 'c1', 'value1')

    def test_batch_mutate_remove_standard_columns(self):
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in range(11,21)]
        _insert_multi(keys)

        mutations = [Mutation(deletion=Deletion(20, predicate=SlicePredicate(column_names=[c.name]))) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)
        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate('Keyspace1', keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for c in _SIMPLE_COLUMNS:
                for key in keys:
                    _assert_no_columnpath('Keyspace1', key, ColumnPath(column_family, column=c.name))

    def test_batch_mutate_remove_standard_row(self):
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in range(11,21)]
        _insert_multi(keys)

        mutations = [Mutation(deletion=Deletion(20))]
        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate('Keyspace1', keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for c in _SIMPLE_COLUMNS:
                for key in keys:
                    _assert_no_columnpath('Keyspace1', key, ColumnPath(column_family, column=c.name))

    def test_batch_mutate_remove_super_columns_with_standard_under(self):
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

        client.batch_mutate('Keyspace1', keyed_mutations, ConsistencyLevel.ZERO)
        time.sleep(0.1)
        for column_family in column_families:
            for sc in _SUPER_COLUMNS:
                for c in sc.columns:
                    for key in keys:
                        _assert_no_columnpath('Keyspace1', key, ColumnPath(column_family, super_column=sc.name, column=c.name))

    def test_batch_mutate_remove_super_columns_with_none_given_underneath(self):
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
                _assert_columnpath_exists('Keyspace1', key, ColumnPath('Super1', super_column=sc.name))

        client.batch_mutate('Keyspace1', keyed_mutations, ConsistencyLevel.ZERO)
        time.sleep(0.1)

        for sc in _SUPER_COLUMNS:
            for c in sc.columns:
                for key in keys:
                    _assert_no_columnpath('Keyspace1', key, ColumnPath('Super1', super_column=sc.name))

    def test_batch_mutate_remove_super_columns_entire_row(self):
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
                _assert_columnpath_exists('Keyspace1', key, ColumnPath('Super1', super_column=sc.name))

        client.batch_mutate('Keyspace1', keyed_mutations, ConsistencyLevel.ZERO)

        for sc in _SUPER_COLUMNS:
          for key in keys:
            waitfor(5, _assert_no_columnpath, 'Keyspace1', key, ColumnPath('Super1', super_column=sc.name))

    def test_batch_mutate_insertions_and_deletions(self):
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
            mutation = {'Super1': [ColumnOrSuperColumn(super_column=sc)]}
            client.batch_insert('Keyspace1', key, mutation, ConsistencyLevel.ONE)

            sc2 = SuperColumn('sc2', [Column(_i64(22), 'value22', 0),
                                      Column(_i64(23), 'value23', 0)])
            mutation2 = {'Super2': [ColumnOrSuperColumn(super_column=sc2)]}
            client.batch_insert('Keyspace1', key, mutation2, ConsistencyLevel.ONE)

        mutation_map = {
            'Super1' : [Mutation(ColumnOrSuperColumn(super_column=first_insert)),
                        Mutation(deletion=Deletion(3, **first_deletion))],
        
            'Super2' : [Mutation(deletion=Deletion(2, **second_deletion)),
                        Mutation(ColumnOrSuperColumn(super_column=second_insert))]
            }

        keyed_mutations = dict((key, mutation_map) for key in keys)
        client.batch_mutate('Keyspace1', keyed_mutations, ConsistencyLevel.ONE)

        for key in keys:
            for c in [_i64(22), _i64(23)]:
                _assert_no_columnpath('Keyspace1',
                                      key,
                                      ColumnPath('Super1', super_column='sc1', column=c))
                _assert_no_columnpath('Keyspace1',
                                      key,
                                      ColumnPath('Super2', super_column='sc2', column=c))

            for c in [_i64(20), _i64(21)]:
                _assert_columnpath_exists('Keyspace1', key,
                                          ColumnPath('Super1',
                                                     super_column='sc1',
                                                     column=c))
                _assert_columnpath_exists('Keyspace1', key,
                                          ColumnPath('Super2',
                                                     super_column='sc1',
                                                     column=c))

    def test_batch_mutate_does_not_accept_cosc_and_deletion_in_same_mutation(self):
        def too_full():
            col = ColumnOrSuperColumn(column=Column("foo", 'bar', 0))
            dele = Deletion(2, predicate=SlicePredicate(column_names=['baz']))
            client.batch_mutate('Keyspace1',
                                {'key_34': {'Standard1': [Mutation(col, dele)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(too_full, InvalidRequestException)

    def test_batch_mutate_does_not_yet_accept_slice_ranges(self):
        def send_range():
            sp = SlicePredicate(slice_range=SliceRange(start='0', finish="", count=10))
            d = Deletion(2, predicate=sp)
            client.batch_mutate('Keyspace1',
                                {'key_35': {'Standard1':[Mutation(deletion=d)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(send_range, InvalidRequestException)

    def test_column_name_lengths(self):
        _expect_exception(lambda: client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column=''), 'value', 0, ConsistencyLevel.ONE), InvalidRequestException)
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='x'*1), 'value', 0, ConsistencyLevel.ONE)
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='x'*127), 'value', 0, ConsistencyLevel.ONE)
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='x'*128), 'value', 0, ConsistencyLevel.ONE)
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='x'*129), 'value', 0, ConsistencyLevel.ONE)
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='x'*255), 'value', 0, ConsistencyLevel.ONE)
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='x'*256), 'value', 0, ConsistencyLevel.ONE)
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='x'*257), 'value', 0, ConsistencyLevel.ONE)
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='x'*(2**16 - 1)), 'value', 0, ConsistencyLevel.ONE)
        _expect_exception(lambda: client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='x'*(2**16)), 'value', 0, ConsistencyLevel.ONE), InvalidRequestException)

    def test_bad_calls(self):
        # missing arguments
        _expect_exception(lambda: client.insert(None, None, None, None, None, None), TApplicationException)
        # supercolumn in a non-super CF
        _expect_exception(lambda: client.insert('Keyspace1', 'key1', ColumnPath('Standard1', 'x', 'y'), 'value', 0, ConsistencyLevel.ONE), InvalidRequestException)
        # key too long
        _expect_exception(lambda: client.get('Keyspace1', 'x' * 2**16, ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE), InvalidRequestException)
        # empty key
        _expect_exception(lambda: client.get('Keyspace1', '', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE), InvalidRequestException)
        cfmap = {'Super1': [ColumnOrSuperColumn(super_column=c) for c in _SUPER_COLUMNS],
                 'Super2': [ColumnOrSuperColumn(super_column=c) for c in _SUPER_COLUMNS]}
        _expect_exception(lambda: client.batch_insert('Keyspace1', '', cfmap, ConsistencyLevel.ONE), InvalidRequestException)
        # empty column name
        _expect_exception(lambda: client.get('Keyspace1', 'key1', ColumnPath('Standard1', column=''), ConsistencyLevel.ONE), InvalidRequestException)
        # get doesn't specify column name
        _expect_exception(lambda: client.get('Keyspace1', 'key1', ColumnPath('Standard1'), ConsistencyLevel.ONE), InvalidRequestException)
        # supercolumn in a non-super CF
        _expect_exception(lambda: client.get('Keyspace1', 'key1', ColumnPath('Standard1', 'x', 'y'), ConsistencyLevel.ONE), InvalidRequestException)
        # get doesn't specify supercolumn name
        _expect_exception(lambda: client.get('Keyspace1', 'key1', ColumnPath('Super1'), ConsistencyLevel.ONE), InvalidRequestException)
        # invalid CF
        _expect_exception(lambda: client.get_range_slice('Keyspace1', ColumnParent('S'), SlicePredicate(column_names=['', '']), '', '', 5, ConsistencyLevel.ONE), InvalidRequestException)
        # 'x' is not a valid Long
        _expect_exception(lambda: client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc1', 'x'), 'value', 0, ConsistencyLevel.ONE), InvalidRequestException)
        # start is not a valid Long
        p = SlicePredicate(slice_range=SliceRange('x', '', False, 1))
        column_parent = ColumnParent('StandardLong1')
        _expect_exception(lambda: client.get_slice('Keyspace1', 'key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish
        p = SlicePredicate(slice_range=SliceRange(_i64(10), _i64(0), False, 1))
        column_parent = ColumnParent('StandardLong1')
        _expect_exception(lambda: client.get_slice('Keyspace1', 'key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start is not a valid Long, supercolumn version
        p = SlicePredicate(slice_range=SliceRange('x', '', False, 1))
        column_parent = ColumnParent('Super1', 'sc1')
        _expect_exception(lambda: client.get_slice('Keyspace1', 'key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish, supercolumn version
        p = SlicePredicate(slice_range=SliceRange(_i64(10), _i64(0), False, 1))
        column_parent = ColumnParent('Super1', 'sc1')
        _expect_exception(lambda: client.get_slice('Keyspace1', 'key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish, key version
        _expect_exception(lambda: client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['']), 'z', 'a', 1, ConsistencyLevel.ONE), InvalidRequestException)
        # don't allow super_column in Deletion for standard ColumnFamily
        deletion = Deletion(1, 'supercolumn', None)
        mutation = Mutation(deletion=deletion)
        mutations = { 'key' : { 'Standard1' : [ mutation ] } }
        _expect_exception(lambda: client.batch_mutate("Keyspace1", mutations, ConsistencyLevel.QUORUM),
                          InvalidRequestException)

    def test_batch_insert_super(self):
         cfmap = {'Super1': [ColumnOrSuperColumn(super_column=c) for c in _SUPER_COLUMNS],
                  'Super2': [ColumnOrSuperColumn(super_column=c) for c in _SUPER_COLUMNS]}
         client.batch_insert('Keyspace1', 'key1', cfmap, ConsistencyLevel.ZERO)
         time.sleep(0.1)
         _verify_super('Super1')
         _verify_super('Super2')

    def test_batch_insert_super_blocking(self):
         cfmap = {'Super1': [ColumnOrSuperColumn(super_column=c) for c in _SUPER_COLUMNS],
                  'Super2': [ColumnOrSuperColumn(super_column=c) for c in _SUPER_COLUMNS]}
         client.batch_insert('Keyspace1', 'key1', cfmap, ConsistencyLevel.ONE)
         _verify_super('Super1')
         _verify_super('Super2')

    def test_cf_remove_column(self):
        _insert_simple()
        client.remove('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 1, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE))
        assert client.get('Keyspace1', 'key1', ColumnPath('Standard1', column='c2'), ConsistencyLevel.ONE).column \
            == Column('c2', 'value2', 0)
        assert _big_slice('Keyspace1', 'key1', ColumnParent('Standard1')) \
            == [ColumnOrSuperColumn(column=Column('c2', 'value2', 0))]

        # New insert, make sure it shows up post-remove:
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c3'), 'value3', 0, ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice('Keyspace1', 'key1', ColumnParent('Standard1'))]
        assert columns == [Column('c2', 'value2', 0), Column('c3', 'value3', 0)], columns

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 0, ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice('Keyspace1', 'key1', ColumnParent('Standard1'))]
        assert columns == [Column('c2', 'value2', 0), Column('c3', 'value3', 0)], columns
        # Next, w/ a newer timestamp; it should come back:
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 2, ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice('Keyspace1', 'key1', ColumnParent('Standard1'))]
        assert columns == [Column('c1', 'value1', 2), Column('c2', 'value2', 0), Column('c3', 'value3', 0)], columns


    def test_cf_remove(self):
        _insert_simple()
        _insert_super()

        # Remove the key1:Standard1 cf; verify super is unaffected
        client.remove('Keyspace1', 'key1', ColumnPath('Standard1'), 3, ConsistencyLevel.ONE)
        assert _big_slice('Keyspace1', 'key1', ColumnParent('Standard1')) == []
        _verify_super()

        # Test resurrection.  First, re-insert a value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 0, ConsistencyLevel.ONE)
        assert _big_slice('Keyspace1', 'key1', ColumnParent('Standard1')) == []
        # Next, w/ a newer timestamp; it should come back:
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 4, ConsistencyLevel.ONE)
        result = _big_slice('Keyspace1', 'key1', ColumnParent('Standard1'))
        assert result == [ColumnOrSuperColumn(column=Column('c1', 'value1', 4))], result

        # check removing the entire super cf, too.
        client.remove('Keyspace1', 'key1', ColumnPath('Super1'), 3, ConsistencyLevel.ONE)
        assert _big_slice('Keyspace1', 'key1', ColumnParent('Super1')) == []
        assert _big_slice('Keyspace1', 'key1', ColumnParent('Super1', 'sc1')) == []


    def test_super_cf_remove_column(self):
        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 5, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), ConsistencyLevel.ONE))
        super_columns = [result.super_column for result in _big_slice('Keyspace1', 'key1', ColumnParent('Super1'))]
        assert super_columns == [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
                                 SuperColumn(name='sc2', columns=[Column(_i64(6), 'value6', 0)])]
        _verify_simple()

        # New insert, make sure it shows up post-remove:
        client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(7)), 'value7', 0, ConsistencyLevel.ONE)
        super_columns_expected = [SuperColumn(name='sc1', 
                                              columns=[Column(_i64(4), 'value4', 0)]),
                                  SuperColumn(name='sc2', 
                                              columns=[Column(_i64(6), 'value6', 0), Column(_i64(7), 'value7', 0)])]

        super_columns = [result.super_column for result in _big_slice('Keyspace1', 'key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, actual

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 0, ConsistencyLevel.ONE)

        super_columns = [result.super_column for result in _big_slice('Keyspace1', 'key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Next, w/ a newer timestamp; it should come back
        client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 6, ConsistencyLevel.ONE)
        super_columns = [result.super_column for result in _big_slice('Keyspace1', 'key1', ColumnParent('Super1'))]
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]), 
                                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 6), 
                                                                   Column(_i64(6), 'value6', 0), 
                                                                   Column(_i64(7), 'value7', 0)])]
        assert super_columns == super_columns_expected, super_columns

    def test_super_cf_remove_supercolumn(self):
        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove('Keyspace1', 'key1', ColumnPath('Super1', 'sc2'), 5, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), ConsistencyLevel.ONE))
        super_columns = _big_slice('Keyspace1', 'key1', ColumnParent('Super1', 'sc2'))
        assert super_columns == [], super_columns
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)])]
        super_columns = [result.super_column
                         for result in _big_slice('Keyspace1', 'key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns
        _verify_simple()

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 1, ConsistencyLevel.ONE)
        super_columns = [result.super_column
                         for result in _big_slice('Keyspace1', 'key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Next, w/ a newer timestamp; it should come back
        client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 6, ConsistencyLevel.ONE)
        super_columns = [result.super_column
                         for result in _big_slice('Keyspace1', 'key1', ColumnParent('Super1'))]
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
                                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 6)])]
        assert super_columns == super_columns_expected, super_columns

        # check slicing at the subcolumn level too
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
        columns = [result.column
                   for result in client.get_slice('Keyspace1', 'key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE)]
        assert columns == [Column(_i64(5), 'value5', 6)], columns


    def test_super_cf_resurrect_subcolumn(self):
        key = 'vijay'
        client.insert('Keyspace1', key, ColumnPath('Super1', 'sc1', _i64(4)), 'value4', 0, ConsistencyLevel.ONE)

        client.remove('Keyspace1', key, ColumnPath('Super1', 'sc1'), 1, ConsistencyLevel.ONE)

        client.insert('Keyspace1', key, ColumnPath('Super1', 'sc1', _i64(4)), 'value4', 2, ConsistencyLevel.ONE)

        result = client.get('Keyspace1', key, ColumnPath('Super1', 'sc1'), ConsistencyLevel.ONE)
        assert result.super_column.columns is not None, result.super_column


    def test_empty_range(self):
        assert client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE) == []
        _insert_simple()
        assert client.get_range_slice('Keyspace1', ColumnParent('Super1'), SlicePredicate(column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE) == []

    def test_range_with_remove(self):
        _insert_simple()
        assert client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), 'key1', '', 1000, ConsistencyLevel.ONE)[0].key == 'key1'

        client.remove('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 1, ConsistencyLevel.ONE)
        client.remove('Keyspace1', 'key1', ColumnPath('Standard1', column='c2'), 1, ConsistencyLevel.ONE)
        actual = client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c2']), '', '', 1000, ConsistencyLevel.ONE)
        assert actual == [KeySlice(columns=[], key='key1')], actual

    def test_range_with_remove_cf(self):
        _insert_simple()
        assert client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), 'key1', '', 1000, ConsistencyLevel.ONE)[0].key == 'key1'

        client.remove('Keyspace1', 'key1', ColumnPath('Standard1'), 1, ConsistencyLevel.ONE)
        actual = client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE)
        assert actual == [KeySlice(columns=[], key='key1')], actual

    def test_range_collation(self):
        for key in ['-a', '-b', 'a', 'b'] + [str(i) for i in xrange(100)]:
            client.insert('Keyspace1', key, ColumnPath('Standard1', column=key), 'v', 0, ConsistencyLevel.ONE)
        slices = client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '', '', 1000, ConsistencyLevel.ONE)
        # note the collated ordering rather than ascii
        L = ['0', '1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2', '20', '21', '22', '23', '24', '25', '26', '27','28', '29', '3', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '4', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '5', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '6', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '7', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '8', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '9', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', 'a', '-a', 'b', '-b']
        assert len(slices) == len(L)
        for key, ks in zip(L, slices):
            assert key == ks.key

    def test_range_partial(self):
        for key in ['-a', '-b', 'a', 'b'] + [str(i) for i in xrange(100)]:
            client.insert('Keyspace1', key, ColumnPath('Standard1', column=key), 'v', 0, ConsistencyLevel.ONE)
        
        def check_slices_against_keys(keyList, sliceList):
            assert len(keyList) == len(sliceList)
            for key, ks in zip(keyList, sliceList):
                assert key == ks.key
        
        slices = client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), 'a', '', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['a', '-a', 'b', '-b'], slices)
        
        slices = client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '', '15', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['0', '1', '10', '11', '12', '13', '14', '15'], slices)

        slices = client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '50', '51', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['50', '51'], slices)
        
        slices = client.get_range_slice('Keyspace1', ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '1', '', 10, ConsistencyLevel.ONE)
        check_slices_against_keys(['1', '10', '11', '12', '13', '14', '15', '16', '17', '18'], slices)

    def test_get_slice_range(self):
        _insert_range()
        _verify_range()
        
    def test_get_slice_super_range(self):
        _insert_super_range()
        _verify_super_range()

    def test_get_range_slices_tokens(self):
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert('Keyspace2', key, ColumnPath('Super3', 'sc1', cname), 'v-' + cname, 0, ConsistencyLevel.ONE)

        cp = ColumnParent('Super3', 'sc1')
        predicate = SlicePredicate(column_names=['col1', 'col3'])
        range = KeyRange(start_token='55', end_token='55', count=100)
        result = client.get_range_slices("Keyspace2", cp, predicate, range, ConsistencyLevel.ONE)
        assert len(result) == 5
        assert result[0].columns[0].column.name == 'col1'
        assert result[0].columns[1].column.name == 'col3'

    def test_get_range_slice_super(self):
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert('Keyspace2', key, ColumnPath('Super3', 'sc1', cname), 'v-' + cname, 0, ConsistencyLevel.ONE)

        cp = ColumnParent('Super3', 'sc1')
        result = client.get_range_slice("Keyspace2", cp, SlicePredicate(column_names=['col1', 'col3']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3
        assert result[0].columns[0].column.name == 'col1'
        assert result[0].columns[1].column.name == 'col3'

        cp = ColumnParent('Super3')
        result = client.get_range_slice("Keyspace2", cp, SlicePredicate(column_names=['sc1']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3
        assert list(set(row.columns[0].super_column.name for row in result))[0] == 'sc1'
        
    def test_get_range_slice(self):
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert('Keyspace1', key, ColumnPath('Standard1', column=cname), 'v-' + cname, 0, ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')

        # test empty slice
        result = client.get_range_slice("Keyspace1", cp, SlicePredicate(column_names=['col1', 'col3']), 'key6', '', 1, ConsistencyLevel.ONE)
        assert len(result) == 0

        # test empty columns
        result = client.get_range_slice("Keyspace1", cp, SlicePredicate(column_names=['a']), 'key2', '', 1, ConsistencyLevel.ONE)
        assert len(result) == 1
        assert len(result[0].columns) == 0

        # test column_names predicate
        result = client.get_range_slice("Keyspace1", cp, SlicePredicate(column_names=['col1', 'col3']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3, result
        assert result[0].columns[0].column.name == 'col1'
        assert result[0].columns[1].column.name == 'col3'

        # row limiting via count.
        result = client.get_range_slice("Keyspace1", cp, SlicePredicate(column_names=['col1', 'col3']), 'key2', 'key4', 1, ConsistencyLevel.ONE)
        assert len(result) == 1

        # test column slice predicate
        result = client.get_range_slice('Keyspace1', cp, SlicePredicate(slice_range=SliceRange(start='col2', finish='col4', reversed=False, count=5)), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].key == 'key1'
        assert result[1].key == 'key2'
        assert len(result[0].columns) == 3
        assert result[0].columns[0].column.name == 'col2'
        assert result[0].columns[2].column.name == 'col4'

        # col limiting via count
        result = client.get_range_slice('Keyspace1', cp, SlicePredicate(slice_range=SliceRange(start='col2', finish='col4', reversed=False, count=2)), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 2

        # and reversed 
        result = client.get_range_slice('Keyspace1', cp, SlicePredicate(slice_range=SliceRange(start='col4', finish='col2', reversed=True, count=5)), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert result[0].columns[0].column.name == 'col4'
        assert result[0].columns[2].column.name == 'col2'

        # row limiting via count
        result = client.get_range_slice('Keyspace1', cp, SlicePredicate(slice_range=SliceRange(start='col2', finish='col4', reversed=False, count=5)), 'key1', 'key2', 1, ConsistencyLevel.ONE)
        assert len(result) == 1

        # removed data
        client.remove('Keyspace1', 'key1', ColumnPath('Standard1', column='col1'), 1, ConsistencyLevel.ONE)
        result = client.get_range_slice('Keyspace1', cp, SlicePredicate(slice_range=SliceRange('', '')), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert len(result) == 2, result
        assert result[0].columns[0].column.name == 'col2', result[0].columns[0].column.name
        assert result[1].columns[0].column.name == 'col1'
        
    
    def test_wrapped_range_slices(self):
        def copp_token(key):
            # I cheated and generated this from Java
            return {'a': '00530000000100000001', 
                    'b': '00540000000100000001', 
                    'c': '00550000000100000001',
                    'd': '00560000000100000001', 
                    'e': '00580000000100000001'}[key]
        for key in ['a', 'b', 'c', 'd', 'e']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert('Keyspace1', key, ColumnPath('Standard1', column=cname), 'v-' + cname, 0, ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')

        result = client.get_range_slices("Keyspace1", cp, SlicePredicate(column_names=['col1', 'col3']), KeyRange(start_token=copp_token('e'), end_token=copp_token('e')), ConsistencyLevel.ONE)
        assert [row.key for row in result] == ['a', 'b', 'c', 'd', 'e',], [row.key for row in result]

        result = client.get_range_slices("Keyspace1", cp, SlicePredicate(column_names=['col1', 'col3']), KeyRange(start_token=copp_token('c'), end_token=copp_token('c')), ConsistencyLevel.ONE)
        assert [row.key for row in result] == ['d', 'e', 'a', 'b', 'c',], [row.key for row in result]
        

    def test_get_slice_by_names(self):
        _insert_range()
        p = SlicePredicate(column_names=['c1', 'c2'])
        result = client.get_slice('Keyspace1', 'key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) 
        assert len(result) == 2
        assert result[0].column.name == 'c1'
        assert result[1].column.name == 'c2'

        _insert_super()
        p = SlicePredicate(column_names=[_i64(4)])
        result = client.get_slice('Keyspace1', 'key1', ColumnParent('Super1', 'sc1'), p, ConsistencyLevel.ONE) 
        assert len(result) == 1
        assert result[0].column.name == _i64(4)

    def test_multiget(self):
        """Insert multiple keys and retrieve them using the multiget interface"""

        # Generate a list of 10 keys and insert them
        num_keys = 10
        keys = ['key'+str(i) for i in range(1, num_keys+1)]
        _insert_multi(keys)

        # Retrieve all 10 keys
        rows = client.multiget('Keyspace1', keys, ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE)
        keys1 = rows.keys().sort()
        keys2 = keys.sort()

        # Validate if the returned rows have the keys requested and if the ColumnOrSuperColumn is what was inserted
        for key in keys:
            assert rows.has_key(key) == True
            assert rows[key] == ColumnOrSuperColumn(column=Column(timestamp=0, name='c1', value='value1'))

    def test_multiget_slice(self):
        """Insert multiple keys and retrieve them using the multiget_slice interface"""

        # Generate a list of 10 keys and insert them
        num_keys = 10
        keys = ['key'+str(i) for i in range(1, num_keys+1)]
        _insert_multi(keys)

        # Retrieve all 10 key slices
        rows = _big_multislice('Keyspace1', keys, ColumnParent('Standard1'))
        keys1 = rows.keys().sort()
        keys2 = keys.sort()

        columns = [ColumnOrSuperColumn(c) for c in _SIMPLE_COLUMNS]
        # Validate if the returned rows have the keys requested and if the ColumnOrSuperColumn is what was inserted
        for key in keys:
            assert rows.has_key(key) == True
            assert columns == rows[key]

    def test_batch_mutate_super_deletion(self):
        _insert_super('test')
        d = Deletion(1, predicate=SlicePredicate(column_names=['sc1']))
        cfmap = {'Super1': [Mutation(deletion=d)]}
        mutation_map = {'test': cfmap}
        client.batch_mutate('Keyspace1', mutation_map, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('Keyspace1', 'key1', ColumnPath('Super1', 'sc1'), ConsistencyLevel.ONE))

    def test_describe_keyspace(self):
        """ Test keyspace description """
        kspaces = client.describe_keyspaces()
        assert len(kspaces) == 5, kspaces # ['Keyspace1', 'Keyspace2', 'Keyspace3', 'Keyspace4', 'system']
        ks1 = client.describe_keyspace("Keyspace1")
        assert set(ks1.keys()) == set(['Super1', 'Standard1', 'Standard2', 'Standard3', 'Standard4', 'StandardLong1', 'StandardLong2', 'Super3', 'Super2', 'Super4'])
        sysks = client.describe_keyspace("system")

    def test_describe(self):
        server_version = client.describe_version()
        assert server_version == VERSION, (server_version, VERSION)
        assert client.describe_cluster_name() == 'Test Cluster'

    def test_describe_ring(self):
        assert list(client.describe_ring('Keyspace1'))[0].endpoints == ['127.0.0.1']
    
    def test_describe_ring_on_invalid_keyspace(self):
        def req():
            client.describe_ring('system')
        _expect_exception(req, InvalidRequestException)
