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


def _i64(n):
    return struct.pack('<q', n) # little endian, to match cassandra.db.marshal.LongType

_SIMPLE_COLUMNS = [Column('c1', 'value1', 0),
                   Column('c2', 'value2', 0)]
_SUPER_COLUMNS = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 0),
                                                   Column(_i64(6), 'value6', 0)])]

def _insert_simple(block=True):
    if block:
        consistencyLevel = ConsistencyLevel.ONE
    else:
        consistencyLevel = ConsistencyLevel.ZERO
    client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 0, consistencyLevel)
    client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c2'), 'value2', 0, consistencyLevel)

def _insert_batch(block):
    cfmap = {'Standard1': _SIMPLE_COLUMNS,
             'Standard2': _SIMPLE_COLUMNS}
    if block:
        consistencyLevel = ConsistencyLevel.ONE
    else:
        consistencyLevel = ConsistencyLevel.ZERO

    client.batch_insert('Keyspace1', BatchMutation(key='key1', cfmap=cfmap), consistencyLevel)

def _verify_batch():
    _verify_simple()
    L = client.get_slice('Keyspace1', 'key1', ColumnParent('Standard2'), '', '', True, 1000, ConsistencyLevel.ONE)
    assert L == _SIMPLE_COLUMNS, L

def _verify_simple():
    assert client.get_column('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE) == Column('c1', 'value1', 0)
    L = client.get_slice('Keyspace1', 'key1', ColumnParent('Standard1'), '', '', True, 1000, ConsistencyLevel.ONE)
    assert L == _SIMPLE_COLUMNS, L

def _insert_super():
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc1', _i64(4)), 'value4', 0, ConsistencyLevel.ZERO)
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 0, ConsistencyLevel.ZERO)
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(6)), 'value6', 0, ConsistencyLevel.ZERO)
    time.sleep(0.1)

def _insert_range():
    client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 0, ConsistencyLevel.ONE)
    client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c2'), 'value2', 0, ConsistencyLevel.ONE)
    client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c3'), 'value3', 0, ConsistencyLevel.ONE)
    time.sleep(0.1)

def _verify_range():
    result = client.get_slice('Keyspace1','key1', ColumnParent('Standard1'), 'c1', 'c2', True, 1000, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].name == 'c1'
    assert result[1].name == 'c2'

    result = client.get_slice('Keyspace1','key1', ColumnParent('Standard1'), 'c3', 'c2', False, 1000, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].name == 'c3'
    assert result[1].name == 'c2'

    result = client.get_slice('Keyspace1','key1', ColumnParent('Standard1'), 'a', 'z' , True, 1000, ConsistencyLevel.ONE)
    assert len(result) == 3, result
    
    result = client.get_slice('Keyspace1','key1', ColumnParent('Standard1'), 'a', 'z' , True, 2, ConsistencyLevel.ONE)
    assert len(result) == 2, result
	 	
def _insert_super_range():
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc1', _i64(4)), 'value4', 0, False)
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 0, False)
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(6)), 'value6', 0, False)
    client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc3', _i64(7)), 'value7', 0, False)
    time.sleep(0.1)

def _verify_super_range():
    result = client.get_slice_super('Keyspace1','key1', 'Super1', 'sc2', 'sc3', True, 2, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].name == 'sc2'
    assert result[1].name == 'sc3'

    result = client.get_slice_super('Keyspace1','key1', 'Super1', 'sc3', 'sc2', False, 2, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].name == 'sc3'
    assert result[1].name == 'sc2'

def _verify_super(supercf='Super1'):
    assert client.get_column('Keyspace1', 'key1', ColumnPath(supercf, 'sc1', _i64(4)), ConsistencyLevel.ONE) == Column(_i64(4), 'value4', 0)
    slice = client.get_slice_super('Keyspace1', 'key1', 'Super1', '', '', True, 1000, ConsistencyLevel.ONE)
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


class TestMutations(CassandraTester):
    def test_insert(self):
        _insert_simple(False)
        time.sleep(0.1)
        _verify_simple()

    def test_empty_slice(self):
        assert client.get_slice('Keyspace1', 'key1', ColumnParent('Standard2'), '', '', True, 1000, ConsistencyLevel.ONE) == []

    def test_empty_slice_super(self):
        assert client.get_slice_super('Keyspace1', 'key1', 'Super1', '', '', True, 1000, ConsistencyLevel.ONE) == []

    def test_missing_super(self):
        _expect_missing(lambda: client.get_column('Keyspace1', 'key1', ColumnPath('Super1', 'sc1', 'c1'), ConsistencyLevel.ONE))

    def test_count(self):
        _insert_simple()
        _insert_super()
        assert client.get_column_count('Keyspace1', 'key1', ColumnParent('Standard2'), ConsistencyLevel.ONE) == 0
        assert client.get_column_count('Keyspace1', 'key1', ColumnParent('Standard1'), ConsistencyLevel.ONE) == 2
        assert client.get_column_count('Keyspace1', 'key1', ColumnParent('Super1', 'sc2'), ConsistencyLevel.ONE) == 2
        assert client.get_column_count('Keyspace1', 'key1', ColumnParent('Super1'), ConsistencyLevel.ONE) == 2

    def test_insert_blocking(self):
        _insert_simple()
        _verify_simple()

    def test_super_insert(self):
        _insert_super()
        _verify_super()

    def test_batch_insert(self):
        _insert_batch(False)
        time.sleep(0.1)
        _verify_batch()

    def test_batch_insert_blocking(self):
        _insert_batch(True)
        _verify_batch()

    def test_bad_calls(self):
        _expect_exception(lambda: client.insert('Keyspace1', 'key1', ColumnPath('Standard1', 'x', 'y'), 'value', 0, ConsistencyLevel.ONE), InvalidRequestException)

        _expect_exception(lambda: client.get_column('Keyspace1', 'key1', ColumnPath('Standard1'), ConsistencyLevel.ONE), InvalidRequestException)
        _expect_exception(lambda: client.get_column('Keyspace1', 'key1', ColumnPath('Standard1', 'x', 'y'), ConsistencyLevel.ONE), InvalidRequestException)
        _expect_exception(lambda: client.get_column('Keyspace1', 'key1', ColumnPath('Super1'), ConsistencyLevel.ONE), InvalidRequestException)
        _expect_exception(lambda: client.get_column('Keyspace1', 'key1', ColumnPath('Super1', 'x'), ConsistencyLevel.ONE), InvalidRequestException)
        _expect_exception(lambda: client.get_key_range('Keyspace1', 'S', '', '', 1000), InvalidRequestException)

    def test_batch_insert_super(self):
         cfmap = {'Super1': _SUPER_COLUMNS,
                  'Super2': _SUPER_COLUMNS}
         client.batch_insert_super_column('Keyspace1', BatchMutation(key='key1', cfmap=cfmap), ConsistencyLevel.ZERO)
         time.sleep(0.1)
         _verify_super('Super1')
         _verify_super('Super2')

    def test_batch_insert_super_blocking(self):
         cfmap = {'Super1': _SUPER_COLUMNS,
                  'Super2': _SUPER_COLUMNS}
         client.batch_insert_super_column('Keyspace1', BatchMutation(key='key1', cfmap=cfmap), ConsistencyLevel.ONE)
         _verify_super('Super1')
         _verify_super('Super2')

    def test_cf_remove_column(self):
        _insert_simple()
        client.remove('Keyspace1', 'key1', ColumnPathOrParent('Standard1', column='c1'), 1, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get_column('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE))
        assert client.get_column('Keyspace1', 'key1', ColumnPath('Standard1', column='c2'), ConsistencyLevel.ONE) \
            == Column('c2', 'value2', 0)
        assert client.get_slice('Keyspace1', 'key1', ColumnParent('Standard1'), '', '', True, 1000, ConsistencyLevel.ONE) \
            == [Column('c2', 'value2', 0)]

        # New insert, make sure it shows up post-remove:
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c3'), 'value3', 0, ConsistencyLevel.ONE)
        assert client.get_slice('Keyspace1', 'key1', ColumnParent('Standard1'), '', '', True, 1000, ConsistencyLevel.ONE) == \
            [Column('c2', 'value2', 0), Column('c3', 'value3', 0)]

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 0, ConsistencyLevel.ONE)
        assert client.get_slice('Keyspace1', 'key1', ColumnParent('Standard1'), '', '', True, 1000, ConsistencyLevel.ONE) == \
            [Column('c2', 'value2', 0), Column('c3', 'value3', 0)]
        # Next, w/ a newer timestamp; it should come back:
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 2, ConsistencyLevel.ONE)
        assert client.get_slice('Keyspace1', 'key1', ColumnParent('Standard1'), '', '', True, 1000, ConsistencyLevel.ONE) == \
            [Column('c1', 'value1', 2), Column('c2', 'value2', 0), Column('c3', 'value3', 0)]


    def test_cf_remove(self):
        _insert_simple()
        _insert_super()

        # Remove the key1:Standard1 cf:
        client.remove('Keyspace1', 'key1', ColumnPathOrParent('Standard1'), 3, ConsistencyLevel.ONE)
        assert client.get_slice('Keyspace1', 'key1', ColumnParent('Standard1'), '', '', True, 1000, ConsistencyLevel.ONE) == []
        _verify_super()

        # Test resurrection.  First, re-insert a value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 0, ConsistencyLevel.ONE)
        assert client.get_slice('Keyspace1', 'key1', ColumnParent('Standard1'), '', '', True, 1000, ConsistencyLevel.ONE) == []
        # Next, w/ a newer timestamp; it should come back:
        client.insert('Keyspace1', 'key1', ColumnPath('Standard1', column='c1'), 'value1', 4, ConsistencyLevel.ONE)
        assert client.get_slice('Keyspace1', 'key1', ColumnParent('Standard1'), '', '', True, 1000, ConsistencyLevel.ONE) == \
            [Column('c1', 'value1', 4)]


    def test_super_cf_remove_column(self):
        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove('Keyspace1', 'key1', ColumnPathOrParent('Super1', 'sc2', _i64(5)), 5, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get_column('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), ConsistencyLevel.ONE))
        assert client.get_slice_super('Keyspace1', 'key1', 'Super1', '', '', True, 1000, ConsistencyLevel.ONE) == \
            [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
             SuperColumn(name='sc2', columns=[Column(_i64(6), 'value6', 0)])]
        _verify_simple()

        # New insert, make sure it shows up post-remove:
        client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(7)), 'value7', 0, ConsistencyLevel.ONE)
        scs = [SuperColumn(name='sc1', 
                           columns=[Column(_i64(4), 'value4', 0)]),
               SuperColumn(name='sc2', 
                           columns=[Column(_i64(6), 'value6', 0), Column(_i64(7), 'value7', 0)])]

        actual = client.get_slice_super('Keyspace1', 'key1', 'Super1', '', '', True, 1000, ConsistencyLevel.ONE)
        assert client.get_slice_super('Keyspace1', 'key1', 'Super1', '', '', True, 1000, ConsistencyLevel.ONE) == scs, actual

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 0, ConsistencyLevel.ONE)

        actual = client.get_slice_super('Keyspace1', 'key1', 'Super1', '', '', True, 1000, ConsistencyLevel.ONE)
        assert actual == scs, actual

        # Next, w/ a newer timestamp; it should come back
        client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 6, ConsistencyLevel.ONE)
        actual = client.get_slice_super('Keyspace1', 'key1', 'Super1', '', '', True, 1000, ConsistencyLevel.ONE)
        assert actual == \
            [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]), 
             SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 6), 
                                              Column(_i64(6), 'value6', 0), 
                                              Column(_i64(7), 'value7', 0)])], actual

    def test_super_cf_remove_supercolumn(self):
        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove('Keyspace1', 'key1', ColumnPathOrParent('Super1', 'sc2'), 5, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get_column('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), ConsistencyLevel.ONE))
        actual = client.get_slice('Keyspace1', 'key1', ColumnParent('Super1', 'sc2'), '', '', True, 1000, ConsistencyLevel.ONE)
        assert actual == [], actual
        scs = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)])]
        actual = client.get_slice_super('Keyspace1', 'key1', 'Super1', '', '', True, 1000, ConsistencyLevel.ONE)
        assert actual == scs, actual
        _verify_simple()

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 0, ConsistencyLevel.ONE)
        actual = client.get_slice_super('Keyspace1', 'key1', 'Super1', '', '', True, 1000, ConsistencyLevel.ONE)
        assert actual == scs, actual

        # Next, w/ a newer timestamp; it should come back
        client.insert('Keyspace1', 'key1', ColumnPath('Super1', 'sc2', _i64(5)), 'value5', 6, ConsistencyLevel.ONE)
        actual = client.get_slice_super('Keyspace1', 'key1', 'Super1', '', '', True, 1000, ConsistencyLevel.ONE)
        assert actual == \
            [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
             SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 6)])], actual


    def test_empty_range(self):
        assert client.get_key_range('Keyspace1', 'Standard1', '', '', 1000) == []
        _insert_simple()
        assert client.get_key_range('Keyspace1', 'Super1', '', '', 1000) == []

    def test_range_with_remove(self):
        _insert_simple()
        assert client.get_key_range('Keyspace1', 'Standard1', 'key1', '', 1000) == ['key1']

        client.remove('Keyspace1', 'key1', ColumnPathOrParent('Standard1', column='c1'), 1, ConsistencyLevel.ONE)
        client.remove('Keyspace1', 'key1', ColumnPathOrParent('Standard1', column='c2'), 1, ConsistencyLevel.ONE)
        actual = client.get_key_range('Keyspace1', 'Standard1', '', '', 1000)
        assert actual == [], actual

    def test_range_with_remove_cf(self):
        _insert_simple()
        assert client.get_key_range('Keyspace1', 'Standard1', 'key1', '', 1000) == ['key1']

        client.remove('Keyspace1', 'key1', ColumnPathOrParent('Standard1'), 1, ConsistencyLevel.ONE)
        actual = client.get_key_range('Keyspace1', 'Standard1', '', '', 1000)
        assert actual == [], actual

    def test_range_collation(self):
        for key in ['-a', '-b', 'a', 'b'] + [str(i) for i in xrange(100)]:
            client.insert('Keyspace1', key, ColumnPath('Standard1', column=key), 'v', 0, ConsistencyLevel.ONE)
        L = client.get_key_range('Keyspace1', 'Standard1', '', '', 1000)
        # note the collated ordering rather than ascii
        assert L == ['0', '1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2', '20', '21', '22', '23', '24', '25', '26', '27','28', '29', '3', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '4', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '5', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '6', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '7', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '8', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '9', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', 'a', '-a', 'b', '-b'], L

    def test_range_partial(self):
        for key in ['-a', '-b', 'a', 'b'] + [str(i) for i in xrange(100)]:
            client.insert('Keyspace1', key, ColumnPath('Standard1', column=key), 'v', 0, ConsistencyLevel.ONE)

        L = client.get_key_range('Keyspace1', 'Standard1', 'a', '', 1000)
        assert L == ['a', '-a', 'b', '-b'], L

        L = client.get_key_range('Keyspace1', 'Standard1', '', '15', 1000)
        assert L == ['0', '1', '10', '11', '12', '13', '14', '15'], L

        L = client.get_key_range('Keyspace1', 'Standard1', '50', '51', 1000)
        assert L == ['50', '51'], L
    
        L = client.get_key_range('Keyspace1', 'Standard1', '1', '', 10)
        assert L == ['1', '10', '11', '12', '13', '14', '15', '16', '17', '18'], L

    def test_get_slice_range(self):
	_insert_range()
	_verify_range()
        
    def test_get_slice_super_range(self):
	_insert_super_range()
	_verify_super_range()
        
    def test_get_slice_by_names(self):
        _insert_range()
        result = client.get_slice_by_names('Keyspace1', 'key1', ColumnParent('Standard1'), ['c1', 'c2'], ConsistencyLevel.ONE) 
        assert len(result) == 2
        assert result[0].name == 'c1'
        assert result[1].name == 'c2'

        _insert_super()
        result = client.get_slice_by_names('Keyspace1', 'key1', ColumnParent('Super1', 'sc1'), [_i64(4)], ConsistencyLevel.ONE) 
        assert len(result) == 1
        assert result[0].name == _i64(4)
