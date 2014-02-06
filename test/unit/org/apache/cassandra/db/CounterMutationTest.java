/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CounterMutationTest extends SchemaLoader
{
    private static final String KS = "CounterCacheSpace";
    private static final String CF1 = "Counter1";
    private static final String CF2 = "Counter2";

    @Test
    public void testSingleCell() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KS).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        // Do the initial update (+1)
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), 1L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        ColumnFamily current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertEquals(1L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));

        // Make another increment (+2)
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), 2L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertEquals(3L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));

        // Decrement to 0 (-3)
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), -3L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertEquals(0L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));
        assertEquals(ClockAndCount.create(3L, 0L), cfs.getCachedCounter(bytes(1), cellname(1)));
    }

    @Test
    public void testTwoCells() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KS).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        // Do the initial update (+1, -1)
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), 1L);
        cells.addCounter(cellname(2), -1L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        ColumnFamily current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertEquals(1L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));
        assertEquals(-1L, CounterContext.instance().total(current.getColumn(cellname(2)).value()));

        // Make another increment (+2, -2)
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), 2L);
        cells.addCounter(cellname(2), -2L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertEquals(3L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));

        // Decrement to 0 (-3, +3)
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), -3L);
        cells.addCounter(cellname(2), 3L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertEquals(0L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));
        assertEquals(0L, CounterContext.instance().total(current.getColumn(cellname(2)).value()));

        // Check the caches, separately
        assertEquals(ClockAndCount.create(3L, 0L), cfs.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(3L, 0L), cfs.getCachedCounter(bytes(1), cellname(2)));
    }

    @Test
    public void testBatch() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs1 = Keyspace.open(KS).getColumnFamilyStore(CF1);
        ColumnFamilyStore cfs2 = Keyspace.open(KS).getColumnFamilyStore(CF2);

        cfs1.truncateBlocking();
        cfs2.truncateBlocking();

        // Do the update (+1, -1), (+2, -2)
        ColumnFamily cells1 = ArrayBackedSortedColumns.factory.create(cfs1.metadata);
        cells1.addCounter(cellname(1), 1L);
        cells1.addCounter(cellname(2), -1L);

        ColumnFamily cells2 = ArrayBackedSortedColumns.factory.create(cfs2.metadata);
        cells2.addCounter(cellname(1), 2L);
        cells2.addCounter(cellname(2), -2L);

        Mutation mutation = new Mutation(KS, bytes(1));
        mutation.add(cells1);
        mutation.add(cells2);

        new CounterMutation(mutation, ConsistencyLevel.ONE).apply();

        // Validate all values
        ColumnFamily current1 = cfs1.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        ColumnFamily current2 = cfs2.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF2, System.currentTimeMillis()));

        assertEquals(1L, CounterContext.instance().total(current1.getColumn(cellname(1)).value()));
        assertEquals(-1L, CounterContext.instance().total(current1.getColumn(cellname(2)).value()));
        assertEquals(2L, CounterContext.instance().total(current2.getColumn(cellname(1)).value()));
        assertEquals(-2L, CounterContext.instance().total(current2.getColumn(cellname(2)).value()));

        // Check the caches, separately
        assertEquals(ClockAndCount.create(1L, 1L), cfs1.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(1L, -1L), cfs1.getCachedCounter(bytes(1), cellname(2)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs2.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(1L, -2L), cfs2.getCachedCounter(bytes(1), cellname(2)));
    }

    @Test
    public void testDeletes() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KS).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        // Do the initial update (+1, -1)
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), 1L);
        cells.addCounter(cellname(2), 1L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        ColumnFamily current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertEquals(1L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));
        assertEquals(1L, CounterContext.instance().total(current.getColumn(cellname(2)).value()));

        // Remove the first counter, increment the second counter
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addTombstone(cellname(1), (int) System.currentTimeMillis() / 1000, FBUtilities.timestampMicros());
        cells.addCounter(cellname(2), 1L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertNull(current.getColumn(cellname(1)));
        assertEquals(2L, CounterContext.instance().total(current.getColumn(cellname(2)).value()));

        // Increment the first counter, make sure it's still shadowed by the tombstone
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), 1L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertNull(current.getColumn(cellname(1)));

        // Get rid of the complete partition
        Mutation mutation = new Mutation(KS, bytes(1));
        mutation.delete(CF1, FBUtilities.timestampMicros());
        new CounterMutation(mutation, ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertNull(current.getColumn(cellname(1)));
        assertNull(current.getColumn(cellname(2)));

        // Increment both counters, ensure that both stay dead
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), 1L);
        cells.addCounter(cellname(2), 1L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        assertNull(current.getColumn(cellname(1)));
        assertNull(current.getColumn(cellname(2)));
    }

    @Test
    public void testDuplicateCells() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KS).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), 1L);
        cells.addCounter(cellname(1), 2L);
        cells.addCounter(cellname(1), 3L);
        cells.addCounter(cellname(1), 4L);
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();

        ColumnFamily current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(bytes(1)), CF1, System.currentTimeMillis()));
        ByteBuffer context = current.getColumn(cellname(1)).value();
        assertEquals(10L, CounterContext.instance().total(context));
        assertEquals(ClockAndCount.create(1L, 10L), CounterContext.instance().getLocalClockAndCount(context));
        assertEquals(ClockAndCount.create(1L, 10L), cfs.getCachedCounter(bytes(1), cellname(1)));
    }
}
