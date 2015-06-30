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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class CounterMutationTest
{
    private static final String KEYSPACE1 = "CounterMutationTest";
    private static final String CF1 = "Counter1";
    private static final String CF2 = "Counter2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF2));
    }

    @Test
    public void testSingleCell() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));

        // Do the initial update (+1)
        addAndCheck(cfs, 1, 1);

        // Make another increment (+2)
        addAndCheck(cfs, 2, 3);

        // Decrement to 0 (-3)
        addAndCheck(cfs, -3, 0);
    }

    private void addAndCheck(ColumnFamilyStore cfs, long toAdd, long expected)
    {
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));
        Mutation m = new RowUpdateBuilder(cfs.metadata, 5, "key1").clustering("cc").add("val", toAdd).build();
        new CounterMutation(m, ConsistencyLevel.ONE).apply();

        Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val").build());
        assertEquals(expected, CounterContext.instance().total(row.getCell(cDef).value()));
    }

    @Test
    public void testTwoCells() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        // Do the initial update (+1, -1)
        addTwoAndCheck(cfs, 1L, 1L, -1L, -1L);

        // Make another increment (+2, -2)
        addTwoAndCheck(cfs, 2L, 3L, -2L, -3L);

        // Decrement to 0 (-3, +3)
        addTwoAndCheck(cfs, -3L, 0L, 3L, 0L);
    }

    private void addTwoAndCheck(ColumnFamilyStore cfs, long addOne, long expectedOne, long addTwo, long expectedTwo)
    {
        ColumnDefinition cDefOne = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));
        ColumnDefinition cDefTwo = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val2"));

        Mutation m = new RowUpdateBuilder(cfs.metadata, 5, "key1")
            .clustering("cc")
            .add("val", addOne)
            .add("val2", addTwo)
            .build();
        new CounterMutation(m, ConsistencyLevel.ONE).apply();

        Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());
        assertEquals(expectedOne, CounterContext.instance().total(row.getCell(cDefOne).value()));
        assertEquals(expectedTwo, CounterContext.instance().total(row.getCell(cDefTwo).value()));
    }

    @Test
    public void testBatch() throws WriteTimeoutException
    {
        ColumnFamilyStore cfsOne = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        ColumnFamilyStore cfsTwo = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF2);

        cfsOne.truncateBlocking();
        cfsTwo.truncateBlocking();

        // Do the update (+1, -1), (+2, -2)
        Mutation batch = new Mutation(KEYSPACE1, Util.dk("key1"));
        batch.add(new RowUpdateBuilder(cfsOne.metadata, 5, "key1")
            .clustering("cc")
            .add("val", 1L)
            .add("val2", -1L)
            .build().get(cfsOne.metadata));

        batch.add(new RowUpdateBuilder(cfsTwo.metadata, 5, "key1")
            .clustering("cc")
            .add("val", 2L)
            .add("val2", -2L)
            .build().get(cfsTwo.metadata));

        new CounterMutation(batch, ConsistencyLevel.ONE).apply();

        ColumnDefinition c1cfs1 = cfsOne.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));
        ColumnDefinition c2cfs1 = cfsOne.metadata.getColumnDefinition(ByteBufferUtil.bytes("val2"));

        Row row = Util.getOnlyRow(Util.cmd(cfsOne).includeRow("cc").columns("val", "val2").build());
        assertEquals(1L, CounterContext.instance().total(row.getCell(c1cfs1).value()));
        assertEquals(-1L, CounterContext.instance().total(row.getCell(c2cfs1).value()));

        ColumnDefinition c1cfs2 = cfsTwo.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));
        ColumnDefinition c2cfs2 = cfsTwo.metadata.getColumnDefinition(ByteBufferUtil.bytes("val2"));
        row = Util.getOnlyRow(Util.cmd(cfsTwo).includeRow("cc").columns("val", "val2").build());
        assertEquals(2L, CounterContext.instance().total(row.getCell(c1cfs2).value()));
        assertEquals(-2L, CounterContext.instance().total(row.getCell(c2cfs2).value()));

        // Check the caches, separately
        CBuilder cb = CBuilder.create(cfsOne.metadata.comparator);
        cb.add("cc");

        assertEquals(ClockAndCount.create(1L, 1L), cfsOne.getCachedCounter(Util.dk("key1").getKey(), cb.build(), c1cfs1, null));
        assertEquals(ClockAndCount.create(1L, -1L), cfsOne.getCachedCounter(Util.dk("key1").getKey(), cb.build(), c2cfs1, null));

        assertEquals(ClockAndCount.create(1L, 2L), cfsTwo.getCachedCounter(Util.dk("key1").getKey(), cb.build(), c1cfs2, null));
        assertEquals(ClockAndCount.create(1L, -2L), cfsTwo.getCachedCounter(Util.dk("key1").getKey(), cb.build(), c2cfs2, null));
    }

    @Test
    public void testDeletes() throws WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();
        ColumnDefinition cOne = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));
        ColumnDefinition cTwo = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val2"));

        // Do the initial update (+1, -1)
        new CounterMutation(
            new RowUpdateBuilder(cfs.metadata, 5, "key1")
                .clustering("cc")
                .add("val", 1L)
                .add("val2", -1L)
                .build(),
            ConsistencyLevel.ONE).apply();

        Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());
        assertEquals(1L, CounterContext.instance().total(row.getCell(cOne).value()));
        assertEquals(-1L, CounterContext.instance().total(row.getCell(cTwo).value()));

        // Remove the first counter, increment the second counter
        new CounterMutation(
            new RowUpdateBuilder(cfs.metadata, 5, "key1")
                .clustering("cc")
                .delete(cOne)
                .add("val2", -5L)
                .build(),
            ConsistencyLevel.ONE).apply();

        row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());
        assertEquals(null, row.getCell(cOne));
        assertEquals(-6L, CounterContext.instance().total(row.getCell(cTwo).value()));

        // Increment the first counter, make sure it's still shadowed by the tombstone
        new CounterMutation(
            new RowUpdateBuilder(cfs.metadata, 5, "key1")
                .clustering("cc")
                .add("val", 1L)
                .build(),
            ConsistencyLevel.ONE).apply();
        row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());
        assertEquals(null, row.getCell(cOne));

        // Get rid of the complete partition
        RowUpdateBuilder.deleteRow(cfs.metadata, 6, "key1", "cc").applyUnsafe();
        Util.assertEmpty(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());

        // Increment both counters, ensure that both stay dead
        new CounterMutation(
            new RowUpdateBuilder(cfs.metadata, 6, "key1")
                .clustering("cc")
                .add("val", 1L)
                .add("val2", 1L)
                .build(),
            ConsistencyLevel.ONE).apply();
        Util.assertEmpty(Util.cmd(cfs).includeRow("cc").columns("val", "val2").build());
    }
}
