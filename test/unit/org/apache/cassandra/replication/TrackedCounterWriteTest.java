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
package org.apache.cassandra.replication;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for Token-Aware (Normal Path) Counter Mutations for Mutation Tracking
 * In these tests, the coordinator IS a replica.
 */
public class TrackedCounterWriteTest
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedCounterWriteTest.class);

    private static final String KEYSPACE_TRACKED = "TrackedCounterTest";
    private static final String CF_COUNTER = "Counter1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE_TRACKED,
                                    KeyspaceParams.simple(3, ReplicationType.tracked),
                                    SchemaLoader.counterCFMD(KEYSPACE_TRACKED, CF_COUNTER));
    }

    /**
     * Tests that counter writes are properly tracked when the coordinator is a replica.
     * Verifies that:
     * 1. CounterMutation initially has no ID before perform()
     * 2. TrackedWriteRequest.perform() successfully processes the mutation
     * 3. The counter value is correctly incremented
     */
    @Test
    public void testTrackedCounterWrite_CoordinatorIsReplica() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE_TRACKED).getColumnFamilyStore(CF_COUNTER);
        cfs.truncateBlocking();

        ColumnMetadata cDef = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));

        Mutation m = new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), "testkey1")
                         .clustering("cc")
                         .add("val", 5L)
                         .build();

        CounterMutation counterMutation = new CounterMutation(m, ConsistencyLevel.ONE);

        assertTrue("CounterMutation should not have an ID before perform()",
                    counterMutation.id().isNone());

        AbstractWriteResponseHandler<?> handler = TrackedWriteRequest.perform(
            counterMutation,
            ConsistencyLevel.ONE,
            Dispatcher.RequestTime.forImmediateExecution()
        );
        assertNotNull("Handler should not be null", handler);

        Thread.sleep(100); // Waiting for async operations

        Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val").build());
        long counterValue = CounterContext.instance().total(row.getCell(cDef));
        assertEquals("Counter should be incremented to 5", 5L, counterValue);
    }

    /**
     * Tests that multiple counter updates on the same key accumulate correctly with tracking.
     * Verifies that:
     * 1. Multiple sequential increments/decrements are properly tracked
     * 2. Counter values accumulate correctly across updates
     */
    @Test
    public void testTrackedCounterWrite_MultipleIncrements() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE_TRACKED).getColumnFamilyStore(CF_COUNTER);
        cfs.truncateBlocking();

        ColumnMetadata cDef = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));

        performCounterUpdate(cfs, "testkey2", 3L); // First increment (+3)

        Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val").build());
        long value = CounterContext.instance().total(row.getCell(cDef));
        assertEquals("Counter should be 3", 3L, value);

        performCounterUpdate(cfs, "testkey2", 7L); // Second increment (+7)

        row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val").build());
        value = CounterContext.instance().total(row.getCell(cDef));
        assertEquals("Counter should be 10", 10L, value);

        performCounterUpdate(cfs, "testkey2", -4L); // Decrement (-4)

        row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val").build());
        value = CounterContext.instance().total(row.getCell(cDef));
        assertEquals("Counter should be 6", 6L, value);
    }

    /**
     * Performs a counter update and waits for async replication to complete
     */
    private void performCounterUpdate(ColumnFamilyStore cfs, String key, long delta) throws Exception
    {
        Mutation m = new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), key)
                         .clustering("cc")
                         .add("val", delta)
                         .build();

        CounterMutation counterMutation = new CounterMutation(m, ConsistencyLevel.ONE);

        TrackedWriteRequest.perform(
            counterMutation,
            ConsistencyLevel.ONE,
            Dispatcher.RequestTime.forImmediateExecution()
        );

        Thread.sleep(50);
    }
}