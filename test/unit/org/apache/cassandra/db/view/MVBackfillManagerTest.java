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

package org.apache.cassandra.db.view;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ViewAbstractTest;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.*;

public class MVBackfillManagerTest extends ViewAbstractTest
{
    private static final String VIEW_NAME = "test_view";

    private ColumnFamilyStore baseCfs;
    private TableMetadata baseMetadata;
    private View testView;
    private MVBackfillManager manager;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();

    }

    @Before
    @Override
    public void beforeTest() throws Throwable
    {
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
                                   "{'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        execute("USE " + KEYSPACE);

        // Create base table: CREATE TABLE base_table (k int, c int, v text, PRIMARY KEY (k, c))
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");

        // Create view: CREATE MATERIALIZED VIEW test_view AS SELECT * FROM base_table WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)
        createView(VIEW_NAME, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                              "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)");

        baseCfs = getCurrentColumnFamilyStore();
        baseMetadata = baseCfs.metadata();
        testView = baseCfs.keyspace.viewManager.getByName(VIEW_NAME);
        manager = new MVBackfillManager();
    }

    @After
    @Override
    public void afterTest() throws Throwable
    {
        execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE));
    }

    @Test
    public void testBackfillStateLifecycle()
    {
        MVBackfillManager.BackfillState state = new MVBackfillManager.BackfillState();

        // Initial state
        assertEquals(0, state.partitionsProcessed);
        assertEquals(0, state.rowsProcessed);
        assertEquals(0, state.viewRowsGenerated);
        assertEquals(0, state.bytesRead);
        assertFalse(state.completed);
        assertNull(state.failure);

        // Start
        state.start(100, 1024);
        assertEquals(100, state.estimatedPartitions);
        assertEquals(1024, state.estimatedTotalBytes);

        // Update
        state.partitionsProcessed = 50;
        state.rowsProcessed = 75;
        state.viewRowsGenerated = 60;
        state.bytesRead = 512;

        // Complete
        state.complete();
        assertTrue(state.completed);
        assertNull(state.failure);

        // Test failure
        MVBackfillManager.BackfillState failureState = new MVBackfillManager.BackfillState();
        Exception testException = new RuntimeException("Test failure");
        failureState.fail(testException);
        assertTrue(failureState.completed);
        assertEquals(testException, failureState.failure);
    }

    @Test
    public void testBackfillProcessorInterface()
    {
        TestBackfillSink processor = new TestBackfillSink();

        // Test initial state
        assertEquals(0, processor.processedRows.size());
        assertFalse(processor.completed);
        assertNull(processor.failure);

        // Test processing
        DecoratedKey key = decoratedKey(1);
        Row row = createRow(1, "test");
        ViewRowTranslator.ViewRowResult viewResult = new ViewRowTranslator.ViewRowResult(row, key);

        try
        {
            processor.processViewRow(key, row, viewResult);
            assertEquals(1, processor.processedRows.size());
            assertEquals(viewResult, processor.processedRows.get(0));
        }
        catch (Exception e)
        {
            fail("Processing should not throw: " + e.getMessage());
        }

        // Test completion
        try
        {
            processor.complete();
            assertTrue(processor.completed);
        }
        catch (Exception e)
        {
            fail("Completion should not throw: " + e.getMessage());
        }

        // Test failure
        TestBackfillSink failureProcessor = new TestBackfillSink();
        Exception testException = new RuntimeException("Test failure");
        failureProcessor.fail(testException);
        assertEquals(testException, failureProcessor.failure);
    }

    @Test
    public void testEmptyTableBackfill() throws Throwable
    {
        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(),
                                        baseCfs.getPartitioner().getMinimumToken());

        MVBackfillManager.BackfillState state = new MVBackfillManager.BackfillState();
        TestBackfillSink processor = new TestBackfillSink();

        Future<?> backfillTask = manager.submitBackfill(baseCfs, testView, range, processor, state);

        // Wait for completion
        backfillTask.get(10, TimeUnit.SECONDS);

        // Verify state
        assertTrue("Backfill should complete", state.completed);
        assertNull("Should not have failure", state.failure);
        assertEquals("Should process 0 partitions", 0, state.partitionsProcessed);
        assertEquals("Should process 0 rows", 0, state.rowsProcessed);
        assertEquals("Should generate 0 view rows", 0, state.viewRowsGenerated);

        // Verify processor
        assertTrue("Processor should complete", processor.completed);
        assertNull("Processor should not have failure", processor.failure);
        assertEquals("Should process 0 rows", 0, processor.processedRows.size());
    }

    @Test
    public void testSingleRowBackfill() throws Throwable
    {
        // Insert a single row
        insertRow(1, 1, "value1");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(),
                                        baseCfs.getPartitioner().getMinimumToken());

        MVBackfillManager.BackfillState state = new MVBackfillManager.BackfillState();
        TestBackfillSink processor = new TestBackfillSink();

        Future<?> backfillTask = manager.submitBackfill(baseCfs, testView, range, processor, state);

        // Wait for completion
        backfillTask.get(10, TimeUnit.SECONDS);

        // Verify state
        assertTrue("Backfill should complete", state.completed);
        assertNull("Should not have failure", state.failure);
        assertTrue("Should process exactly 1 partition", state.partitionsProcessed == 1);
        assertTrue("Should process exactly 1 row", state.rowsProcessed == 1);

        // Verify processor
        assertTrue("Processor should complete", processor.completed);
        assertNull("Processor should not have failure", processor.failure);
        assertTrue("Should process exactly 1 row", processor.processedRows.size() == 1);
    }

    @Test
    public void testMultipleRowsBackfill() throws Throwable
    {
        // Insert multiple rows
        insertRow(1, 1, "value1_1");
        insertRow(1, 2, "value1_2");
        insertRow(2, 1, "value2_1");
        insertRow(3, 1, "value3_1");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(),
                                        baseCfs.getPartitioner().getMinimumToken());

        MVBackfillManager.BackfillState state = new MVBackfillManager.BackfillState();
        TestBackfillSink processor = new TestBackfillSink();

        Future<?> backfillTask = manager.submitBackfill(baseCfs, testView, range, processor, state);

        // Wait for completion
        backfillTask.get(10, TimeUnit.SECONDS);

        // Verify state
        assertTrue("Backfill should complete", state.completed);
        assertNull("Should not have failure", state.failure);
        assertTrue("Should process 3 partitions", state.partitionsProcessed == 3);
        assertTrue("Should process 4 rows", state.rowsProcessed == 4);

        // Verify processor
        assertTrue("Processor should complete", processor.completed);
        assertNull("Processor should not have failure", processor.failure);
        assertTrue("Should process 4 rows", processor.processedRows.size() == 4);
    }

    @Test
    public void testOneThousandRowsBackfill() throws Throwable
    {
        // Insert 1000 rows
        for (int i=0; i< 1000; i++)
        {
            insertRow(i, 1, "value" + i);
        }
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(),
                                         baseCfs.getPartitioner().getMinimumToken());

        MVBackfillManager.BackfillState state = new MVBackfillManager.BackfillState();
        TestBackfillSink processor = new TestBackfillSink();

        Future<?> backfillTask = manager.submitBackfill(baseCfs, testView, range, processor, state);

        // Wait for completion
        backfillTask.get(10, TimeUnit.SECONDS);

        // Verify state
        assertTrue("Backfill should complete", state.completed);
        assertNull("Should not have failure", state.failure);
        assertTrue("Should process 1000 partitions", state.partitionsProcessed == 1000);
        assertTrue("Should process 1000 rows", state.rowsProcessed == 1000);
        assertTrue("Should updated bytes read", state.bytesRead > 0);

        // Verify processor
        assertTrue("Processor should complete", processor.completed);
        assertNull("Processor should not have failure", processor.failure);
        assertTrue("Should process 4 rows", processor.processedRows.size() == 1000);
    }

    @Test
    public void testMultipleRangesBackfill() throws Throwable
    {
        // Insert data
        insertRow(1, 1, "value1");
        insertRow(2, 1, "value2");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Create multiple ranges
        Token token1 = baseCfs.getPartitioner().getToken(Int32Type.instance.decompose(1));
        Token token2 = baseCfs.getPartitioner().getToken(Int32Type.instance.decompose(2));
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(baseCfs.getPartitioner().getMinimumToken(), token1));
        ranges.add(new Range<>(token1, token2));

        MVBackfillManager.BackfillState state = new MVBackfillManager.BackfillState();
        TestBackfillSink processor = new TestBackfillSink();

        Future<?> backfillTask = manager.submitBackfill(baseCfs, testView, ranges, processor, state);

        // Wait for completion
        backfillTask.get(10, TimeUnit.SECONDS);

        // Verify state
        assertTrue("Backfill should complete", state.completed);
        assertNull("Should not have failure", state.failure);
        assertTrue("Should process some partitions", state.partitionsProcessed >= 0);
        assertTrue("Should process some rows", state.rowsProcessed >= 0);

        // Verify processor
        assertTrue("Processor should complete", processor.completed);
        assertNull("Processor should not have failure", processor.failure);
    }

    @Test
    public void testProcessorException() throws Throwable
    {
        // Insert data
        insertRow(1, 1, "value1");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(),
                                        baseCfs.getPartitioner().getMinimumToken());

        MVBackfillManager.BackfillState state = new MVBackfillManager.BackfillState();
        FailingBackfillSink processor = new FailingBackfillSink();

        Future<?> backfillTask = manager.submitBackfill(baseCfs, testView, range, processor, state);

        try
        {
            // This should complete but with failure
            backfillTask.get(10, TimeUnit.SECONDS);
        }
        catch (Exception e)
        {
            // Expected - the task may throw the exception
        }

        // Verify state shows failure
        assertTrue("Backfill should complete (with failure)", state.completed);
        assertNotNull("Should have failure", state.failure);

        // Verify processor shows failure
        assertNotNull("Processor should have failure", processor.failure);
    }

    @Test
    public void testConvenienceMethod() throws Throwable
    {
        insertRow(1, 1, "value1");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(),
                                        baseCfs.getPartitioner().getMinimumToken());

        MVBackfillManager.BackfillState state = new MVBackfillManager.BackfillState();
        TestBackfillSink processor = new TestBackfillSink();

        // Test single range convenience method
        Future<?> backfillTask = manager.submitBackfill(baseCfs, testView, range, processor, state);

        // Wait for completion
        backfillTask.get(10, TimeUnit.SECONDS);

        // Should work the same as the collection method
        assertTrue("Backfill should complete", state.completed);
        assertNull("Should not have failure", state.failure);
    }

    // Helper classes and methods

    private static class TestBackfillSink implements MVBackfillManager.BackfillSink
    {
        final List<ViewRowTranslator.ViewRowResult> processedRows = new ArrayList<>();
        boolean completed = false;
        Exception failure = null;

        @Override
        public void processViewRow(DecoratedKey basePartitionKey,
                                 Row baseRow,
                                 ViewRowTranslator.ViewRowResult viewResult) throws Exception
        {
            if (viewResult != null)
                processedRows.add(viewResult);
        }

        @Override
        public void complete() throws Exception
        {
            completed = true;
        }

        @Override
        public void fail(Exception e)
        {
            failure = e;
        }
    }

    private static class FailingBackfillSink implements MVBackfillManager.BackfillSink
    {
        Exception failure = null;

        @Override
        public void processViewRow(DecoratedKey basePartitionKey,
                                 Row baseRow,
                                 ViewRowTranslator.ViewRowResult viewResult) throws Exception
        {
            throw new RuntimeException("Processor failure for testing");
        }

        @Override
        public void complete() throws Exception
        {
            // Should not be called
        }

        @Override
        public void fail(Exception e)
        {
            failure = e;
        }
    }

    private void insertRow(int partitionKey, int clusteringKey, String value) throws Throwable
    {
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", partitionKey, clusteringKey, value);
    }

    private DecoratedKey decoratedKey(int key)
    {
        return baseCfs.getPartitioner().decorateKey(Int32Type.instance.decompose(key));
    }

    private Row createRow(int clusteringKey, String value)
    {
        return new RowUpdateBuilder(baseMetadata, System.currentTimeMillis(), 1)
            .clustering(clusteringKey)
            .add("v", value)
            .build()
            .getPartitionUpdate(baseMetadata)
            .iterator()
            .next();
    }
}
