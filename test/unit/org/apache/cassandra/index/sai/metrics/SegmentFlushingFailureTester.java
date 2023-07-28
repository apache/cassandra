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
package org.apache.cassandra.index.sai.metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.cassandra.utils.FBUtilities;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.config.StorageAttachedIndexOptions;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.SSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;

import static org.apache.cassandra.inject.Injections.newCounter;
import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class SegmentFlushingFailureTester extends SAITester
{
    static final long DEFAULT_BYTES_LIMIT = 1024L * 1024L * StorageAttachedIndexOptions.DEFAULT_SEGMENT_BUFFER_MB;

    @Before
    public void initialize() throws Throwable
    {
        requireNetwork();

        Injections.inject(memoryTrackingCounter, writerAbortCounter);
        memoryTrackingCounter.enable();
        writerAbortCounter.enable();
    }

    private static final Injections.Counter memoryTrackingCounter =
            newCounter("memoryTrackingCounter").add(newInvokePoint()
                                               .onClass(NamedMemoryLimiter.class)
                                               .onMethod("increment")
                                               .atEntry()).build();

    private static final Injections.Counter writerAbortCounter =
            newCounter("writerAbortCounter").add(newInvokePoint()
                                            .onClass(SSTableIndexWriter.class)
                                            .onMethod("abort")
                                            .atEntry()).build();

    private static final Injection v1sstableComponentsWriterFailure =
            newFailureOnEntry("sstableComponentsWriterFailure",
                              SSTableComponentsWriter.class,
                              "complete",
                              RuntimeException.class);

    private static final Injection segmentFlushFailure =
            newFailureOnEntry("segmentFlushFailure", SegmentBuilder.class, "flush", RuntimeException.class);

    private static final Injection segmentFlushIOFailure =
            newFailureOnEntry("segmentFlushIOFailure", SegmentBuilder.class, "flush", IOException.class);

    @After
    public void resetCounters()
    {
        memoryTrackingCounter.reset();
        writerAbortCounter.reset();
    }

    protected abstract long expectedBytesLimit();

    @Test
    public void testSegmentMemoryTrackerLifecycle()
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        assertEquals(expectedBytesLimit(), getSegmentBufferSpaceLimit());
        assertEquals("Segment buffer memory tracker should start at zero!", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0')");
        flush();
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '1')");
        flush();

        ResultSet rows = executeNet("SELECT * FROM %s WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        compact();

        // The compaction completed successfully:
        Assert.assertEquals(0, writerAbortCounter.get());

        // This is a proxy for making sure we've actually tracked something:
        assertTrue(memoryTrackingCounter.get() > 0);

        assertEquals("Global memory tracker should have reverted to zero.", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());

        rows = executeNet("SELECT * FROM %s WHERE v1 = 0");
        assertEquals(1, rows.all().size());
    }

    @Test
    public void shouldZeroMemoryTrackerOnOffsetsRuntimeFailure() throws Throwable
    {
        shouldZeroMemoryTrackerOnFailure(v1sstableComponentsWriterFailure, "v1");
    }

    @Test
    public void shouldZeroMemoryTrackerOnSegmentFlushIOFailure() throws Throwable
    {
        shouldZeroMemoryTrackerOnFailure(segmentFlushIOFailure, "v1");
    }

    @Test
    public void shouldZeroMemoryTrackerOnSegmentFlushRuntimeFailure() throws Throwable
    {
        shouldZeroMemoryTrackerOnFailure(segmentFlushFailure, "v1");
    }

    private void shouldZeroMemoryTrackerOnFailure(Injection failure, String column) throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, column));

        assertEquals(expectedBytesLimit(), getSegmentBufferSpaceLimit());
        assertEquals("Segment buffer memory tracker should start at zero!", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0')");
        flush();
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '1')");
        flush();

        // Verify that we abort exactly once and zero the memory tracker:
        verifyCompactionIndexBuilds(1, failure, currentTable());

        // We should still be able to query the index if compaction is aborted:
        String select = String.format("SELECT * FROM %%s WHERE %s = %s", column, column.equals("v1") ? "0" : "'0'");
        ResultSet rows = executeNet(select);
        assertEquals(1, rows.all().size());
    }

    @Test
    public void shouldZeroMemoryAfterConcurrentIndexFailures() throws Throwable
    {
        String table1 = createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String table2 = createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        assertEquals(expectedBytesLimit(), getSegmentBufferSpaceLimit());
        assertEquals("Segment buffer memory tracker should start at zero!", 0L, getSegmentBufferUsedBytes());
        assertEquals("There should be no segment builders in progress.", 0L, getColumnIndexBuildsInProgress());

        execute("INSERT INTO " + KEYSPACE + "." + table1 + "(id1, v1, v2) VALUES ('0', 0, '0')");
        flush(KEYSPACE, table1);
        execute("INSERT INTO " + KEYSPACE + "." + table1 + "(id1, v1, v2) VALUES ('1', 1, '1')");
        flush(KEYSPACE, table1);

        execute("INSERT INTO " + KEYSPACE + "." + table2 + "(id1, v1, v2) VALUES ('0', 0, '0')");
        flush(KEYSPACE, table2);
        execute("INSERT INTO " + KEYSPACE + "." + table2 + "(id1, v1, v2) VALUES ('1', 1, '1')");
        flush(KEYSPACE, table2);

        // Start compaction against both tables/indexes and verify that they are aborted safely:
        verifyCompactionIndexBuilds(2, segmentFlushFailure, table1, table2);

        // We should still be able to query the indexes if compaction is aborted:
        ResultSet rows = executeNet(String.format("SELECT * FROM %s WHERE v1 = 0", KEYSPACE + "." + table1));
        assertEquals(1, rows.all().size());
        rows = executeNet(String.format("SELECT * FROM %s WHERE v1 = 0", KEYSPACE + "." + table2));
        assertEquals(1, rows.all().size());
    }

    private void verifyCompactionIndexBuilds(int aborts, Injection failure, String... tables) throws Throwable
    {
        Injections.inject(failure);
        failure.enable();

        try
        {
            ExecutorService executor = Executors.newFixedThreadPool(tables.length);
            List<Future<?>> results = new ArrayList<>();

            for (String table : tables)
                results.add(executor.submit(() -> compact(KEYSPACE, table)));
            
            assertThatThrownBy(() -> FBUtilities.waitOnFutures(results)).hasRootCauseMessage("Injected failure!");
            executor.shutdownNow();

            Assert.assertEquals(aborts, writerAbortCounter.get());

            assertEquals("Global memory tracker should have reverted to zero.", 0L, getSegmentBufferUsedBytes());
            assertEquals("There should be no segment builders in progress.", 0, getColumnIndexBuildsInProgress());
        }
        finally
        {
            failure.disable();
        }
    }
}
