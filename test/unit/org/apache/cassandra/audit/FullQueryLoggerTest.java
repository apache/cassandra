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
package org.apache.cassandra.audit;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.Unpooled;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.audit.FullQueryLogger.Query;
import org.apache.cassandra.audit.FullQueryLogger.Batch;
import org.apache.cassandra.cql3.statements.BatchStatement.Type;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.binlog.BinLogTest;
import org.apache.cassandra.utils.binlog.DeletingArchiver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.audit.FullQueryLogger.BATCH;
import static org.apache.cassandra.audit.FullQueryLogger.BATCH_TYPE;
import static org.apache.cassandra.audit.FullQueryLogger.GENERATED_NOW_IN_SECONDS;
import static org.apache.cassandra.audit.FullQueryLogger.GENERATED_TIMESTAMP;
import static org.apache.cassandra.audit.FullQueryLogger.PROTOCOL_VERSION;
import static org.apache.cassandra.audit.FullQueryLogger.QUERIES;
import static org.apache.cassandra.audit.FullQueryLogger.QUERY;
import static org.apache.cassandra.audit.FullQueryLogger.QUERY_OPTIONS;
import static org.apache.cassandra.audit.FullQueryLogger.QUERY_START_TIME;
import static org.apache.cassandra.audit.FullQueryLogger.SINGLE_QUERY;
import static org.apache.cassandra.audit.FullQueryLogger.TYPE;
import static org.apache.cassandra.audit.FullQueryLogger.VALUES;
import static org.apache.cassandra.audit.FullQueryLogger.VERSION;

public class FullQueryLoggerTest extends CQLTester
{
    private static Path tempDir;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        tempDir = BinLogTest.tempDir();
    }

    private FullQueryLogger instance;
    
    @Before
    public void setUp()
    {
        instance = AuditLogManager.getInstance().getFullQueryLogger();
    }
    
    @After
    public void tearDown()
    {
        instance.reset(tempDir.toString());
    }

    @Test(expected = NullPointerException.class)
    public void testConfigureNullPath() throws Exception
    {
        instance.configure(null, "", true, 1, 1, StringUtils.EMPTY, 10);
    }

    @Test(expected = NullPointerException.class)
    public void testConfigureNullRollCycle() throws Exception
    {
        instance.configure(BinLogTest.tempDir(), null, true, 1, 1, StringUtils.EMPTY, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigureInvalidRollCycle() throws Exception
    {
        instance.configure(BinLogTest.tempDir(), "foobar", true, 1, 1, StringUtils.EMPTY, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigureInvalidMaxQueueWeight() throws Exception
    {
        instance.configure(BinLogTest.tempDir(), "DAILY", true, 0, 1, StringUtils.EMPTY, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigureInvalidMaxQueueLogSize() throws Exception
    {
        instance.configure(BinLogTest.tempDir(), "DAILY", true, 1, 0, StringUtils.EMPTY, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigureOverExistingFile()
    {
        File f = FileUtils.createTempFile("foo", "bar");
        f.deleteOnExit();
        instance.configure(f.toPath(), "TEST_SECONDLY", true, 1, 1, StringUtils.EMPTY, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCanRead() throws Exception
    {
        tempDir.toFile().setReadable(false);
        try
        {
            configureFQL();
        }
        finally
        {
            tempDir.toFile().setReadable(true);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCanWrite() throws Exception
    {
        tempDir.toFile().setWritable(false);
        try
        {
            configureFQL();
        }
        finally
        {
            tempDir.toFile().setWritable(true);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCanExecute() throws Exception
    {
        tempDir.toFile().setExecutable(false);
        try
        {
            configureFQL();
        }
        finally
        {
            tempDir.toFile().setExecutable(true);
        }
    }

    @Test
    public void testResetWithoutConfigure() throws Exception
    {
        instance.reset(tempDir.toString());
        instance.reset(tempDir.toString());
    }

    @Test
    public void stopWithoutConfigure() throws Exception
    {
        instance.stop();
        instance.stop();
    }

    /**
     * Both the last used and supplied directory should get cleaned
     */
    @Test
    public void testResetCleansPaths() throws Exception
    {
        configureFQL();
        File tempA = File.createTempFile("foo", "bar", tempDir.toFile());
        assertTrue(tempA.exists());
        File tempB = File.createTempFile("foo", "bar", BinLogTest.tempDir().toFile());
        instance.reset(tempB.getParent());
        assertFalse(tempA.exists());
        assertFalse(tempB.exists());
    }

    /**
     * The last used and configured directory are the same and it shouldn't be an issue
     */
    @Test
    public void testResetSamePath() throws Exception
    {
        configureFQL();
        File tempA = File.createTempFile("foo", "bar", tempDir.toFile());
        assertTrue(tempA.exists());
        instance.reset(tempA.getParent());
        assertFalse(tempA.exists());
    }

    @Test(expected = IllegalStateException.class)
    public void testDoubleConfigure() throws Exception
    {
        configureFQL();
        configureFQL();
    }

    @Test
    public void testCleansDirectory() throws Exception
    {
        assertTrue(new File(tempDir.toFile(), "foobar").createNewFile());
        configureFQL();
        assertEquals(tempDir.toFile().listFiles().length, 1);
        assertEquals("directory-listing.cq4t", tempDir.toFile().listFiles()[0].getName());
    }

    @Test
    public void testEnabledReset() throws Exception
    {
        assertFalse(instance.enabled());
        configureFQL();
        assertTrue(instance.enabled());
        instance.reset(tempDir.toString());
        assertFalse(instance.enabled());
    }

    @Test
    public void testEnabledStop() throws Exception
    {
        assertFalse(instance.enabled());
        configureFQL();
        assertTrue(instance.enabled());
        instance.stop();
        assertFalse(instance.enabled());
    }

    /**
     * Test that a thread will block if the FQL is over weight, and unblock once the backup is cleared
     */
    @Test
    public void testBlocking() throws Exception
    {
        configureFQL();
        //Prevent the bin log thread from making progress, causing the task queue to block
        Semaphore blockBinLog = new Semaphore(0);
        try
        {
            //Find out when the bin log thread has been blocked, necessary to not run into batch task drain behavior
            Semaphore binLogBlocked = new Semaphore(0);
            instance.binLog.put(new Query("foo1", QueryOptions.DEFAULT, queryState(), 1)
            {

                public void writeMarshallable(WireOut wire)
                {
                    //Notify that the bin log is blocking now
                    binLogBlocked.release();
                    try
                    {
                        //Block the bin log thread so the task queue can be filled
                        blockBinLog.acquire();
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                    super.writeMarshallable(wire);
                }

                public void release()
                {
                    super.release();
                }
            });

            //Wait for the bin log thread to block so it can't batch drain tasks
            Util.spinAssertEquals(true, binLogBlocked::tryAcquire, 60);

            //Now fill the task queue
            logQuery("foo2");

            //Start a thread to block waiting on the bin log queue
            Thread t = new Thread(() ->
                                  {
                                      logQuery("foo3");
                                      //Should be able to log another query without an issue
                                      logQuery("foo4");
                                  });
            t.start();
            Thread.sleep(500);
            //If thread state is terminated then the thread started, finished, and didn't block on the full task queue
            assertTrue(t.getState() != Thread.State.TERMINATED);
        }
        finally
        {
            //Unblock the binlog thread
            blockBinLog.release();
        }
        Util.spinAssertEquals(true, () -> checkForQueries(Arrays.asList("foo1", "foo2", "foo3", "foo4")), 60);
    }

    private boolean checkForQueries(List<String> queries)
    {
        try (ChronicleQueue queue = ChronicleQueueBuilder.single(tempDir.toFile()).rollCycle(RollCycles.TEST_SECONDLY).build())
        {
            ExcerptTailer tailer = queue.createTailer();
            List<String> expectedQueries = new LinkedList<>(queries);
            while (!expectedQueries.isEmpty())
            {
                if (!tailer.readDocument(wire -> {
                    assertEquals(expectedQueries.get(0), wire.read("query").text());
                    expectedQueries.remove(0);
                }))
                {
                    return false;
                }
            }
            assertFalse(tailer.readDocument(wire -> {}));
            return true;
        }
    }

    @Test
    public void testNonBlocking() throws Exception
    {
        instance.configure(tempDir, "TEST_SECONDLY", false, 1, 1024 * 1024 * 256, StringUtils.EMPTY, 10);
        //Prevent the bin log thread from making progress, causing the task queue to refuse tasks
        Semaphore blockBinLog = new Semaphore(0);
        try
        {
            //Find out when the bin log thread has been blocked, necessary to not run into batch task drain behavior
            Semaphore binLogBlocked = new Semaphore(0);
            instance.binLog.put(new Query("foo1", QueryOptions.DEFAULT, queryState(), 1)
            {

                public void writeMarshallable(WireOut wire)
                {
                    //Notify that the bin log is blocking now
                    binLogBlocked.release();
                    try
                    {
                        //Block the bin log thread so the task queue can be filled
                        blockBinLog.acquire();
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                    super.writeMarshallable(wire);
                }

                public void release()
                {
                    super.release();
                }
            });

            //Wait for the bin log thread to block so it can't batch drain tasks
            Util.spinAssertEquals(true, binLogBlocked::tryAcquire, 60);

            //Now fill the task queue
            logQuery("foo2");

            //This sample should get dropped AKA released without being written
            AtomicInteger releasedCount = new AtomicInteger(0);
            AtomicInteger writtenCount = new AtomicInteger(0);
            instance.logRecord(new Query("foo3", QueryOptions.DEFAULT, queryState(), 1) {
                public void writeMarshallable(WireOut wire)
                {
                    writtenCount.incrementAndGet();
                    super.writeMarshallable(wire);
                }

                public void release()
                {
                    releasedCount.incrementAndGet();
                    super.release();
                }
            }, instance.binLog);

            Util.spinAssertEquals(1, releasedCount::get, 60);
            assertEquals(0, writtenCount.get());
        }
        finally
        {
            blockBinLog.release();
        }
        //Wait for tasks to drain so there should be space in the queue
        Util.spinAssertEquals(true, () -> checkForQueries(Arrays.asList("foo1", "foo2")), 60);
        //Should be able to log again
        logQuery("foo4");
        Util.spinAssertEquals(true, () -> checkForQueries(Arrays.asList("foo1", "foo2", "foo4")), 60);
    }

    @Test
    public void testRoundTripQuery() throws Exception
    {
        configureFQL();
        logQuery("foo");
        Util.spinAssertEquals(true, () -> checkForQueries(Arrays.asList("foo")), 60);
        assertRoundTripQuery(null);
    }

    @Test
    public void testRoundTripQueryWithKeyspace() throws Exception
    {
        configureFQL();
        logQuery("foo", "abcdefg");
        Util.spinAssertEquals(true, () -> checkForQueries(Arrays.asList("foo")), 60);
        assertRoundTripQuery("abcdefg");
    }

    private void assertRoundTripQuery(@Nullable String keyspace)
    {
        try (ChronicleQueue queue = ChronicleQueueBuilder.single(tempDir.toFile()).rollCycle(RollCycles.TEST_SECONDLY).build())
        {
            ExcerptTailer tailer = queue.createTailer();
            assertTrue(tailer.readDocument(wire ->
            {
                assertEquals(0, wire.read(VERSION).int16());
                assertEquals(SINGLE_QUERY, wire.read(TYPE).text());

                assertEquals(1L, wire.read(QUERY_START_TIME).int64());

                ProtocolVersion protocolVersion = ProtocolVersion.decode(wire.read(PROTOCOL_VERSION).int32(), true);
                assertEquals(ProtocolVersion.CURRENT, protocolVersion);

                QueryOptions queryOptions = QueryOptions.codec.decode(Unpooled.wrappedBuffer(wire.read(QUERY_OPTIONS).bytes()), protocolVersion);
                compareQueryOptions(QueryOptions.DEFAULT, queryOptions);

                String wireKeyspace = wire.read(FullQueryLogger.KEYSPACE).text();
                assertEquals(keyspace, wireKeyspace);

                assertEquals("foo", wire.read(QUERY).text());
            }));
        }
    }

    @Test
    public void testRoundTripBatchWithKeyspace() throws Exception
    {
        configureFQL();
        instance.logBatch(Type.UNLOGGED,
                          Arrays.asList("foo1", "foo2"),
                          Arrays.asList(Arrays.asList(ByteBuffer.allocate(1),
                                                      ByteBuffer.allocateDirect(2)),
                                        Collections.emptyList()),
                          QueryOptions.DEFAULT,
                          queryState("abcdefgh"),
                          1);

        Util.spinAssertEquals(true, () ->
        {
            try (ChronicleQueue queue = ChronicleQueueBuilder.single(tempDir.toFile()).rollCycle(RollCycles.TEST_SECONDLY).build())
            {
                return queue.createTailer().readingDocument().isPresent();
            }
        }, 60);

        assertRoundTripBatch("abcdefgh");
    }

    @Test
    public void testRoundTripBatchWithKeyspaceNull() throws Exception
    {
        configureFQL();
        instance.logBatch(Type.UNLOGGED,
                          Arrays.asList("foo1", "foo2"),
                          Arrays.asList(Arrays.asList(ByteBuffer.allocate(1),
                                                      ByteBuffer.allocateDirect(2)),
                                        Collections.emptyList()),
                          QueryOptions.DEFAULT,
                          queryState(),
                          1);

        Util.spinAssertEquals(true, () ->
        {
            try (ChronicleQueue queue = ChronicleQueueBuilder.single(tempDir.toFile()).rollCycle(RollCycles.TEST_SECONDLY).build())
            {
                return queue.createTailer().readingDocument().isPresent();
            }
        }, 60);

        assertRoundTripBatch(null);
    }


    private void assertRoundTripBatch(@Nullable String keyspace)
    {
        try (ChronicleQueue queue = ChronicleQueueBuilder.single(tempDir.toFile()).rollCycle(RollCycles.TEST_SECONDLY).build())
        {
            ExcerptTailer tailer = queue.createTailer();
            assertTrue(tailer.readDocument(wire -> {
                assertEquals(0, wire.read(VERSION).int16());
                assertEquals(BATCH, wire.read(TYPE).text());

                assertEquals(1L, wire.read(QUERY_START_TIME).int64());

                ProtocolVersion protocolVersion = ProtocolVersion.decode(wire.read(PROTOCOL_VERSION).int32(), true);
                assertEquals(ProtocolVersion.CURRENT, protocolVersion);

                QueryOptions queryOptions = QueryOptions.codec.decode(Unpooled.wrappedBuffer(wire.read(QUERY_OPTIONS).bytes()), protocolVersion);
                compareQueryOptions(QueryOptions.DEFAULT, queryOptions);

                assertEquals(Long.MIN_VALUE, wire.read(GENERATED_TIMESTAMP).int64());
                assertEquals(Integer.MIN_VALUE, wire.read(GENERATED_NOW_IN_SECONDS).int32());
                assertEquals(keyspace, wire.read(FullQueryLogger.KEYSPACE).text());
                assertEquals("UNLOGGED", wire.read(BATCH_TYPE).text());
                ValueIn in = wire.read(QUERIES);
                assertEquals(2, in.int32());
                assertEquals("foo1", in.text());
                assertEquals("foo2", in.text());
                in = wire.read(VALUES);
                assertEquals(2, in.int32());
                assertEquals(2, in.int32());
                assertTrue(Arrays.equals(new byte[1], in.bytes()));
                assertTrue(Arrays.equals(new byte[2], in.bytes()));
                assertEquals(0, in.int32());
            }));
        }
    }


    @Test
    public void testQueryWeight()
    {
        //Empty query should have some weight
        Query query = new Query("", QueryOptions.DEFAULT, queryState(), 1);
        assertTrue(query.weight() >= 95);

        StringBuilder sb = new StringBuilder();
        for (int ii = 0; ii < 1024 * 1024; ii++)
        {
            sb.append('a');
        }
        query = new Query(sb.toString(), QueryOptions.DEFAULT, queryState(), 1);

        //A large query should be reflected in the size, * 2 since characters are still two bytes
        assertTrue(query.weight() > ObjectSizes.measureDeep(sb.toString()));

        //Large query options should be reflected
        QueryOptions largeOptions = QueryOptions.forInternalCalls(Arrays.asList(ByteBuffer.allocate(1024 * 1024)));
        query = new Query("", largeOptions, queryState(), 1);
        assertTrue(query.weight() > 1024 * 1024);
        System.out.printf("weight %d%n", query.weight());
    }

    @Test
    public void testBatchWeight()
    {
        //An empty batch should have weight
        Batch batch = new Batch(Type.UNLOGGED, new ArrayList<>(), new ArrayList<>(), QueryOptions.DEFAULT, queryState(), 1);
        assertTrue(batch.weight() > 0);

        // make sure that a batch with keyspace set has a higher weight
        Batch batch2 = new Batch(Type.UNLOGGED, new ArrayList<>(), new ArrayList<>(), QueryOptions.DEFAULT, queryState("ABABA"), 1);
        assertTrue(batch.weight() < batch2.weight());

        StringBuilder sb = new StringBuilder();
        for (int ii = 0; ii < 1024 * 1024; ii++)
        {
            sb.append('a');
        }

        //The weight of the list containing queries should be reflected
        List<String> bigList = new ArrayList(100000);
        for (int ii = 0; ii < 100000; ii++)
        {
            bigList.add("");
        }
        batch = new Batch(Type.UNLOGGED, bigList, new ArrayList<>(), QueryOptions.DEFAULT, queryState(), 1);
        assertTrue(batch.weight() > ObjectSizes.measureDeep(bigList));

        //The size of the query should be reflected
        bigList = new ArrayList(1);
        bigList.add(sb.toString());
        batch = new Batch(Type.UNLOGGED, bigList, new ArrayList<>(), QueryOptions.DEFAULT, queryState(), 1);
        assertTrue(batch.weight() > ObjectSizes.measureDeep(bigList));

        bigList = null;
        //The size of the list of values should be reflected
        List<List<ByteBuffer>> bigValues = new ArrayList<>(100000);
        for (int ii = 0; ii < 100000; ii++)
        {
            bigValues.add(new ArrayList<>(0));
        }
        bigValues.get(0).add(ByteBuffer.allocate(1024 * 1024 * 5));
        batch = new Batch(Type.UNLOGGED, new ArrayList<>(), bigValues, QueryOptions.DEFAULT, queryState(), 1);
        assertTrue(batch.weight() > ObjectSizes.measureDeep(bigValues));

        //As should the size of the values
        QueryOptions largeOptions = QueryOptions.forInternalCalls(Arrays.asList(ByteBuffer.allocate(1024 * 1024)));
        batch = new Batch(Type.UNLOGGED, new ArrayList<>(), new ArrayList<>(), largeOptions, queryState(), 1);
        assertTrue(batch.weight() > 1024 * 1024);
    }

    @Test(expected = NullPointerException.class)
    public void testLogBatchNullType() throws Exception
    {
        instance.logBatch(null, new ArrayList<>(), new ArrayList<>(), QueryOptions.DEFAULT, queryState(), 1);
    }

    @Test(expected = NullPointerException.class)
    public void testLogBatchNullQueries() throws Exception
    {
        instance.logBatch(Type.UNLOGGED, null, new ArrayList<>(), QueryOptions.DEFAULT, queryState(), 1);
    }

    @Test(expected = NullPointerException.class)
    public void testLogBatchNullQueriesQuery() throws Exception
    {
        configureFQL();
        instance.logBatch(Type.UNLOGGED, Arrays.asList((String)null), new ArrayList<>(), QueryOptions.DEFAULT, queryState(), 1);
    }

    @Test(expected = NullPointerException.class)
    public void testLogBatchNullValues() throws Exception
    {
        instance.logBatch(Type.UNLOGGED, new ArrayList<>(), null, QueryOptions.DEFAULT, queryState(), 1);
    }

    @Test(expected = NullPointerException.class)
    public void testLogBatchNullValuesValue() throws Exception
    {
        instance.logBatch(Type.UNLOGGED, new ArrayList<>(), Arrays.asList((List<ByteBuffer>)null), null, queryState(), 1);
    }

    @Test(expected = NullPointerException.class)
    public void testLogBatchNullQueryOptions() throws Exception
    {
        instance.logBatch(Type.UNLOGGED, new ArrayList<>(), new ArrayList<>(), null, queryState(), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLogBatchNegativeTime() throws Exception
    {
        instance.logBatch(Type.UNLOGGED, new ArrayList<>(), new ArrayList<>(), QueryOptions.DEFAULT, queryState(), -1);
    }

    @Test(expected = NullPointerException.class)
    public void testLogQueryNullQuery() throws Exception
    {
        instance.logQuery(null, QueryOptions.DEFAULT, queryState(), 1);
    }

    @Test(expected = NullPointerException.class)
    public void testLogQueryNullQueryOptions() throws Exception
    {
        instance.logQuery("", null, queryState(), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLogQueryNegativeTime() throws Exception
    {
        instance.logQuery("", QueryOptions.DEFAULT, queryState(), -1);
    }

    private static void compareQueryOptions(QueryOptions a, QueryOptions b)
    {
        assertEquals(a.getClass(), b.getClass());
        assertEquals(a.getProtocolVersion(), b.getProtocolVersion());
        assertEquals(a.getPageSize(), b.getPageSize());
        assertEquals(a.getConsistency(), b.getConsistency());
        assertEquals(a.getPagingState(), b.getPagingState());
        assertEquals(a.getValues(), b.getValues());
        assertEquals(a.getSerialConsistency(), b.getSerialConsistency());
    }

    private void configureFQL() throws Exception
    {
        instance.configure(tempDir, "TEST_SECONDLY", true, 1, 1024 * 1024 * 256, StringUtils.EMPTY, 10);
    }

    private void logQuery(String query)
    {
        instance.logQuery(query, QueryOptions.DEFAULT, queryState(), 1);
    }

    private void logQuery(String query, String keyspace)
    {
        instance.logQuery(query, QueryOptions.DEFAULT, queryState(keyspace), 1);
    }

    private QueryState queryState(String keyspace)
    {
        ClientState clientState = ClientState.forInternalCalls(keyspace);
        return new QueryState(clientState);
    }

    private QueryState queryState()
    {
        return new QueryState(ClientState.forInternalCalls());
    }
}

