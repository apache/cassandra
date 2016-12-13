/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogArchiver;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.db.commitlog.CommitLogReplayer;

@RunWith(Parameterized.class)
public class RecoveryManagerTest
{
    private static Logger logger = LoggerFactory.getLogger(RecoveryManagerTest.class);

    private static final String KEYSPACE1 = "RecoveryManagerTest1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_COUNTER1 = "Counter1";

    private static final String KEYSPACE2 = "RecoveryManagerTest2";
    private static final String CF_STANDARD3 = "Standard3";

    public RecoveryManagerTest(ParameterizedClass commitLogCompression, EncryptionContext encryptionContext)
    {
        DatabaseDescriptor.setCommitLogCompression(commitLogCompression);
        DatabaseDescriptor.setEncryptionContext(encryptionContext);
    }

    @Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
            {null, EncryptionContextGenerator.createDisabledContext()}, // No compression, no encryption
            {null, EncryptionContextGenerator.createContext(true)}, // Encryption
            {new ParameterizedClass(LZ4Compressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()},
            {new ParameterizedClass(SnappyCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()},
            {new ParameterizedClass(DeflateCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()}});
    }

    @Before
    public void setUp() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF_COUNTER1));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD3));
    }

    @Before
    public void clearData()
    {
        // clear data
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUNTER1).truncateBlocking();
        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD3).truncateBlocking();
    }

    @Test
    public void testNothingToRecover() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @Test
    public void testRecoverBlocksOnBytesOutstanding() throws Exception
    {
        long originalMaxOutstanding = CommitLogReplayer.MAX_OUTSTANDING_REPLAY_BYTES;
        CommitLogReplayer.MAX_OUTSTANDING_REPLAY_BYTES = 1;
        CommitLogReplayer.MutationInitiator originalInitiator = CommitLogReplayer.mutationInitiator;
        MockInitiator mockInitiator = new MockInitiator();
        CommitLogReplayer.mutationInitiator = mockInitiator;
        try
        {
            CommitLog.instance.resetUnsafe(true);
            Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
            Keyspace keyspace2 = Keyspace.open(KEYSPACE2);

            UnfilteredRowIterator upd1 = Util.apply(new RowUpdateBuilder(keyspace1.getColumnFamilyStore(CF_STANDARD1).metadata, 1L, 0, "keymulti")
                .clustering("col1").add("val", "1")
                .build());

            UnfilteredRowIterator upd2 = Util.apply(new RowUpdateBuilder(keyspace2.getColumnFamilyStore(CF_STANDARD3).metadata, 1L, 0, "keymulti")
                                           .clustering("col2").add("val", "1")
                                           .build());

            keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
            keyspace2.getColumnFamilyStore("Standard3").clearUnsafe();

            DecoratedKey dk = Util.dk("keymulti");
            Assert.assertTrue(Util.getAllUnfiltered(Util.cmd(keyspace1.getColumnFamilyStore(CF_STANDARD1), dk).build()).isEmpty());
            Assert.assertTrue(Util.getAllUnfiltered(Util.cmd(keyspace2.getColumnFamilyStore(CF_STANDARD3), dk).build()).isEmpty());

            final AtomicReference<Throwable> err = new AtomicReference<Throwable>();
            Thread t = NamedThreadFactory.createThread(() ->
            {
                try
                {
                    CommitLog.instance.resetUnsafe(false); // disassociate segments from live CL
                }
                catch (Throwable x)
                {
                    err.set(x);
                }
            });
            t.start();
            Assert.assertTrue(mockInitiator.blocked.tryAcquire(1, 20, TimeUnit.SECONDS));
            Thread.sleep(100);
            Assert.assertTrue(t.isAlive());
            mockInitiator.blocker.release(Integer.MAX_VALUE);
            t.join(20 * 1000);

            if (err.get() != null)
                throw new RuntimeException(err.get());

            if (t.isAlive())
            {
                Throwable toPrint = new Throwable();
                toPrint.setStackTrace(Thread.getAllStackTraces().get(t));
                toPrint.printStackTrace(System.out);
            }
            Assert.assertFalse(t.isAlive());

            Assert.assertTrue(Util.sameContent(upd1, Util.getOnlyPartitionUnfiltered(Util.cmd(keyspace1.getColumnFamilyStore(CF_STANDARD1), dk).build()).unfilteredIterator()));
            Assert.assertTrue(Util.sameContent(upd2, Util.getOnlyPartitionUnfiltered(Util.cmd(keyspace2.getColumnFamilyStore(CF_STANDARD3), dk).build()).unfilteredIterator()));
        }
        finally
        {
            CommitLogReplayer.mutationInitiator = originalInitiator;
            CommitLogReplayer.MAX_OUTSTANDING_REPLAY_BYTES = originalMaxOutstanding;
        }
    }


    @Test
    public void testOne() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        Keyspace keyspace2 = Keyspace.open(KEYSPACE2);

        UnfilteredRowIterator upd1 = Util.apply(new RowUpdateBuilder(keyspace1.getColumnFamilyStore(CF_STANDARD1).metadata, 1L, 0, "keymulti")
            .clustering("col1").add("val", "1")
            .build());

        UnfilteredRowIterator upd2 = Util.apply(new RowUpdateBuilder(keyspace2.getColumnFamilyStore(CF_STANDARD3).metadata, 1L, 0, "keymulti")
                                       .clustering("col2").add("val", "1")
                                       .build());

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        keyspace2.getColumnFamilyStore("Standard3").clearUnsafe();

        CommitLog.instance.resetUnsafe(false);

        DecoratedKey dk = Util.dk("keymulti");
        Assert.assertTrue(Util.sameContent(upd1, Util.getOnlyPartitionUnfiltered(Util.cmd(keyspace1.getColumnFamilyStore(CF_STANDARD1), dk).build()).unfilteredIterator()));
        Assert.assertTrue(Util.sameContent(upd2, Util.getOnlyPartitionUnfiltered(Util.cmd(keyspace2.getColumnFamilyStore(CF_STANDARD3), dk).build()).unfilteredIterator()));
    }

    @Test
    public void testRecoverCounter() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace1.getColumnFamilyStore(CF_COUNTER1);

        for (int i = 0; i < 10; ++i)
        {
            new CounterMutation(new RowUpdateBuilder(cfs.metadata, 1L, 0, "key")
                .clustering("cc").add("val", CounterContext.instance().createLocal(1L))
                .build(), ConsistencyLevel.ALL).apply();
        }

        keyspace1.getColumnFamilyStore("Counter1").clearUnsafe();

        int replayed = CommitLog.instance.resetUnsafe(false);

        ColumnDefinition counterCol = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));
        Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val").build());
        assertEquals(10L, CounterContext.instance().total(row.getCell(counterCol).value()));
    }

    @Test
    public void testRecoverPIT() throws Exception
    {
        CommitLog.instance.resetUnsafe(true);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        Date date = CommitLogArchiver.format.parse("2112:12:12 12:12:12");
        long timeMS = date.getTime() - 5000;

        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        for (int i = 0; i < 10; ++i)
        {
            long ts = TimeUnit.MILLISECONDS.toMicros(timeMS + (i * 1000));
            new RowUpdateBuilder(cfs.metadata, ts, "name-" + i)
                .clustering("cc")
                .add("val", Integer.toString(i))
                .build()
                .apply();
        }

        // Sanity check row count prior to clear and replay
        assertEquals(10, Util.getAll(Util.cmd(cfs).build()).size());

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        CommitLog.instance.resetUnsafe(false);

        assertEquals(6, Util.getAll(Util.cmd(cfs).build()).size());
    }

    @Test
    public void testRecoverPITUnordered() throws Exception
    {
        CommitLog.instance.resetUnsafe(true);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        Date date = CommitLogArchiver.format.parse("2112:12:12 12:12:12");
        long timeMS = date.getTime();

        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);

        // Col 0 and 9 are the only ones to be recovered
        for (int i = 0; i < 10; ++i)
        {
            long ts;
            if (i == 9)
                ts = TimeUnit.MILLISECONDS.toMicros(timeMS - 1000);
            else
                ts = TimeUnit.MILLISECONDS.toMicros(timeMS + (i * 1000));

            new RowUpdateBuilder(cfs.metadata, ts, "name-" + i)
                .clustering("cc")
                .add("val", Integer.toString(i))
                .build()
                .apply();
        }

        // Sanity check row count prior to clear and replay
        assertEquals(10, Util.getAll(Util.cmd(cfs).build()).size());

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        CommitLog.instance.resetUnsafe(false);

        assertEquals(2, Util.getAll(Util.cmd(cfs).build()).size());
    }

    private static class MockInitiator extends CommitLogReplayer.MutationInitiator
    {
        final Semaphore blocker = new Semaphore(0);
        final Semaphore blocked = new Semaphore(0);

        @Override
        protected Future<Integer> initiateMutation(final Mutation mutation,
                final long segmentId,
                final int serializedSize,
                final int entryLocation,
                final CommitLogReplayer clr)
        {
            final Future<Integer> toWrap = super.initiateMutation(mutation,
                                                                  segmentId,
                                                                  serializedSize,
                                                                  entryLocation,
                                                                  clr);
            return new Future<Integer>()
            {

                @Override
                public boolean cancel(boolean mayInterruptIfRunning)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isCancelled()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isDone()
                {
                    return blocker.availablePermits() > 0 && toWrap.isDone();
                }

                @Override
                public Integer get() throws InterruptedException, ExecutionException
                {
                    System.out.println("Got blocker once");
                    blocked.release();
                    blocker.acquire();
                    return toWrap.get();
                }

                @Override
                public Integer get(long timeout, TimeUnit unit)
                        throws InterruptedException, ExecutionException, TimeoutException
                {
                    blocked.release();
                    blocker.tryAcquire(1, timeout, unit);
                    return toWrap.get(timeout, unit);
                }

            };
        }
    };
}
