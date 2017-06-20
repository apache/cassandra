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

package org.apache.cassandra.repair.consistent;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Transactional;

public class PendingAntiCompactionTest
{
    private static final Logger logger = LoggerFactory.getLogger(PendingAntiCompactionTest.class);
    private static final Collection<Range<Token>> FULL_RANGE;
    static
    {
        DatabaseDescriptor.daemonInitialization();
        Token minToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        FULL_RANGE = Collections.singleton(new Range<>(minToken, minToken));
    }

    private String ks;
    private final String tbl = "tbl";
    private TableMetadata cfm;
    private ColumnFamilyStore cfs;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
    }

    @Before
    public void setup()
    {
        ks = "ks_" + System.currentTimeMillis();
        cfm = CreateTableStatement.parse(String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT)", ks, tbl), ks).build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
    }

    private void makeSSTables(int num)
    {
        for (int i = 0; i < num; i++)
        {
            int val = i * 2;  // multiplied to prevent ranges from overlapping
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", ks, tbl), val, val);
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", ks, tbl), val+1, val+1);
            cfs.forceBlockingFlush();
        }
        Assert.assertEquals(num, cfs.getLiveSSTables().size());
    }

    private static class InstrumentedAcquisitionCallback extends PendingAntiCompaction.AcquisitionCallback
    {
        public InstrumentedAcquisitionCallback(UUID parentRepairSession, Collection<Range<Token>> ranges)
        {
            super(parentRepairSession, ranges);
        }

        Set<TableId> submittedCompactions = new HashSet<>();

        ListenableFuture<?> submitPendingAntiCompaction(PendingAntiCompaction.AcquireResult result)
        {
            submittedCompactions.add(result.cfs.metadata.id);
            result.abort();  // prevent ref leak complaints
            return ListenableFutureTask.create(() -> {}, null);
        }
    }

    /**
     * verify the pending anti compaction happy path
     */
    @Test
    public void successCase() throws Exception
    {
        Assert.assertSame(ByteOrderedPartitioner.class, DatabaseDescriptor.getPartitioner().getClass());
        cfs.disableAutoCompaction();

        // create 2 sstables, one that will be split, and another that will be moved
        for (int i = 0; i < 8; i++)
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", ks, tbl), i, i);
        }
        cfs.forceBlockingFlush();
        for (int i = 8; i < 12; i++)
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", ks, tbl), i, i);
        }
        cfs.forceBlockingFlush();
        Assert.assertEquals(2, cfs.getLiveSSTables().size());

        Token left = ByteOrderedPartitioner.instance.getToken(ByteBufferUtil.bytes((int) 6));
        Token right = ByteOrderedPartitioner.instance.getToken(ByteBufferUtil.bytes((int) 16));
        Collection<Range<Token>> ranges = Collections.singleton(new Range<>(left, right));

        // create a session so the anti compaction can fine it
        UUID sessionID = UUIDGen.getTimeUUID();
        ActiveRepairService.instance.registerParentRepairSession(sessionID, InetAddress.getLocalHost(), Lists.newArrayList(cfs), ranges, true, 1, true, PreviewKind.NONE);

        PendingAntiCompaction pac;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            pac = new PendingAntiCompaction(sessionID, ranges, executor);
            pac.run().get();
        }
        finally
        {
            executor.shutdown();
        }

        Assert.assertEquals(3, cfs.getLiveSSTables().size());
        int pendingRepair = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable.isPendingRepair())
                pendingRepair++;
        }
        Assert.assertEquals(2, pendingRepair);
    }

    @Test
    public void acquisitionSuccess() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(6);
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        List<SSTableReader> expected = sstables.subList(0, 3);
        Collection<Range<Token>> ranges = new HashSet<>();
        for (SSTableReader sstable : expected)
        {
            ranges.add(new Range<>(sstable.first.getToken(), sstable.last.getToken()));
        }

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, ranges, UUIDGen.getTimeUUID());

        logger.info("SSTables: {}", sstables);
        logger.info("Expected: {}", expected);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);
        logger.info("Originals: {}", result.txn.originals());
        Assert.assertEquals(3, result.txn.originals().size());
        for (SSTableReader sstable : expected)
        {
            logger.info("Checking {}", sstable);
            Assert.assertTrue(result.txn.originals().contains(sstable));
        }

        Assert.assertEquals(Transactional.AbstractTransactional.State.IN_PROGRESS, result.txn.state());
        result.abort();
    }

    @Test
    public void repairedSSTablesAreNotAcquired() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        Assert.assertEquals(2, sstables.size());
        SSTableReader repaired = sstables.get(0);
        SSTableReader unrepaired = sstables.get(1);
        Assert.assertTrue(repaired.intersects(FULL_RANGE));
        Assert.assertTrue(unrepaired.intersects(FULL_RANGE));

        repaired.descriptor.getMetadataSerializer().mutateRepaired(repaired.descriptor, 1, null);
        repaired.reloadSSTableMetadata();

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID());
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);

        logger.info("Originals: {}", result.txn.originals());
        Assert.assertEquals(1, result.txn.originals().size());
        Assert.assertTrue(result.txn.originals().contains(unrepaired));
        result.abort(); // release sstable refs
    }

    @Test
    public void pendingRepairSSTablesAreNotAcquired() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        Assert.assertEquals(2, sstables.size());
        SSTableReader repaired = sstables.get(0);
        SSTableReader unrepaired = sstables.get(1);
        Assert.assertTrue(repaired.intersects(FULL_RANGE));
        Assert.assertTrue(unrepaired.intersects(FULL_RANGE));

        repaired.descriptor.getMetadataSerializer().mutateRepaired(repaired.descriptor, 0, UUIDGen.getTimeUUID());
        repaired.reloadSSTableMetadata();
        Assert.assertTrue(repaired.isPendingRepair());

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID());
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);

        logger.info("Originals: {}", result.txn.originals());
        Assert.assertEquals(1, result.txn.originals().size());
        Assert.assertTrue(result.txn.originals().contains(unrepaired));
        result.abort();  // releases sstable refs
    }

    @Test
    public void pendingRepairNoSSTablesExist() throws Exception
    {
        cfs.disableAutoCompaction();

        Assert.assertEquals(0, cfs.getLiveSSTables().size());

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID());
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);

        result.abort();  // There's nothing to release, but we should exit cleanly
    }

    /**
     * anti compaction task should be submitted if everything is ok
     */
    @Test
    public void callbackSuccess() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID());
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);

        InstrumentedAcquisitionCallback cb = new InstrumentedAcquisitionCallback(UUIDGen.getTimeUUID(), FULL_RANGE);
        Assert.assertTrue(cb.submittedCompactions.isEmpty());
        cb.apply(Lists.newArrayList(result));

        Assert.assertEquals(1, cb.submittedCompactions.size());
        Assert.assertTrue(cb.submittedCompactions.contains(cfm.id));
    }

    /**
     * If one of the supplied AcquireResults is null, either an Exception was thrown, or
     * we couldn't get a transaction for the sstables. In either case we need to cancel the repair, and release
     * any sstables acquired for other tables
     */
    @Test
    public void callbackNullResult() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID());
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);
        Assert.assertEquals(Transactional.AbstractTransactional.State.IN_PROGRESS, result.txn.state());

        InstrumentedAcquisitionCallback cb = new InstrumentedAcquisitionCallback(UUIDGen.getTimeUUID(), FULL_RANGE);
        Assert.assertTrue(cb.submittedCompactions.isEmpty());
        cb.apply(Lists.newArrayList(result, null));

        Assert.assertTrue(cb.submittedCompactions.isEmpty());
        Assert.assertEquals(Transactional.AbstractTransactional.State.ABORTED, result.txn.state());
    }

    /**
     * If an AcquireResult has a null txn, there were no sstables to acquire references
     * for, so no anti compaction should have been submitted.
     */
    @Test
    public void callbackNullTxn() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID());
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);

        ColumnFamilyStore cfs2 = Schema.instance.getColumnFamilyStoreInstance(Schema.instance.getTableMetadata("system", "peers").id);
        PendingAntiCompaction.AcquireResult fakeResult = new PendingAntiCompaction.AcquireResult(cfs2, null, null);

        InstrumentedAcquisitionCallback cb = new InstrumentedAcquisitionCallback(UUIDGen.getTimeUUID(), FULL_RANGE);
        Assert.assertTrue(cb.submittedCompactions.isEmpty());
        cb.apply(Lists.newArrayList(result, fakeResult));

        Assert.assertEquals(1, cb.submittedCompactions.size());
        Assert.assertTrue(cb.submittedCompactions.contains(cfm.id));
        Assert.assertFalse(cb.submittedCompactions.contains(cfs2.metadata.id));
    }
}
