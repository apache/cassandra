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

package org.apache.cassandra.db.repair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.Transactional;

public class PendingAntiCompactionTest extends AbstractPendingAntiCompactionTest
{
    static final Logger logger = LoggerFactory.getLogger(PendingAntiCompactionTest.class);

    private static class InstrumentedAcquisitionCallback extends PendingAntiCompaction.AcquisitionCallback
    {
        public InstrumentedAcquisitionCallback(UUID parentRepairSession, RangesAtEndpoint ranges)
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
        List<ColumnFamilyStore> tables = Lists.newArrayList(cfs);
        Collection<Range<Token>> ranges = Collections.singleton(new Range<>(left, right));

        // create a session so the anti compaction can fine it
        UUID sessionID = UUIDGen.getTimeUUID();
        ActiveRepairService.instance.registerParentRepairSession(sessionID, InetAddressAndPort.getLocalHost(), tables, ranges, true, 1, true, PreviewKind.NONE);

        PendingAntiCompaction pac;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            pac = new PendingAntiCompaction(sessionID, tables, atEndpoint(ranges, NO_RANGES), executor);
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

        repaired.descriptor.getMetadataSerializer().mutateRepairMetadata(repaired.descriptor, 1, null, false);
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
    public void finalizedPendingRepairSSTablesAreNotAcquired() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        Assert.assertEquals(2, sstables.size());
        SSTableReader repaired = sstables.get(0);
        SSTableReader unrepaired = sstables.get(1);
        Assert.assertTrue(repaired.intersects(FULL_RANGE));
        Assert.assertTrue(unrepaired.intersects(FULL_RANGE));

        UUID sessionId = prepareSession();
        LocalSessionAccessor.finalizeUnsafe(sessionId);
        repaired.descriptor.getMetadataSerializer().mutateRepairMetadata(repaired.descriptor, 0, sessionId, false);
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
    public void conflictingSessionAcquisitionFailure() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        Assert.assertEquals(2, sstables.size());
        SSTableReader repaired = sstables.get(0);
        SSTableReader unrepaired = sstables.get(1);
        Assert.assertTrue(repaired.intersects(FULL_RANGE));
        Assert.assertTrue(unrepaired.intersects(FULL_RANGE));

        UUID sessionId = prepareSession();
        repaired.descriptor.getMetadataSerializer().mutateRepairMetadata(repaired.descriptor, 0, sessionId, false);
        repaired.reloadSSTableMetadata();
        Assert.assertTrue(repaired.isPendingRepair());

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID());
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNull(result);
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

        InstrumentedAcquisitionCallback cb = new InstrumentedAcquisitionCallback(UUIDGen.getTimeUUID(), atEndpoint(FULL_RANGE, NO_RANGES));
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

        InstrumentedAcquisitionCallback cb = new InstrumentedAcquisitionCallback(UUIDGen.getTimeUUID(), atEndpoint(FULL_RANGE, Collections.emptyList()));
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

        InstrumentedAcquisitionCallback cb = new InstrumentedAcquisitionCallback(UUIDGen.getTimeUUID(), atEndpoint(FULL_RANGE, NO_RANGES));
        Assert.assertTrue(cb.submittedCompactions.isEmpty());
        cb.apply(Lists.newArrayList(result, fakeResult));

        Assert.assertEquals(1, cb.submittedCompactions.size());
        Assert.assertTrue(cb.submittedCompactions.contains(cfm.id));
        Assert.assertFalse(cb.submittedCompactions.contains(cfs2.metadata.id));
    }


    @Test
    public void singleAnticompaction() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID());
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        UUID sessionID = UUIDGen.getTimeUUID();
        ActiveRepairService.instance.registerParentRepairSession(sessionID,
                                                                 InetAddressAndPort.getByName("127.0.0.1"),
                                                                 Lists.newArrayList(cfs),
                                                                 FULL_RANGE,
                                                                 true,0,
                                                                 true,
                                                                 PreviewKind.NONE);
        CompactionManager.instance.performAnticompaction(result.cfs, atEndpoint(FULL_RANGE, NO_RANGES), result.refs, result.txn, sessionID);

    }

    /**
     * Makes sure that PendingAntiCompaction fails when anticompaction throws exception
     */
    @Test
    public void antiCompactionException()
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);
        UUID prsid = UUID.randomUUID();
        ListeningExecutorService es = MoreExecutors.listeningDecorator(MoreExecutors.newDirectExecutorService());
        PendingAntiCompaction pac = new PendingAntiCompaction(prsid, Collections.singleton(cfs), atEndpoint(FULL_RANGE, NO_RANGES), es) {
            @Override
            protected AcquisitionCallback getAcquisitionCallback(UUID prsId, RangesAtEndpoint tokenRanges)
            {
                return new AcquisitionCallback(prsid, tokenRanges)
                {
                    @Override
                    ListenableFuture<?> submitPendingAntiCompaction(AcquireResult result)
                    {
                        Runnable r = new WrappedRunnable()
                        {
                            protected void runMayThrow()
                            {
                                throw new CompactionInterruptedException(null);
                            }
                        };
                        return es.submit(r);
                    }
                };
            }
        };
        ListenableFuture<?> fut = pac.run();
        try
        {
            fut.get();
            Assert.fail("Should throw exception");
        }
        catch(Throwable t)
        {
        }
    }

    @Test
    public void testBlockedAcquisition() throws ExecutionException, InterruptedException
    {
        cfs.disableAutoCompaction();
        ExecutorService es = Executors.newFixedThreadPool(1);

        makeSSTables(2);
        UUID prsid = UUID.randomUUID();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        List<ISSTableScanner> scanners = sstables.stream().map(SSTableReader::getScanner).collect(Collectors.toList());
        try
        {
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
                 CompactionController controller = new CompactionController(cfs, sstables, 0);
                 CompactionIterator ci = CompactionManager.getAntiCompactionIterator(scanners, controller, 0, UUID.randomUUID(), CompactionManager.instance.getMetrics()))
            {
                // `ci` is our imaginary ongoing anticompaction which makes no progress until after 30s
                // now we try to start a new AC, which will try to cancel all ongoing compactions

                CompactionManager.instance.getMetrics().beginCompaction(ci);
                PendingAntiCompaction pac = new PendingAntiCompaction(prsid, Collections.singleton(cfs), atEndpoint(FULL_RANGE, NO_RANGES), es);
                ListenableFuture fut = pac.run();
                try
                {
                    fut.get(30, TimeUnit.SECONDS);
                }
                catch (TimeoutException e)
                {
                    // expected, we wait 1 minute for compactions to get cancelled in runWithCompactionsDisabled
                }
                Assert.assertTrue(ci.hasNext());
                ci.next(); // this would throw exception if the CompactionIterator was abortable
                try
                {
                    fut.get();
                    Assert.fail("We should get exception when trying to start a new anticompaction with the same sstables");
                }
                catch (Throwable t)
                {

                }
            }
        }
        finally
        {
            es.shutdown();
            ISSTableScanner.closeAllAndPropagate(scanners, null);
        }
    }

    @Test
    public void testUnblockedAcquisition() throws ExecutionException, InterruptedException
    {
        cfs.disableAutoCompaction();
        ExecutorService es = Executors.newFixedThreadPool(1);
        makeSSTables(2);
        UUID prsid = prepareSession();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        List<ISSTableScanner> scanners = sstables.stream().map(SSTableReader::getScanner).collect(Collectors.toList());
        try
        {
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
                 CompactionController controller = new CompactionController(cfs, sstables, 0);
                 CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners, controller, 0, UUID.randomUUID()))
            {
                // `ci` is our imaginary ongoing anticompaction which makes no progress until after 5s
                // now we try to start a new AC, which will try to cancel all ongoing compactions

                CompactionManager.instance.getMetrics().beginCompaction(ci);
                PendingAntiCompaction pac = new PendingAntiCompaction(prsid, Collections.singleton(cfs), atEndpoint(FULL_RANGE, NO_RANGES), es);
                ListenableFuture fut = pac.run();
                try
                {
                    fut.get(5, TimeUnit.SECONDS);
                }
                catch (TimeoutException e)
                {
                    // expected, we wait 1 minute for compactions to get cancelled in runWithCompactionsDisabled, but we are not iterating
                    // CompactionIterator so the compaction is not actually cancelled
                }
                try
                {
                    Assert.assertTrue(ci.hasNext());
                    ci.next();
                    Assert.fail("CompactionIterator should be abortable");
                }
                catch (CompactionInterruptedException e)
                {
                    CompactionManager.instance.getMetrics().finishCompaction(ci);
                    txn.abort();
                    // expected
                }
                CountDownLatch cdl = new CountDownLatch(1);
                Futures.addCallback(fut, new FutureCallback<Object>()
                {
                    public void onSuccess(@Nullable Object o)
                    {
                        cdl.countDown();
                    }

                    public void onFailure(Throwable throwable)
                    {
                    }
                });
                Assert.assertTrue(cdl.await(1, TimeUnit.MINUTES));
            }
        }
        finally
        {
            es.shutdown();
        }
    }

    private static RangesAtEndpoint atEndpoint(Collection<Range<Token>> full, Collection<Range<Token>> trans)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(local);
        for (Range<Token> range : full)
            builder.add(new Replica(local, range, true));

        for (Range<Token> range : trans)
            builder.add(new Replica(local, range, false));

        return builder.build();
    }
}
