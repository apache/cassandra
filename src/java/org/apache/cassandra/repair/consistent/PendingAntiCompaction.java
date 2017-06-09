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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Performs an anti compaction on a set of tables and token ranges, isolating the unrepaired sstables
 * for a give token range into a pending repair group so they can't be compacted with other sstables
 * while they are being repaired.
 */
public class PendingAntiCompaction
{
    private static final Logger logger = LoggerFactory.getLogger(PendingAntiCompaction.class);

    static class AcquireResult
    {
        final ColumnFamilyStore cfs;
        final Refs<SSTableReader> refs;
        final LifecycleTransaction txn;

        AcquireResult(ColumnFamilyStore cfs, Refs<SSTableReader> refs, LifecycleTransaction txn)
        {
            this.cfs = cfs;
            this.refs = refs;
            this.txn = txn;
        }

        void abort()
        {
            if (txn != null)
                txn.abort();
            if (refs != null)
                refs.release();
        }
    }

    static class AcquisitionCallable implements Callable<AcquireResult>
    {
        private final ColumnFamilyStore cfs;
        private final Collection<Range<Token>> ranges;
        private final UUID sessionID;

        public AcquisitionCallable(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, UUID sessionID)
        {
            this.cfs = cfs;
            this.ranges = ranges;
            this.sessionID = sessionID;
        }

        private Iterable<SSTableReader> getSSTables()
        {
            return Iterables.filter(cfs.getLiveSSTables(), s -> !s.isRepaired() && !s.isPendingRepair() && s.intersects(ranges));
        }

        @SuppressWarnings("resource")
        private AcquireResult acquireTuple()
        {
            List<SSTableReader> sstables = Lists.newArrayList(getSSTables());
            if (sstables.isEmpty())
                return new AcquireResult(cfs, null, null);

            LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
            if (txn != null)
                return new AcquireResult(cfs, Refs.ref(sstables), txn);
            else
                return null;
        }

        public AcquireResult call() throws Exception
        {
            logger.debug("acquiring sstables for pending anti compaction on session {}", sessionID);
            AcquireResult refTxn = acquireTuple();
            if (refTxn != null)
                return refTxn;

            // try to modify after cancelling running compactions. This will attempt to cancel in flight compactions for
            // up to a minute, after which point, null will be returned
            return cfs.runWithCompactionsDisabled(this::acquireTuple, false, false);
        }
    }

    static class AcquisitionCallback implements AsyncFunction<List<AcquireResult>, Object>
    {
        private final UUID parentRepairSession;
        private final Collection<Range<Token>> ranges;

        public AcquisitionCallback(UUID parentRepairSession, Collection<Range<Token>> ranges)
        {
            this.parentRepairSession = parentRepairSession;
            this.ranges = ranges;
        }

        ListenableFuture<?> submitPendingAntiCompaction(AcquireResult result)
        {
            return CompactionManager.instance.submitPendingAntiCompaction(result.cfs, ranges, result.refs, result.txn, parentRepairSession);
        }

        public ListenableFuture apply(List<AcquireResult> results) throws Exception
        {
            if (Iterables.any(results, t -> t == null))
            {
                // Release all sstables, and report failure back to coordinator
                for (AcquireResult result : results)
                {
                    if (result != null)
                    {
                        logger.info("Releasing acquired sstables for {}.{}", result.cfs.metadata.keyspace, result.cfs.metadata.name);
                        result.abort();
                    }
                }
                return Futures.immediateFailedFuture(new RuntimeException("unable to acquire sstables"));
            }
            else
            {
                List<ListenableFuture<?>> pendingAntiCompactions = new ArrayList<>(results.size());
                for (AcquireResult result : results)
                {
                    if (result.txn != null)
                    {
                        ListenableFuture<?> future = submitPendingAntiCompaction(result);
                        pendingAntiCompactions.add(future);
                    }
                }

                return Futures.allAsList(pendingAntiCompactions);
            }
        }
    }

    private final UUID prsId;
    private final Collection<Range<Token>> ranges;
    private final ExecutorService executor;

    public PendingAntiCompaction(UUID prsId, Collection<Range<Token>> ranges, ExecutorService executor)
    {
        this.prsId = prsId;
        this.ranges = ranges;
        this.executor = executor;
    }

    public ListenableFuture run()
    {
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(prsId);
        List<ListenableFutureTask<AcquireResult>> tasks = new ArrayList<>();
        for (ColumnFamilyStore cfs : prs.getColumnFamilyStores())
        {
            cfs.forceBlockingFlush();
            ListenableFutureTask<AcquireResult> task = ListenableFutureTask.create(new AcquisitionCallable(cfs, ranges, prsId));
            executor.submit(task);
            tasks.add(task);
        }
        ListenableFuture<List<AcquireResult>> acquisitionResults = Futures.successfulAsList(tasks);
        ListenableFuture compactionResult = Futures.transform(acquisitionResults, new AcquisitionCallback(prsId, ranges));
        return compactionResult;
    }
}
