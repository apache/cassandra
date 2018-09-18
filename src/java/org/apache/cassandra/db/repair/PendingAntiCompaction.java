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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;

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

    static class SSTableAcquisitionException extends RuntimeException {}

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
            Set<UUID> conflictingSessions = new HashSet<>();

            Iterable<SSTableReader> sstables = cfs.getLiveSSTables().stream().filter(sstable -> {
                if (!sstable.intersects(ranges))
                    return false;

                StatsMetadata metadata = sstable.getSSTableMetadata();

                // exclude repaired sstables
                if (metadata.repairedAt != UNREPAIRED_SSTABLE)
                    return false;

                // exclude sstables pending repair, but record session ids for
                // non-finalized sessions for a later error message
                if (metadata.pendingRepair != NO_PENDING_REPAIR)
                {
                    if (!ActiveRepairService.instance.consistent.local.isSessionFinalized(metadata.pendingRepair))
                    {
                        conflictingSessions.add(metadata.pendingRepair);
                    }
                    return false;
                }

                return true;
            }).collect(Collectors.toList());

            // If there are sstables we'd like to acquire that are currently held by other sessions, we need to bail out. If we
            // didn't bail out here and the other repair sessions we're seeing were to fail, incremental repair behavior would be
            // confusing. You generally expect all data received before a repair session to be repaired when the session completes,
            // and that wouldn't be the case if the other session failed and moved it's data back to unrepaired.
            if (!conflictingSessions.isEmpty())
            {
                logger.warn("Prepare phase for incremental repair session {} has failed because it encountered " +
                            "intersecting sstables belonging to another incremental repair session(s) ({}). This is " +
                            "caused by starting an incremental repair session before a previous one has completed. " +
                            "Check nodetool repair_admin for hung sessions and fix them.",
                            sessionID, conflictingSessions);
                throw new SSTableAcquisitionException();
            }

            return sstables;
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
            try
            {
                AcquireResult refTxn = acquireTuple();
                if (refTxn != null)
                    return refTxn;
            }
            catch (SSTableAcquisitionException e)
            {
                return null;
            }

            // try to modify after cancelling running compactions. This will attempt to cancel in flight compactions for
            // up to a minute, after which point, null will be returned
            return cfs.runWithCompactionsDisabled(this::acquireTuple, false, false);
        }
    }

    static class AcquisitionCallback implements AsyncFunction<List<AcquireResult>, Object>
    {
        private final UUID parentRepairSession;
        private final RangesAtEndpoint tokenRanges;

        public AcquisitionCallback(UUID parentRepairSession, RangesAtEndpoint tokenRanges)
        {
            this.parentRepairSession = parentRepairSession;
            this.tokenRanges = tokenRanges;
        }

        ListenableFuture<?> submitPendingAntiCompaction(AcquireResult result)
        {
            return CompactionManager.instance.submitPendingAntiCompaction(result.cfs, tokenRanges, result.refs, result.txn, parentRepairSession);
        }

        private static boolean shouldAbort(AcquireResult result)
        {
            if (result == null)
                return true;

            // sstables in the acquire result are now marked compacting and are locked to this anti compaction. If any
            // of them are marked repaired or pending repair, acquisition raced with another pending anti-compaction, or
            // possibly even a repair session, and we need to abort to prevent sstables from moving between sessions.
            return result.refs != null && Iterables.any(result.refs, sstable -> {
                StatsMetadata metadata = sstable.getSSTableMetadata();
                return metadata.pendingRepair != NO_PENDING_REPAIR || metadata.repairedAt != UNREPAIRED_SSTABLE;
            });
        }

        public ListenableFuture apply(List<AcquireResult> results) throws Exception
        {
            if (Iterables.any(results, AcquisitionCallback::shouldAbort))
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
                logger.warn("Prepare phase for incremental repair session {} was unable to " +
                            "acquire exclusive access to the neccesary sstables. " +
                            "This is usually caused by running multiple incremental repairs on nodes that share token ranges",
                            parentRepairSession);
                return Futures.immediateFailedFuture(new SSTableAcquisitionException());
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
    private final Collection<ColumnFamilyStore> tables;
    private final RangesAtEndpoint tokenRanges;
    private final ExecutorService executor;

    public PendingAntiCompaction(UUID prsId,
                                 Collection<ColumnFamilyStore> tables,
                                 RangesAtEndpoint tokenRanges,
                                 ExecutorService executor)
    {
        this.prsId = prsId;
        this.tables = tables;
        this.tokenRanges = tokenRanges;
        this.executor = executor;
    }

    public ListenableFuture run()
    {
        List<ListenableFutureTask<AcquireResult>> tasks = new ArrayList<>(tables.size());
        for (ColumnFamilyStore cfs : tables)
        {
            cfs.forceBlockingFlush();
            ListenableFutureTask<AcquireResult> task = ListenableFutureTask.create(new AcquisitionCallable(cfs, tokenRanges.ranges(), prsId));
            executor.submit(task);
            tasks.add(task);
        }
        ListenableFuture<List<AcquireResult>> acquisitionResults = Futures.successfulAsList(tasks);
        ListenableFuture compactionResult = Futures.transformAsync(acquisitionResults, new AcquisitionCallback(prsId, tokenRanges), MoreExecutors.directExecutor());
        return compactionResult;
    }
}
