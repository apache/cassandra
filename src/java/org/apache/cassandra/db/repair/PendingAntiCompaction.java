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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.concurrent.FutureTask;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInfo;
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
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Performs an anti compaction on a set of tables and token ranges, isolating the unrepaired sstables
 * for a give token range into a pending repair group so they can't be compacted with other sstables
 * while they are being repaired.
 */
public class PendingAntiCompaction
{
    private static final Logger logger = LoggerFactory.getLogger(PendingAntiCompaction.class);
    private static final int ACQUIRE_SLEEP_MS = CassandraRelevantProperties.ACQUIRE_SLEEP_MS.getInt();
    private static final int ACQUIRE_RETRY_SECONDS = CassandraRelevantProperties.ACQUIRE_RETRY_SECONDS.getInt();

    public static class AcquireResult
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

        @VisibleForTesting
        public void abort()
        {
            if (txn != null)
                txn.abort();
            if (refs != null)
                refs.release();
        }
    }

    static class SSTableAcquisitionException extends RuntimeException
    {
        SSTableAcquisitionException(String message)
        {
            super(message);
        }
    }

    @VisibleForTesting
    static class AntiCompactionPredicate implements Predicate<SSTableReader>
    {
        private final Collection<Range<Token>> ranges;
        private final TimeUUID prsid;

        public AntiCompactionPredicate(Collection<Range<Token>> ranges, TimeUUID prsid)
        {
            this.ranges = ranges;
            this.prsid = prsid;
        }

        public boolean apply(SSTableReader sstable)
        {
            if (!sstable.intersects(ranges))
                return false;

            StatsMetadata metadata = sstable.getSSTableMetadata();

            // exclude repaired sstables
            if (metadata.repairedAt != UNREPAIRED_SSTABLE)
                return false;

            if (!sstable.descriptor.version.hasPendingRepair())
            {
                String message = String.format("Prepare phase failed because it encountered legacy sstables that don't " +
                                               "support pending repair, run upgradesstables before starting incremental " +
                                               "repairs, repair session (%s)", prsid);
                throw new SSTableAcquisitionException(message);
            }

            // exclude sstables pending repair, but record session ids for
            // non-finalized sessions for a later error message
            if (metadata.pendingRepair != NO_PENDING_REPAIR)
            {
                if (!ActiveRepairService.instance().consistent.local.isSessionFinalized(metadata.pendingRepair))
                {
                    String message = String.format("Prepare phase for incremental repair session %s has failed because it encountered " +
                                                   "intersecting sstables belonging to another incremental repair session (%s). This is " +
                                                   "caused by starting an incremental repair session before a previous one has completed. " +
                                                   "Check nodetool repair_admin for hung sessions and fix them.", prsid, metadata.pendingRepair);
                    throw new SSTableAcquisitionException(message);
                }
                return false;
            }
            Collection<CompactionInfo> cis = CompactionManager.instance.active.getCompactionsForSSTable(sstable, OperationType.ANTICOMPACTION);
            if (cis != null && !cis.isEmpty())
            {
                // todo: start tracking the parent repair session id that created the anticompaction to be able to give a better error messsage here:
                StringBuilder sb = new StringBuilder();
                sb.append("Prepare phase for incremental repair session ");
                sb.append(prsid);
                sb.append(" has failed because it encountered intersecting sstables belonging to another incremental repair session. ");
                sb.append("This is caused by starting multiple conflicting incremental repairs at the same time. ");
                sb.append("Conflicting anticompactions: ");
                for (CompactionInfo ci : cis)
                    sb.append(ci.getTaskId() == null ? "no compaction id" : ci.getTaskId()).append(':').append(ci.getSSTables()).append(',');
                throw new SSTableAcquisitionException(sb.toString());
            }
            return true;
        }
    }

    public static class AcquisitionCallable implements Callable<AcquireResult>
    {
        private final ColumnFamilyStore cfs;
        private final TimeUUID sessionID;
        private final AntiCompactionPredicate predicate;
        private final int acquireRetrySeconds;
        private final int acquireSleepMillis;

        @VisibleForTesting
        public AcquisitionCallable(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, TimeUUID sessionID, int acquireRetrySeconds, int acquireSleepMillis)
        {
            this(cfs, sessionID, acquireRetrySeconds, acquireSleepMillis, new AntiCompactionPredicate(ranges, sessionID));
        }

        @VisibleForTesting
        AcquisitionCallable(ColumnFamilyStore cfs, TimeUUID sessionID, int acquireRetrySeconds, int acquireSleepMillis, AntiCompactionPredicate predicate)
        {
            this.cfs = cfs;
            this.sessionID = sessionID;
            this.predicate = predicate;
            this.acquireRetrySeconds = acquireRetrySeconds;
            this.acquireSleepMillis = acquireSleepMillis;
        }

        private AcquireResult acquireTuple()
        {
            // this method runs with compactions stopped & disabled
            try
            {
                // using predicate might throw if there are conflicting ranges
                Set<SSTableReader> sstables = cfs.getLiveSSTables().stream().filter(predicate).collect(Collectors.toSet());
                if (sstables.isEmpty())
                    return new AcquireResult(cfs, null, null);

                LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
                if (txn != null)
                    return new AcquireResult(cfs, Refs.ref(sstables), txn);
                else
                    logger.error("Could not mark compacting for {} (sstables = {}, compacting = {})", sessionID, sstables, cfs.getTracker().getCompacting());
            }
            catch (SSTableAcquisitionException e)
            {
                logger.warn(e.getMessage());
                logger.debug("Got exception trying to acquire sstables", e);
            }

            return null;
        }

        protected AcquireResult acquireSSTables()
        {
            return cfs.runWithCompactionsDisabled(this::acquireTuple, predicate, OperationType.ANTICOMPACTION, false, false, false);
        }

        public AcquireResult call()
        {
            logger.debug("acquiring sstables for pending anti compaction on session {}", sessionID);
            // try to modify after cancelling running compactions. This will attempt to cancel in flight compactions including the given sstables for
            // up to a minute, after which point, null will be returned
            long start = currentTimeMillis();
            long delay = TimeUnit.SECONDS.toMillis(acquireRetrySeconds);
            // Note that it is `predicate` throwing SSTableAcquisitionException if it finds a conflicting sstable
            // and we only retry when runWithCompactionsDisabled throws when uses the predicate, not when acquireTuple is.
            // This avoids the case when we have an sstable [0, 100] and a user starts a repair on [0, 50] and then [51, 100] before
            // anticompaction has finished but not when the second repair is [25, 75] for example - then we will fail it without retry.
            do
            {
                try
                {
                    // Note that anticompactions are not disabled when running this. This is safe since runWithCompactionsDisabled
                    // is synchronized - acquireTuple and predicate can only be run by a single thread (for the given cfs).
                    return acquireSSTables();
                }
                catch (SSTableAcquisitionException e)
                {
                    logger.warn("Session {} failed acquiring sstables: {}, retrying every {}ms for another {}s",
                                sessionID,
                                e.getMessage(),
                                acquireSleepMillis,
                                TimeUnit.SECONDS.convert(delay + start - currentTimeMillis(), TimeUnit.MILLISECONDS));
                    Uninterruptibles.sleepUninterruptibly(acquireSleepMillis, TimeUnit.MILLISECONDS);

                    if (currentTimeMillis() - start > delay)
                        logger.warn("{} Timed out waiting to acquire sstables", sessionID, e);

                }
                catch (Throwable t)
                {
                    logger.error("Got exception disabling compactions for session {}", sessionID, t);
                    throw t;
                }
            } while (currentTimeMillis() - start < delay);
            return null;
        }
    }

    static class AcquisitionCallback implements Function<List<AcquireResult>, Future<List<Void>>>
    {
        private final TimeUUID parentRepairSession;
        private final RangesAtEndpoint tokenRanges;
        private final BooleanSupplier isCancelled;

        public AcquisitionCallback(TimeUUID parentRepairSession, RangesAtEndpoint tokenRanges, BooleanSupplier isCancelled)
        {
            this.parentRepairSession = parentRepairSession;
            this.tokenRanges = tokenRanges;
            this.isCancelled = isCancelled;
        }

        Future<Void> submitPendingAntiCompaction(AcquireResult result)
        {
            return CompactionManager.instance.submitPendingAntiCompaction(result.cfs, tokenRanges, result.refs, result.txn, parentRepairSession, isCancelled);
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

        public Future<List<Void>> apply(List<AcquireResult> results)
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
                String message = String.format("Prepare phase for incremental repair session %s was unable to " +
                                               "acquire exclusive access to the neccesary sstables. " +
                                               "This is usually caused by running multiple incremental repairs on nodes that share token ranges",
                                               parentRepairSession);
                logger.warn(message);
                return ImmediateFuture.failure(new SSTableAcquisitionException(message));
            }
            else
            {
                List<Future<Void>> pendingAntiCompactions = new ArrayList<>(results.size());
                for (AcquireResult result : results)
                {
                    if (result.txn != null)
                    {
                        Future<Void> future = submitPendingAntiCompaction(result);
                        pendingAntiCompactions.add(future);
                    }
                }

                return FutureCombiner.allOf(pendingAntiCompactions);
            }
        }
    }

    private final TimeUUID prsId;
    private final Collection<ColumnFamilyStore> tables;
    private final RangesAtEndpoint tokenRanges;
    private final ExecutorService executor;
    private final int acquireRetrySeconds;
    private final int acquireSleepMillis;
    private final BooleanSupplier isCancelled;

    public PendingAntiCompaction(TimeUUID prsId,
                                 Collection<ColumnFamilyStore> tables,
                                 RangesAtEndpoint tokenRanges,
                                 ExecutorService executor,
                                 BooleanSupplier isCancelled)
    {
        this(prsId, tables, tokenRanges, ACQUIRE_RETRY_SECONDS, ACQUIRE_SLEEP_MS, executor, isCancelled);
    }

    @VisibleForTesting
    PendingAntiCompaction(TimeUUID prsId,
                          Collection<ColumnFamilyStore> tables,
                          RangesAtEndpoint tokenRanges,
                          int acquireRetrySeconds,
                          int acquireSleepMillis,
                          ExecutorService executor,
                          BooleanSupplier isCancelled)
    {
        this.prsId = prsId;
        this.tables = tables;
        this.tokenRanges = tokenRanges;
        this.executor = executor;
        this.acquireRetrySeconds = acquireRetrySeconds;
        this.acquireSleepMillis = acquireSleepMillis;
        this.isCancelled = isCancelled;
    }

    public Future<List<Void>> run()
    {
        List<FutureTask<AcquireResult>> tasks = new ArrayList<>(tables.size());
        for (ColumnFamilyStore cfs : tables)
        {
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.ANTICOMPACTION);
            FutureTask<AcquireResult> task = new FutureTask<>(getAcquisitionCallable(cfs, tokenRanges.ranges(), prsId, acquireRetrySeconds, acquireSleepMillis));
            executor.submit(task);
            tasks.add(task);
        }

        Future<List<AcquireResult>> acquisitionResults = FutureCombiner.successfulOf(tasks);
        return acquisitionResults.flatMap(getAcquisitionCallback(prsId, tokenRanges));
    }

    @VisibleForTesting
    protected AcquisitionCallable getAcquisitionCallable(ColumnFamilyStore cfs, Set<Range<Token>> ranges, TimeUUID prsId, int acquireRetrySeconds, int acquireSleepMillis)
    {
        return new AcquisitionCallable(cfs, ranges, prsId, acquireRetrySeconds, acquireSleepMillis);
    }

    @VisibleForTesting
    protected AcquisitionCallback getAcquisitionCallback(TimeUUID prsId, RangesAtEndpoint tokenRanges)
    {
        return new AcquisitionCallback(prsId, tokenRanges, isCancelled);
    }
}
