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
package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

public class CompactionTask extends AbstractCompactionTask
{
    protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);
    protected final int gcBefore;
    protected final boolean keepOriginals;
    protected static long totalBytesCompacted = 0;
    private ActiveCompactionsTracker activeCompactions;

    public CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore)
    {
        this(cfs, txn, gcBefore, false);
    }

    @Deprecated
    public CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, boolean offline, boolean keepOriginals)
    {
        this(cfs, txn, gcBefore, keepOriginals);
    }

    public CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, boolean keepOriginals)
    {
        super(cfs, txn);
        this.gcBefore = gcBefore;
        this.keepOriginals = keepOriginals;
    }

    public static synchronized long addToTotalBytesCompacted(long bytesCompacted)
    {
        return totalBytesCompacted += bytesCompacted;
    }

    protected int executeInternal(ActiveCompactionsTracker activeCompactions)
    {
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        run();
        return transaction.originals().size();
    }

    public boolean reduceScopeForLimitedSpace(Set<SSTableReader> nonExpiredSSTables, long expectedSize)
    {
        if (partialCompactionsAcceptable() && transaction.originals().size() > 1)
        {
            // Try again w/o the largest one.
            logger.warn("insufficient space to compact all requested files. {}MB required, {}",
                        (float) expectedSize / 1024 / 1024,
                        StringUtils.join(transaction.originals(), ", "));

            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the end.
            SSTableReader removedSSTable = cfs.getMaxSizeFile(nonExpiredSSTables);
            transaction.cancel(removedSSTable);
            return true;
        }
        return false;
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     * Caller is in charge of marking/unmarking the sstables as compacting.
     */
    protected void runMayThrow() throws Exception
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert transaction != null;

        if (transaction.originals().isEmpty())
            return;

        // Note that the current compaction strategy, is not necessarily the one this task was created under.
        // This should be harmless; see comments to CFS.maybeReloadCompactionStrategy.
        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();

        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            cfs.snapshotWithoutFlush(System.currentTimeMillis() + "-compact-" + cfs.name);

        try (CompactionController controller = getCompactionController(transaction.originals()))
        {

            final Set<SSTableReader> fullyExpiredSSTables = controller.getFullyExpiredSSTables();

            // select SSTables to compact based on available disk space.
            buildCompactionCandidatesForAvailableDiskSpace(fullyExpiredSSTables);

            // sanity check: all sstables must belong to the same cfs
            assert !Iterables.any(transaction.originals(), new Predicate<SSTableReader>()
            {
                @Override
                public boolean apply(SSTableReader sstable)
                {
                    return !sstable.descriptor.cfname.equals(cfs.name);
                }
            });

            UUID taskId = transaction.opId();

            // new sstables from flush can be added during a compaction, but only the compaction can remove them,
            // so in our single-threaded compaction world this is a valid way of determining if we're compacting
            // all the sstables (that existed when we started)
            StringBuilder ssTableLoggerMsg = new StringBuilder("[");
            for (SSTableReader sstr : transaction.originals())
            {
                ssTableLoggerMsg.append(String.format("%s:level=%d, ", sstr.getFilename(), sstr.getSSTableLevel()));
            }
            ssTableLoggerMsg.append("]");

            logger.info("Compacting ({}) {}", taskId, ssTableLoggerMsg);

            RateLimiter limiter = CompactionManager.instance.getRateLimiter();
            long start = System.nanoTime();
            long startTime = System.currentTimeMillis();
            long totalKeysWritten = 0;
            long estimatedKeys = 0;
            long inputSizeBytes;

            Set<SSTableReader> actuallyCompact = Sets.difference(transaction.originals(), fullyExpiredSSTables);
            Collection<SSTableReader> newSStables;

            long[] mergedRowCounts;
            long totalSourceCQLRows;

            // SSTableScanners need to be closed before markCompactedSSTablesReplaced call as scanners contain references
            // to both ifile and dfile and SSTR will throw deletion errors on Windows if it tries to delete before scanner is closed.
            // See CASSANDRA-8019 and CASSANDRA-8399
            int nowInSec = FBUtilities.nowInSeconds();
            try (Refs<SSTableReader> refs = Refs.ref(actuallyCompact);
                 AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(actuallyCompact);
                 CompactionIterator ci = new CompactionIterator(compactionType, scanners.scanners, controller, nowInSec, taskId))
            {
                long lastCheckObsoletion = start;
                inputSizeBytes = scanners.getTotalCompressedSize();
                double compressionRatio = scanners.getCompressionRatio();
                if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                    compressionRatio = 1.0;

                long lastBytesScanned = 0;

                activeCompactions.beginCompaction(ci);
                try (CompactionAwareWriter writer = getCompactionAwareWriter(cfs, getDirectories(), transaction, actuallyCompact))
                {
                    // Note that we need to re-check this flag after calling beginCompaction above to avoid a window
                    // where the compaction does not exist in activeCompactions but the CSM gets paused.
                    // We already have the sstables marked compacting here so CompactionManager#waitForCessation will
                    // block until the below exception is thrown and the transaction is cancelled.
                    if (!controller.cfs.getCompactionStrategyManager().isActive())
                        throw new CompactionInterruptedException(ci.getCompactionInfo());
                    estimatedKeys = writer.estimatedKeys();
                    while (ci.hasNext())
                    {
                        if (writer.append(ci.next()))
                            totalKeysWritten++;


                        long bytesScanned = scanners.getTotalBytesScanned();

                        //Rate limit the scanners, and account for compression
                        CompactionManager.compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);

                        lastBytesScanned = bytesScanned;

                        if (System.nanoTime() - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L))
                        {
                            controller.maybeRefreshOverlaps();
                            lastCheckObsoletion = System.nanoTime();
                        }
                    }

                    // point of no return
                    newSStables = writer.finish();
                }
                finally
                {
                    activeCompactions.finishCompaction(ci);
                    mergedRowCounts = ci.getMergedRowCounts();
                    totalSourceCQLRows = ci.getTotalSourceCQLRows();
                }
            }

            if (transaction.isOffline())
            {
                Refs.release(Refs.selfRefs(newSStables));
            }
            else
            {
                // log a bunch of statistics about the result and save to system table compaction_history

                long durationInNano = System.nanoTime() - start;
                long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);
                long startsize = inputSizeBytes;
                long endsize = SSTableReader.getTotalBytes(newSStables);
                double ratio = (double) endsize / (double) startsize;

                StringBuilder newSSTableNames = new StringBuilder();
                for (SSTableReader reader : newSStables)
                    newSSTableNames.append(reader.descriptor.baseFilename()).append(",");
                long totalSourceRows = 0;
                for (int i = 0; i < mergedRowCounts.length; i++)
                    totalSourceRows += mergedRowCounts[i] * (i + 1);

                String mergeSummary = updateCompactionHistory(cfs.keyspace.getName(), cfs.getTableName(), mergedRowCounts, startsize, endsize);

                logger.info(String.format("Compacted (%s) %d sstables to [%s] to level=%d.  %s to %s (~%d%% of original) in %,dms.  Read Throughput = %s, Write Throughput = %s, Row Throughput = ~%,d/s.  %,d total partitions merged to %,d.  Partition merge counts were {%s}",
                                           taskId,
                                           transaction.originals().size(),
                                           newSSTableNames.toString(),
                                           getLevel(),
                                           FBUtilities.prettyPrintMemory(startsize),
                                           FBUtilities.prettyPrintMemory(endsize),
                                           (int) (ratio * 100),
                                           dTime,
                                           FBUtilities.prettyPrintMemoryPerSecond(startsize, durationInNano),
                                           FBUtilities.prettyPrintMemoryPerSecond(endsize, durationInNano),
                                           (int) totalSourceCQLRows / (TimeUnit.NANOSECONDS.toSeconds(durationInNano) + 1),
                                           totalSourceRows,
                                           totalKeysWritten,
                                           mergeSummary));
                if (logger.isTraceEnabled())
                {
                    logger.trace("CF Total Bytes Compacted: {}", FBUtilities.prettyPrintMemory(CompactionTask.addToTotalBytesCompacted(endsize)));
                    logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}", totalKeysWritten, estimatedKeys, ((double)(totalKeysWritten - estimatedKeys)/totalKeysWritten));
                }
                cfs.getCompactionStrategyManager().compactionLogger.compaction(startTime, transaction.originals(), System.currentTimeMillis(), newSStables);

                // update the metrics
                cfs.metric.compactionBytesWritten.inc(endsize);
            }
        }
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                          Directories directories,
                                                          LifecycleTransaction transaction,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        return new DefaultCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, keepOriginals, getLevel());
    }

    public static String updateCompactionHistory(String keyspaceName, String columnFamilyName, long[] mergedRowCounts, long startSize, long endSize)
    {
        StringBuilder mergeSummary = new StringBuilder(mergedRowCounts.length * 10);
        Map<Integer, Long> mergedRows = new HashMap<>();
        for (int i = 0; i < mergedRowCounts.length; i++)
        {
            long count = mergedRowCounts[i];
            if (count == 0)
                continue;

            int rows = i + 1;
            mergeSummary.append(String.format("%d:%d, ", rows, count));
            mergedRows.put(rows, count);
        }
        SystemKeyspace.updateCompactionHistory(keyspaceName, columnFamilyName, System.currentTimeMillis(), startSize, endSize, mergedRows);
        return mergeSummary.toString();
    }

    protected Directories getDirectories()
    {
        return cfs.getDirectories();
    }

    public static long getMinRepairedAt(Set<SSTableReader> actuallyCompact)
    {
        long minRepairedAt= Long.MAX_VALUE;
        for (SSTableReader sstable : actuallyCompact)
            minRepairedAt = Math.min(minRepairedAt, sstable.getSSTableMetadata().repairedAt);
        if (minRepairedAt == Long.MAX_VALUE)
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        return minRepairedAt;
    }

    public static UUID getPendingRepair(Set<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
        {
            return ActiveRepairService.NO_PENDING_REPAIR;
        }
        Set<UUID> ids = new HashSet<>();
        for (SSTableReader sstable: sstables)
            ids.add(sstable.getSSTableMetadata().pendingRepair);

        if (ids.size() != 1)
            throw new RuntimeException(String.format("Attempting to compact pending repair sstables with sstables from other repair, or sstables not pending repair: %s", ids));

        return ids.iterator().next();
    }

    public static boolean getIsTransient(Set<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
        {
            return false;
        }

        boolean isTransient = sstables.iterator().next().isTransient();

        if (!Iterables.all(sstables, sstable -> sstable.isTransient() == isTransient))
        {
            throw new RuntimeException("Attempting to compact transient sstables with non transient sstables");
        }

        return isTransient;
    }


    /*
     * Checks if we have enough disk space to execute the compaction.  Drops the largest sstable out of the Task until
     * there's enough space (in theory) to handle the compaction.  Does not take into account space that will be taken by
     * other compactions.
     */
    protected void buildCompactionCandidatesForAvailableDiskSpace(final Set<SSTableReader> fullyExpiredSSTables)
    {
        if(!cfs.isCompactionDiskSpaceCheckEnabled() && compactionType == OperationType.COMPACTION)
        {
            logger.info("Compaction space check is disabled");
            return; // try to compact all SSTables
        }

        final Set<SSTableReader> nonExpiredSSTables = Sets.difference(transaction.originals(), fullyExpiredSSTables);
        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
        int sstablesRemoved = 0;

        while(!nonExpiredSSTables.isEmpty())
        {
            // Only consider write size of non expired SSTables
            long expectedWriteSize = cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType);
            long estimatedSSTables = Math.max(1, expectedWriteSize / strategy.getMaxSSTableBytes());

            if(cfs.getDirectories().hasAvailableDiskSpace(estimatedSSTables, expectedWriteSize))
                break;

            if (!reduceScopeForLimitedSpace(nonExpiredSSTables, expectedWriteSize))
            {
                // we end up here if we can't take any more sstables out of the compaction.
                // usually means we've run out of disk space

                // but we can still compact expired SSTables
                if(partialCompactionsAcceptable() && fullyExpiredSSTables.size() > 0 )
                {
                    // sanity check to make sure we compact only fully expired SSTables.
                    assert transaction.originals().equals(fullyExpiredSSTables);
                    break;
                }

                String msg = String.format("Not enough space for compaction, estimated sstables = %d, expected write size = %d", estimatedSSTables, expectedWriteSize);
                logger.warn(msg);
                CompactionManager.instance.incrementAborted();
                throw new RuntimeException(msg);
            }

            sstablesRemoved++;
            logger.warn("Not enough space for compaction, {}MB estimated.  Reducing scope.",
                        (float) expectedWriteSize / 1024 / 1024);
        }

        if(sstablesRemoved > 0)
        {
            CompactionManager.instance.incrementCompactionsReduced();
            CompactionManager.instance.incrementSstablesDropppedFromCompactions(sstablesRemoved);
        }

    }

    protected int getLevel()
    {
        return 0;
    }

    protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
    {
        return new CompactionController(cfs, toCompact, gcBefore);
    }

    protected boolean partialCompactionsAcceptable()
    {
        return !isUserDefined;
    }

    public static long getMaxDataAge(Collection<SSTableReader> sstables)
    {
        long max = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.maxDataAge > max)
                max = sstable.maxDataAge;
        }
        return max;
    }
}
