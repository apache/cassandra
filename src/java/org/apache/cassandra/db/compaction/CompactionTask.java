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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.SnapshotOptions;
import org.apache.cassandra.service.snapshot.SnapshotType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.db.compaction.CompactionHistoryTabularData.COMPACTION_TYPE_PROPERTY;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class CompactionTask extends AbstractCompactionTask
{
    protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);
    protected final long gcBefore;
    protected final boolean keepOriginals;
    protected static long totalBytesCompacted = 0;
    private ActiveCompactionsTracker activeCompactions;

    public CompactionTask(ColumnFamilyStore cfs, ILifecycleTransaction txn, long gcBefore)
    {
        this(cfs, txn, gcBefore, false);
    }

    public CompactionTask(ColumnFamilyStore cfs, ILifecycleTransaction txn, long gcBefore, boolean keepOriginals)
    {
        super(cfs, txn);
        this.gcBefore = gcBefore;
        this.keepOriginals = keepOriginals;
    }

    public static synchronized long addToTotalBytesCompacted(long bytesCompacted)
    {
        return totalBytesCompacted += bytesCompacted;
    }

    protected void executeInternal(ActiveCompactionsTracker activeCompactions)
    {
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        run();
    }

    public boolean reduceScopeForLimitedSpace(Set<SSTableReader> nonExpiredSSTables, long expectedSize)
    {
        if (partialCompactionsAcceptable() && nonExpiredSSTables.size() > 1)
        {
            // Try again w/o the largest one.
            SSTableReader removedSSTable = cfs.getMaxSizeFile(nonExpiredSSTables);
            logger.warn("insufficient space to compact all requested files. {}MiB required, {} for compaction {} - removing largest SSTable: {}",
                        (float) expectedSize / 1024 / 1024,
                        StringUtils.join(nonExpiredSSTables, ", "),
                        transaction.opIdString(),
                        removedSSTable);
            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the end.
            transaction.cancel(removedSSTable);
            nonExpiredSSTables.remove(removedSSTable);
            return true;
        }
        return false;
    }

    /**
     * @return The token range that the operation should compact. This is usually null, but if we have a parallelizable
     * multi-task operation (see {@link UnifiedCompactionStrategy#createCompactionTasks}), it will specify a subrange.
     */
    protected Range<Token> tokenRange()
    {
        return null;
    }

    /**
     * @return The set of input sstables for this compaction. This must be a subset of the transaction originals and
     * must reflect any removal of sstables from the originals set for correct overlap tracking.
     * See {@link UnifiedCompactionTask} for an example.
     */
    protected Set<SSTableReader> inputSSTables()
    {
        return transaction.originals();
    }

    /**
     * @return True if the task should try to limit the operation size to the available space by removing sstables from
     * the compacting set. This cannot be done if this is part of a multi-task operation with a shared transaction.
     */
    protected boolean shouldReduceScopeForSpace()
    {
        return true;
    }
    
    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     * Caller is in charge of marking/unmarking the sstables as compacting.
     */
    @Override
    protected void runMayThrow() throws Exception
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert transaction != null;

        if (inputSSTables().isEmpty())
            return;

        // Note that the current compaction strategy, is not necessarily the one this task was created under.
        // This should be harmless; see comments to CFS.maybeReloadCompactionStrategy.
        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();

        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
        {
            SnapshotOptions options = SnapshotOptions.systemSnapshot(cfs.name, SnapshotType.COMPACT, cfs.getKeyspaceTableName()).skipFlush().build();
            SnapshotManager.instance.takeSnapshot(options);
        }

        try (CompactionController controller = getCompactionController(inputSSTables()))
        {
            // Note: the controller set-up above relies on using the transaction-provided sstable list, from which
            // fully-expired sstables should not be removed (so that the overlap tracker does not include them), but
            // sstables excluded for scope reduction should be removed.
            Set<SSTableReader> actuallyCompact = new HashSet<>(inputSSTables());

            final Set<SSTableReader> fullyExpiredSSTables = controller.getFullyExpiredSSTables();

            maybeNotifyIndexersAboutRowsInFullyExpiredSSTables(fullyExpiredSSTables);

            if (!fullyExpiredSSTables.isEmpty())
            {
                logger.debug("Compaction {} dropping expired sstables: {}", transaction.opIdString(), fullyExpiredSSTables);
                actuallyCompact.removeAll(fullyExpiredSSTables);
            }

            TimeUUID taskId = transaction.opId();
            // select SSTables to compact based on available disk space.
            final boolean hasExpirations = !fullyExpiredSSTables.isEmpty();
            if ((shouldReduceScopeForSpace() && !buildCompactionCandidatesForAvailableDiskSpace(actuallyCompact, hasExpirations, taskId))
                || hasExpirations)
            {
                // The set of sstables has changed (one or more were excluded due to limited available disk space).
                // We need to recompute the overlaps between sstables. The iterators used in the compaction controller
                // and tracker will reflect the changed set of sstables made by LifecycleTransaction.cancel(),
                // so refreshing the overlaps will be based on the updated set of sstables.
                controller.refreshOverlaps();
            }

            // sanity check: all sstables must belong to the same cfs
            assert !Iterables.any(actuallyCompact, new Predicate<SSTableReader>()
            {
                @Override
                public boolean apply(SSTableReader sstable)
                {
                    return !sstable.descriptor.cfname.equals(cfs.name);
                }
            });

            // new sstables from flush can be added during a compaction, but only the compaction can remove them,
            // so in our single-threaded compaction world this is a valid way of determining if we're compacting
            // all the sstables (that existed when we started)
            StringBuilder ssTableLoggerMsg = new StringBuilder("[");
            for (SSTableReader sstr : actuallyCompact)
            {
                ssTableLoggerMsg.append(sstr.getSSTableLevel() != 0 ? String.format("%s:level=%d", sstr.getFilename(), sstr.getSSTableLevel())
                                                                    : sstr.getFilename());
                ssTableLoggerMsg.append(", ");
            }
            ssTableLoggerMsg.append("]");

            logger.info("Compacting ({}) {}", transaction.opIdString(), ssTableLoggerMsg);

            RateLimiter limiter = CompactionManager.instance.getRateLimiter();
            long start = nanoTime();
            long startTime = currentTimeMillis();
            long totalKeysWritten = 0;
            long estimatedKeys = 0;
            long inputSizeBytes;
            long timeSpentWritingKeys;

            Collection<SSTableReader> newSStables;

            long[] mergedRowCounts;
            long totalSourceCQLRows;

            Range<Token> tokenRange = tokenRange();
            List<Range<Token>> rangeList = tokenRange != null ? ImmutableList.of(tokenRange) : null;

            long nowInSec = FBUtilities.nowInSeconds();
            try (Refs<SSTableReader> refs = Refs.ref(actuallyCompact);
                 AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(actuallyCompact, rangeList);
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

                        ci.setTargetDirectory(writer.getSStableDirectory().path());
                        long bytesScanned = scanners.getTotalBytesScanned();

                        // Rate limit the scanners, and account for compression
                        CompactionManager.instance.compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);

                        lastBytesScanned = bytesScanned;

                        if (nanoTime() - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L))
                        {
                            controller.maybeRefreshOverlaps();
                            lastCheckObsoletion = nanoTime();
                        }
                    }
                    timeSpentWritingKeys = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);

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
                return;

            // log a bunch of statistics about the result and save to system table compaction_history
            long durationInNano = nanoTime() - start;
            long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);
            long startsize = inputSizeBytes;
            long endsize = SSTableReader.getTotalBytes(newSStables);
            double ratio = (double) endsize / (double) startsize;

            StringBuilder newSSTableNames = new StringBuilder();
            for (SSTableReader reader : newSStables)
                newSSTableNames.append(reader.descriptor.baseFile()).append(",");
            long totalSourceRows = 0;
            for (int i = 0; i < mergedRowCounts.length; i++)
                totalSourceRows += mergedRowCounts[i] * (i + 1);

            String mergeSummary = updateCompactionHistory(taskId, cfs.getKeyspaceName(), cfs.getTableName(), mergedRowCounts, startsize, endsize,
                                                          ImmutableMap.of(COMPACTION_TYPE_PROPERTY, compactionType.type));

            logger.info(String.format("Compacted (%s) %d sstables to [%s] to level=%d.  %s to %s (~%d%% of original) in %,dms.  Read Throughput = %s, Write Throughput = %s, Row Throughput = ~%,d/s.  %,d total partitions merged to %,d.  Partition merge counts were {%s}. Time spent writing keys = %,dms",
                                      transaction.opIdString(),
                                      actuallyCompact.size(),
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
                                      mergeSummary,
                                      timeSpentWritingKeys));
            if (logger.isTraceEnabled())
            {
                logger.trace("CF Total Bytes Compacted: {}", FBUtilities.prettyPrintMemory(CompactionTask.addToTotalBytesCompacted(endsize)));
                logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}", totalKeysWritten, estimatedKeys, ((double)(totalKeysWritten - estimatedKeys)/totalKeysWritten));
            }
            cfs.getCompactionStrategyManager().compactionLogger.compaction(startTime, inputSSTables(), currentTimeMillis(), newSStables);

            // update the metrics
            cfs.metric.compactionBytesWritten.inc(endsize);
        }
    }

    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                          Directories directories,
                                                          ILifecycleTransaction transaction,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        return new DefaultCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, keepOriginals, getLevel());
    }

    public static String updateCompactionHistory(TimeUUID taskId, String keyspaceName, String columnFamilyName, long[] mergedRowCounts, long startSize, long endSize, Map<String, String> compactionProperties)
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
        SystemKeyspace.updateCompactionHistory(taskId, keyspaceName, columnFamilyName, currentTimeMillis(), startSize, endSize, mergedRows, compactionProperties);
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

    public static TimeUUID getPendingRepair(Set<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
        {
            return ActiveRepairService.NO_PENDING_REPAIR;
        }
        Set<TimeUUID> ids = new HashSet<>();
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
     * there's enough space (in theory) to handle the compaction.
     *
     * @return true if there is enough disk space to execute the complete compaction, false if some sstables are excluded.
     */
    protected boolean buildCompactionCandidatesForAvailableDiskSpace(final Set<SSTableReader> nonExpiredSSTables, boolean containsExpired, TimeUUID taskId)
    {
        if(!cfs.isCompactionDiskSpaceCheckEnabled() && compactionType == OperationType.COMPACTION)
        {
            logger.info("Compaction space check is disabled - trying to compact all sstables");
            return true;
        }

        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
        int sstablesRemoved = 0;

        while(!nonExpiredSSTables.isEmpty())
        {
            // Only consider write size of non expired SSTables
            long writeSize;
            try
            {
                writeSize = cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType);
                Map<File, Long> expectedNewWriteSize = new HashMap<>();
                List<File> newCompactionDatadirs = cfs.getDirectoriesForFiles(nonExpiredSSTables);
                long writeSizePerOutputDatadir = writeSize / Math.max(newCompactionDatadirs.size(), 1);
                for (File directory : newCompactionDatadirs)
                    expectedNewWriteSize.put(directory, writeSizePerOutputDatadir);

                Map<File, Long> expectedWriteSize = CompactionManager.instance.active.estimatedRemainingWriteBytes();

                // todo: abort streams if they block compactions
                if (cfs.getDirectories().hasDiskSpaceForCompactionsAndStreams(expectedNewWriteSize, expectedWriteSize))
                    break;
            }
            catch (Exception e)
            {
                logger.error("Could not check if there is enough disk space for compaction {}", taskId, e);
                break;
            }

            if (!reduceScopeForLimitedSpace(nonExpiredSSTables, writeSize))
            {
                // we end up here if we can't take any more sstables out of the compaction.
                // usually means we've run out of disk space

                // but we can still remove expired SSTables
                if (partialCompactionsAcceptable() && containsExpired)
                {
                    for (SSTableReader rdr : nonExpiredSSTables)
                        transaction.cancel(rdr);
                    nonExpiredSSTables.clear();
                    assert transaction.originals().size() > 0;
                    break;
                }

                String msg = String.format("Not enough space for compaction (%s) of %s.%s, estimated sstables = %d, expected write size = %d",
                                           taskId,
                                           cfs.getKeyspaceName(),
                                           cfs.name,
                                           Math.max(1, writeSize / strategy.getMaxSSTableBytes()),
                                           writeSize);
                logger.warn(msg);
                CompactionManager.instance.incrementAborted();
                throw new RuntimeException(msg);
            }

            sstablesRemoved++;
            logger.warn("Not enough space for compaction {}, {}MiB estimated. Reducing scope.",
                        taskId, (float) writeSize / 1024 / 1024);
        }

        if(sstablesRemoved > 0)
        {
            CompactionManager.instance.incrementCompactionsReduced();
            CompactionManager.instance.incrementSstablesDropppedFromCompactions(sstablesRemoved);
            return false;
        }
        return true;
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

    private void maybeNotifyIndexersAboutRowsInFullyExpiredSSTables(Set<SSTableReader> fullyExpiredSSTables)
    {
        if (fullyExpiredSSTables.isEmpty())
            return;

        List<Index> indexes = new ArrayList<>();
        for (Index index : cfs.indexManager.listIndexes())
        {
            if (index.notifyIndexerAboutRowsInFullyExpiredSSTables())
                indexes.add(index);
        }

        if (indexes.isEmpty())
            return;

        for (SSTableReader expiredSSTable : fullyExpiredSSTables)
        {
            try (ISSTableScanner scanner = expiredSSTable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next();
                         WriteContext ctx = cfs.keyspace.getWriteHandler().createContextForIndexing())
                    {
                        List<Index.Indexer> indexers = new ArrayList<>();
                        for (int i = 0; i < indexes.size(); i++)
                        {
                            Index.Indexer indexer = indexes.get(i).indexerFor(partition.partitionKey(),
                                                                              partition.columns(),
                                                                              FBUtilities.nowInSeconds(),
                                                                              ctx,
                                                                              IndexTransaction.Type.COMPACTION,
                                                                              null);

                            if (indexer != null)
                                indexers.add(indexer);
                        }

                        if (!indexers.isEmpty())
                        {
                            for (Index.Indexer indexer : indexers)
                                indexer.begin();

                            while (partition.hasNext())
                            {
                                Unfiltered unfiltered = partition.next();
                                if (unfiltered instanceof Row)
                                {
                                    for (Index.Indexer indexer : indexers)
                                        indexer.removeRow((Row) unfiltered);
                                }
                            }

                            for (Index.Indexer indexer : indexers)
                                indexer.finish();
                        }
                    }
                }
            }
        }
    }
}
