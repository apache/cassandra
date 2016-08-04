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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager.CompactionExecutorStatsCollector;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;

public class CompactionTask extends AbstractCompactionTask
{
    protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);
    protected final int gcBefore;
    private final boolean offline;
    protected static long totalBytesCompacted = 0;
    private CompactionExecutorStatsCollector collector;

    public CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, boolean offline)
    {
        super(cfs, txn);
        this.gcBefore = gcBefore;
        this.offline = offline;
    }

    public static synchronized long addToTotalBytesCompacted(long bytesCompacted)
    {
        return totalBytesCompacted += bytesCompacted;
    }

    protected int executeInternal(CompactionExecutorStatsCollector collector)
    {
        this.collector = collector;
        run();
        return transaction.originals().size();
    }

    public boolean reduceScopeForLimitedSpace(long expectedSize)
    {
        if (partialCompactionsAcceptable() && transaction.originals().size() > 1)
        {
            // Try again w/o the largest one.
            logger.warn("insufficient space to compact all requested files. {}MB required, {}",
                        (float) expectedSize / 1024 / 1024,
                        StringUtils.join(transaction.originals(), ", "));
            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the end.
            SSTableReader removedSSTable = cfs.getMaxSizeFile(transaction.originals());
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
        AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();

        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            cfs.snapshotWithoutFlush(System.currentTimeMillis() + "-compact-" + cfs.name);

        // note that we need to do a rough estimate early if we can fit the compaction on disk - this is pessimistic, but
        // since we might remove sstables from the compaction in checkAvailableDiskSpace it needs to be done here

        checkAvailableDiskSpace();

        // sanity check: all sstables must belong to the same cfs
        assert !Iterables.any(transaction.originals(), new Predicate<SSTableReader>()
        {
            @Override
            public boolean apply(SSTableReader sstable)
            {
                return !sstable.descriptor.cfname.equals(cfs.name);
            }
        });

        UUID taskId = offline ? null : SystemKeyspace.startCompaction(cfs, transaction.originals());

        // new sstables from flush can be added during a compaction, but only the compaction can remove them,
        // so in our single-threaded compaction world this is a valid way of determining if we're compacting
        // all the sstables (that existed when we started)
        StringBuilder ssTableLoggerMsg = new StringBuilder("[");
        for (SSTableReader sstr : transaction.originals())
        {
            ssTableLoggerMsg.append(String.format("%s:level=%d, ", sstr.getFilename(), sstr.getSSTableLevel()));
        }
        ssTableLoggerMsg.append("]");
        String taskIdLoggerMsg = taskId == null ? UUIDGen.getTimeUUID().toString() : taskId.toString();
        logger.debug("Compacting ({}) {}", taskIdLoggerMsg, ssTableLoggerMsg);

        long start = System.nanoTime();

        long totalKeysWritten = 0;

        long estimatedKeys = 0;
        try (CompactionController controller = getCompactionController(transaction.originals()))
        {
            Set<SSTableReader> actuallyCompact = Sets.difference(transaction.originals(), controller.getFullyExpiredSSTables());

            SSTableFormat.Type sstableFormat = getFormatType(transaction.originals());

            List<SSTableReader> newSStables;
            AbstractCompactionIterable ci;

            // SSTableScanners need to be closed before markCompactedSSTablesReplaced call as scanners contain references
            // to both ifile and dfile and SSTR will throw deletion errors on Windows if it tries to delete before scanner is closed.
            // See CASSANDRA-8019 and CASSANDRA-8399
            try (Refs<SSTableReader> refs = Refs.ref(actuallyCompact);
                 AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(actuallyCompact))
            {
                ci = new CompactionIterable(compactionType, scanners.scanners, controller, sstableFormat, taskId);
                try (CloseableIterator<AbstractCompactedRow> iter = ci.iterator())
                {
                    long lastCheckObsoletion = start;

                    if (!controller.cfs.getCompactionStrategy().isActive)
                        throw new CompactionInterruptedException(ci.getCompactionInfo());

                    if (collector != null)
                        collector.beginCompaction(ci);

                    try (CompactionAwareWriter writer = getCompactionAwareWriter(cfs, transaction, actuallyCompact))
                    {
                        estimatedKeys = writer.estimatedKeys();
                        while (iter.hasNext())
                        {
                            if (ci.isStopRequested())
                                throw new CompactionInterruptedException(ci.getCompactionInfo());

                            try (AbstractCompactedRow row = iter.next())
                            {
                                if (writer.append(row))
                                    totalKeysWritten++;

                                if (System.nanoTime() - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L))
                                {
                                    controller.maybeRefreshOverlaps();
                                    lastCheckObsoletion = System.nanoTime();
                                }
                            }
                        }

                        // don't replace old sstables yet, as we need to mark the compaction finished in the system table
                        newSStables = writer.finish();
                    }
                    finally
                    {
                        // point of no return -- the new sstables are live on disk; next we'll start deleting the old ones
                        // (in replaceCompactedSSTables)
                        if (taskId != null)
                            SystemKeyspace.finishCompaction(taskId);

                        if (collector != null)
                            collector.finishCompaction(ci);
                    }
                }
            }

            // log a bunch of statistics about the result and save to system table compaction_history
            long dTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            long startsize = SSTableReader.getTotalBytes(transaction.originals());
            long endsize = SSTableReader.getTotalBytes(newSStables);
            double ratio = (double) endsize / (double) startsize;

            StringBuilder newSSTableNames = new StringBuilder();
            for (SSTableReader reader : newSStables)
                newSSTableNames.append(reader.descriptor.baseFilename()).append(",");

            if (offline)
            {
                Refs.release(Refs.selfRefs(newSStables));
            }
            else
            {
                double mbps = dTime > 0 ? (double) endsize / (1024 * 1024) / ((double) dTime / 1000) : 0;
                long totalSourceRows = 0;
                String mergeSummary = updateCompactionHistory(cfs.keyspace.getName(), cfs.getColumnFamilyName(), ci, startsize, endsize);
                logger.debug(String.format("Compacted (%s) %d sstables to [%s] to level=%d.  %,d bytes to %,d (~%d%% of original) in %,dms = %fMB/s.  %,d total partitions merged to %,d.  Partition merge counts were {%s}",
                                           taskIdLoggerMsg, transaction.originals().size(), newSSTableNames.toString(), getLevel(), startsize, endsize, (int) (ratio * 100), dTime, mbps, totalSourceRows, totalKeysWritten, mergeSummary));
                logger.trace(String.format("CF Total Bytes Compacted: %,d", CompactionTask.addToTotalBytesCompacted(endsize)));
                logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}", totalKeysWritten, estimatedKeys, ((double) (totalKeysWritten - estimatedKeys) / totalKeysWritten));
            }
        }
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, LifecycleTransaction transaction, Set<SSTableReader> nonExpiredSSTables)
    {
        return new DefaultCompactionWriter(cfs, transaction, nonExpiredSSTables, offline, compactionType);

    }

    public static String updateCompactionHistory(String keyspaceName, String columnFamilyName, AbstractCompactionIterable ci, long startSize, long endSize)
    {
        long[] counts = ci.getMergedRowCounts();
        StringBuilder mergeSummary = new StringBuilder(counts.length * 10);
        Map<Integer, Long> mergedRows = new HashMap<>();
        for (int i = 0; i < counts.length; i++)
        {
            long count = counts[i];
            if (count == 0)
                continue;

            int rows = i + 1;
            mergeSummary.append(String.format("%d:%d, ", rows, count));
            mergedRows.put(rows, count);
        }
        SystemKeyspace.updateCompactionHistory(keyspaceName, columnFamilyName, System.currentTimeMillis(), startSize, endSize, mergedRows);
        return mergeSummary.toString();
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

    /*
    Checks if we have enough disk space to execute the compaction.  Drops the largest sstable out of the Task until
    there's enough space (in theory) to handle the compaction.  Does not take into account space that will be taken by
    other compactions.
     */
    protected void checkAvailableDiskSpace()
    {
        AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();

        while(true)
        {
            long expectedWriteSize = cfs.getExpectedCompactedFileSize(transaction.originals(), compactionType);
            long estimatedSSTables = Math.max(1, expectedWriteSize / strategy.getMaxSSTableBytes());

            if(cfs.directories.hasAvailableDiskSpace(estimatedSSTables, expectedWriteSize))
                break;

            if (!reduceScopeForLimitedSpace(expectedWriteSize))
            {
                // we end up here if we can't take any more sstables out of the compaction.
                // usually means we've run out of disk space
                String msg = String.format("Not enough space for compaction, estimated sstables = %d, expected write size = %d", estimatedSSTables, expectedWriteSize);
                logger.warn(msg);
                throw new RuntimeException(msg);
            }
            logger.warn("Not enough space for compaction, {}MB estimated.  Reducing scope.",
                            (float) expectedWriteSize / 1024 / 1024);
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

    public static SSTableFormat.Type getFormatType(Collection<SSTableReader> sstables)
    {
        if (sstables.isEmpty() || !SSTableFormat.enableSSTableDevelopmentTestMode)
            return DatabaseDescriptor.getSSTableFormat();

        //Allows us to test compaction of non-default formats
        return sstables.iterator().next().descriptor.formatType;
    }
}
