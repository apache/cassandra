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

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.db.compaction.CompactionManager.compactionRateLimiterAcquire;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemoryPerSecond;

public class CompactionTask extends AbstractCompactionTask
{
    protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);

    protected final int gcBefore;
    protected final boolean keepOriginals;
    /** for trace logging purposes only */
    private static final AtomicLong totalBytesCompacted = new AtomicLong();

    // The compaction strategy is not necessarily available for all compaction tasks (e.g. GC or sstable splitting)
    @Nullable
    private final AbstractCompactionStrategy strategy;

    /**
     * This constructs a compaction tasks that operations that do not normally have a compaction strategy, such as tombstone
     * collection or table splitting, also tests.
     */
    protected CompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, boolean keepOriginals)
    {
        this(cfs, txn, gcBefore, keepOriginals, CompactionObserver.NO_OP, null);
    }

    /**
     * This constructs a compaction task that has been created by a compaction strategy.
     */
    protected CompactionTask(AbstractCompactionStrategy strategy, LifecycleTransaction txn, int gcBefore, boolean keepOriginals)
    {
        this(strategy.cfs, txn, gcBefore, keepOriginals, strategy == null ? CompactionObserver.NO_OP : strategy.getBackgroundCompactions(), strategy);
    }

    private CompactionTask(ColumnFamilyStore cfs,
                           LifecycleTransaction txn,
                           int gcBefore,
                           boolean keepOriginals,
                           CompactionObserver compObserver,
                           @Nullable AbstractCompactionStrategy strategy)
    {
        super(cfs, txn);
        this.gcBefore = gcBefore;
        this.keepOriginals = keepOriginals;
        this.compObserver = compObserver;
        this.strategy = strategy;

        logger.debug("Created compaction task with id {} and strategy {}", txn.opId(), strategy);
    }

    /**
     * Create a compaction task for a generic compaction strategy.
     */
    public static AbstractCompactionTask forCompaction(AbstractCompactionStrategy strategy, LifecycleTransaction txn, int gcBefore)
    {
        return new CompactionTask(strategy, txn, gcBefore, false);
    }

    /**
     * Create a compaction task for {@link TimeWindowCompactionStrategy}.
     */
    static AbstractCompactionTask forTimeWindowCompaction(TimeWindowCompactionStrategy strategy, LifecycleTransaction txn, int gcBefore)
    {
        return new TimeWindowCompactionTask(strategy, txn, gcBefore, strategy.ignoreOverlaps());
    }

    /**
     * Create a compaction task without a compaction strategy, currently only called by tests.
     */
    static AbstractCompactionTask forTesting(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore)
    {
        return new CompactionTask(cfs, txn, gcBefore, false);
    }

    /**
     * Create a compaction task without a compaction strategy, currently only called by tests.
     */
    static AbstractCompactionTask forTesting(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, CompactionObserver compObserver)
    {
        return new CompactionTask(cfs, txn, gcBefore, false, compObserver, null);
    }

    /**
     * Create a compaction task for deleted data collection.
     */
    public static AbstractCompactionTask forGarbageCollection(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, CompactionParams.TombstoneOption tombstoneOption)
    {
        AbstractCompactionTask task = new CompactionTask(cfs, txn, gcBefore, false)
        {
            @Override
            protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
            {
                return new CompactionController(cfs, toCompact, gcBefore, null, tombstoneOption);
            }

            @Override
            protected int getLevel()
            {
                return txn.onlyOne().getSSTableLevel();
            }
        };
        task.setUserDefined(true);
        task.setCompactionType(OperationType.GARBAGE_COLLECT);
        return task;
    }

    private static long addToTotalBytesCompacted(long bytesCompacted)
    {
        return totalBytesCompacted.addAndGet(bytesCompacted);
    }

    @Override
    protected int executeInternal()
    {
        run();
        return transaction.originals().size();
    }

    @VisibleForTesting
    public boolean reduceScopeForLimitedSpace(Set<SSTableReader> nonExpiredSSTables, long expectedSize)
    {
        if (partialCompactionsAcceptable() && transaction.originals().size() > 1)
        {
            // Try again w/o the largest one.
            SSTableReader removedSSTable = cfs.getMaxSizeFile(nonExpiredSSTables);
            logger.warn("insufficient space to compact all requested files. {}MB required, {} for compaction {} - removing largest SSTable: {}",
                        (float) expectedSize / 1024 / 1024,
                        StringUtils.join(transaction.originals(), ", "),
                        transaction.opId(),
                        removedSSTable);
            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the end.
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
    @Override
    protected void runMayThrow() throws Exception
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert transaction != null;

        if (transaction.originals().isEmpty())
            return;

        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            cfs.snapshotWithoutMemtable(System.currentTimeMillis() + "-compact-" + cfs.name);

        try (CompactionController controller = getCompactionController(transaction.originals());
             CompactionOperation operation = new CompactionOperation(controller))
        {
            operation.execute();
        }
    }

    /**
     *  The compaction operation is a special case of an {@link AbstractTableOperation} and takes care of executing the
     *  actual compaction and releasing any resources when the compaction is finished.
     *  <p/>
     *  This class also extends {@link AbstractTableOperation} for reporting compaction-specific progress information.
     */
    public final class CompactionOperation implements AutoCloseable
    {
        private final CompactionController controller;
        private final CompactionStrategyManager strategyManager;
        private final Set<SSTableReader> fullyExpiredSSTables;
        private final UUID taskId;
        private final RateLimiter limiter;
        private final long start;
        private final long startTime;
        private final Set<SSTableReader> actuallyCompact;
        private final CompactionProgress progress;

        // resources that are updated and may be read by another thread
        private volatile Collection<SSTableReader> newSStables;
        private volatile long totalKeysWritten;
        private volatile long estimatedKeys;

        // resources that are updated but only read by this thread
        private boolean completed;

        // resources that need closing
        private Refs<SSTableReader> sstableRefs;
        private AbstractCompactionStrategy.ScannerList scanners;
        private CompactionIterator compactionIterator;
        private TableOperation op;
        private Closeable obsCloseable;
        private CompactionAwareWriter writer;

        /**
         * Create a new compaction operation.
         * <p/>
         *
         * @param controller the compaction controller is needed by the scanners and compaction iterator to manage options
         */
        private CompactionOperation(CompactionController controller)
        {
            this.controller = controller;

            // Note that the current compaction strategy, is not necessarily the one this task was created under.
            // This should be harmless; see comments to CFS.maybeReloadCompactionStrategy.
            this.strategyManager = cfs.getCompactionStrategyManager();
            this.fullyExpiredSSTables = controller.getFullyExpiredSSTables();
            this.taskId = transaction.opId();

            // select SSTables to compact based on available disk space.
            if (!buildCompactionCandidatesForAvailableDiskSpace(fullyExpiredSSTables))
            {
                // The set of sstables has changed (one or more were excluded due to limited available disk space).
                // We need to recompute the overlaps between sstables.
                controller.refreshOverlaps();
            }

            // sanity check: all sstables must belong to the same cfs
            assert !Iterables.any(transaction.originals(), sstable -> !sstable.descriptor.cfname.equals(cfs.name));

            this.limiter = CompactionManager.instance.getRateLimiter();
            this.start = System.nanoTime();
            this.startTime = System.currentTimeMillis();
            this.actuallyCompact = Sets.difference(transaction.originals(), fullyExpiredSSTables);
            this.progress = new Progress();
            this.newSStables = Collections.emptyList();
            this.totalKeysWritten = 0;
            this.estimatedKeys = 0;
            this.completed = false;

            Directories dirs = getDirectories();

            try
            {
                // resources that need closing, must be created last in case of exceptions and released if there is an exception in the c.tor
                this.sstableRefs = Refs.ref(actuallyCompact);
                this.scanners = strategyManager.getScanners(actuallyCompact);
                this.compactionIterator = new CompactionIterator(compactionType, scanners.scanners, controller, FBUtilities.nowInSeconds(), taskId);
                this.op = compactionIterator.getOperation();
                this.writer = getCompactionAwareWriter(cfs, dirs, transaction, actuallyCompact);
                this.obsCloseable = opObserver.onOperationStart(op);

                compObserver.setInProgress(progress);
            }
            catch (Throwable t)
            {
                t = Throwables.close(t, obsCloseable, writer, compactionIterator, scanners, sstableRefs); // ok to close even if null

                Throwables.maybeFail(t);
            }
        }

        private void execute()
        {
            try
            {
                execute0();
            }
            catch (Throwable t)
            {
                Throwables.maybeFail(onError(t));
            }
        }

        private void execute0()
        {
            // new sstables from flush can be added during a compaction, but only the compaction can remove them,
            // so in our single-threaded compaction world this is a valid way of determining if we're compacting
            // all the sstables (that existed when we started)
            if (logger.isDebugEnabled())
            {
                debugLogCompactingMessage(taskId);
            }

            long lastCheckObsoletion = start;
            double compressionRatio = scanners.getCompressionRatio();
            if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                compressionRatio = 1.0;

            long lastBytesScanned = 0;

            if (!controller.cfs.getCompactionStrategyManager().isActive())
                throw new CompactionInterruptedException(op.getProgress());

            estimatedKeys = writer.estimatedKeys();
            while (compactionIterator.hasNext())
            {
                if (op.isStopRequested())
                    throw new CompactionInterruptedException(op.getProgress());

                UnfilteredRowIterator partition = compactionIterator.next();
                if (writer.append(partition))
                    totalKeysWritten++;

                long bytesScanned = scanners.getTotalBytesScanned();

                // Rate limit the scanners, and account for compression
                if (compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio))
                    lastBytesScanned = bytesScanned;

                long now = System.nanoTime();
                if (now - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L))
                {
                    controller.maybeRefreshOverlaps();
                    lastCheckObsoletion = now;
                }
            }

            // point of no return
            newSStables = writer.finish();


            completed = true;
        }

        private Throwable onError(Throwable e)
        {
            if (e instanceof AssertionError)
            {
                // Add additional information to help operators.
                AssertionError error = new AssertionError(
                String.format("Illegal input has been generated, most probably due to corruption in the input sstables\n" +
                              "\t%s\n" +
                              "Try scrubbing the sstables by running\n" +
                              "\tnodetool scrub %s %s\n",
                              transaction.originals(),
                              cfs.keyspace.getName(),
                              cfs.getTableName()));
                error.addSuppressed(e);
                return error;
            }

            return e;
        }

        //
        // Closeable
        //

        @Override
        public void close()
        {
            Throwable err = Throwables.close((Throwable) null, obsCloseable, writer, compactionIterator, scanners, sstableRefs);

            if (transaction.isOffline())
                return;

            if (completed)
            {
                updateCompactionHistory(taskId, cfs.keyspace.getName(), cfs.getTableName(), progress);

                if (logger.isDebugEnabled())
                    debugLogCompactionSummaryInfo(taskId, start, totalKeysWritten, newSStables, progress);

                if (logger.isTraceEnabled())
                    traceLogCompactionSummaryInfo(totalKeysWritten, estimatedKeys, progress);

                cfs.getCompactionLogger().compaction(startTime, transaction.originals(), System.currentTimeMillis(), newSStables);

                // update the metrics
                cfs.metric.compactionBytesWritten.inc(progress.outputDiskSize());
            }

            Throwables.maybeFail(err);
        }


        //
        // CompactionProgress
        //

        private final class Progress implements CompactionProgress
        {
            //
            // TableOperation.Progress methods
            //

            @Override
            public Optional<String> keyspace()
            {
                return Optional.of(metadata().keyspace);
            }

            @Override
            public Optional<String> table()
            {
                return Optional.of(metadata().name);
            }

            @Override
            public TableMetadata metadata()
            {
                return cfs.metadata();
            }

            /**
             * @return the number of bytes read by the compaction iterator. For compressed or encrypted sstables,
             * this is the number of bytes processed by the iterator after decompression, so this is the current
             * position in the uncompressed sstable files.
             */
            @Override
            public long completed()
            {
                return compactionIterator.bytesRead();
            }

            /**
             * @return the initial number of bytes for input sstables. For compressed or encrypted sstables,
             * this is the number of bytes after decompression, so this is the uncompressed length of sstable files.
             */
            public long total()
            {
                return compactionIterator.totalBytes();
            }

            @Override
            public OperationType operationType()
            {
                return compactionType;
            }

            @Override
            public UUID operationId()
            {
                return taskId;
            }

            @Override
            public TableOperation.Unit unit()
            {
                return TableOperation.Unit.BYTES;
            }

            @Override
            public Set<SSTableReader> sstables()
            {
                return transaction.originals();
            }

            //
            // CompactionProgress
            //

            @Override
            @Nullable
            public AbstractCompactionStrategy strategy()
            {
                return strategy;
            }

            @Override
            public boolean isStopRequested()
            {
                return op.isStopRequested();
            }

            @Override
            public Collection<SSTableReader> inSSTables()
            {
                // TODO should we use transaction.originals() and include the expired sstables?
                // This would be more correct but all the metrics we get from CompactionIterator will not be compatible
                return actuallyCompact;
            }

            @Override
            public Collection<SSTableReader> outSSTables()
            {
                return newSStables;
            }

            @Override
            public long inputDiskSize()
            {
                return SSTableReader.getTotalBytes(actuallyCompact);
            }

            @Override
            public long inputUncompressedSize()
            {
                return compactionIterator.totalBytes();
            }

            @Override
            public long adjustedInputDiskSize()
            {
                return scanners.getTotalCompressedSize();
            }

            @Override
            public long outputDiskSize()
            {
                return SSTableReader.getTotalBytes(newSStables);
            }

            @Override
            public long uncompressedBytesRead()
            {
                return compactionIterator.bytesRead();
            }

            @Override
            public long uncompressedBytesRead(int level)
            {
                return compactionIterator.bytesRead(level);
            }

            @Override
            public long uncompressedBytesWritten()
            {
                return writer.bytesWritten();
            }

            @Override
            public long durationInNanos()
            {
                return System.nanoTime() - start;
            }

            @Override
            public long partitionsRead()
            {
                return compactionIterator.totalSourcePartitions();
            }

            @Override
            public long rowsRead()
            {
                return compactionIterator.totalSourceRows();
            }

            @Override
            public long[] partitionsHistogram()
            {
                return compactionIterator.mergedPartitionsHistogram();
            }

            @Override
            public long[] rowsHistogram()
            {
                return compactionIterator.mergedRowsHistogram();
            }

            @Override
            public double sizeRatio()
            {
                long estInputSizeBytes = adjustedInputDiskSize();
                if (estInputSizeBytes > 0)
                    return outputDiskSize() / (double) estInputSizeBytes;

                // this is a valid case, when there are no sstables to actually compact
                // the previous code would return a NaN that would be logged as zero
                return 0;
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

    public static String updateCompactionHistory(UUID taskId, String keyspaceName, String columnFamilyName, long[] mergedRowCounts, long startSize, long endSize)
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
        SystemKeyspace.updateCompactionHistory(taskId, keyspaceName, columnFamilyName, System.currentTimeMillis(), startSize, endSize, mergedRows);
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


    /**
     * Checks if we have enough disk space to execute the compaction.  Drops the largest sstable out of the Task until
     * there's enough space (in theory) to handle the compaction.  Does not take into account space that will be taken by
     * other compactions.
     *
     * @return true if there is enough disk space to execute the complete compaction, false if some sstables are excluded.
     */
    protected boolean buildCompactionCandidatesForAvailableDiskSpace(final Set<SSTableReader> fullyExpiredSSTables)
    {
        if(!cfs.isCompactionDiskSpaceCheckEnabled() && compactionType == OperationType.COMPACTION)
        {
            logger.info("Compaction space check is disabled - trying to compact all sstables");
            return true;
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

    private void debugLogCompactionSummaryInfo(UUID taskId,
                                               long start,
                                               long totalKeysWritten,
                                               Collection<SSTableReader> newSStables,
                                               CompactionProgress progress)
    {
        // log a bunch of statistics about the result and save to system table compaction_history
        long durationInNano = System.nanoTime() - start;
        long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);

        long totalMergedPartitions = 0;
        long[] mergedPartitionCounts = progress.partitionsHistogram();
        StringBuilder mergeSummary = new StringBuilder(mergedPartitionCounts.length * 10);
        mergeSummary.append('{');
        for (int i = 0; i < mergedPartitionCounts.length; i++)
        {
            long mergedPartitionCount = mergedPartitionCounts[i];
            if (mergedPartitionCount != 0)
            {
                totalMergedPartitions += mergedPartitionCount * (i + 1);
                mergeSummary.append(i).append(':').append(mergedPartitionCount).append(", ");
            }
        }
        mergeSummary.append('}');

        StringBuilder newSSTableNames = new StringBuilder(newSStables.size() * 100);
        for (SSTableReader reader : newSStables)
            newSSTableNames.append(reader.descriptor.baseFilename()).append(",");
        logger.debug("Compacted ({}) {} sstables to [{}] to level={}. {} to {} (~{}% of original) in {}ms. " +
                     "Read Throughput = {}, Write Throughput = {}, Row Throughput = ~{}/s, Partition Throughput = ~{}/s." +
                     " {} total partitions merged to {}. Partition merge counts were {}.",
                     taskId,
                     transaction.originals().size(),
                     newSSTableNames.toString(),
                     getLevel(),
                     prettyPrintMemory(progress.adjustedInputDiskSize()),
                     prettyPrintMemory(progress.outputDiskSize()),
                     (int) (progress.sizeRatio() * 100),
                     dTime,
                     prettyPrintMemoryPerSecond(progress.adjustedInputDiskSize(), durationInNano),
                     prettyPrintMemoryPerSecond(progress.outputDiskSize(), durationInNano),
                     progress.rowsRead() / (TimeUnit.NANOSECONDS.toSeconds(durationInNano) + 1),
                     (int) progress.partitionsRead() / (TimeUnit.NANOSECONDS.toSeconds(progress.durationInNanos()) + 1),
                     totalMergedPartitions,
                     totalKeysWritten,
                     mergeSummary.toString());
    }

    private void debugLogCompactingMessage(UUID taskId)
    {
        Set<SSTableReader> originals = transaction.originals();
        StringBuilder ssTableLoggerMsg = new StringBuilder(originals.size() * 100);
        ssTableLoggerMsg.append("Compacting (").append(taskId).append(')').append(" [");
        for (SSTableReader sstr : originals)
        {
            ssTableLoggerMsg.append(sstr.getFilename())
                            .append(":level=")
                            .append(sstr.getSSTableLevel())
                            .append(", ");
        }
        ssTableLoggerMsg.append("]");

        logger.debug(ssTableLoggerMsg.toString());
    }


    private static void updateCompactionHistory(UUID id,
                                                String keyspaceName,
                                                String columnFamilyName,
                                                CompactionProgress progress)
    {
        long[] mergedPartitionsHistogram = progress.partitionsHistogram();
        Map<Integer, Long> mergedPartitions = new HashMap<>(mergedPartitionsHistogram.length);
        for (int i = 0; i < mergedPartitionsHistogram.length; i++)
        {
            long count = mergedPartitionsHistogram[i];
            if (count == 0)
                continue;

            int rows = i + 1;
            mergedPartitions.put(rows, count);
        }
        SystemKeyspace.updateCompactionHistory(id,
                                               keyspaceName,
                                               columnFamilyName,
                                               System.currentTimeMillis(),
                                               progress.adjustedInputDiskSize(),
                                               progress.outputDiskSize(),
                                               mergedPartitions);
    }

    private void traceLogCompactionSummaryInfo(long totalKeysWritten,
                                               long estimatedKeys,
                                               CompactionProgress progress)
    {
        logger.trace("CF Total Bytes Compacted: {}", prettyPrintMemory(addToTotalBytesCompacted(progress.outputDiskSize())));
        logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}",
                     totalKeysWritten,
                     estimatedKeys,
                     ((double) (totalKeysWritten - estimatedKeys) / totalKeysWritten));
    }
}
