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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionInfo.Holder;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.lifecycle.WrappedLifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.ViewBuilder;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IndexSummaryRedistribution;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.Refs;

import static java.util.Collections.singleton;

/**
 * <p>
 * A singleton which manages a private executor of ongoing compactions.
 * </p>
 * Scheduling for compaction is accomplished by swapping sstables to be compacted into
 * a set via Tracker. New scheduling attempts will ignore currently compacting
 * sstables.
 */
public class CompactionManager implements CompactionManagerMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
    public static final CompactionManager instance;

    public static final int NO_GC = Integer.MIN_VALUE;
    public static final int GC_ALL = Integer.MAX_VALUE;

    // A thread local that tells us if the current thread is owned by the compaction manager. Used
    // by CounterContext to figure out if it should log a warning for invalid counter shards.
    public static final FastThreadLocal<Boolean> isCompactionManager = new FastThreadLocal<Boolean>()
    {
        @Override
        protected Boolean initialValue()
        {
            return false;
        }
    };

    static
    {
        instance = new CompactionManager();

        MBeanWrapper.instance.registerMBean(instance, MBEAN_OBJECT_NAME);
    }

    private final CompactionExecutor executor = new CompactionExecutor();
    private final CompactionExecutor validationExecutor = new ValidationExecutor();
    private final CompactionExecutor cacheCleanupExecutor = new CacheCleanupExecutor();

    private final CompactionMetrics metrics = new CompactionMetrics(executor, validationExecutor);
    @VisibleForTesting
    final Multiset<ColumnFamilyStore> compactingCF = ConcurrentHashMultiset.create();

    // used to temporarily pause non-strategy managed compactions (like index summary redistribution)
    private final AtomicInteger globalCompactionPauseCount = new AtomicInteger(0);

    private final RateLimiter compactionRateLimiter = RateLimiter.create(Double.MAX_VALUE);

    /**
     * Gets compaction rate limiter.
     * Rate unit is bytes per sec.
     *
     * @return RateLimiter with rate limit set
     */
    public RateLimiter getRateLimiter()
    {
        setRate(DatabaseDescriptor.getCompactionThroughputMbPerSec());
        return compactionRateLimiter;
    }

    /**
     * Sets the rate for the rate limiter. When compaction_throughput_mb_per_sec is 0 or node is bootstrapping,
     * this sets the rate to Double.MAX_VALUE bytes per second.
     * @param throughPutMbPerSec throughput to set in mb per second
     */
    public void setRate(final double throughPutMbPerSec)
    {
        double throughput = throughPutMbPerSec * 1024.0 * 1024.0;
        // if throughput is set to 0, throttling is disabled
        if (throughput == 0 || StorageService.instance.isBootstrapMode())
            throughput = Double.MAX_VALUE;
        if (compactionRateLimiter.getRate() != throughput)
            compactionRateLimiter.setRate(throughput);
    }

    /**
     * Call this whenever a compaction might be needed on the given columnfamily.
     * It's okay to over-call (within reason) if a call is unnecessary, it will
     * turn into a no-op in the bucketing/candidate-scan phase.
     */
    public List<Future<?>> submitBackground(final ColumnFamilyStore cfs)
    {
        if (cfs.isAutoCompactionDisabled())
        {
            logger.trace("Autocompaction is disabled");
            return Collections.emptyList();
        }

        /**
         * If a CF is currently being compacted, and there are no idle threads, submitBackground should be a no-op;
         * we can wait for the current compaction to finish and re-submit when more information is available.
         * Otherwise, we should submit at least one task to prevent starvation by busier CFs, and more if there
         * are idle threads stil. (CASSANDRA-4310)
         */
        int count = compactingCF.count(cfs);
        if (count > 0 && executor.getActiveCount() >= executor.getMaximumPoolSize())
        {
            logger.trace("Background compaction is still running for {}.{} ({} remaining). Skipping",
                         cfs.keyspace.getName(), cfs.name, count);
            return Collections.emptyList();
        }

        logger.trace("Scheduling a background task check for {}.{} with {}",
                     cfs.keyspace.getName(),
                     cfs.name,
                     cfs.getCompactionStrategyManager().getName());

        List<Future<?>> futures = new ArrayList<>(1);
        Future<?> fut = executor.submitIfRunning(new BackgroundCompactionCandidate(cfs), "background task");
        if (!fut.isCancelled())
            futures.add(fut);
        else
            compactingCF.remove(cfs);
        return futures;
    }

    public boolean isCompacting(Iterable<ColumnFamilyStore> cfses)
    {
        for (ColumnFamilyStore cfs : cfses)
            if (!cfs.getTracker().getCompacting().isEmpty())
                return true;
        return false;
    }

    /**
     * Shutdowns both compaction and validation executors, cancels running compaction / validation,
     * and waits for tasks to complete if tasks were not cancelable.
     */
    public void forceShutdown()
    {
        // shutdown executors to prevent further submission
        executor.shutdown();
        validationExecutor.shutdown();
        cacheCleanupExecutor.shutdown();

        // interrupt compactions and validations
        for (Holder compactionHolder : CompactionMetrics.getCompactions())
        {
            compactionHolder.stop();
        }

        // wait for tasks to terminate
        // compaction tasks are interrupted above, so it shuold be fairy quick
        // until not interrupted tasks to complete.
        for (ExecutorService exec : Arrays.asList(executor, validationExecutor, cacheCleanupExecutor))
        {
            try
            {
                if (!exec.awaitTermination(1, TimeUnit.MINUTES))
                    logger.warn("Failed to wait for compaction executors shutdown");
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting for tasks to be terminated", e);
            }
        }
    }

    public void finishCompactionsAndShutdown(long timeout, TimeUnit unit) throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination(timeout, unit);
    }

    // the actual sstables to compact are not determined until we run the BCT; that way, if new sstables
    // are created between task submission and execution, we execute against the most up-to-date information
    class BackgroundCompactionCandidate implements Runnable
    {
        private final ColumnFamilyStore cfs;

        BackgroundCompactionCandidate(ColumnFamilyStore cfs)
        {
            compactingCF.add(cfs);
            this.cfs = cfs;
        }

        public void run()
        {
            try
            {
                logger.trace("Checking {}.{}", cfs.keyspace.getName(), cfs.name);
                if (!cfs.isValid())
                {
                    logger.trace("Aborting compaction for dropped CF");
                    return;
                }

                CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
                AbstractCompactionTask task = strategy.getNextBackgroundTask(getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()));
                if (task == null)
                {
                    logger.trace("No tasks available");
                    return;
                }
                task.execute(metrics);
            }
            finally
            {
                compactingCF.remove(cfs);
            }
            submitBackground(cfs);
        }
    }

    /**
     * Run an operation over all sstables using jobs threads
     *
     * @param cfs the column family store to run the operation on
     * @param operation the operation to run
     * @param jobs the number of threads to use - 0 means use all available. It never uses more than concurrent_compactors threads
     * @return status of the operation
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @SuppressWarnings("resource")
    private AllSSTableOpStatus parallelAllSSTableOperation(final ColumnFamilyStore cfs, final OneSSTableOperation operation, int jobs, OperationType operationType) throws ExecutionException, InterruptedException
    {
        logger.info("Starting {} for {}.{}", operationType, cfs.keyspace.getName(), cfs.getTableName());
        List<LifecycleTransaction> transactions = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        try (LifecycleTransaction compacting = cfs.markAllCompacting(operationType))
        {
            if (compacting == null)
                return AllSSTableOpStatus.UNABLE_TO_CANCEL;

            Iterable<SSTableReader> sstables = Lists.newArrayList(operation.filterSSTables(compacting));
            if (Iterables.isEmpty(sstables))
            {
                logger.info("No sstables to {} for {}.{}", operationType.name(), cfs.keyspace.getName(), cfs.name);
                return AllSSTableOpStatus.SUCCESSFUL;
            }

            for (final SSTableReader sstable : sstables)
            {
                final LifecycleTransaction txn = compacting.split(singleton(sstable));
                transactions.add(txn);
                Callable<Object> callable = new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        operation.execute(txn);
                        return this;
                    }
                };
                Future<?> fut = executor.submitIfRunning(callable, "paralell sstable operation");
                if (!fut.isCancelled())
                    futures.add(fut);
                else
                    return AllSSTableOpStatus.ABORTED;

                if (jobs > 0 && futures.size() == jobs)
                {
                    Future<?> f = FBUtilities.waitOnFirstFuture(futures);
                    futures.remove(f);
                }
            }
            FBUtilities.waitOnFutures(futures);
            assert compacting.originals().isEmpty();
            logger.info("Finished {} for {}.{} successfully", operationType, cfs.keyspace.getName(), cfs.getTableName());
            return AllSSTableOpStatus.SUCCESSFUL;
        }
        finally
        {
            // wait on any unfinished futures to make sure we don't close an ongoing transaction
            try
            {
                FBUtilities.waitOnFutures(futures);
            }
            catch (Throwable t)
            {
               // these are handled/logged in CompactionExecutor#afterExecute
            }
            Throwable fail = Throwables.close(null, transactions);
            if (fail != null)
                logger.error("Failed to cleanup lifecycle transactions ({} for {}.{})", operationType, cfs.keyspace.getName(), cfs.getTableName(), fail);
        }
    }

    private static interface OneSSTableOperation
    {
        Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction);
        void execute(LifecycleTransaction input) throws IOException;
    }

    public enum AllSSTableOpStatus
    {
        SUCCESSFUL(0),
        ABORTED(1),
        UNABLE_TO_CANCEL(2);

        public final int statusCode;

        AllSSTableOpStatus(int statusCode)
        {
            this.statusCode = statusCode;
        }
    }

    public AllSSTableOpStatus performScrub(final ColumnFamilyStore cfs, final boolean skipCorrupted, final boolean checkData,
                                           int jobs)
    throws InterruptedException, ExecutionException
    {
        return performScrub(cfs, skipCorrupted, checkData, false, jobs);
    }

    public AllSSTableOpStatus performScrub(final ColumnFamilyStore cfs, final boolean skipCorrupted, final boolean checkData,
                                           final boolean reinsertOverflowedTTL, int jobs)
    throws InterruptedException, ExecutionException
    {
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction input)
            {
                return input.originals();
            }

            @Override
            public void execute(LifecycleTransaction input) throws IOException
            {
                scrubOne(cfs, input, skipCorrupted, checkData, reinsertOverflowedTTL);
            }
        }, jobs, OperationType.SCRUB);
    }

    public AllSSTableOpStatus performVerify(final ColumnFamilyStore cfs, final boolean extendedVerify) throws InterruptedException, ExecutionException
    {
        assert !cfs.isIndex();
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction input)
            {
                return input.originals();
            }

            @Override
            public void execute(LifecycleTransaction input) throws IOException
            {
                verifyOne(cfs, input.onlyOne(), extendedVerify);
            }
        }, 0, OperationType.VERIFY);
    }

    public AllSSTableOpStatus performSSTableRewrite(final ColumnFamilyStore cfs, final boolean excludeCurrentVersion, int jobs) throws InterruptedException, ExecutionException
    {
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
                Collections.sort(sortedSSTables, SSTableReader.sizeComparator.reversed());
                Iterator<SSTableReader> iter = sortedSSTables.iterator();
                while (iter.hasNext())
                {
                    SSTableReader sstable = iter.next();
                    if (excludeCurrentVersion && sstable.descriptor.version.equals(sstable.descriptor.getFormat().getLatestVersion()))
                    {
                        transaction.cancel(sstable);
                        iter.remove();
                    }
                }
                return sortedSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn)
            {
                AbstractCompactionTask task = cfs.getCompactionStrategyManager().getCompactionTask(txn, NO_GC, Long.MAX_VALUE);
                task.setUserDefined(true);
                task.setCompactionType(OperationType.UPGRADE_SSTABLES);
                task.execute(metrics);
            }
        }, jobs, OperationType.UPGRADE_SSTABLES);
    }

    public AllSSTableOpStatus performCleanup(final ColumnFamilyStore cfStore, int jobs) throws InterruptedException, ExecutionException
    {
        assert !cfStore.isIndex();
        Keyspace keyspace = cfStore.keyspace;
        if (!StorageService.instance.isJoined())
        {
            logger.info("Cleanup cannot run before a node has joined the ring");
            return AllSSTableOpStatus.ABORTED;
        }
        // if local ranges is empty, it means no data should remain
        final Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
        final boolean hasIndexes = cfStore.indexManager.hasIndexes();

        return parallelAllSSTableOperation(cfStore, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
                Iterator<SSTableReader> sstableIter = sortedSSTables.iterator();
                int totalSSTables = 0;
                int skippedSStables = 0;
                while (sstableIter.hasNext())
                {
                    SSTableReader sstable = sstableIter.next();
                    totalSSTables++;
                    if (!needsCleanup(sstable, ranges))
                    {
                        logger.debug("Not cleaning up {} ([{}, {}]) - no tokens outside owned ranges {}",
                                     sstable, sstable.first.getToken(), sstable.last.getToken(), ranges);
                        sstableIter.remove();
                        transaction.cancel(sstable);
                        skippedSStables++;
                    }
                }
                logger.info("Skipping cleanup for {}/{} sstables for {}.{} since they are fully contained in owned ranges ({})",
                            skippedSStables, totalSSTables, cfStore.keyspace.getName(), cfStore.getTableName(), ranges);
                sortedSSTables.sort(SSTableReader.sizeComparator);
                return sortedSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn) throws IOException
            {
                CleanupStrategy cleanupStrategy = CleanupStrategy.get(cfStore, ranges, FBUtilities.nowInSeconds());
                doCleanupOne(cfStore, txn, cleanupStrategy, ranges, hasIndexes);
            }
        }, jobs, OperationType.CLEANUP);
    }

    public AllSSTableOpStatus performGarbageCollection(final ColumnFamilyStore cfStore, TombstoneOption tombstoneOption, int jobs) throws InterruptedException, ExecutionException
    {
        assert !cfStore.isIndex();

        return parallelAllSSTableOperation(cfStore, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                Iterable<SSTableReader> originals = transaction.originals();
                if (cfStore.getCompactionStrategyManager().onlyPurgeRepairedTombstones())
                    originals = Iterables.filter(originals, SSTableReader::isRepaired);
                List<SSTableReader> sortedSSTables = Lists.newArrayList(originals);
                Collections.sort(sortedSSTables, SSTableReader.maxTimestampAscending);
                return sortedSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn) throws IOException
            {
                logger.debug("Garbage collecting {}", txn.originals());
                CompactionTask task = new CompactionTask(cfStore, txn, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()))
                {
                    @Override
                    protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
                    {
                        return new CompactionController(cfStore, toCompact, gcBefore, null, tombstoneOption);
                    }
                };
                task.setUserDefined(true);
                task.setCompactionType(OperationType.GARBAGE_COLLECT);
                task.execute(metrics);
            }
        }, jobs, OperationType.GARBAGE_COLLECT);
    }

    public AllSSTableOpStatus relocateSSTables(final ColumnFamilyStore cfs, int jobs) throws ExecutionException, InterruptedException
    {
        if (!cfs.getPartitioner().splitter().isPresent())
        {
            logger.info("Partitioner does not support splitting");
            return AllSSTableOpStatus.ABORTED;
        }
        final Collection<Range<Token>> r = StorageService.instance.getLocalRanges(cfs.keyspace.getName());

        if (r.isEmpty())
        {
            logger.info("Relocate cannot run before a node has joined the ring");
            return AllSSTableOpStatus.ABORTED;
        }

        final DiskBoundaries diskBoundaries = cfs.getDiskBoundaries();

        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                Set<SSTableReader> originals = Sets.newHashSet(transaction.originals());
                Set<SSTableReader> needsRelocation = originals.stream().filter(s -> !inCorrectLocation(s)).collect(Collectors.toSet());
                transaction.cancel(Sets.difference(originals, needsRelocation));

                Map<Integer, List<SSTableReader>> groupedByDisk = groupByDiskIndex(needsRelocation);

                int maxSize = 0;
                for (List<SSTableReader> diskSSTables : groupedByDisk.values())
                    maxSize = Math.max(maxSize, diskSSTables.size());

                List<SSTableReader> mixedSSTables = new ArrayList<>();

                for (int i = 0; i < maxSize; i++)
                    for (List<SSTableReader> diskSSTables : groupedByDisk.values())
                        if (i < diskSSTables.size())
                            mixedSSTables.add(diskSSTables.get(i));

                return mixedSSTables;
            }

            public Map<Integer, List<SSTableReader>> groupByDiskIndex(Set<SSTableReader> needsRelocation)
            {
                return needsRelocation.stream().collect(Collectors.groupingBy((s) -> diskBoundaries.getDiskIndex(s)));
            }

            private boolean inCorrectLocation(SSTableReader sstable)
            {
                if (!cfs.getPartitioner().splitter().isPresent())
                    return true;

                int diskIndex = diskBoundaries.getDiskIndex(sstable);
                PartitionPosition diskLast = diskBoundaries.positions.get(diskIndex);

                // the location we get from directoryIndex is based on the first key in the sstable
                // now we need to make sure the last key is less than the boundary as well:
                Directories.DataDirectory dataDirectory = cfs.getDirectories().getDataDirectoryForFile(sstable.descriptor);
                return diskBoundaries.directories.get(diskIndex).equals(dataDirectory) && sstable.last.compareTo(diskLast) <= 0;
            }

            @Override
            public void execute(LifecycleTransaction txn)
            {
                logger.debug("Relocating {}", txn.originals());
                AbstractCompactionTask task = cfs.getCompactionStrategyManager().getCompactionTask(txn, NO_GC, Long.MAX_VALUE);
                task.setUserDefined(true);
                task.setCompactionType(OperationType.RELOCATE);
                task.execute(metrics);
            }
        }, jobs, OperationType.RELOCATE);
    }

    /**
     * Submit anti-compactions for a collection of SSTables over a set of repaired ranges and marks corresponding SSTables
     * as repaired.
     *
     * @param cfs Column family for anti-compaction
     * @param ranges Repaired ranges to be anti-compacted into separate SSTables.
     * @param sstables {@link Refs} of SSTables within CF to anti-compact.
     * @param repairedAt Unix timestamp of when repair was completed.
     * @param parentRepairSession Corresponding repair session
     * @return Futures executing anti-compaction.
     */
    public ListenableFuture<?> submitAntiCompaction(final ColumnFamilyStore cfs,
                                          final Collection<Range<Token>> ranges,
                                          final Refs<SSTableReader> sstables,
                                          final long repairedAt,
                                          final UUID parentRepairSession)
    {
        Runnable runnable = new WrappedRunnable()
        {
            @Override
            @SuppressWarnings("resource")
            public void runMayThrow() throws Exception
            {
                LifecycleTransaction modifier = null;
                while (modifier == null)
                {
                    for (SSTableReader compactingSSTable : cfs.getTracker().getCompacting())
                        sstables.releaseIfHolds(compactingSSTable);
                    // We don't anti-compact any SSTable that has been compacted during repair as it may have been compacted
                    // with unrepaired data.
                    Set<SSTableReader> compactedSSTables = new HashSet<>();
                    for (SSTableReader sstable : sstables)
                        if (sstable.isMarkedCompacted())
                            compactedSSTables.add(sstable);
                    sstables.release(compactedSSTables);
                    modifier = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
                }
                performAnticompaction(cfs, ranges, sstables, modifier, repairedAt, parentRepairSession);
            }
        };

        ListenableFuture<?> ret = null;
        try
        {
            ret = executor.submitIfRunning(runnable, "anticompaction");
            return ret;
        }
        finally
        {
            if (ret == null || ret.isCancelled())
                sstables.release();
        }
    }

    /**
     * Make sure the {validatedForRepair} are marked for compaction before calling this.
     *
     * Caller must reference the validatedForRepair sstables (via ParentRepairSession.getActiveRepairedSSTableRefs(..)).
     *
     * NOTE: Repairs can take place on both unrepaired (incremental + full) and repaired (full) data.
     * Although anti-compaction could work on repaired sstables as well and would result in having more accurate
     * repairedAt values for these, we avoid anti-compacting already repaired sstables, as we currently don't
     * make use of any actual repairedAt value and splitting up sstables just for that is not worth it. However, we will
     * still update repairedAt if the SSTable is fully contained within the repaired ranges, as this does not require
     * anticompaction.
     *
     * @param cfs
     * @param ranges Ranges that the repair was carried out on
     * @param validatedForRepair SSTables containing the repaired ranges. Should be referenced before passing them.
     * @param txn Transaction across all SSTables that were repaired.
     * @param parentRepairSession parent repair session ID
     * @throws InterruptedException
     * @throws IOException
     */
    public void performAnticompaction(ColumnFamilyStore cfs,
                                      Collection<Range<Token>> ranges,
                                      Refs<SSTableReader> validatedForRepair,
                                      LifecycleTransaction txn,
                                      long repairedAt,
                                      UUID parentRepairSession) throws InterruptedException, IOException
    {
        logger.info("[repair #{}] Starting anticompaction for {}.{} on {}/{} sstables", parentRepairSession, cfs.keyspace.getName(), cfs.getTableName(), validatedForRepair.size(), cfs.getLiveSSTables());
        logger.trace("[repair #{}] Starting anticompaction for ranges {}", parentRepairSession, ranges);
        Set<SSTableReader> sstables = new HashSet<>(validatedForRepair);
        Set<SSTableReader> mutatedRepairStatuses = new HashSet<>(); // SSTables that were completely repaired only
        Set<SSTableReader> nonAnticompacting = new HashSet<>();

        Iterator<SSTableReader> sstableIterator = sstables.iterator();
        try
        {
            List<Range<Token>> normalizedRanges = Range.normalize(ranges);

            while (sstableIterator.hasNext())
            {
                SSTableReader sstable = sstableIterator.next();
                List<String> anticompactRanges = new ArrayList<>();
                // We don't anti-compact SSTables already marked repaired. See CASSANDRA-13153
                // and CASSANDRA-14423.
                if (sstable.isRepaired()) // We never anti-compact already repaired SSTables
                    nonAnticompacting.add(sstable);

                Bounds<Token> sstableBounds = new Bounds<>(sstable.first.getToken(), sstable.last.getToken());

                boolean shouldAnticompact = false;

                for (Range<Token> r : normalizedRanges)
                {
                    if (r.contains(sstableBounds.left) && r.contains(sstableBounds.right))
                    {
                        logger.info("[repair #{}] SSTable {} fully contained in range {}, mutating repairedAt instead of anticompacting", parentRepairSession, sstable, r);
                        sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, repairedAt);
                        sstable.reloadSSTableMetadata();
                        if (!nonAnticompacting.contains(sstable)) // don't notify if the SSTable was already repaired
                            mutatedRepairStatuses.add(sstable);
                        sstableIterator.remove();
                        shouldAnticompact = true;
                        break;
                    }
                    else if (r.intersects(sstableBounds) && !nonAnticompacting.contains(sstable))
                    {
                        anticompactRanges.add(r.toString());
                        shouldAnticompact = true;
                    }
                }

                if (!anticompactRanges.isEmpty())
                    logger.info("[repair #{}] SSTable {} ({}) will be anticompacted on range {}", parentRepairSession, sstable, sstableBounds, String.join(", ", anticompactRanges));

                if (!shouldAnticompact)
                {
                    logger.info("[repair #{}] SSTable {} ({}) not subject to anticompaction of repaired ranges {}, not touching repairedAt.", parentRepairSession, sstable, sstableBounds, normalizedRanges);
                    nonAnticompacting.add(sstable);
                    sstableIterator.remove();
                }
            }
            cfs.getTracker().notifySSTableRepairedStatusChanged(mutatedRepairStatuses);
            txn.cancel(Sets.union(nonAnticompacting, mutatedRepairStatuses));
            validatedForRepair.release(Sets.union(nonAnticompacting, mutatedRepairStatuses));
            assert txn.originals().equals(sstables);
            if (!sstables.isEmpty())
                doAntiCompaction(cfs, ranges, txn, repairedAt);
            txn.finish();
        }
        finally
        {
            validatedForRepair.release();
            txn.close();
        }

        logger.info("[repair #{}] Completed anticompaction successfully", parentRepairSession);
    }

    public void performMaximal(final ColumnFamilyStore cfStore, boolean splitOutput)
    {
        FBUtilities.waitOnFutures(submitMaximal(cfStore, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()), splitOutput));
    }

    public List<Future<?>> submitMaximal(final ColumnFamilyStore cfStore, final int gcBefore, boolean splitOutput)
    {
        // here we compute the task off the compaction executor, so having that present doesn't
        // confuse runWithCompactionsDisabled -- i.e., we don't want to deadlock ourselves, waiting
        // for ourselves to finish/acknowledge cancellation before continuing.
        final Collection<AbstractCompactionTask> tasks = cfStore.getCompactionStrategyManager().getMaximalTasks(gcBefore, splitOutput);

        if (tasks == null)
            return Collections.emptyList();

        List<Future<?>> futures = new ArrayList<>();

        int nonEmptyTasks = 0;
        for (final AbstractCompactionTask task : tasks)
        {
            if (task.transaction.originals().size() > 0)
                nonEmptyTasks++;

            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow()
                {
                    task.execute(metrics);
                }
            };

            Future<?> fut = executor.submitIfRunning(runnable, "maximal task");
            if (!fut.isCancelled())
                futures.add(fut);
        }
        if (nonEmptyTasks > 1)
            logger.info("Major compaction will not result in a single sstable - repaired and unrepaired data is kept separate and compaction runs per data_file_directory.");


        return futures;
    }

    public void forceCompactionForTokenRange(ColumnFamilyStore cfStore, Collection<Range<Token>> ranges)
    {
        Callable<Collection<AbstractCompactionTask>> taskCreator = () -> {
            Collection<SSTableReader> sstables = sstablesInBounds(cfStore, ranges);
            if (sstables == null || sstables.isEmpty())
            {
                logger.debug("No sstables found for the provided token range");
                return null;
            }
            return cfStore.getCompactionStrategyManager().getUserDefinedTasks(sstables, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()));
        };

        final Collection<AbstractCompactionTask> tasks = cfStore.runWithCompactionsDisabled(taskCreator, false, false);

        if (tasks == null)
            return;

        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                try
                {
                    for (AbstractCompactionTask task : tasks)
                        if (task != null)
                            task.execute(metrics);
                }
                finally
                {
                    FBUtilities.closeAll(tasks.stream().map(task -> task.transaction).collect(Collectors.toList()));
                }
            }
        };

        FBUtilities.waitOnFuture(executor.submitIfRunning(runnable, "force compaction for token range"));
    }

    private static Collection<SSTableReader> sstablesInBounds(ColumnFamilyStore cfs, Collection<Range<Token>> tokenRangeCollection)
    {
        final Set<SSTableReader> sstables = new HashSet<>();
        Iterable<SSTableReader> liveTables = cfs.getTracker().getView().select(SSTableSet.LIVE);
        SSTableIntervalTree tree = SSTableIntervalTree.build(liveTables);

        for (Range<Token> tokenRange : tokenRangeCollection)
        {
            Iterable<SSTableReader> ssTableReaders = View.sstablesInBounds(tokenRange.left.minKeyBound(), tokenRange.right.maxKeyBound(), tree);
            Iterables.addAll(sstables, ssTableReaders);
        }
        return sstables;
    }

    public void forceUserDefinedCompaction(String dataFiles)
    {
        String[] filenames = dataFiles.split(",");
        Multimap<ColumnFamilyStore, Descriptor> descriptors = ArrayListMultimap.create();

        for (String filename : filenames)
        {
            // extract keyspace and columnfamily name from filename
            Descriptor desc = Descriptor.fromFilename(filename.trim());
            if (Schema.instance.getCFMetaData(desc) == null)
            {
                logger.warn("Schema does not exist for file {}. Skipping.", filename);
                continue;
            }
            // group by keyspace/columnfamily
            ColumnFamilyStore cfs = Keyspace.open(desc.ksname).getColumnFamilyStore(desc.cfname);
            descriptors.put(cfs, cfs.getDirectories().find(new File(filename.trim()).getName()));
        }

        List<Future<?>> futures = new ArrayList<>();
        int nowInSec = FBUtilities.nowInSeconds();
        for (ColumnFamilyStore cfs : descriptors.keySet())
            futures.add(submitUserDefined(cfs, descriptors.get(cfs), getDefaultGcBefore(cfs, nowInSec)));
        FBUtilities.waitOnFutures(futures);
    }

    public void forceUserDefinedCleanup(String dataFiles)
    {
        String[] filenames = dataFiles.split(",");
        HashMap<ColumnFamilyStore, Descriptor> descriptors = Maps.newHashMap();

        for (String filename : filenames)
        {
            // extract keyspace and columnfamily name from filename
            Descriptor desc = Descriptor.fromFilename(filename.trim());
            if (Schema.instance.getCFMetaData(desc) == null)
            {
                logger.warn("Schema does not exist for file {}. Skipping.", filename);
                continue;
            }
            // group by keyspace/columnfamily
            ColumnFamilyStore cfs = Keyspace.open(desc.ksname).getColumnFamilyStore(desc.cfname);
            desc = cfs.getDirectories().find(new File(filename.trim()).getName());
            if (desc != null)
                descriptors.put(cfs, desc);
        }

        if (!StorageService.instance.isJoined())
        {
            logger.error("Cleanup cannot run before a node has joined the ring");
            return;
        }

        for (Map.Entry<ColumnFamilyStore,Descriptor> entry : descriptors.entrySet())
        {
            ColumnFamilyStore cfs = entry.getKey();
            Keyspace keyspace = cfs.keyspace;
            Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
            boolean hasIndexes = cfs.indexManager.hasIndexes();
            SSTableReader sstable = lookupSSTable(cfs, entry.getValue());

            if (sstable == null)
            {
                logger.warn("Will not clean {}, it is not an active sstable", entry.getValue());
            }
            else
            {
                CleanupStrategy cleanupStrategy = CleanupStrategy.get(cfs, ranges, FBUtilities.nowInSeconds());
                try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.CLEANUP))
                {
                    doCleanupOne(cfs, txn, cleanupStrategy, ranges, hasIndexes);
                }
                catch (IOException e)
                {
                    logger.error("forceUserDefinedCleanup failed: {}", e.getLocalizedMessage());
                }
            }
        }
    }


    public Future<?> submitUserDefined(final ColumnFamilyStore cfs, final Collection<Descriptor> dataFiles, final int gcBefore)
    {
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                // look up the sstables now that we're on the compaction executor, so we don't try to re-compact
                // something that was already being compacted earlier.
                Collection<SSTableReader> sstables = new ArrayList<>(dataFiles.size());
                for (Descriptor desc : dataFiles)
                {
                    // inefficient but not in a performance sensitive path
                    SSTableReader sstable = lookupSSTable(cfs, desc);
                    if (sstable == null)
                    {
                        logger.info("Will not compact {}: it is not an active sstable", desc);
                    }
                    else
                    {
                        sstables.add(sstable);
                    }
                }

                if (sstables.isEmpty())
                {
                    logger.info("No files to compact for user defined compaction");
                }
                else
                {
                    List<AbstractCompactionTask> tasks = cfs.getCompactionStrategyManager().getUserDefinedTasks(sstables, gcBefore);
                    try
                    {
                        for (AbstractCompactionTask task : tasks)
                        {
                            if (task != null)
                                task.execute(metrics);
                        }
                    }
                    finally
                    {
                        FBUtilities.closeAll(tasks.stream().map(task -> task.transaction).collect(Collectors.toList()));
                    }
                }
            }
        };

        return executor.submitIfRunning(runnable, "user defined task");
    }

    // This acquire a reference on the sstable
    // This is not efficient, do not use in any critical path
    private SSTableReader lookupSSTable(final ColumnFamilyStore cfs, Descriptor descriptor)
    {
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
        {
            if (sstable.descriptor.equals(descriptor))
                return sstable;
        }
        return null;
    }

    /**
     * Does not mutate data, so is not scheduled.
     */
    public Future<?> submitValidation(final ColumnFamilyStore cfStore, final Validator validator)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                try
                {
                    doValidationCompaction(cfStore, validator);
                }
                catch (Throwable e)
                {
                    // we need to inform the remote end of our failure, otherwise it will hang on repair forever
                    validator.fail();
                    throw e;
                }
                return this;
            }
        };

        return validationExecutor.submitIfRunning(callable, "validation");
    }

    /* Used in tests. */
    public void disableAutoCompaction()
    {
        for (String ksname : Schema.instance.getNonSystemKeyspaces())
        {
            for (ColumnFamilyStore cfs : Keyspace.open(ksname).getColumnFamilyStores())
                cfs.disableAutoCompaction();
        }
    }

    private void scrubOne(ColumnFamilyStore cfs, LifecycleTransaction modifier, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL) throws IOException
    {
        CompactionInfo.Holder scrubInfo = null;

        try (Scrubber scrubber = new Scrubber(cfs, modifier, skipCorrupted, checkData, reinsertOverflowedTTL))
        {
            scrubInfo = scrubber.getScrubInfo();
            metrics.beginCompaction(scrubInfo);
            scrubber.scrub();
        }
        finally
        {
            if (scrubInfo != null)
                metrics.finishCompaction(scrubInfo);
        }
    }

    private void verifyOne(ColumnFamilyStore cfs, SSTableReader sstable, boolean extendedVerify) throws IOException
    {
        CompactionInfo.Holder verifyInfo = null;

        try (Verifier verifier = new Verifier(cfs, sstable, false))
        {
            verifyInfo = verifier.getVerifyInfo();
            metrics.beginCompaction(verifyInfo);
            verifier.verify(extendedVerify);
        }
        finally
        {
            if (verifyInfo != null)
                metrics.finishCompaction(verifyInfo);
        }
    }

    /**
     * Determines if a cleanup would actually remove any data in this SSTable based
     * on a set of owned ranges.
     */
    @VisibleForTesting
    public static boolean needsCleanup(SSTableReader sstable, Collection<Range<Token>> ownedRanges)
    {
        if (ownedRanges.isEmpty())
        {
            return true; // all data will be cleaned
        }

        // unwrap and sort the ranges by LHS token
        List<Range<Token>> sortedRanges = Range.normalize(ownedRanges);

        // see if there are any keys LTE the token for the start of the first range
        // (token range ownership is exclusive on the LHS.)
        Range<Token> firstRange = sortedRanges.get(0);
        if (sstable.first.getToken().compareTo(firstRange.left) <= 0)
            return true;

        // then, iterate over all owned ranges and see if the next key beyond the end of the owned
        // range falls before the start of the next range
        for (int i = 0; i < sortedRanges.size(); i++)
        {
            Range<Token> range = sortedRanges.get(i);
            if (range.right.isMinimum())
            {
                // we split a wrapping range and this is the second half.
                // there can't be any keys beyond this (and this is the last range)
                return false;
            }

            DecoratedKey firstBeyondRange = sstable.firstKeyBeyond(range.right.maxKeyBound());
            if (firstBeyondRange == null)
            {
                // we ran off the end of the sstable looking for the next key; we don't need to check any more ranges
                return false;
            }

            if (i == (sortedRanges.size() - 1))
            {
                // we're at the last range and we found a key beyond the end of the range
                return true;
            }

            Range<Token> nextRange = sortedRanges.get(i + 1);
            if (firstBeyondRange.getToken().compareTo(nextRange.left) <= 0)
            {
                // we found a key in between the owned ranges
                return true;
            }
        }

        return false;
    }

    /**
     * This function goes over a file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    private void doCleanupOne(final ColumnFamilyStore cfs, LifecycleTransaction txn, CleanupStrategy cleanupStrategy, Collection<Range<Token>> ranges, boolean hasIndexes) throws IOException
    {
        assert !cfs.isIndex();

        SSTableReader sstable = txn.onlyOne();

        // if ranges is empty and no index, entire sstable is discarded
        if (!hasIndexes && !new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges))
        {
            txn.obsoleteOriginals();
            txn.finish();
            logger.info("SSTable {} ([{}, {}]) does not intersect the owned ranges ({}), dropping it", sstable, sstable.first.getToken(), sstable.last.getToken(), ranges);
            return;
        }

        long start = System.nanoTime();

        long totalkeysWritten = 0;

        long expectedBloomFilterSize = Math.max(cfs.metadata.params.minIndexInterval,
                                               SSTableReader.getApproximateKeyCount(txn.originals()));
        if (logger.isTraceEnabled())
            logger.trace("Expected bloom filter size : {}", expectedBloomFilterSize);

        logger.info("Cleaning up {}", sstable);

        File compactionFileLocation = sstable.descriptor.directory;
        RateLimiter limiter = getRateLimiter();
        double compressionRatio = sstable.getCompressionRatio();
        if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
            compressionRatio = 1.0;

        List<SSTableReader> finished;

        int nowInSec = FBUtilities.nowInSeconds();
        try (SSTableRewriter writer = SSTableRewriter.construct(cfs, txn, false, sstable.maxDataAge);
             ISSTableScanner scanner = cleanupStrategy.getScanner(sstable, null);
             CompactionController controller = new CompactionController(cfs, txn.originals(), getDefaultGcBefore(cfs, nowInSec));
             Refs<SSTableReader> refs = Refs.ref(Collections.singleton(sstable));
             CompactionIterator ci = new CompactionIterator(OperationType.CLEANUP, Collections.singletonList(scanner), controller, nowInSec, UUIDGen.getTimeUUID(), metrics))
        {
            writer.switchWriter(createWriter(cfs, compactionFileLocation, expectedBloomFilterSize, sstable.getSSTableMetadata().repairedAt, sstable, txn));
            long lastBytesScanned = 0;


            while (ci.hasNext())
            {
                if (ci.isStopRequested())
                    throw new CompactionInterruptedException(ci.getCompactionInfo());

                try (UnfilteredRowIterator partition = ci.next();
                     UnfilteredRowIterator notCleaned = cleanupStrategy.cleanup(partition))
                {
                    if (notCleaned == null)
                        continue;

                    if (writer.append(notCleaned) != null)
                        totalkeysWritten++;

                    long bytesScanned = scanner.getBytesScanned();

                    compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);

                    lastBytesScanned = bytesScanned;
                }
            }

            // flush to ensure we don't lose the tombstones on a restart, since they are not commitlog'd
            cfs.indexManager.flushAllIndexesBlocking();

            finished = writer.finish();
        }

        if (!finished.isEmpty())
        {
            String format = "Cleaned up to %s.  %s to %s (~%d%% of original) for %,d keys.  Time: %,dms.";
            long dTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            long startsize = sstable.onDiskLength();
            long endsize = 0;
            for (SSTableReader newSstable : finished)
                endsize += newSstable.onDiskLength();
            double ratio = (double) endsize / (double) startsize;
            logger.info(String.format(format, finished.get(0).getFilename(), FBUtilities.prettyPrintMemory(startsize),
                                      FBUtilities.prettyPrintMemory(endsize), (int) (ratio * 100), totalkeysWritten, dTime));
        }

    }

    static void compactionRateLimiterAcquire(RateLimiter limiter, long bytesScanned, long lastBytesScanned, double compressionRatio)
    {
        long lengthRead = (long) ((bytesScanned - lastBytesScanned) * compressionRatio) + 1;
        while (lengthRead >= Integer.MAX_VALUE)
        {
            limiter.acquire(Integer.MAX_VALUE);
            lengthRead -= Integer.MAX_VALUE;
        }
        if (lengthRead > 0)
        {
            limiter.acquire((int) lengthRead);
        }
    }

    private static abstract class CleanupStrategy
    {
        protected final Collection<Range<Token>> ranges;
        protected final int nowInSec;

        protected CleanupStrategy(Collection<Range<Token>> ranges, int nowInSec)
        {
            this.ranges = ranges;
            this.nowInSec = nowInSec;
        }

        public static CleanupStrategy get(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, int nowInSec)
        {
            return cfs.indexManager.hasIndexes()
                 ? new Full(cfs, ranges, nowInSec)
                 : new Bounded(cfs, ranges, nowInSec);
        }

        public abstract ISSTableScanner getScanner(SSTableReader sstable, RateLimiter limiter);
        public abstract UnfilteredRowIterator cleanup(UnfilteredRowIterator partition);

        private static final class Bounded extends CleanupStrategy
        {
            public Bounded(final ColumnFamilyStore cfs, Collection<Range<Token>> ranges, int nowInSec)
            {
                super(ranges, nowInSec);
                instance.cacheCleanupExecutor.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        cfs.cleanupCache();
                    }
                });
            }

            @Override
            public ISSTableScanner getScanner(SSTableReader sstable, RateLimiter limiter)
            {
                return sstable.getScanner(ranges, limiter);
            }

            @Override
            public UnfilteredRowIterator cleanup(UnfilteredRowIterator partition)
            {
                return partition;
            }
        }

        private static final class Full extends CleanupStrategy
        {
            private final ColumnFamilyStore cfs;

            public Full(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, int nowInSec)
            {
                super(ranges, nowInSec);
                this.cfs = cfs;
            }

            @Override
            public ISSTableScanner getScanner(SSTableReader sstable, RateLimiter limiter)
            {
                return sstable.getScanner(limiter);
            }

            @Override
            public UnfilteredRowIterator cleanup(UnfilteredRowIterator partition)
            {
                if (Range.isInRanges(partition.partitionKey().getToken(), ranges))
                    return partition;

                cfs.invalidateCachedPartition(partition.partitionKey());

                cfs.indexManager.deletePartition(partition, nowInSec);
                return null;
            }
        }
    }

    public static SSTableWriter createWriter(ColumnFamilyStore cfs,
                                             File compactionFileLocation,
                                             long expectedBloomFilterSize,
                                             long repairedAt,
                                             SSTableReader sstable,
                                             LifecycleTransaction txn)
    {
        FileUtils.createDirectory(compactionFileLocation);
        SerializationHeader header = sstable.header;
        if (header == null)
            header = SerializationHeader.make(sstable.metadata, Collections.singleton(sstable));

        return SSTableWriter.create(cfs.metadata,
                                    Descriptor.fromFilename(cfs.getSSTablePath(compactionFileLocation)),
                                    expectedBloomFilterSize,
                                    repairedAt,
                                    sstable.getSSTableLevel(),
                                    header,
                                    cfs.indexManager.listIndexes(),
                                    txn);
    }

    public static SSTableWriter createWriterForAntiCompaction(ColumnFamilyStore cfs,
                                                              File compactionFileLocation,
                                                              int expectedBloomFilterSize,
                                                              long repairedAt,
                                                              Collection<SSTableReader> sstables,
                                                              ILifecycleTransaction txn)
    {
        FileUtils.createDirectory(compactionFileLocation);
        int minLevel = Integer.MAX_VALUE;
        // if all sstables have the same level, we can compact them together without creating overlap during anticompaction
        // note that we only anticompact from unrepaired sstables, which is not leveled, but we still keep original level
        // after first migration to be able to drop the sstables back in their original place in the repaired sstable manifest
        for (SSTableReader sstable : sstables)
        {
            if (minLevel == Integer.MAX_VALUE)
                minLevel = sstable.getSSTableLevel();

            if (minLevel != sstable.getSSTableLevel())
            {
                minLevel = 0;
                break;
            }
        }
        return SSTableWriter.create(Descriptor.fromFilename(cfs.getSSTablePath(compactionFileLocation)),
                                    (long) expectedBloomFilterSize,
                                    repairedAt,
                                    cfs.metadata,
                                    new MetadataCollector(sstables, cfs.metadata.comparator, minLevel),
                                    SerializationHeader.make(cfs.metadata, sstables),
                                    cfs.indexManager.listIndexes(),
                                    txn);
    }


    /**
     * Performs a readonly "compaction" of all sstables in order to validate complete rows,
     * but without writing the merge result
     */
    @SuppressWarnings("resource")
    private void doValidationCompaction(ColumnFamilyStore cfs, Validator validator) throws IOException
    {
        // this isn't meant to be race-proof, because it's not -- it won't cause bugs for a CFS to be dropped
        // mid-validation, or to attempt to validate a droped CFS.  this is just a best effort to avoid useless work,
        // particularly in the scenario where a validation is submitted before the drop, and there are compactions
        // started prior to the drop keeping some sstables alive.  Since validationCompaction can run
        // concurrently with other compactions, it would otherwise go ahead and scan those again.
        if (!cfs.isValid())
            return;

        Refs<SSTableReader> sstables = null;
        try
        {

            int gcBefore;
            int nowInSec = FBUtilities.nowInSeconds();
            UUID parentRepairSessionId = validator.desc.parentSessionId;
            String snapshotName;
            boolean isGlobalSnapshotValidation = cfs.snapshotExists(parentRepairSessionId.toString());
            if (isGlobalSnapshotValidation)
                snapshotName = parentRepairSessionId.toString();
            else
                snapshotName = validator.desc.sessionId.toString();
            boolean isSnapshotValidation = cfs.snapshotExists(snapshotName);

            if (isSnapshotValidation)
            {
                // If there is a snapshot created for the session then read from there.
                // note that we populate the parent repair session when creating the snapshot, meaning the sstables in the snapshot are the ones we
                // are supposed to validate.
                sstables = cfs.getSnapshotSSTableReader(snapshotName);


                // Computing gcbefore based on the current time wouldn't be very good because we know each replica will execute
                // this at a different time (that's the whole purpose of repair with snaphsot). So instead we take the creation
                // time of the snapshot, which should give us roughtly the same time on each replica (roughtly being in that case
                // 'as good as in the non-snapshot' case)
                gcBefore = cfs.gcBefore((int)(cfs.getSnapshotCreationTime(snapshotName) / 1000));
            }
            else
            {
                // flush first so everyone is validating data that is as similar as possible
                StorageService.instance.forceKeyspaceFlush(cfs.keyspace.getName(), cfs.name);
                sstables = getSSTablesToValidate(cfs, validator);
                if (sstables == null)
                    return; // this means the parent repair session was removed - the repair session failed on another node and we removed it
                if (validator.gcBefore > 0)
                    gcBefore = validator.gcBefore;
                else
                    gcBefore = getDefaultGcBefore(cfs, nowInSec);
            }

            // Create Merkle trees suitable to hold estimated partitions for the given ranges.
            // We blindly assume that a partition is evenly distributed on all sstables for now.
            MerkleTrees tree = createMerkleTrees(sstables, validator.desc.ranges, cfs);
            RateLimiter limiter = getRateLimiter();
            long start = System.nanoTime();
            try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables, validator.desc.ranges);
                 ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore);
                 CompactionIterator ci = new ValidationCompactionIterator(scanners.scanners, controller, nowInSec, metrics))
            {
                double compressionRatio = scanners.getCompressionRatio();
                if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                    compressionRatio = 1.0;

                long lastBytesScanned = 0;

                // validate the CF as we iterate over it
                validator.prepare(cfs, tree);
                while (ci.hasNext())
                {
                    if (ci.isStopRequested())
                        throw new CompactionInterruptedException(ci.getCompactionInfo());
                    try (UnfilteredRowIterator partition = ci.next())
                    {
                        validator.add(partition);
                    }

                    long bytesScanned = scanners.getTotalBytesScanned();
                    compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);
                    lastBytesScanned = bytesScanned;

                }
                validator.complete();
            }
            finally
            {
                if (isSnapshotValidation && !isGlobalSnapshotValidation)
                {
                    // we can only clear the snapshot if we are not doing a global snapshot validation (we then clear it once anticompaction
                    // is done).
                    cfs.clearSnapshot(snapshotName);
                }
            }

            if (logger.isDebugEnabled())
            {
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                logger.debug("Validation finished in {} msec, for {}",
                             duration,
                             validator.desc);
            }
        }
        finally
        {
            if (sstables != null)
                sstables.release();
        }
    }

    private static MerkleTrees createMerkleTrees(Iterable<SSTableReader> sstables, Collection<Range<Token>> ranges, ColumnFamilyStore cfs)
    {
        MerkleTrees tree = new MerkleTrees(cfs.getPartitioner());
        long allPartitions = 0;
        Map<Range<Token>, Long> rangePartitionCounts = Maps.newHashMapWithExpectedSize(ranges.size());
        for (Range<Token> range : ranges)
        {
            long numPartitions = 0;
            for (SSTableReader sstable : sstables)
                numPartitions += sstable.estimatedKeysForRanges(Collections.singleton(range));
            rangePartitionCounts.put(range, numPartitions);
            allPartitions += numPartitions;
        }

        for (Range<Token> range : ranges)
        {
            long numPartitions = rangePartitionCounts.get(range);
            double rangeOwningRatio = allPartitions > 0 ? (double)numPartitions / allPartitions : 0;
            // determine max tree depth proportional to range size to avoid blowing up memory with multiple tress,
            // capping at a configurable depth (default 18) to prevent large tree (CASSANDRA-11390, CASSANDRA-14096)
            int maxDepth = rangeOwningRatio > 0
                           ? (int) Math.floor(Math.max(0.0, DatabaseDescriptor.getRepairSessionMaxTreeDepth() -
                                                            Math.log(1 / rangeOwningRatio) / Math.log(2)))
                           : 0;

            // determine tree depth from number of partitions, capping at max tree depth (CASSANDRA-5263)
            int depth = numPartitions > 0 ? (int) Math.min(Math.ceil(Math.log(numPartitions) / Math.log(2)), maxDepth) : 0;
            tree.addMerkleTree((int) Math.pow(2, depth), range);
        }
        if (logger.isDebugEnabled())
        {
            // MT serialize may take time
            logger.debug("Created {} merkle trees with merkle trees size {}, {} partitions, {} bytes", tree.ranges().size(), tree.size(), allPartitions, MerkleTrees.serializer.serializedSize(tree, 0));
        }

        return tree;
    }

    private synchronized Refs<SSTableReader> getSSTablesToValidate(ColumnFamilyStore cfs, Validator validator)
    {
        Refs<SSTableReader> sstables;

        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(validator.desc.parentSessionId);
        if (prs == null)
            return null;
        Set<SSTableReader> sstablesToValidate = new HashSet<>();
        if (prs.isGlobal)
            prs.markSSTablesRepairing(cfs.metadata.cfId, validator.desc.parentSessionId);
        // note that we always grab all existing sstables for this - if we were to just grab the ones that
        // were marked as repairing, we would miss any ranges that were compacted away and this would cause us to overstream
        try (ColumnFamilyStore.RefViewFragment sstableCandidates = cfs.selectAndReference(View.select(SSTableSet.CANONICAL, (s) -> !prs.isIncremental || !s.isRepaired())))
        {
            for (SSTableReader sstable : sstableCandidates.sstables)
            {
                if (new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(validator.desc.ranges))
                {
                    sstablesToValidate.add(sstable);
                }
            }

            sstables = Refs.tryRef(sstablesToValidate);
            if (sstables == null)
            {
                logger.error("Could not reference sstables");
                throw new RuntimeException("Could not reference sstables");
            }
        }

        return sstables;
    }

    /**
     * Splits up an sstable into two new sstables. The first of the new tables will store repaired ranges, the second
     * will store the non-repaired ranges. Once anticompation is completed, the original sstable is marked as compacted
     * and subsequently deleted.
     * @param cfs
     * @param repaired a transaction over the repaired sstables to anticompacy
     * @param ranges Repaired ranges to be placed into one of the new sstables. The repaired table will be tracked via
     * the {@link org.apache.cassandra.io.sstable.metadata.StatsMetadata#repairedAt} field.
     */
    private void doAntiCompaction(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, LifecycleTransaction repaired, long repairedAt)
    {
        int numAnticompact = repaired.originals().size();
        logger.info("Performing anticompaction on {} sstables", numAnticompact);

        //Group SSTables
        Collection<Collection<SSTableReader>> groupedSSTables = cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(repaired.originals());
        // iterate over sstables to check if the repaired / unrepaired ranges intersect them.
        int antiCompactedSSTableCount = 0;
        for (Collection<SSTableReader> sstableGroup : groupedSSTables)
        {
            try (LifecycleTransaction txn = repaired.split(sstableGroup))
            {
                int antiCompacted = antiCompactGroup(cfs, ranges, txn, repairedAt);
                antiCompactedSSTableCount += antiCompacted;
            }
        }

        String format = "Anticompaction completed successfully, anticompacted from {} to {} sstable(s).";
        logger.info(format, numAnticompact, antiCompactedSSTableCount);
    }


    @VisibleForTesting
    int antiCompactGroup(ColumnFamilyStore cfs, Collection<Range<Token>> ranges,
                         LifecycleTransaction anticompactionGroup, long repairedAt)
    {
        long groupMaxDataAge = -1;

        for (Iterator<SSTableReader> i = anticompactionGroup.originals().iterator(); i.hasNext();)
        {
            SSTableReader sstable = i.next();
            if (groupMaxDataAge < sstable.maxDataAge)
                groupMaxDataAge = sstable.maxDataAge;
        }

        if (anticompactionGroup.originals().size() == 0)
        {
            logger.info("No valid anticompactions for this group, All sstables were compacted and are no longer available");
            return 0;
        }

        logger.info("Anticompacting {}", anticompactionGroup);
        Set<SSTableReader> sstableAsSet = anticompactionGroup.originals();

        File destination = cfs.getDirectories().getWriteableLocationAsFile(cfs.getExpectedCompactedFileSize(sstableAsSet, OperationType.ANTICOMPACTION));
        long repairedKeyCount = 0;
        long unrepairedKeyCount = 0;
        int nowInSec = FBUtilities.nowInSeconds();

        /**
         * HACK WARNING
         *
         * We have multiple writers operating over the same Transaction, producing different sets of sstables that all
         * logically replace the transaction's originals.  The SSTableRewriter assumes it has exclusive control over
         * the transaction state, and this will lead to temporarily inconsistent sstable/tracker state if we do not
         * take special measures to avoid it.
         *
         * Specifically, if a number of rewriter have prepareToCommit() invoked in sequence, then two problematic things happen:
         *   1. The obsoleteOriginals() call of the first rewriter immediately remove the originals from the tracker, despite
         *      their having been only partially replaced.  To avoid this, we must either avoid obsoleteOriginals() or checkpoint()
         *   2. The LifecycleTransaction may only have prepareToCommit() invoked once, and this will checkpoint() also.
         *
         * Similarly commit() would finalise partially complete on-disk state.
         *
         * To avoid these problems, we introduce a SharedTxn that proxies all calls onto the underlying transaction
         * except prepareToCommit(), checkpoint(), obsoleteOriginals(), and commit().
         * We then invoke these methods directly once each of the rewriter has updated the transaction
         * with their share of replacements.
         *
         * Note that for the same essential reason we also explicitly disable early open.
         * By noop-ing checkpoint we avoid any of the problems with early open, but by continuing to explicitly
         * disable it we also prevent any of the extra associated work from being performed.
         */
        class SharedTxn extends WrappedLifecycleTransaction
        {
            public SharedTxn(ILifecycleTransaction delegate) { super(delegate); }
            public Throwable commit(Throwable accumulate) { return accumulate; }
            public void prepareToCommit() {}
            public void checkpoint() {}
            public void obsoleteOriginals() {}
            public void close() {}
        }

        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
        try (SharedTxn sharedTxn = new SharedTxn(anticompactionGroup);
             SSTableRewriter repairedSSTableWriter = SSTableRewriter.constructWithoutEarlyOpening(sharedTxn, false, groupMaxDataAge);
             SSTableRewriter unRepairedSSTableWriter = SSTableRewriter.constructWithoutEarlyOpening(sharedTxn, false, groupMaxDataAge);
             AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(anticompactionGroup.originals());
             CompactionController controller = new CompactionController(cfs, sstableAsSet, getDefaultGcBefore(cfs, nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.ANTICOMPACTION, scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID(), metrics))
        {
            int expectedBloomFilterSize = Math.max(cfs.metadata.params.minIndexInterval, (int)(SSTableReader.getApproximateKeyCount(sstableAsSet)));

            repairedSSTableWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, repairedAt, sstableAsSet, sharedTxn));
            unRepairedSSTableWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, ActiveRepairService.UNREPAIRED_SSTABLE, sstableAsSet, sharedTxn));
            Range.OrderedRangeContainmentChecker containmentChecker = new Range.OrderedRangeContainmentChecker(ranges);
            while (ci.hasNext())
            {
                try (UnfilteredRowIterator partition = ci.next())
                {
                    // if current range from sstable is repaired, save it into the new repaired sstable
                    if (containmentChecker.contains(partition.partitionKey().getToken()))
                    {
                        repairedSSTableWriter.append(partition);
                        repairedKeyCount++;
                    }
                    // otherwise save into the new 'non-repaired' table
                    else
                    {
                        unRepairedSSTableWriter.append(partition);
                        unrepairedKeyCount++;
                    }
                }
            }

            List<SSTableReader> anticompactedSSTables = new ArrayList<>();

            repairedSSTableWriter.setRepairedAt(repairedAt).prepareToCommit();
            unRepairedSSTableWriter.prepareToCommit();
            anticompactionGroup.checkpoint();
            anticompactionGroup.obsoleteOriginals();
            anticompactionGroup.prepareToCommit();
            anticompactedSSTables.addAll(repairedSSTableWriter.finished());
            anticompactedSSTables.addAll(unRepairedSSTableWriter.finished());
            repairedSSTableWriter.commit();
            unRepairedSSTableWriter.commit();
            Throwables.maybeFail(anticompactionGroup.commit(null));

            logger.trace("Repaired {} keys out of {} for {}/{} in {}", repairedKeyCount,
                                                                       repairedKeyCount + unrepairedKeyCount,
                                                                       cfs.keyspace.getName(),
                                                                       cfs.getColumnFamilyName(),
                                                                       anticompactionGroup);
            return anticompactedSSTables.size();
        }
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.error("Error anticompacting " + anticompactionGroup, e);
        }
        return 0;
    }

    /**
     * Is not scheduled, because it is performing disjoint work from sstable compaction.
     */
    public Future<?> submitIndexBuild(final SecondaryIndexBuilder builder)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                metrics.beginCompaction(builder);
                try
                {
                    builder.build();
                }
                finally
                {
                    metrics.finishCompaction(builder);
                }
            }
        };

        return executor.submitIfRunning(runnable, "index build");
    }

    public Future<?> submitCacheWrite(final AutoSavingCache.Writer writer)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (!AutoSavingCache.flushInProgress.add(writer.cacheType()))
                {
                    logger.trace("Cache flushing was already in progress: skipping {}", writer.getCompactionInfo());
                    return;
                }
                try
                {
                    metrics.beginCompaction(writer);
                    try
                    {
                        writer.saveCache();
                    }
                    finally
                    {
                        metrics.finishCompaction(writer);
                    }
                }
                finally
                {
                    AutoSavingCache.flushInProgress.remove(writer.cacheType());
                }
            }
        };

        return executor.submitIfRunning(runnable, "cache write");
    }

    public List<SSTableReader> runIndexSummaryRedistribution(IndexSummaryRedistribution redistribution) throws IOException
    {
        metrics.beginCompaction(redistribution);

        try
        {
            return redistribution.redistributeSummaries();
        }
        finally
        {
            metrics.finishCompaction(redistribution);
        }
    }

    public static int getDefaultGcBefore(ColumnFamilyStore cfs, int nowInSec)
    {
        // 2ndary indexes have ExpiringColumns too, so we need to purge tombstones deleted before now. We do not need to
        // add any GcGrace however since 2ndary indexes are local to a node.
        return cfs.isIndex() ? nowInSec : cfs.gcBefore(nowInSec);
    }

    private static class ValidationCompactionIterator extends CompactionIterator
    {
        public ValidationCompactionIterator(List<ISSTableScanner> scanners, ValidationCompactionController controller, int nowInSec, CompactionMetrics metrics)
        {
            super(OperationType.VALIDATION, scanners, controller, nowInSec, UUIDGen.getTimeUUID(), metrics);
        }
    }

    /*
     * Controller for validation compaction that always purges.
     * Note that we should not call cfs.getOverlappingSSTables on the provided
     * sstables because those sstables are not guaranteed to be active sstables
     * (since we can run repair on a snapshot).
     */
    private static class ValidationCompactionController extends CompactionController
    {
        public ValidationCompactionController(ColumnFamilyStore cfs, int gcBefore)
        {
            super(cfs, gcBefore);
        }

        @Override
        public Predicate<Long> getPurgeEvaluator(DecoratedKey key)
        {
            /*
             * The main reason we always purge is that including gcable tombstone would mean that the
             * repair digest will depends on the scheduling of compaction on the different nodes. This
             * is still not perfect because gcbefore is currently dependend on the current time at which
             * the validation compaction start, which while not too bad for normal repair is broken for
             * repair on snapshots. A better solution would be to agree on a gcbefore that all node would
             * use, and we'll do that with CASSANDRA-4932.
             * Note validation compaction includes all sstables, so we don't have the problem of purging
             * a tombstone that could shadow a column in another sstable, but this is doubly not a concern
             * since validation compaction is read-only.
             */
            return time -> true;
        }
    }

    public Future<?> submitViewBuilder(final ViewBuilder builder)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                metrics.beginCompaction(builder);
                try
                {
                    builder.run();
                }
                finally
                {
                    metrics.finishCompaction(builder);
                }
            }
        };
        if (executor.isShutdown())
        {
            logger.info("Compaction executor has shut down, not submitting index build");
            return null;
        }

        return executor.submit(runnable);
    }
    public int getActiveCompactions()
    {
        return CompactionMetrics.getCompactions().size();
    }

    static class CompactionExecutor extends JMXEnabledThreadPoolExecutor
    {
        protected CompactionExecutor(int minThreads, int maxThreads, String name, BlockingQueue<Runnable> queue)
        {
            super(minThreads, maxThreads, 60, TimeUnit.SECONDS, queue, new NamedThreadFactory(name, Thread.MIN_PRIORITY), "internal");
        }

        private CompactionExecutor(int threadCount, String name)
        {
            this(threadCount, threadCount, name, new LinkedBlockingQueue<Runnable>());
        }

        public CompactionExecutor()
        {
            this(Math.max(1, DatabaseDescriptor.getConcurrentCompactors()), "CompactionExecutor");
        }

        protected void beforeExecute(Thread t, Runnable r)
        {
            // can't set this in Thread factory, so we do it redundantly here
            isCompactionManager.set(true);
            super.beforeExecute(t, r);
        }

        // modified from DebuggableThreadPoolExecutor so that CompactionInterruptedExceptions are not logged
        @Override
        public void afterExecute(Runnable r, Throwable t)
        {
            DebuggableThreadPoolExecutor.maybeResetTraceSessionWrapper(r);

            if (t == null)
                t = DebuggableThreadPoolExecutor.extractThrowable(r);

            if (t != null)
            {
                if (t instanceof CompactionInterruptedException)
                {
                    logger.info(t.getMessage());
                    if (t.getSuppressed() != null && t.getSuppressed().length > 0)
                        logger.warn("Interruption of compaction encountered exceptions:", t);
                    else
                        logger.trace("Full interruption stack trace:", t);
                }
                else
                {
                    DebuggableThreadPoolExecutor.handleOrLog(t);
                }
            }

            // Snapshots cannot be deleted on Windows while segments of the root element are mapped in NTFS. Compactions
            // unmap those segments which could free up a snapshot for successful deletion.
            SnapshotDeletingTask.rescheduleFailedTasks();
        }

        public ListenableFuture<?> submitIfRunning(Runnable task, String name)
        {
            return submitIfRunning(Executors.callable(task, null), name);
        }

        /**
         * Submit the task but only if the executor has not been shutdown.If the executor has
         * been shutdown, or in case of a rejected execution exception return a cancelled future.
         *
         * @param task - the task to submit
         * @param name - the task name to use in log messages
         *
         * @return the future that will deliver the task result, or a future that has already been
         *         cancelled if the task could not be submitted.
         */
        public ListenableFuture<?> submitIfRunning(Callable<?> task, String name)
        {
            if (isShutdown())
            {
                logger.info("Executor has been shut down, not submitting {}", name);
                return Futures.immediateCancelledFuture();
            }

            try
            {
                ListenableFutureTask ret = ListenableFutureTask.create(task);
                execute(ret);
                return ret;
            }
            catch (RejectedExecutionException ex)
            {
                if (isShutdown())
                    logger.info("Executor has shut down, could not submit {}", name);
                else
                    logger.error("Failed to submit {}", name, ex);

                return Futures.immediateCancelledFuture();
            }
        }
    }

    private static class ValidationExecutor extends CompactionExecutor
    {
        public ValidationExecutor()
        {
            super(1, Integer.MAX_VALUE, "ValidationExecutor", new SynchronousQueue<Runnable>());
        }
    }

    private static class CacheCleanupExecutor extends CompactionExecutor
    {
        public CacheCleanupExecutor()
        {
            super(1, "CacheCleanupExecutor");
        }
    }

    public interface CompactionExecutorStatsCollector
    {
        void beginCompaction(CompactionInfo.Holder ci);

        void finishCompaction(CompactionInfo.Holder ci);
    }

    public List<Map<String, String>> getCompactions()
    {
        List<Holder> compactionHolders = CompactionMetrics.getCompactions();
        List<Map<String, String>> out = new ArrayList<Map<String, String>>(compactionHolders.size());
        for (CompactionInfo.Holder ci : compactionHolders)
            out.add(ci.getCompactionInfo().asMap());
        return out;
    }

    public List<String> getCompactionSummary()
    {
        List<Holder> compactionHolders = CompactionMetrics.getCompactions();
        List<String> out = new ArrayList<String>(compactionHolders.size());
        for (CompactionInfo.Holder ci : compactionHolders)
            out.add(ci.getCompactionInfo().toString());
        return out;
    }

    public TabularData getCompactionHistory()
    {
        try
        {
            return SystemKeyspace.getCompactionHistory();
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long getTotalBytesCompacted()
    {
        return metrics.bytesCompacted.getCount();
    }

    public long getTotalCompactionsCompleted()
    {
        return metrics.totalCompactionsCompleted.getCount();
    }

    public int getPendingTasks()
    {
        return metrics.pendingTasks.getValue();
    }

    public long getCompletedTasks()
    {
        return metrics.completedTasks.getValue();
    }

    public void stopCompaction(String type)
    {
        OperationType operation = OperationType.valueOf(type);
        for (Holder holder : CompactionMetrics.getCompactions())
        {
            if (holder.getCompactionInfo().getTaskType() == operation)
                holder.stop();
        }
    }

    public void stopCompactionById(String compactionId)
    {
        for (Holder holder : CompactionMetrics.getCompactions())
        {
            UUID holderId = holder.getCompactionInfo().compactionId();
            if (holderId != null && holderId.equals(UUID.fromString(compactionId)))
                holder.stop();
        }
    }

    public void setConcurrentCompactors(int value)
    {
        if (value > executor.getCorePoolSize())
        {
            // we are increasing the value
            executor.setMaximumPoolSize(value);
            executor.setCorePoolSize(value);
        }
        else if (value < executor.getCorePoolSize())
        {
            // we are reducing the value
            executor.setCorePoolSize(value);
            executor.setMaximumPoolSize(value);
        }
    }

    public int getCoreCompactorThreads()
    {
        return executor.getCorePoolSize();
    }

    public void setCoreCompactorThreads(int number)
    {
        executor.setCorePoolSize(number);
    }

    public int getMaximumCompactorThreads()
    {
        return executor.getMaximumPoolSize();
    }

    public void setMaximumCompactorThreads(int number)
    {
        executor.setMaximumPoolSize(number);
    }

    public int getCoreValidationThreads()
    {
        return validationExecutor.getCorePoolSize();
    }

    public void setCoreValidationThreads(int number)
    {
        validationExecutor.setCorePoolSize(number);
    }

    public int getMaximumValidatorThreads()
    {
        return validationExecutor.getMaximumPoolSize();
    }

    public void setMaximumValidatorThreads(int number)
    {
        validationExecutor.setMaximumPoolSize(number);
    }

    /**
     * Try to stop all of the compactions for given ColumnFamilies.
     *
     * Note that this method does not wait for all compactions to finish; you'll need to loop against
     * isCompacting if you want that behavior.
     *
     * @param columnFamilies The ColumnFamilies to try to stop compaction upon.
     * @param interruptValidation true if validation operations for repair should also be interrupted
     *
     */
    public void interruptCompactionFor(Iterable<CFMetaData> columnFamilies, boolean interruptValidation)
    {
        assert columnFamilies != null;

        // interrupt in-progress compactions
        for (Holder compactionHolder : CompactionMetrics.getCompactions())
        {
            CompactionInfo info = compactionHolder.getCompactionInfo();
            if ((info.getTaskType() == OperationType.VALIDATION) && !interruptValidation)
                continue;

            // cfmetadata is null for index summary redistributions which are 'global' - they involve all keyspaces/tables
            if (info.getCFMetaData() == null || Iterables.contains(columnFamilies, info.getCFMetaData()))
                compactionHolder.stop(); // signal compaction to stop
        }
    }

    public void interruptCompactionForCFs(Iterable<ColumnFamilyStore> cfss, boolean interruptValidation)
    {
        List<CFMetaData> metadata = new ArrayList<>();
        for (ColumnFamilyStore cfs : cfss)
            metadata.add(cfs.metadata);

        interruptCompactionFor(metadata, interruptValidation);
    }

    public void waitForCessation(Iterable<ColumnFamilyStore> cfss)
    {
        long start = System.nanoTime();
        long delay = TimeUnit.MINUTES.toNanos(1);
        while (System.nanoTime() - start < delay)
        {
            if (CompactionManager.instance.isCompacting(cfss))
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            else
                break;
        }
    }

    /**
     * Return whether "global" compactions should be paused, used by ColumnFamilyStore#runWithCompactionsDisabled
     *
     * a global compaction is one that includes several/all tables, currently only IndexSummaryBuilder
     */
    public boolean isGlobalCompactionPaused()
    {
        return globalCompactionPauseCount.get() > 0;
    }

    public CompactionPauser pauseGlobalCompaction()
    {
        CompactionPauser pauser = globalCompactionPauseCount::decrementAndGet;
        globalCompactionPauseCount.incrementAndGet();
        return pauser;
    }

    public interface CompactionPauser extends AutoCloseable
    {
        public void close();
    }
}
