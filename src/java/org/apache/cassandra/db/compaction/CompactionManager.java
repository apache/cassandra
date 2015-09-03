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
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionInfo.Holder;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.MaterializedViewBuilder;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.repair.Validator;
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
    public static final ThreadLocal<Boolean> isCompactionManager = new ThreadLocal<Boolean>()
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
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_OBJECT_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private final CompactionExecutor executor = new CompactionExecutor();
    private final CompactionExecutor validationExecutor = new ValidationExecutor();
    private final static CompactionExecutor cacheCleanupExecutor = new CacheCleanupExecutor();

    private final CompactionMetrics metrics = new CompactionMetrics(executor, validationExecutor);
    private final Multiset<ColumnFamilyStore> compactingCF = ConcurrentHashMultiset.create();

    private final RateLimiter compactionRateLimiter = RateLimiter.create(Double.MAX_VALUE);

    /**
     * Gets compaction rate limiter. When compaction_throughput_mb_per_sec is 0 or node is bootstrapping,
     * this returns rate limiter with the rate of Double.MAX_VALUE bytes per second.
     * Rate unit is bytes per sec.
     *
     * @return RateLimiter with rate limit set
     */
    public RateLimiter getRateLimiter()
    {
        double currentThroughput = DatabaseDescriptor.getCompactionThroughputMbPerSec() * 1024.0 * 1024.0;
        // if throughput is set to 0, throttling is disabled
        if (currentThroughput == 0 || StorageService.instance.isBootstrapMode())
            currentThroughput = Double.MAX_VALUE;
        if (compactionRateLimiter.getRate() != currentThroughput)
            compactionRateLimiter.setRate(currentThroughput);
        return compactionRateLimiter;
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
            logger.debug("Autocompaction is disabled");
            return Collections.emptyList();
        }

        int count = compactingCF.count(cfs);
        if (count > 0 && executor.getActiveCount() >= executor.getMaximumPoolSize())
        {
            logger.debug("Background compaction is still running for {}.{} ({} remaining). Skipping",
                         cfs.keyspace.getName(), cfs.name, count);
            return Collections.emptyList();
        }

        logger.debug("Scheduling a background task check for {}.{} with {}",
                     cfs.keyspace.getName(),
                     cfs.name,
                     cfs.getCompactionStrategyManager().getName());
        List<Future<?>> futures = new ArrayList<>();
        // we must schedule it at least once, otherwise compaction will stop for a CF until next flush
        if (executor.isShutdown())
        {
            logger.info("Executor has shut down, not submitting background task");
            return Collections.emptyList();
        }
        compactingCF.add(cfs);
        futures.add(executor.submit(new BackgroundCompactionCandidate(cfs)));

        return futures;
    }

    public boolean isCompacting(Iterable<ColumnFamilyStore> cfses)
    {
        for (ColumnFamilyStore cfs : cfses)
            if (!cfs.getTracker().getCompacting().isEmpty())
                return true;
        return false;
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
            this.cfs = cfs;
        }

        public void run()
        {
            try
            {
                logger.debug("Checking {}.{}", cfs.keyspace.getName(), cfs.name);
                if (!cfs.isValid())
                {
                    logger.debug("Aborting compaction for dropped CF");
                    return;
                }

                CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
                AbstractCompactionTask task = strategy.getNextBackgroundTask(getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()));
                if (task == null)
                {
                    logger.debug("No tasks available");
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

    @SuppressWarnings("resource")
    private AllSSTableOpStatus parallelAllSSTableOperation(final ColumnFamilyStore cfs, final OneSSTableOperation operation, OperationType operationType) throws ExecutionException, InterruptedException
    {
        try (LifecycleTransaction compacting = cfs.markAllCompacting(operationType);)
        {
            Iterable<SSTableReader> sstables = Lists.newArrayList(operation.filterSSTables(compacting));
            if (Iterables.isEmpty(sstables))
            {
                logger.info("No sstables for {}.{}", cfs.keyspace.getName(), cfs.name);
                return AllSSTableOpStatus.SUCCESSFUL;
            }

            List<Pair<LifecycleTransaction,Future<Object>>> futures = new ArrayList<>();

            for (final SSTableReader sstable : sstables)
            {
                if (executor.isShutdown())
                {
                    logger.info("Executor has shut down, not submitting task");
                    return AllSSTableOpStatus.ABORTED;
                }

                final LifecycleTransaction txn = compacting.split(singleton(sstable));
                futures.add(Pair.create(txn,executor.submit(new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        operation.execute(txn);
                        return this;
                    }
                })));
            }

            assert compacting.originals().isEmpty();


            //Collect all exceptions
            Exception exception = null;

            for (Pair<LifecycleTransaction, Future<Object>> f : futures)
            {
                try
                {
                    f.right.get();
                }
                catch (InterruptedException | ExecutionException e)
                {
                    if (exception == null)
                        exception = new Exception();

                    exception.addSuppressed(e);
                }
                finally
                {
                    f.left.close();
                }
            }

            if (exception != null)
                Throwables.propagate(exception);

            return AllSSTableOpStatus.SUCCESSFUL;
        }
    }

    private static interface OneSSTableOperation
    {
        Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction);
        void execute(LifecycleTransaction input) throws IOException;
    }

    public enum AllSSTableOpStatus { ABORTED(1), SUCCESSFUL(0);
        public final int statusCode;

        AllSSTableOpStatus(int statusCode)
        {
            this.statusCode = statusCode;
        }
    }

    public AllSSTableOpStatus performScrub(final ColumnFamilyStore cfs, final boolean skipCorrupted, final boolean checkData)
    throws InterruptedException, ExecutionException
    {
        return performScrub(cfs, skipCorrupted, checkData, false);
    }

    public AllSSTableOpStatus performScrub(final ColumnFamilyStore cfs, final boolean skipCorrupted, final boolean checkData, final boolean offline)
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
                scrubOne(cfs, input, skipCorrupted, checkData, offline);
            }
        }, OperationType.SCRUB);
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
        }, OperationType.VERIFY);
    }

    public AllSSTableOpStatus performSSTableRewrite(final ColumnFamilyStore cfs, final boolean excludeCurrentVersion) throws InterruptedException, ExecutionException
    {
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                Iterable<SSTableReader> sstables = new ArrayList<>(transaction.originals());
                Iterator<SSTableReader> iter = sstables.iterator();
                while (iter.hasNext())
                {
                    SSTableReader sstable = iter.next();
                    if (excludeCurrentVersion && sstable.descriptor.version.equals(sstable.descriptor.getFormat().getLatestVersion()))
                    {
                        transaction.cancel(sstable);
                        iter.remove();
                    }
                }
                return sstables;
            }

            @Override
            public void execute(LifecycleTransaction txn) throws IOException
            {
                AbstractCompactionTask task = cfs.getCompactionStrategyManager().getCompactionTask(txn, NO_GC, Long.MAX_VALUE);
                task.setUserDefined(true);
                task.setCompactionType(OperationType.UPGRADE_SSTABLES);
                task.execute(metrics);
            }
        }, OperationType.UPGRADE_SSTABLES);
    }

    public AllSSTableOpStatus performCleanup(final ColumnFamilyStore cfStore) throws InterruptedException, ExecutionException
    {
        assert !cfStore.isIndex();
        Keyspace keyspace = cfStore.keyspace;
        final Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
        if (ranges.isEmpty())
        {
            logger.info("Cleanup cannot run before a node has joined the ring");
            return AllSSTableOpStatus.ABORTED;
        }
        final boolean hasIndexes = cfStore.indexManager.hasIndexes();

        return parallelAllSSTableOperation(cfStore, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
                Collections.sort(sortedSSTables, new SSTableReader.SizeComparator());
                return sortedSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn) throws IOException
            {
                CleanupStrategy cleanupStrategy = CleanupStrategy.get(cfStore, ranges, FBUtilities.nowInSeconds());
                doCleanupOne(cfStore, txn, cleanupStrategy, ranges, hasIndexes);
            }
        }, OperationType.CLEANUP);
    }

    public ListenableFuture<?> submitAntiCompaction(final ColumnFamilyStore cfs,
                                          final Collection<Range<Token>> ranges,
                                          final Refs<SSTableReader> sstables,
                                          final long repairedAt)
    {
        Runnable runnable = new WrappedRunnable() {
            @Override
            @SuppressWarnings("resource")
            public void runMayThrow() throws Exception
            {
                LifecycleTransaction modifier = null;
                while (modifier == null)
                {
                    for (SSTableReader compactingSSTable : cfs.getTracker().getCompacting())
                        sstables.releaseIfHolds(compactingSSTable);
                    Set<SSTableReader> compactedSSTables = new HashSet<>();
                    for (SSTableReader sstable : sstables)
                        if (sstable.isMarkedCompacted())
                            compactedSSTables.add(sstable);
                    sstables.release(compactedSSTables);
                    modifier = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
                }
                performAnticompaction(cfs, ranges, sstables, modifier, repairedAt);
            }
        };
        if (executor.isShutdown())
        {
            logger.info("Compaction executor has shut down, not submitting anticompaction");
            sstables.release();
            return Futures.immediateCancelledFuture();
        }

        ListenableFutureTask<?> task = ListenableFutureTask.create(runnable, null);
        executor.submit(task);
        return task;
    }

    /**
     * Make sure the {validatedForRepair} are marked for compaction before calling this.
     *
     * Caller must reference the validatedForRepair sstables (via ParentRepairSession.getAndReferenceSSTables(..)).
     *
     * @param cfs
     * @param ranges Ranges that the repair was carried out on
     * @param validatedForRepair SSTables containing the repaired ranges. Should be referenced before passing them.
     * @throws InterruptedException
     * @throws IOException
     */
    public void performAnticompaction(ColumnFamilyStore cfs,
                                      Collection<Range<Token>> ranges,
                                      Refs<SSTableReader> validatedForRepair,
                                      LifecycleTransaction txn,
                                      long repairedAt) throws InterruptedException, IOException
    {
        logger.info("Starting anticompaction for {}.{} on {}/{} sstables", cfs.keyspace.getName(), cfs.getColumnFamilyName(), validatedForRepair.size(), cfs.getLiveSSTables());
        logger.debug("Starting anticompaction for ranges {}", ranges);
        Set<SSTableReader> sstables = new HashSet<>(validatedForRepair);
        Set<SSTableReader> mutatedRepairStatuses = new HashSet<>();
        Set<SSTableReader> nonAnticompacting = new HashSet<>();
        Iterator<SSTableReader> sstableIterator = sstables.iterator();
        try
        {
            while (sstableIterator.hasNext())
            {
                SSTableReader sstable = sstableIterator.next();
                for (Range<Token> r : Range.normalize(ranges))
                {
                    Range<Token> sstableRange = new Range<>(sstable.first.getToken(), sstable.last.getToken());
                    if (r.contains(sstableRange))
                    {
                        logger.info("SSTable {} fully contained in range {}, mutating repairedAt instead of anticompacting", sstable, r);
                        sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, repairedAt);
                        sstable.reloadSSTableMetadata();
                        mutatedRepairStatuses.add(sstable);
                        sstableIterator.remove();
                        break;
                    }
                    else if (!sstableRange.intersects(r))
                    {
                        logger.info("SSTable {} ({}) does not intersect repaired range {}, not touching repairedAt.", sstable, sstableRange, r);
                        nonAnticompacting.add(sstable);
                        sstableIterator.remove();
                        break;
                    }
                    else
                    {
                        logger.info("SSTable {} ({}) will be anticompacted on range {}", sstable, sstableRange, r);
                    }
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

        logger.info("Completed anticompaction successfully");
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

        for (final AbstractCompactionTask task : tasks)
        {
            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws IOException
                {
                    task.execute(metrics);
                }
            };
            if (executor.isShutdown())
            {
                logger.info("Compaction executor has shut down, not submitting task");
                return Collections.emptyList();
            }
            futures.add(executor.submit(runnable));
        }
        return futures;
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

    public Future<?> submitUserDefined(final ColumnFamilyStore cfs, final Collection<Descriptor> dataFiles, final int gcBefore)
    {
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws IOException
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
                    AbstractCompactionTask task = cfs.getCompactionStrategyManager().getUserDefinedTask(sstables, gcBefore);
                    if (task != null)
                        task.execute(metrics);
                }
            }
        };
        if (executor.isShutdown())
        {
            logger.info("Compaction executor has shut down, not submitting task");
            return Futures.immediateCancelledFuture();
        }

        return executor.submit(runnable);
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
    public Future<Object> submitValidation(final ColumnFamilyStore cfStore, final Validator validator)
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
        return validationExecutor.submit(callable);
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

    private void scrubOne(ColumnFamilyStore cfs, LifecycleTransaction modifier, boolean skipCorrupted, boolean checkData, boolean offline) throws IOException
    {
        CompactionInfo.Holder scrubInfo = null;

        try (Scrubber scrubber = new Scrubber(cfs, modifier, skipCorrupted, offline, checkData))
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
    static boolean needsCleanup(SSTableReader sstable, Collection<Range<Token>> ownedRanges)
    {
        assert !ownedRanges.isEmpty(); // cleanup checks for this

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
            if (!nextRange.contains(firstBeyondRange.getToken()))
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

        if (!hasIndexes && !new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges))
        {
            txn.obsoleteOriginals();
            txn.finish();
            return;
        }
        if (!needsCleanup(sstable, ranges))
        {
            logger.debug("Skipping {} for cleanup; all rows should be kept", sstable);
            return;
        }

        long start = System.nanoTime();

        long totalkeysWritten = 0;

        long expectedBloomFilterSize = Math.max(cfs.metadata.params.minIndexInterval,
                                               SSTableReader.getApproximateKeyCount(txn.originals()));
        if (logger.isDebugEnabled())
            logger.debug("Expected bloom filter size : {}", expectedBloomFilterSize);

        logger.info("Cleaning up {}", sstable);

        File compactionFileLocation = cfs.getDirectories().getWriteableLocationAsFile(cfs.getExpectedCompactedFileSize(txn.originals(), OperationType.CLEANUP));
        if (compactionFileLocation == null)
            throw new IOException("disk full");

        List<SSTableReader> finished;
        int nowInSec = FBUtilities.nowInSeconds();
        try (SSTableRewriter writer = new SSTableRewriter(cfs, txn, sstable.maxDataAge, false);
             ISSTableScanner scanner = cleanupStrategy.getScanner(sstable, getRateLimiter());
             CompactionController controller = new CompactionController(cfs, txn.originals(), getDefaultGcBefore(cfs, nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.CLEANUP, Collections.singletonList(scanner), controller, nowInSec, UUIDGen.getTimeUUID(), metrics))
        {
            writer.switchWriter(createWriter(cfs, compactionFileLocation, expectedBloomFilterSize, sstable.getSSTableMetadata().repairedAt, sstable, txn));

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
                }
            }

            // flush to ensure we don't lose the tombstones on a restart, since they are not commitlog'd
            cfs.indexManager.flushAllIndexesBlocking();

            finished = writer.finish();
        }

        if (!finished.isEmpty())
        {
            String format = "Cleaned up to %s.  %,d to %,d (~%d%% of original) bytes for %,d keys.  Time: %,dms.";
            long dTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            long startsize = sstable.onDiskLength();
            long endsize = 0;
            for (SSTableReader newSstable : finished)
                endsize += newSstable.onDiskLength();
            double ratio = (double) endsize / (double) startsize;
            logger.info(String.format(format, finished.get(0).getFilename(), startsize, endsize, (int) (ratio * 100), totalkeysWritten, dTime));
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
                cacheCleanupExecutor.submit(new Runnable()
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

        return SSTableWriter.create(cfs.metadata,
                                    Descriptor.fromFilename(cfs.getSSTablePath(compactionFileLocation)),
                                    expectedBloomFilterSize,
                                    repairedAt,
                                    sstable.getSSTableLevel(),
                                    sstable.header,
                                    txn);
    }

    public static SSTableWriter createWriterForAntiCompaction(ColumnFamilyStore cfs,
                                                              File compactionFileLocation,
                                                              int expectedBloomFilterSize,
                                                              long repairedAt,
                                                              Collection<SSTableReader> sstables,
                                                              LifecycleTransaction txn)
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

            String snapshotName = validator.desc.sessionId.toString();
            int gcBefore;
            int nowInSec = FBUtilities.nowInSeconds();
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
                ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(validator.desc.parentSessionId);
                ColumnFamilyStore.RefViewFragment sstableCandidates = cfs.selectAndReference(View.select(SSTableSet.CANONICAL, (s) -> !prs.isIncremental || !s.isRepaired()));
                Set<SSTableReader> sstablesToValidate = new HashSet<>();

                for (SSTableReader sstable : sstableCandidates.sstables)
                {
                    if (new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(validator.desc.ranges))
                    {
                        sstablesToValidate.add(sstable);
                    }
                }

                Set<SSTableReader> currentlyRepairing = ActiveRepairService.instance.currentlyRepairing(cfs.metadata.cfId, validator.desc.parentSessionId);

                if (!Sets.intersection(currentlyRepairing, sstablesToValidate).isEmpty())
                {
                    logger.error("Cannot start multiple repair sessions over the same sstables");
                    throw new RuntimeException("Cannot start multiple repair sessions over the same sstables");
                }

                sstables = Refs.tryRef(sstablesToValidate);
                if (sstables == null)
                {
                    logger.error("Could not reference sstables");
                    throw new RuntimeException("Could not reference sstables");
                }
                sstableCandidates.release();
                prs.addSSTables(cfs.metadata.cfId, sstablesToValidate);

                if (validator.gcBefore > 0)
                    gcBefore = validator.gcBefore;
                else
                    gcBefore = getDefaultGcBefore(cfs, nowInSec);
            }

            // Create Merkle trees suitable to hold estimated partitions for the given ranges.
            // We blindly assume that a partition is evenly distributed on all sstables for now.
            long numPartitions = 0;
            for (SSTableReader sstable : sstables)
            {
                numPartitions += sstable.estimatedKeysForRanges(validator.desc.ranges);
            }
            // determine tree depth from number of partitions, but cap at 20 to prevent large tree.
            int depth = numPartitions > 0 ? (int) Math.min(Math.floor(Math.log(numPartitions)), 20) : 0;
            MerkleTrees tree = new MerkleTrees(cfs.getPartitioner());
            tree.addMerkleTrees((int) Math.pow(2, depth), validator.desc.ranges);

            long start = System.nanoTime();
            try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables, validator.desc.ranges);
                 ValidationCompactionController controller = new ValidationCompactionController(cfs, gcBefore);
                 CompactionIterator ci = new ValidationCompactionIterator(scanners.scanners, controller, nowInSec, metrics))
            {
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
                }
                validator.complete();
            }
            finally
            {
                if (isSnapshotValidation)
                {
                    cfs.clearSnapshot(snapshotName);
                }
            }

            if (logger.isDebugEnabled())
            {
                // MT serialize may take time
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                logger.debug("Validation finished in {} msec, depth {} for {} keys, serialized size {} bytes for {}",
                             duration,
                             depth,
                             numPartitions,
                             MerkleTrees.serializer.serializedSize(tree, 0),
                             validator.desc);
            }
        }
        finally
        {
            if (sstables != null)
                sstables.release();
        }
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
        logger.info("Performing anticompaction on {} sstables", repaired.originals().size());

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
        logger.info(format, repaired.originals().size(), antiCompactedSSTableCount);
    }

    private int antiCompactGroup(ColumnFamilyStore cfs, Collection<Range<Token>> ranges,
                             LifecycleTransaction anticompactionGroup, long repairedAt)
    {
        long groupMaxDataAge = -1;

        // check that compaction hasn't stolen any sstables used in previous repair sessions
        // if we need to skip the anticompaction, it will be carried out by the next repair
        for (Iterator<SSTableReader> i = anticompactionGroup.originals().iterator(); i.hasNext();)
        {
            SSTableReader sstable = i.next();
            if (!new File(sstable.getFilename()).exists())
            {
                logger.info("Skipping anticompaction for {}, required sstable was compacted and is no longer available.", sstable);
                i.remove();
                continue;
            }
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

        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
        try (SSTableRewriter repairedSSTableWriter = new SSTableRewriter(cfs, anticompactionGroup, groupMaxDataAge, false, false);
             SSTableRewriter unRepairedSSTableWriter = new SSTableRewriter(cfs, anticompactionGroup, groupMaxDataAge, false, false);
             AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(anticompactionGroup.originals());
             CompactionController controller = new CompactionController(cfs, sstableAsSet, getDefaultGcBefore(cfs, nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.ANTICOMPACTION, scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID(), metrics))
        {
            int expectedBloomFilterSize = Math.max(cfs.metadata.params.minIndexInterval, (int)(SSTableReader.getApproximateKeyCount(sstableAsSet)));

            repairedSSTableWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, repairedAt, sstableAsSet, anticompactionGroup));
            unRepairedSSTableWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, ActiveRepairService.UNREPAIRED_SSTABLE, sstableAsSet, anticompactionGroup));

            while (ci.hasNext())
            {
                try (UnfilteredRowIterator partition = ci.next())
                {
                    // if current range from sstable is repaired, save it into the new repaired sstable
                    if (Range.isInRanges(partition.partitionKey().getToken(), ranges))
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
            // since both writers are operating over the same Transaction, we cannot use the convenience Transactional.finish() method,
            // as on the second finish() we would prepareToCommit() on a Transaction that has already been committed, which is forbidden by the API
            // (since it indicates misuse). We call permitRedundantTransitions so that calls that transition to a state already occupied are permitted.
            anticompactionGroup.permitRedundantTransitions();
            repairedSSTableWriter.setRepairedAt(repairedAt).prepareToCommit();
            unRepairedSSTableWriter.prepareToCommit();
            anticompactedSSTables.addAll(repairedSSTableWriter.finished());
            anticompactedSSTables.addAll(unRepairedSSTableWriter.finished());
            repairedSSTableWriter.commit();
            unRepairedSSTableWriter.commit();

            logger.debug("Repaired {} keys out of {} for {}/{} in {}", repairedKeyCount,
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
        if (executor.isShutdown())
        {
            logger.info("Compaction executor has shut down, not submitting index build");
            return null;
        }

        return executor.submit(runnable);
    }

    public Future<?> submitCacheWrite(final AutoSavingCache.Writer writer)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (!AutoSavingCache.flushInProgress.add(writer.cacheType()))
                {
                    logger.debug("Cache flushing was already in progress: skipping {}", writer.getCompactionInfo());
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
        if (executor.isShutdown())
        {
            logger.info("Executor has shut down, not submitting background task");
            Futures.immediateCancelledFuture();
        }
        return executor.submit(runnable);
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
        public long maxPurgeableTimestamp(DecoratedKey key)
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
            return Long.MAX_VALUE;
        }
    }

    public Future<?> submitMaterializedViewBuilder(final MaterializedViewBuilder builder)
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

    private static class CompactionExecutor extends JMXEnabledThreadPoolExecutor
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
                        logger.debug("Full interruption stack trace:", t);
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

            if (Iterables.contains(columnFamilies, info.getCFMetaData()))
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
}
