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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInfo.Holder;
import org.apache.cassandra.db.index.SecondaryIndexBuilder;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.*;

import org.apache.cassandra.utils.concurrent.Refs;

/**
 * <p>
 * A singleton which manages a private executor of ongoing compactions.
 * </p>
 * Scheduling for compaction is accomplished by swapping sstables to be compacted into
 * a set via DataTracker. New scheduling attempts will ignore currently compacting
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
                     cfs.getCompactionStrategy().getName());
        List<Future<?>> futures = new ArrayList<Future<?>>();
        // we must schedule it at least once, otherwise compaction will stop for a CF until next flush
        do {
            if (executor.isShutdown())
            {
                logger.info("Executor has shut down, not submitting background task");
                return Collections.emptyList();
            }
            compactingCF.add(cfs);
            futures.add(executor.submit(new BackgroundCompactionTask(cfs)));
            // if we have room for more compactions, then fill up executor
        } while (executor.getActiveCount() + futures.size() < executor.getMaximumPoolSize());

        return futures;
    }

    public boolean isCompacting(Iterable<ColumnFamilyStore> cfses)
    {
        for (ColumnFamilyStore cfs : cfses)
            if (!cfs.getDataTracker().getCompacting().isEmpty())
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
    class BackgroundCompactionTask implements Runnable
    {
        private final ColumnFamilyStore cfs;

        BackgroundCompactionTask(ColumnFamilyStore cfs)
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

                AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();
                AbstractCompactionTask task = strategy.getNextBackgroundTask(getDefaultGcBefore(cfs));
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

    private AllSSTableOpStatus parallelAllSSTableOperation(final ColumnFamilyStore cfs, final OneSSTableOperation operation) throws ExecutionException, InterruptedException
    {
        Iterable<SSTableReader> compactingSSTables = cfs.markAllCompacting();
        if (compactingSSTables == null)
        {
            logger.info("Aborting operation on {}.{} after failing to interrupt other compaction operations", cfs.keyspace.getName(), cfs.name);
            return AllSSTableOpStatus.ABORTED;
        }
        if (Iterables.isEmpty(compactingSSTables))
        {
            logger.info("No sstables for {}.{}", cfs.keyspace.getName(), cfs.name);
            return AllSSTableOpStatus.SUCCESSFUL;
        }
        try
        {
            Iterable<SSTableReader> sstables = operation.filterSSTables(compactingSSTables);
            List<Future<Object>> futures = new ArrayList<>();

            for (final SSTableReader sstable : sstables)
            {
                if (executor.isShutdown())
                {
                    logger.info("Executor has shut down, not submitting task");
                    return AllSSTableOpStatus.ABORTED;
                }

                futures.add(executor.submit(new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        operation.execute(sstable);
                        return this;
                    }
                }));
            }

            for (Future<Object> f : futures)
                f.get();
        }
        finally
        {
            cfs.getDataTracker().unmarkCompacting(compactingSSTables);
        }
        return AllSSTableOpStatus.SUCCESSFUL;
    }

    private static interface OneSSTableOperation
    {
        Iterable<SSTableReader> filterSSTables(Iterable<SSTableReader> input);
        void execute(SSTableReader input) throws IOException;
    }

    public enum AllSSTableOpStatus { ABORTED(1), SUCCESSFUL(0);
        public final int statusCode;

        AllSSTableOpStatus(int statusCode)
        {
            this.statusCode = statusCode;
        }
    }

    public AllSSTableOpStatus performScrub(final ColumnFamilyStore cfs, final boolean skipCorrupted) throws InterruptedException, ExecutionException
    {
        assert !cfs.isIndex();
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(Iterable<SSTableReader> input)
            {
                return input;
            }

            @Override
            public void execute(SSTableReader input) throws IOException
            {
                scrubOne(cfs, input, skipCorrupted);
            }
        });
    }

    public AllSSTableOpStatus performSSTableRewrite(final ColumnFamilyStore cfs, final boolean excludeCurrentVersion) throws InterruptedException, ExecutionException
    {
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(Iterable<SSTableReader> input)
            {
                return Iterables.filter(input, new Predicate<SSTableReader>()
                {
                    @Override
                    public boolean apply(SSTableReader sstable)
                    {
                        return !(excludeCurrentVersion && sstable.descriptor.version.equals(sstable.descriptor.getFormat().getLatestVersion()));
                    }
                });
            }

            @Override
            public void execute(SSTableReader input) throws IOException
            {
                AbstractCompactionTask task = cfs.getCompactionStrategy().getCompactionTask(Collections.singleton(input), NO_GC, Long.MAX_VALUE);
                task.setUserDefined(true);
                task.setCompactionType(OperationType.UPGRADE_SSTABLES);
                task.execute(metrics);
            }
        });
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
            public Iterable<SSTableReader> filterSSTables(Iterable<SSTableReader> input)
            {
                List<SSTableReader> sortedSSTables = Lists.newArrayList(input);
                Collections.sort(sortedSSTables, new SSTableReader.SizeComparator());
                return sortedSSTables;
            }

            @Override
            public void execute(SSTableReader input) throws IOException
            {
                CleanupStrategy cleanupStrategy = CleanupStrategy.get(cfStore, ranges);
                doCleanupOne(cfStore, input, cleanupStrategy, ranges, hasIndexes);
            }
        });
    }

    public Future<?> submitAntiCompaction(final ColumnFamilyStore cfs,
                                          final Collection<Range<Token>> ranges,
                                          final Refs<SSTableReader> sstables,
                                          final long repairedAt)
    {
        Runnable runnable = new WrappedRunnable() {
            @Override
            public void runMayThrow() throws Exception
            {
                boolean success = false;
                while (!success)
                {
                    for (SSTableReader compactingSSTable : cfs.getDataTracker().getCompacting())
                        sstables.releaseIfHolds(compactingSSTable);
                    Set<SSTableReader> compactedSSTables = new HashSet<>();
                    for (SSTableReader sstable : sstables)
                        if (sstable.isMarkedCompacted())
                            compactedSSTables.add(sstable);
                    sstables.release(compactedSSTables);
                    success = sstables.isEmpty() || cfs.getDataTracker().markCompacting(sstables);
                }
                performAnticompaction(cfs, ranges, sstables, repairedAt);
            }
        };
        if (executor.isShutdown())
        {
            logger.info("Compaction executor has shut down, not submitting anticompaction");
            return Futures.immediateCancelledFuture();
        }

        return executor.submit(runnable);
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
                                      long repairedAt) throws InterruptedException, IOException
    {
        logger.info("Starting anticompaction for {}.{} on {}/{} sstables", cfs.keyspace.getName(), cfs.getColumnFamilyName(), validatedForRepair.size(), cfs.getSSTables().size());
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
            cfs.getDataTracker().notifySSTableRepairedStatusChanged(mutatedRepairStatuses);
            cfs.getDataTracker().unmarkCompacting(Sets.union(nonAnticompacting, mutatedRepairStatuses));
            validatedForRepair.release(Sets.union(nonAnticompacting, mutatedRepairStatuses));
            if (!sstables.isEmpty())
                doAntiCompaction(cfs, ranges, sstables, repairedAt);
        }
        finally
        {
            validatedForRepair.release();
            cfs.getDataTracker().unmarkCompacting(sstables);
        }

        logger.info(String.format("Completed anticompaction successfully"));
    }

    public void performMaximal(final ColumnFamilyStore cfStore)
    {
        FBUtilities.waitOnFutures(submitMaximal(cfStore, getDefaultGcBefore(cfStore)));
    }

    public List<Future<?>> submitMaximal(final ColumnFamilyStore cfStore, final int gcBefore)
    {
        // here we compute the task off the compaction executor, so having that present doesn't
        // confuse runWithCompactionsDisabled -- i.e., we don't want to deadlock ourselves, waiting
        // for ourselves to finish/acknowledge cancellation before continuing.
        final Collection<AbstractCompactionTask> tasks = cfStore.getCompactionStrategy().getMaximalTask(gcBefore);

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
            descriptors.put(cfs, cfs.directories.find(new File(filename.trim()).getName()));
        }

        List<Future<?>> futures = new ArrayList<>();
        for (ColumnFamilyStore cfs : descriptors.keySet())
            futures.add(submitUserDefined(cfs, descriptors.get(cfs), getDefaultGcBefore(cfs)));
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
                    AbstractCompactionTask task = cfs.getCompactionStrategy().getUserDefinedTask(sstables, gcBefore);
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
        for (SSTableReader sstable : cfs.getSSTables())
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

    private void scrubOne(ColumnFamilyStore cfs, SSTableReader sstable, boolean skipCorrupted) throws IOException
    {
        Scrubber scrubber = new Scrubber(cfs, sstable, skipCorrupted, false);

        CompactionInfo.Holder scrubInfo = scrubber.getScrubInfo();
        metrics.beginCompaction(scrubInfo);
        try
        {
            scrubber.scrub();
        }
        finally
        {
            scrubber.close();
            metrics.finishCompaction(scrubInfo);
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
    private void doCleanupOne(final ColumnFamilyStore cfs, SSTableReader sstable, CleanupStrategy cleanupStrategy, Collection<Range<Token>> ranges, boolean hasIndexes) throws IOException
    {
        assert !cfs.isIndex();

        Set<SSTableReader> sstableSet = Collections.singleton(sstable);

        if (!hasIndexes && !new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges))
        {
            cfs.getDataTracker().markCompactedSSTablesReplaced(sstableSet, Collections.<SSTableReader>emptyList(), OperationType.CLEANUP);
            return;
        }
        if (!needsCleanup(sstable, ranges))
        {
            logger.debug("Skipping {} for cleanup; all rows should be kept", sstable);
            return;
        }

        long start = System.nanoTime();

        long totalkeysWritten = 0;

        int expectedBloomFilterSize = Math.max(cfs.metadata.getMinIndexInterval(),
                                               (int) (SSTableReader.getApproximateKeyCount(sstableSet)));
        if (logger.isDebugEnabled())
            logger.debug("Expected bloom filter size : {}", expectedBloomFilterSize);

        logger.info("Cleaning up {}", sstable);

        File compactionFileLocation = cfs.directories.getWriteableLocationAsFile(cfs.getExpectedCompactedFileSize(sstableSet, OperationType.CLEANUP));
        if (compactionFileLocation == null)
            throw new IOException("disk full");

        ISSTableScanner scanner = cleanupStrategy.getScanner(sstable, getRateLimiter());
        CleanupInfo ci = new CleanupInfo(sstable, scanner);

        metrics.beginCompaction(ci);
        Set<SSTableReader> oldSSTable = Sets.newHashSet(sstable);
        SSTableRewriter writer = new SSTableRewriter(cfs, oldSSTable, sstable.maxDataAge, false);
        List<SSTableReader> finished;
        try (CompactionController controller = new CompactionController(cfs, sstableSet, getDefaultGcBefore(cfs)))
        {
            writer.switchWriter(createWriter(cfs, compactionFileLocation, expectedBloomFilterSize, sstable.getSSTableMetadata().repairedAt, sstable));

            while (scanner.hasNext())
            {
                if (ci.isStopRequested())
                    throw new CompactionInterruptedException(ci.getCompactionInfo());

                SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                row = cleanupStrategy.cleanup(row);
                if (row == null)
                    continue;
                AbstractCompactedRow compactedRow = new LazilyCompactedRow(controller, Collections.singletonList(row));
                if (writer.append(compactedRow) != null)
                    totalkeysWritten++;
            }

            // flush to ensure we don't lose the tombstones on a restart, since they are not commitlog'd
            cfs.indexManager.flushIndexesBlocking();

            finished = writer.finish();
            cfs.getDataTracker().markCompactedSSTablesReplaced(oldSSTable, finished, OperationType.CLEANUP);
        }
        catch (Throwable e)
        {
            writer.abort();
            throw Throwables.propagate(e);
        }
        finally
        {
            scanner.close();
            metrics.finishCompaction(ci);
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
        public static CleanupStrategy get(ColumnFamilyStore cfs, Collection<Range<Token>> ranges)
        {
            return cfs.indexManager.hasIndexes()
                 ? new Full(cfs, ranges)
                 : new Bounded(cfs, ranges);
        }

        public abstract ISSTableScanner getScanner(SSTableReader sstable, RateLimiter limiter);
        public abstract SSTableIdentityIterator cleanup(SSTableIdentityIterator row);

        private static final class Bounded extends CleanupStrategy
        {
            private final Collection<Range<Token>> ranges;

            public Bounded(final ColumnFamilyStore cfs, Collection<Range<Token>> ranges)
            {
                this.ranges = ranges;
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
            public SSTableIdentityIterator cleanup(SSTableIdentityIterator row)
            {
                return row;
            }
        }

        private static final class Full extends CleanupStrategy
        {
            private final Collection<Range<Token>> ranges;
            private final ColumnFamilyStore cfs;
            private List<Cell> indexedColumnsInRow;

            public Full(ColumnFamilyStore cfs, Collection<Range<Token>> ranges)
            {
                this.cfs = cfs;
                this.ranges = ranges;
                this.indexedColumnsInRow = null;
            }

            @Override
            public ISSTableScanner getScanner(SSTableReader sstable, RateLimiter limiter)
            {
                return sstable.getScanner(limiter);
            }

            @Override
            public SSTableIdentityIterator cleanup(SSTableIdentityIterator row)
            {
                if (Range.isInRanges(row.getKey().getToken(), ranges))
                    return row;

                cfs.invalidateCachedRow(row.getKey());

                if (indexedColumnsInRow != null)
                    indexedColumnsInRow.clear();

                while (row.hasNext())
                {
                    OnDiskAtom column = row.next();

                    if (column instanceof Cell && cfs.indexManager.indexes((Cell) column))
                    {
                        if (indexedColumnsInRow == null)
                            indexedColumnsInRow = new ArrayList<>();

                        indexedColumnsInRow.add((Cell) column);
                    }
                }

                if (indexedColumnsInRow != null && !indexedColumnsInRow.isEmpty())
                {
                    // acquire memtable lock here because secondary index deletion may cause a race. See CASSANDRA-3712
                    try (OpOrder.Group opGroup = cfs.keyspace.writeOrder.start())
                    {
                        cfs.indexManager.deleteFromIndexes(row.getKey(), indexedColumnsInRow, opGroup);
                    }
                }
                return null;
            }
        }
    }

    public static SSTableWriter createWriter(ColumnFamilyStore cfs,
                                             File compactionFileLocation,
                                             int expectedBloomFilterSize,
                                             long repairedAt,
                                             SSTableReader sstable)
    {
        FileUtils.createDirectory(compactionFileLocation);

        return SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(compactionFileLocation)), expectedBloomFilterSize, repairedAt, sstable.getSSTableLevel());
    }

    public static SSTableWriter createWriterForAntiCompaction(ColumnFamilyStore cfs,
                                             File compactionFileLocation,
                                             int expectedBloomFilterSize,
                                             long repairedAt,
                                             Collection<SSTableReader> sstables)
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
        return SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(compactionFileLocation)),
                                 (long)expectedBloomFilterSize,
                                 repairedAt,
                                 cfs.metadata,
                                 cfs.partitioner,
                                 new MetadataCollector(sstables, cfs.metadata.comparator, minLevel));
    }


    /**
     * Performs a readonly "compaction" of all sstables in order to validate complete rows,
     * but without writing the merge result
     */
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
                gcBefore = cfs.gcBefore(cfs.getSnapshotCreationTime(snapshotName));
            }
            else
            {
                // flush first so everyone is validating data that is as similar as possible
                StorageService.instance.forceKeyspaceFlush(cfs.keyspace.getName(), cfs.name);
                ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(validator.desc.parentSessionId);
                ColumnFamilyStore.RefViewFragment sstableCandidates = cfs.selectAndReference(prs.isIncremental ? ColumnFamilyStore.UNREPAIRED_SSTABLES : ColumnFamilyStore.ALL_SSTABLES);
                Set<SSTableReader> sstablesToValidate = new HashSet<>();

                for (SSTableReader sstable : sstableCandidates.sstables)
                {
                    if (!(new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(Arrays.asList(validator.desc.range))))
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
                    gcBefore = getDefaultGcBefore(cfs);
            }

            // Create Merkle tree suitable to hold estimated partitions for given range.
            // We blindly assume that partition is evenly distributed on all sstables for now.
            long numPartitions = 0;
            for (SSTableReader sstable : sstables)
            {
                numPartitions += sstable.estimatedKeysForRanges(Collections.singleton(validator.desc.range));
            }
            // determine tree depth from number of partitions, but cap at 20 to prevent large tree.
            int depth = numPartitions > 0 ? (int) Math.min(Math.floor(Math.log(numPartitions)), 20) : 0;
            MerkleTree tree = new MerkleTree(cfs.partitioner, validator.desc.range, MerkleTree.RECOMMENDED_DEPTH, (int) Math.pow(2, depth));

            long start = System.nanoTime();
            try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategy().getScanners(sstables, validator.desc.range))
            {
                CompactionIterable ci = new ValidationCompactionIterable(cfs, scanners.scanners, gcBefore);
                Iterator<AbstractCompactedRow> iter = ci.iterator();
                metrics.beginCompaction(ci);
                try
                {
                    // validate the CF as we iterate over it
                    validator.prepare(cfs, tree);
                    while (iter.hasNext())
                    {
                        if (ci.isStopRequested())
                            throw new CompactionInterruptedException(ci.getCompactionInfo());
                        AbstractCompactedRow row = iter.next();
                        validator.add(row);
                    }
                    validator.complete();
                }
                finally
                {
                    if (isSnapshotValidation)
                    {
                        cfs.clearSnapshot(snapshotName);
                    }

                    metrics.finishCompaction(ci);
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
                             MerkleTree.serializer.serializedSize(tree, 0),
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
     * @param repairedSSTables
     * @param ranges Repaired ranges to be placed into one of the new sstables. The repaired table will be tracked via
     * the {@link org.apache.cassandra.io.sstable.metadata.StatsMetadata#repairedAt} field.
     */
    private void doAntiCompaction(ColumnFamilyStore cfs, Collection<Range<Token>> ranges,
                                                       Collection<SSTableReader> repairedSSTables, long repairedAt)
    {

        // TODO(5351): we can do better here:
        logger.info("Performing anticompaction on {} sstables", repairedSSTables.size());

        //Group SSTables
        Collection<Collection<SSTableReader>> groupedSSTables = cfs.getCompactionStrategy().groupSSTablesForAntiCompaction(repairedSSTables);
        // iterate over sstables to check if the repaired / unrepaired ranges intersect them.
        int antiCompactedSSTableCount = 0;
        for (Collection<SSTableReader> sstableGroup : groupedSSTables)
        {
            int antiCompacted = antiCompactGroup(cfs, ranges, sstableGroup, repairedAt);
            antiCompactedSSTableCount += antiCompacted;
        }

        String format = "Anticompaction completed successfully, anticompacted from {} to {} sstable(s).";
        logger.info(format, repairedSSTables.size(), antiCompactedSSTableCount);
    }

    private int antiCompactGroup(ColumnFamilyStore cfs, Collection<Range<Token>> ranges,
                             Collection<SSTableReader> anticompactionGroup, long repairedAt)
    {
        long groupMaxDataAge = -1;

        // check that compaction hasn't stolen any sstables used in previous repair sessions
        // if we need to skip the anticompaction, it will be carried out by the next repair
        for (Iterator<SSTableReader> i = anticompactionGroup.iterator(); i.hasNext();)
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

     
        if (anticompactionGroup.size() == 0)
        {
            logger.info("No valid anticompactions for this group, All sstables were compacted and are no longer available");
            return 0;
        }

        logger.info("Anticompacting {}", anticompactionGroup);
        Set<SSTableReader> sstableAsSet = new HashSet<>(anticompactionGroup);

        File destination = cfs.directories.getWriteableLocationAsFile(cfs.getExpectedCompactedFileSize(sstableAsSet, OperationType.ANTICOMPACTION));
        SSTableRewriter repairedSSTableWriter = new SSTableRewriter(cfs, sstableAsSet, groupMaxDataAge, false);
        SSTableRewriter unRepairedSSTableWriter = new SSTableRewriter(cfs, sstableAsSet, groupMaxDataAge, false);

        long repairedKeyCount = 0;
        long unrepairedKeyCount = 0;
        AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();
        try (AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(anticompactionGroup);
             CompactionController controller = new CompactionController(cfs, sstableAsSet, CFMetaData.DEFAULT_GC_GRACE_SECONDS))
        {
            int expectedBloomFilterSize = Math.max(cfs.metadata.getMinIndexInterval(), (int)(SSTableReader.getApproximateKeyCount(anticompactionGroup)));

            repairedSSTableWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, repairedAt, sstableAsSet));
            unRepairedSSTableWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, ActiveRepairService.UNREPAIRED_SSTABLE, sstableAsSet));

            CompactionIterable ci = new CompactionIterable(OperationType.ANTICOMPACTION, scanners.scanners, controller, DatabaseDescriptor.getSSTableFormat());
            Iterator<AbstractCompactedRow> iter = ci.iterator();
            while(iter.hasNext())
            {
                AbstractCompactedRow row = iter.next();
                // if current range from sstable is repaired, save it into the new repaired sstable
                if (Range.isInRanges(row.key.getToken(), ranges))
                {
                    repairedSSTableWriter.append(row);
                    repairedKeyCount++;
                }
                // otherwise save into the new 'non-repaired' table
                else
                {
                    unRepairedSSTableWriter.append(row);
                    unrepairedKeyCount++;
                }
            }
            // we have the same readers being rewritten by both writers, so we ask the first one NOT to close them
            // so that the second one can do so safely, without leaving us with references < 0 or any other ugliness
            List<SSTableReader> anticompactedSSTables = new ArrayList<>();
            anticompactedSSTables.addAll(repairedSSTableWriter.finish(repairedAt));
            anticompactedSSTables.addAll(unRepairedSSTableWriter.finish(ActiveRepairService.UNREPAIRED_SSTABLE));
            cfs.getDataTracker().markCompactedSSTablesReplaced(sstableAsSet, anticompactedSSTables, OperationType.ANTICOMPACTION);

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
            repairedSSTableWriter.abort();
            unRepairedSSTableWriter.abort();
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

    static int getDefaultGcBefore(ColumnFamilyStore cfs)
    {
        // 2ndary indexes have ExpiringColumns too, so we need to purge tombstones deleted before now. We do not need to
        // add any GcGrace however since 2ndary indexes are local to a node.
        return cfs.isIndex() ? (int) (System.currentTimeMillis() / 1000) : cfs.gcBefore(System.currentTimeMillis());
    }

    private static class ValidationCompactionIterable extends CompactionIterable
    {
        public ValidationCompactionIterable(ColumnFamilyStore cfs, List<ISSTableScanner> scanners, int gcBefore)
        {
            super(OperationType.VALIDATION, scanners, new ValidationCompactionController(cfs, gcBefore), DatabaseDescriptor.getSSTableFormat());
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

    private static class CleanupInfo extends CompactionInfo.Holder
    {
        private final SSTableReader sstable;
        private final ISSTableScanner scanner;

        public CleanupInfo(SSTableReader sstable, ISSTableScanner scanner)
        {
            this.sstable = sstable;
            this.scanner = scanner;
        }

        public CompactionInfo getCompactionInfo()
        {
            try
            {
                return new CompactionInfo(sstable.metadata,
                                          OperationType.CLEANUP,
                                          scanner.getCurrentPosition(),
                                          scanner.getLengthInBytes());
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
        }
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
}
