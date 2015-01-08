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
import com.google.common.util.concurrent.RateLimiter;
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
import org.apache.cassandra.db.index.SecondaryIndexBuilder;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

/**
 * A singleton which manages a private executor of ongoing compactions.
 * <p/>
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
                     cfs.getCompactionStrategy().getClass().getSimpleName());
        List<Future<?>> futures = new ArrayList<Future<?>>();

        // we must schedule it at least once, otherwise compaction will stop for a CF until next flush
        do {
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

    private static interface AllSSTablesOperation
    {
        public void perform(ColumnFamilyStore store, Iterable<SSTableReader> sstables) throws IOException;
    }

    private void performAllSSTableOperation(final ColumnFamilyStore cfs, final AllSSTablesOperation operation) throws InterruptedException, ExecutionException
    {
        final Iterable<SSTableReader> sstables = cfs.markAllCompacting();
        if (sstables == null)
            return;

        Callable<Object> runnable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                try
                {
                    operation.perform(cfs, sstables);
                }
                finally
                {
                    cfs.getDataTracker().unmarkCompacting(sstables);
                }
                return this;
            }
        };
        executor.submit(runnable).get();
    }

    public void performScrub(ColumnFamilyStore cfStore, final boolean skipCorrupted) throws InterruptedException, ExecutionException
    {
        performAllSSTableOperation(cfStore, new AllSSTablesOperation()
        {
            public void perform(ColumnFamilyStore store, Iterable<SSTableReader> sstables) throws IOException
            {
                doScrub(store, sstables, skipCorrupted);
            }
        });
    }

    public void performSSTableRewrite(ColumnFamilyStore cfStore, final boolean excludeCurrentVersion) throws InterruptedException, ExecutionException
    {
        performAllSSTableOperation(cfStore, new AllSSTablesOperation()
        {
            public void perform(ColumnFamilyStore cfs, Iterable<SSTableReader> sstables)
            {
                for (final SSTableReader sstable : sstables)
                {
                    if (excludeCurrentVersion && sstable.descriptor.version.equals(Descriptor.Version.CURRENT))
                        continue;

                    // SSTables are marked by the caller
                    // NOTE: it is important that the task create one and only one sstable, even for Leveled compaction (see LeveledManifest.replace())
                    AbstractCompactionTask task = cfs.getCompactionStrategy().getCompactionTask(Collections.singleton(sstable), NO_GC, Long.MAX_VALUE);
                    task.setUserDefined(true);
                    task.setCompactionType(OperationType.UPGRADE_SSTABLES);
                    task.execute(metrics);
                }
            }
        });
    }

    public void performCleanup(ColumnFamilyStore cfStore, final CounterId.OneShotRenewer renewer) throws InterruptedException, ExecutionException
    {
        performAllSSTableOperation(cfStore, new AllSSTablesOperation()
        {
            public void perform(ColumnFamilyStore store, Iterable<SSTableReader> sstables) throws IOException
            {
                // Sort the column families in order of SSTable size, so cleanup of smaller CFs
                // can free up space for larger ones
                List<SSTableReader> sortedSSTables = Lists.newArrayList(sstables);
                Collections.sort(sortedSSTables, new SSTableReader.SizeComparator());

                doCleanupCompaction(store, sortedSSTables, renewer);
            }
        });
    }

    public void performMaximal(final ColumnFamilyStore cfStore) throws InterruptedException, ExecutionException
    {
        submitMaximal(cfStore, getDefaultGcBefore(cfStore)).get();
    }

    public Future<?> submitMaximal(final ColumnFamilyStore cfStore, final int gcBefore)
    {
        // here we compute the task off the compaction executor, so having that present doesn't
        // confuse runWithCompactionsDisabled -- i.e., we don't want to deadlock ourselves, waiting
        // for ourselves to finish/acknowledge cancellation before continuing.
        final AbstractCompactionTask task = cfStore.getCompactionStrategy().getMaximalTask(gcBefore);
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws IOException
            {
                if (task == null)
                    return;
                task.execute(metrics);
            }
        };
        return executor.submit(runnable);
    }

    public void forceUserDefinedCompaction(String dataFiles)
    {
        String[] filenames = dataFiles.split(",");
        Multimap<Pair<String, String>, Descriptor> descriptors = ArrayListMultimap.create();

        for (String filename : filenames)
        {
            // extract keyspace and columnfamily name from filename
            Descriptor desc = Descriptor.fromFilename(filename.trim());
            if (Schema.instance.getCFMetaData(desc) == null)
            {
                logger.warn("Schema does not exist for file {}. Skipping.", filename);
                continue;
            }
            File directory = new File(desc.ksname + File.separator + desc.cfname);
            // group by keyspace/columnfamily
            Pair<Descriptor, String> p = Descriptor.fromFilename(directory, filename.trim());
            Pair<String, String> key = Pair.create(p.left.ksname, p.left.cfname);
            descriptors.put(key, p.left);
        }

        List<Future<?>> futures = new ArrayList<>();
        for (Pair<String, String> key : descriptors.keySet())
        {
            ColumnFamilyStore cfs = Keyspace.open(key.left).getColumnFamilyStore(key.right);
            futures.add(submitUserDefined(cfs, descriptors.get(key), getDefaultGcBefore(cfs)));
        }
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
                Collection<SSTableReader> sstables = new ArrayList<SSTableReader>(dataFiles.size());
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
        return executor.submit(runnable);
    }

    // This acquire a reference on the sstable
    // This is not efficent, do not use in any critical path
    private SSTableReader lookupSSTable(final ColumnFamilyStore cfs, Descriptor descriptor)
    {
        for (SSTableReader sstable : cfs.getSSTables())
        {
            // .equals() with no other changes won't work because in sstable.descriptor, the directory is an absolute path.
            // We could construct descriptor with an absolute path too but I haven't found any satisfying way to do that
            // (DB.getDataFileLocationForTable() may not return the right path if you have multiple volumes). Hence the
            // endsWith.
            if (sstable.descriptor.toString().endsWith(descriptor.toString()))
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

    /**
     * Deserialize everything in the CFS and re-serialize w/ the newest version.  Also attempts to recover
     * from bogus row keys / sizes using data from the index, and skips rows with garbage columns that resulted
     * from early ByteBuffer bugs.
     *
     * @throws IOException
     */
    private void doScrub(ColumnFamilyStore cfs, Iterable<SSTableReader> sstables, boolean skipCorrupted) throws IOException
    {
        assert !cfs.isIndex();
        for (final SSTableReader sstable : sstables)
            scrubOne(cfs, sstable, skipCorrupted);
    }

    private void scrubOne(ColumnFamilyStore cfs, SSTableReader sstable, boolean skipCorrupted) throws IOException
    {
        Scrubber scrubber = new Scrubber(cfs, sstable, skipCorrupted);

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

        if (scrubber.getNewInOrderSSTable() != null)
            cfs.addSSTable(scrubber.getNewInOrderSSTable());

        if (scrubber.getNewSSTable() == null)
            cfs.markObsolete(Collections.singletonList(sstable), OperationType.SCRUB);
        else
            cfs.replaceCompactedSSTables(Collections.singletonList(sstable), Collections.singletonList(scrubber.getNewSSTable()), OperationType.SCRUB);
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
        if (sstable.first.token.compareTo(firstRange.left) <= 0)
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
            if (!nextRange.contains(firstBeyondRange.token))
            {
                // we found a key in between the owned ranges
                return true;
            }
        }

        return false;
    }

    /**
     * This function goes over each file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    private void doCleanupCompaction(final ColumnFamilyStore cfs, Collection<SSTableReader> sstables, CounterId.OneShotRenewer renewer) throws IOException
    {
        assert !cfs.isIndex();
        Keyspace keyspace = cfs.keyspace;
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
        if (ranges.isEmpty())
        {
            logger.info("Cleanup cannot run before a node has joined the ring");
            return;
        }

        boolean hasIndexes = cfs.indexManager.hasIndexes();
        CleanupStrategy cleanupStrategy = CleanupStrategy.get(cfs, ranges, renewer);

        for (SSTableReader sstable : sstables)
        {
            if (!hasIndexes && !new Bounds<Token>(sstable.first.token, sstable.last.token).intersects(ranges))
            {
                cfs.replaceCompactedSSTables(Arrays.asList(sstable), Collections.<SSTableReader>emptyList(), OperationType.CLEANUP);
                continue;
            }
            if (!needsCleanup(sstable, ranges))
            {
                logger.debug("Skipping {} for cleanup; all rows should be kept", sstable);
                continue;
            }

            CompactionController controller = new CompactionController(cfs, Collections.singleton(sstable), getDefaultGcBefore(cfs));
            long start = System.nanoTime();

            long totalkeysWritten = 0;

            int expectedBloomFilterSize = Math.max(cfs.metadata.getIndexInterval(),
                                                   (int) (SSTableReader.getApproximateKeyCount(Arrays.asList(sstable), cfs.metadata)));
            if (logger.isDebugEnabled())
                logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

            logger.info("Cleaning up " + sstable);
            File compactionFileLocation = cfs.directories.getWriteableLocationAsFile(cfs.getExpectedCompactedFileSize(sstables, OperationType.CLEANUP));
            if (compactionFileLocation == null)
                throw new IOException("disk full");

            ICompactionScanner scanner = cleanupStrategy.getScanner(sstable, getRateLimiter());
            CleanupInfo ci = new CleanupInfo(sstable, scanner);

            metrics.beginCompaction(ci);
            SSTableWriter writer = createWriter(cfs,
                                                compactionFileLocation,
                                                expectedBloomFilterSize,
                                                sstable);
            SSTableReader newSstable = null;
            try
            {
                while (scanner.hasNext())
                {
                    if (ci.isStopRequested())
                        throw new CompactionInterruptedException(ci.getCompactionInfo());
                    SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();

                    row = cleanupStrategy.cleanup(row);
                    if (row == null)
                        continue;
                    AbstractCompactedRow compactedRow = controller.getCompactedRow(row);
                    if (writer.append(compactedRow) != null)
                        totalkeysWritten++;
                }
                if (totalkeysWritten > 0)
                    newSstable = writer.closeAndOpenReader(sstable.maxDataAge);
                else
                    writer.abort();
            }
            catch (Throwable e)
            {
                writer.abort();
                throw Throwables.propagate(e);
            }
            finally
            {
                controller.close();
                scanner.close();
                metrics.finishCompaction(ci);
            }

            List<SSTableReader> results = new ArrayList<SSTableReader>(1);
            if (newSstable != null)
            {
                results.add(newSstable);

                String format = "Cleaned up to %s.  %,d to %,d (~%d%% of original) bytes for %,d keys.  Time: %,dms.";
                long dTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                long startsize = sstable.onDiskLength();
                long endsize = newSstable.onDiskLength();
                double ratio = (double) endsize / (double) startsize;
                logger.info(String.format(format, writer.getFilename(), startsize, endsize, (int) (ratio * 100), totalkeysWritten, dTime));
            }

            // flush to ensure we don't lose the tombstones on a restart, since they are not commitlog'd
            cfs.indexManager.flushIndexesBlocking();

            cfs.replaceCompactedSSTables(Arrays.asList(sstable), results, OperationType.CLEANUP);
        }
    }

    private static abstract class CleanupStrategy
    {
        public static CleanupStrategy get(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, CounterId.OneShotRenewer renewer)
        {
            if (cfs.indexManager.hasIndexes() || cfs.metadata.getDefaultValidator().isCommutative())
                return new Full(cfs, ranges, renewer);

            return new Bounded(cfs, ranges);
        }

        public abstract ICompactionScanner getScanner(SSTableReader sstable, RateLimiter limiter);
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
            public ICompactionScanner getScanner(SSTableReader sstable, RateLimiter limiter)
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
            private List<Column> indexedColumnsInRow;
            private final CounterId.OneShotRenewer renewer;

            public Full(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, CounterId.OneShotRenewer renewer)
            {
                this.cfs = cfs;
                this.ranges = ranges;
                this.indexedColumnsInRow = null;
                this.renewer = renewer;
            }

            @Override
            public ICompactionScanner getScanner(SSTableReader sstable, RateLimiter limiter)
            {
                return sstable.getScanner(limiter);
            }

            @Override
            public SSTableIdentityIterator cleanup(SSTableIdentityIterator row)
            {
                if (Range.isInRanges(row.getKey().token, ranges))
                    return row;

                cfs.invalidateCachedRow(row.getKey());

                if (indexedColumnsInRow != null)
                    indexedColumnsInRow.clear();

                while (row.hasNext())
                {
                    OnDiskAtom column = row.next();
                    if (column instanceof CounterColumn)
                        renewer.maybeRenew((CounterColumn) column);

                    if (column instanceof Column && cfs.indexManager.indexes((Column) column))
                    {
                        if (indexedColumnsInRow == null)
                            indexedColumnsInRow = new ArrayList<>();

                        indexedColumnsInRow.add((Column) column);
                    }
                }

                if (indexedColumnsInRow != null && !indexedColumnsInRow.isEmpty())
                {
                    // acquire memtable lock here because secondary index deletion may cause a race. See CASSANDRA-3712
                    Keyspace.switchLock.readLock().lock();
                    try
                    {
                        cfs.indexManager.deleteFromIndexes(row.getKey(), indexedColumnsInRow);
                    }
                    finally
                    {
                        Keyspace.switchLock.readLock().unlock();
                    }
                }
                return null;
            }
        }
    }

    public static SSTableWriter createWriter(ColumnFamilyStore cfs,
                                             File compactionFileLocation,
                                             int expectedBloomFilterSize,
                                             SSTableReader sstable)
    {
        FileUtils.createDirectory(compactionFileLocation);
        return new SSTableWriter(cfs.getTempSSTablePath(compactionFileLocation),
                                 expectedBloomFilterSize,
                                 cfs.metadata,
                                 cfs.partitioner,
                                 SSTableMetadata.createCollector(Collections.singleton(sstable), cfs.metadata.comparator, sstable.getSSTableLevel()));
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

        Collection<SSTableReader> sstables;
        String snapshotName = validator.desc.sessionId.toString();
        int gcBefore;
        boolean isSnapshotValidation = cfs.snapshotExists(snapshotName);
        if (isSnapshotValidation)
        {
            // If there is a snapshot created for the session then read from there.
            sstables = cfs.getSnapshotSSTableReader(snapshotName);

            // Computing gcbefore based on the current time wouldn't be very good because we know each replica will execute
            // this at a different time (that's the whole purpose of repair with snapshot). So instead we take the creation
            // time of the snapshot, which should give us roughly the same time on each replica (roughly being in that case
            // 'as good as in the non-snapshot' case)
            gcBefore = cfs.gcBefore(cfs.getSnapshotCreationTime(snapshotName));
        }
        else
        {
            // flush first so everyone is validating data that is as similar as possible
            StorageService.instance.forceKeyspaceFlush(cfs.keyspace.getName(), cfs.name);

            // we don't mark validating sstables as compacting in DataTracker, so we have to mark them referenced
            // instead so they won't be cleaned up if they do get compacted during the validation
            sstables = cfs.markCurrentSSTablesReferenced();
            if (validator.gcBefore > 0)
                gcBefore = validator.gcBefore;
            else
                gcBefore = getDefaultGcBefore(cfs);
        }

        CompactionIterable ci = new ValidationCompactionIterable(cfs, sstables, validator.desc.range, gcBefore);
        CloseableIterator<AbstractCompactedRow> iter = ci.iterator();
        metrics.beginCompaction(ci);
        try
        {
            // validate the CF as we iterate over it
            validator.prepare(cfs);
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
            iter.close();
            SSTableReader.releaseReferences(sstables);
            if (isSnapshotValidation)
            {
                cfs.clearSnapshot(snapshotName);
            }

            metrics.finishCompaction(ci);
        }
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
        public ValidationCompactionIterable(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, Range<Token> range, int gcBefore)
        {
            super(OperationType.VALIDATION,
                  cfs.getCompactionStrategy().getScanners(sstables, range),
                  new ValidationCompactionController(cfs, gcBefore));
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
        public boolean shouldPurge(DecoratedKey key, long delTimestamp)
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
            return true;
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
        return metrics.bytesCompacted.count();
    }

    public long getTotalCompactionsCompleted()
    {
        return metrics.totalCompactionsCompleted.count();
    }

    public int getPendingTasks()
    {
        return metrics.pendingTasks.value();
    }

    public long getCompletedTasks()
    {
        return metrics.completedTasks.value();
    }

    private static class CleanupInfo extends CompactionInfo.Holder
    {
        private final SSTableReader sstable;
        private final ICompactionScanner scanner;

        public CleanupInfo(SSTableReader sstable, ICompactionScanner scanner)
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
