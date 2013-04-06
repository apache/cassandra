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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Throwables;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.CompactionInfo.Holder;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexBuilder;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.service.AntiEntropyService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * A singleton which manages a private executor of ongoing compactions. A readwrite lock
 * controls whether compactions can proceed: an external consumer can completely stop
 * compactions by acquiring the write half of the lock via getCompactionLock().
 *
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
    public static final ThreadLocal<Boolean> isCompactionManager = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue()
        {
            return false;
        }
    };

    /**
     * compactionLock has two purposes:
     * - "Special" compactions will acquire writelock instead of readlock to make sure that all
     * other compaction activity is quiesced and they can grab ALL the sstables to do something.
     * - Some schema migrations cannot run concurrently with compaction.  (Currently, this is
     *   only when changing compaction strategy -- see CFS.maybeReloadCompactionStrategy.)
     *
     * TODO this is too big a hammer -- we should only care about quiescing all for the given CFS.
     */
    private final ReentrantReadWriteLock compactionLock = new ReentrantReadWriteLock();

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
    private final CompactionMetrics metrics = new CompactionMetrics(executor, validationExecutor);
    private final Multiset<ColumnFamilyStore> compactingCF = ConcurrentHashMultiset.create();

    /**
     * @return A lock, for which acquisition means no compactions can run.
     */
    public Lock getCompactionLock()
    {
        return compactionLock.writeLock();
    }

    /**
     * Call this whenever a compaction might be needed on the given columnfamily.
     * It's okay to over-call (within reason) since the compactions are single-threaded,
     * and if a call is unnecessary, it will just be no-oped in the bucketing phase.
     */
    public List<Future<?>> submitBackground(final ColumnFamilyStore cfs)
    {
        int count = compactingCF.count(cfs);
        if (count > 0 && executor.getActiveCount() >= executor.getMaximumPoolSize())
        {
            logger.debug("Background compaction is still running for {}.{} ({} remaining). Skipping",
                         new Object[] {cfs.table.name, cfs.columnFamily, count});
            return Collections.emptyList();
        }

        logger.debug("Scheduling a background task check for {}.{} with {}",
                     new Object[] {cfs.table.name,
                                   cfs.columnFamily,
                                   cfs.getCompactionStrategy().getClass().getSimpleName()});
        List<Future<?>> futures = new ArrayList<Future<?>>();
        // if we have room for more compactions, then fill up executor
        while (executor.getActiveCount() + futures.size() < executor.getMaximumPoolSize())
        {
            futures.add(executor.submit(new BackgroundCompactionTask(cfs)));
            compactingCF.add(cfs);
        }
        return futures;
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
            compactionLock.readLock().lock();
            try
            {
                logger.debug("Checking {}.{}", cfs.table.name, cfs.columnFamily); // log after we get the lock so we can see delays from that if any
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
                compactionLock.readLock().unlock();
            }
            submitBackground(cfs);
        }
    }

    private static interface AllSSTablesOperation
    {
        public void perform(ColumnFamilyStore store, Collection<SSTableReader> sstables) throws IOException;
    }

    private void performAllSSTableOperation(final ColumnFamilyStore cfs, final AllSSTablesOperation operation) throws InterruptedException, ExecutionException
    {
        Callable<Object> runnable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                compactionLock.writeLock().lock();
                try
                {
                    Collection<SSTableReader> sstables;
                    while (true)
                    {
                        sstables = cfs.getDataTracker().getUncompactingSSTables();
                        if (sstables.isEmpty())
                            return this;
                        if (cfs.getDataTracker().markCompacting(sstables))
                            break;
                    }

                    try
                    {
                        // downgrade the lock acquisition
                        compactionLock.readLock().lock();
                        compactionLock.writeLock().unlock();
                        try
                        {
                            operation.perform(cfs, sstables);
                        }
                        finally
                        {
                            compactionLock.readLock().unlock();
                        }
                    }
                    finally
                    {
                        cfs.getDataTracker().unmarkCompacting(sstables);
                    }
                    return this;
                }
                finally
                {
                    // we probably already downgraded
                    if (compactionLock.writeLock().isHeldByCurrentThread())
                        compactionLock.writeLock().unlock();
                }
            }
        };
        executor.submit(runnable).get();
    }

    public void performScrub(ColumnFamilyStore cfStore) throws InterruptedException, ExecutionException
    {
        performAllSSTableOperation(cfStore, new AllSSTablesOperation()
        {
            public void perform(ColumnFamilyStore store, Collection<SSTableReader> sstables) throws IOException
            {
                doScrub(store, sstables);
            }
        });
    }

    public void performSSTableRewrite(ColumnFamilyStore cfStore, final boolean excludeCurrentVersion) throws InterruptedException, ExecutionException
    {
        performAllSSTableOperation(cfStore, new AllSSTablesOperation()
        {
            public void perform(ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
            {
                for (final SSTableReader sstable : sstables)
                {
                    if (excludeCurrentVersion && sstable.descriptor.version.equals(Descriptor.Version.CURRENT))
                        continue;

                    // SSTables are marked by the caller
                    // NOTE: it is important that the task create one and only one sstable, even for Leveled compaction (see LeveledManifest.replace())
                    CompactionTask task = new CompactionTask(cfs, Collections.singletonList(sstable), NO_GC);
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
            public void perform(ColumnFamilyStore store, Collection<SSTableReader> sstables) throws IOException
            {
                // Sort the column families in order of SSTable size, so cleanup of smaller CFs
                // can free up space for larger ones
                List<SSTableReader> sortedSSTables = new ArrayList<SSTableReader>(sstables);
                Collections.sort(sortedSSTables, new Comparator<SSTableReader>()
                {
                    public int compare(SSTableReader o1, SSTableReader o2)
                    {
                        return Longs.compare(o1.onDiskLength(), o2.onDiskLength());
                    }
                });

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
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws IOException
            {
                // acquire the write lock long enough to schedule all sstables
                compactionLock.writeLock().lock();
                try
                {
                    AbstractCompactionTask task = cfStore.getCompactionStrategy().getMaximalTask(gcBefore);
                    if (task == null)
                        return;
                    // downgrade the lock acquisition
                    compactionLock.readLock().lock();
                    compactionLock.writeLock().unlock();
                    try
                    {
                        task.execute(metrics);
                    }
                    finally
                    {
                        compactionLock.readLock().unlock();
                    }
                }
                finally
                {
                    // we probably already downgraded
                    if (compactionLock.writeLock().isHeldByCurrentThread())
                        compactionLock.writeLock().unlock();
                }
            }
        };
        return executor.submit(runnable);
    }

    public void forceUserDefinedCompaction(String ksname, String dataFiles)
    {
        if (!Schema.instance.getTables().contains(ksname))
            throw new IllegalArgumentException("Unknown keyspace " + ksname);

        String[] filenames = dataFiles.split(",");
        Collection<Descriptor> descriptors = new ArrayList<Descriptor>(filenames.length);

        String cfname = null;
        for (String filename : filenames)
        {
            // extract keyspace and columnfamily name from filename
            Descriptor desc = Descriptor.fromFilename(filename.trim());
            if (!desc.ksname.equals(ksname))
            {
                throw new IllegalArgumentException("Given keyspace " + ksname + " does not match with file " + filename);
            }
            if (cfname == null)
            {
                cfname = desc.cfname;
            }
            else if (!cfname.equals(desc.cfname))
            {
                throw new IllegalArgumentException("All provided sstables should be for the same column family");
            }
            File directory = new File(ksname + File.separator + cfname);
            Pair<Descriptor, String> p = Descriptor.fromFilename(directory, filename.trim());
            if (!p.right.equals(Component.DATA.name()))
            {
                throw new IllegalArgumentException(filename + " does not appear to be a data file");
            }
            descriptors.add(p.left);
        }

        ColumnFamilyStore cfs = Table.open(ksname).getColumnFamilyStore(cfname);
        submitUserDefined(cfs, descriptors, getDefaultGcBefore(cfs));
    }

    public Future<?> submitUserDefined(final ColumnFamilyStore cfs, final Collection<Descriptor> dataFiles, final int gcBefore)
    {
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws IOException
            {
                compactionLock.readLock().lock();
                try
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
                finally
                {
                    compactionLock.readLock().unlock();
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
    public Future<Object> submitValidation(final ColumnFamilyStore cfStore, final AntiEntropyService.Validator validator)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    doValidationCompaction(cfStore, validator);
                    return this;
                }
                finally
                {
                    compactionLock.readLock().unlock();
                }
            }
        };
        return validationExecutor.submit(callable);
    }

    /* Used in tests. */
    public void disableAutoCompaction()
    {
        for (String ksname : Schema.instance.getNonSystemTables())
        {
            for (ColumnFamilyStore cfs : Table.open(ksname).getColumnFamilyStores())
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
    private void doScrub(ColumnFamilyStore cfs, Collection<SSTableReader> sstables) throws IOException
    {
        assert !cfs.isIndex();
        for (final SSTableReader sstable : sstables)
            scrubOne(cfs, sstable);
    }

    private void scrubOne(ColumnFamilyStore cfs, SSTableReader sstable) throws IOException
    {
        Scrubber scrubber = new Scrubber(cfs, sstable);

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
            cfs.markCompacted(Collections.singletonList(sstable), OperationType.SCRUB);
        else
            cfs.replaceCompactedSSTables(Collections.singletonList(sstable), Collections.singletonList(scrubber.getNewSSTable()), OperationType.SCRUB);
    }

    /**
     * This function goes over each file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    private void doCleanupCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, CounterId.OneShotRenewer renewer) throws IOException
    {
        assert !cfs.isIndex();
        Table table = cfs.table;
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(table.name);
        if (ranges.isEmpty())
        {
            logger.info("Cleanup cannot run before a node has joined the ring");
            return;
        }

        boolean isCommutative = cfs.metadata.getDefaultValidator().isCommutative();
        boolean hasIndexes = !cfs.indexManager.getIndexes().isEmpty();

        for (SSTableReader sstable : sstables)
        {
            if (!hasIndexes && !new Bounds<Token>(sstable.first.token, sstable.last.token).intersects(ranges))
            {
                cfs.replaceCompactedSSTables(Arrays.asList(sstable), Collections.<SSTableReader>emptyList(), OperationType.CLEANUP);
                continue;
            }

            CompactionController controller = new CompactionController(cfs, Collections.singletonList(sstable), getDefaultGcBefore(cfs));
            long startTime = System.currentTimeMillis();

            long totalkeysWritten = 0;

            int expectedBloomFilterSize = Math.max(DatabaseDescriptor.getIndexInterval(),
                                                   (int)(SSTableReader.getApproximateKeyCount(Arrays.asList(sstable))));
            if (logger.isDebugEnabled())
              logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

            SSTableWriter writer = null;
            SSTableReader newSstable = null;

            logger.info("Cleaning up " + sstable);
            // Calculate the expected compacted filesize
            long expectedRangeFileSize = cfs.getExpectedCompactedFileSize(Arrays.asList(sstable), OperationType.CLEANUP);
            File compactionFileLocation = cfs.directories.getDirectoryForNewSSTables(expectedRangeFileSize);
            if (compactionFileLocation == null)
                throw new IOException("disk full");

            SSTableScanner scanner = sstable.getDirectScanner();
            long rowsRead = 0;
            List<IColumn> indexedColumnsInRow = null;

            CleanupInfo ci = new CleanupInfo(sstable, scanner);
            metrics.beginCompaction(ci);
            try
            {
                while (scanner.hasNext())
                {
                    if (ci.isStopRequested())
                        throw new CompactionInterruptedException(ci.getCompactionInfo());
                    SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                    if (Range.isInRanges(row.getKey().token, ranges))
                    {
                        AbstractCompactedRow compactedRow = controller.getCompactedRow(row);
                        if (compactedRow.isEmpty())
                            continue;
                        writer = maybeCreateWriter(cfs, compactionFileLocation, expectedBloomFilterSize, writer, Collections.singletonList(sstable));
                        writer.append(compactedRow);
                        totalkeysWritten++;
                    }
                    else
                    {
                        cfs.invalidateCachedRow(row.getKey());

                        if (hasIndexes || isCommutative)
                        {
                            if (indexedColumnsInRow != null)
                                indexedColumnsInRow.clear();

                            while (row.hasNext())
                            {
                                OnDiskAtom column = row.next();
                                if (column instanceof CounterColumn)
                                    renewer.maybeRenew((CounterColumn) column);
                                if (column instanceof IColumn && cfs.indexManager.indexes((IColumn)column))
                                {
                                    if (indexedColumnsInRow == null)
                                        indexedColumnsInRow = new ArrayList<IColumn>();

                                    indexedColumnsInRow.add((IColumn)column);
                                }
                            }

                            if (indexedColumnsInRow != null && !indexedColumnsInRow.isEmpty())
                            {
                                // acquire memtable lock here because secondary index deletion may cause a race. See CASSANDRA-3712
                                Table.switchLock.readLock().lock();
                                try
                                {
                                    cfs.indexManager.deleteFromIndexes(row.getKey(), indexedColumnsInRow);
                                }
                                finally
                                {
                                    Table.switchLock.readLock().unlock();
                                }
                            }
                        }
                    }
                    if ((rowsRead++ % 1000) == 0)
                        controller.mayThrottle(scanner.getCurrentPosition());
                }
                if (writer != null)
                    newSstable = writer.closeAndOpenReader(sstable.maxDataAge);
            }
            catch (Throwable e)
            {
                if (writer != null)
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
                long dTime = System.currentTimeMillis() - startTime;
                long startsize = sstable.onDiskLength();
                long endsize = newSstable.onDiskLength();
                double ratio = (double)endsize / (double)startsize;
                logger.info(String.format(format, writer.getFilename(), startsize, endsize, (int)(ratio*100), totalkeysWritten, dTime));
            }

            // flush to ensure we don't lose the tombstones on a restart, since they are not commitlog'd
            cfs.indexManager.flushIndexesBlocking();

            cfs.replaceCompactedSSTables(Arrays.asList(sstable), results, OperationType.CLEANUP);
        }
    }

    public static SSTableWriter maybeCreateWriter(ColumnFamilyStore cfs,
                                                  File compactionFileLocation,
                                                  int expectedBloomFilterSize,
                                                  SSTableWriter writer,
                                                  Collection<SSTableReader> sstables)
    {
        if (writer == null)
        {
            FileUtils.createDirectory(compactionFileLocation);
            writer = cfs.createCompactionWriter(expectedBloomFilterSize, compactionFileLocation, sstables);
        }
        return writer;
    }

    /**
     * Performs a readonly "compaction" of all sstables in order to validate complete rows,
     * but without writing the merge result
     */
    private void doValidationCompaction(ColumnFamilyStore cfs, AntiEntropyService.Validator validator) throws IOException
    {
        // this isn't meant to be race-proof, because it's not -- it won't cause bugs for a CFS to be dropped
        // mid-validation, or to attempt to validate a droped CFS.  this is just a best effort to avoid useless work,
        // particularly in the scenario where a validation is submitted before the drop, and there are compactions
        // started prior to the drop keeping some sstables alive.  Since validationCompaction can run
        // concurrently with other compactions, it would otherwise go ahead and scan those again.
        if (!cfs.isValid())
            return;

        Collection<SSTableReader> sstables;
        int gcBefore;
        if (cfs.snapshotExists(validator.request.sessionid))
        {
            // If there is a snapshot created for the session then read from there.
            sstables = cfs.getSnapshotSSTableReader(validator.request.sessionid);

            // Computing gcbefore based on the current time wouldn't be very good because we know each replica will execute
            // this at a different time (that's the whole purpose of repair with snaphsot). So instead we take the creation
            // time of the snapshot, which should give us roughtly the same time on each replica (roughtly being in that case
            // 'as good as in the non-snapshot' case)
            gcBefore = (int)(cfs.getSnapshotCreationTime(validator.request.sessionid) / 1000) - cfs.metadata.getGcGraceSeconds();
        }
        else
        {
            // flush first so everyone is validating data that is as similar as possible
            try
            {
                StorageService.instance.forceTableFlush(cfs.table.name, cfs.getColumnFamilyName());
            }
            catch (ExecutionException e)
            {
                throw new IOException(e);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }

            // we don't mark validating sstables as compacting in DataTracker, so we have to mark them referenced
            // instead so they won't be cleaned up if they do get compacted during the validation
            sstables = cfs.markCurrentSSTablesReferenced();
            gcBefore = getDefaultGcBefore(cfs);
        }

        CompactionIterable ci = new ValidationCompactionIterable(cfs, sstables, validator.request.range, gcBefore);
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
                if (row.isEmpty())
                    row.close();
                else
                    validator.add(row);
            }
            validator.complete();
        }
        finally
        {
            SSTableReader.releaseReferences(sstables);
            iter.close();
            if (cfs.table.snapshotExists(validator.request.sessionid))
                cfs.table.clearSnapshot(validator.request.sessionid);

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
                compactionLock.readLock().lock();
                try
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
                finally
                {
                    compactionLock.readLock().unlock();
                }
            }
        };

        // don't submit to the executor if the compaction lock is held by the current thread. Instead return a simple
        // future that will be immediately immediately get()ed and executed. Happens during a migration, which locks
        // the compaction thread and then reinitializes a ColumnFamilyStore. Under normal circumstances, CFS spawns
        // index jobs to the compaction manager (this) and blocks on them.
        if (compactionLock.isWriteLockedByCurrentThread())
            return new SimpleFuture(runnable);
        else
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

    public Future<?> submitTruncate(final ColumnFamilyStore main, final long truncatedAt)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                compactionLock.writeLock().lock();

                try
                {
                    ReplayPosition replayAfter = main.discardSSTables(truncatedAt);

                    for (SecondaryIndex index : main.indexManager.getIndexes())
                        index.truncate(truncatedAt);

                    SystemTable.saveTruncationPosition(main, replayAfter);

                    for (RowCacheKey key : CacheService.instance.rowCache.getKeySet())
                    {
                        if (key.cfId == main.metadata.cfId)
                            CacheService.instance.rowCache.remove(key);
                    }
                }
                finally
                {
                    compactionLock.writeLock().unlock();
                }
            }
        };

        return executor.submit(runnable);
    }

    static int getDefaultGcBefore(ColumnFamilyStore cfs)
    {
        // 2ndary indexes have ExpiringColumns too, so we need to purge tombstones deleted before now. We do not need to
        // add any GcGrace however since 2ndary indexes are local to a node.
        return cfs.isIndex()
               ? (int) (System.currentTimeMillis() / 1000)
               : (int) (System.currentTimeMillis() / 1000) - cfs.metadata.getGcGraceSeconds();
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

    private static class CompactionExecutor extends ThreadPoolExecutor
    {

        protected CompactionExecutor(int minThreads, int maxThreads, String name, BlockingQueue<Runnable> queue)
        {
            super(minThreads, maxThreads, 60, TimeUnit.SECONDS, queue, new NamedThreadFactory(name, Thread.MIN_PRIORITY));
            allowCoreThreadTimeOut(true);
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
            super.afterExecute(r,t);

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

    private static class SimpleFuture implements Future
    {
        private Runnable runnable;

        private SimpleFuture(Runnable r)
        {
            runnable = r;
        }

        public boolean cancel(boolean mayInterruptIfRunning)
        {
            throw new IllegalStateException("May not call SimpleFuture.cancel()");
        }

        public boolean isCancelled()
        {
            return false;
        }

        public boolean isDone()
        {
            return runnable == null;
        }

        public Object get() throws InterruptedException, ExecutionException
        {
            runnable.run();
            runnable = null;
            return runnable;
        }

        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            throw new IllegalStateException("May not call SimpleFuture.get(long, TimeUnit)");
        }
    }

    private static class CleanupInfo extends CompactionInfo.Holder
    {
        private final SSTableReader sstable;
        private final SSTableScanner scanner;
        public CleanupInfo(SSTableReader sstable, SSTableScanner scanner)
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
     * Note that this method does not wait indefinitely for all compactions to finish, maximum wait time is 30 secs.
     *
     * @param columnFamilies The ColumnFamilies to try to stop compaction upon.
     */
    public void stopCompactionFor(Collection<CFMetaData> columnFamilies)
    {
        assert columnFamilies != null;

        for (Holder compactionHolder : CompactionMetrics.getCompactions())
        {
            CompactionInfo info = compactionHolder.getCompactionInfo();

            if (columnFamilies.contains(info.getCFMetaData()))
                compactionHolder.stop(); // signal compaction to stop
        }
    }
}
