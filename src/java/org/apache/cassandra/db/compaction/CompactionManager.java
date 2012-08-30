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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
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
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

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
    public Future<?> submitBackground(final ColumnFamilyStore cfs)
    {
        logger.debug("Scheduling a background task check for {}.{} with {}",
                     new Object[] {cfs.table.name,
                                   cfs.columnFamily,
                                   cfs.getCompactionStrategy().getClass().getSimpleName()});
        Runnable runnable = new Runnable()
        {
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
                    if (!task.markSSTablesForCompaction())
                    {
                        logger.debug("Unable to mark SSTables for {}", task);
                        return;
                    }

                    try
                    {
                        task.execute(metrics);
                    }
                    finally
                    {
                        task.unmarkSSTables();
                    }
                    submitBackground(cfs);
                }
                finally
                {
                    compactionLock.readLock().unlock();
                }
            }
        };
        return executor.submit(runnable);
    }

    private static interface AllSSTablesOperation
    {
        public void perform(ColumnFamilyStore store, Collection<SSTableReader> sstables) throws IOException;
    }

    private void performAllSSTableOperation(final ColumnFamilyStore cfStore, final AllSSTablesOperation operation) throws InterruptedException, ExecutionException
    {
        Callable<Object> runnable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                compactionLock.writeLock().lock();
                try
                {
                    Collection<SSTableReader> sstables = cfStore.getDataTracker().markCompacting(cfStore.getSSTables(), 1, Integer.MAX_VALUE);
                    if (sstables == null || sstables.isEmpty())
                        return this;
                    try
                    {
                        // downgrade the lock acquisition
                        compactionLock.readLock().lock();
                        compactionLock.writeLock().unlock();
                        try
                        {
                            operation.perform(cfStore, sstables);
                        }
                        finally
                        {
                            compactionLock.readLock().unlock();
                        }
                    }
                    finally
                    {
                        cfStore.getDataTracker().unmarkCompacting(sstables);
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

    public void performSSTableRewrite(ColumnFamilyStore cfStore) throws InterruptedException, ExecutionException
    {
        performAllSSTableOperation(cfStore, new AllSSTablesOperation()
        {
            public void perform(ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
            {
                assert !cfs.isIndex();
                for (final SSTableReader sstable : sstables)
                {
                    // SSTables are marked by the caller
                    // NOTE: it is important that the task create one and only one sstable, even for Leveled compaction (see LeveledManifest.replace())
                    CompactionTask task = new CompactionTask(cfs, Collections.singletonList(sstable), NO_GC);
                    task.isUserDefined(true);
                    task.setCompactionType(OperationType.UPGRADE_SSTABLES);
                    task.execute(metrics);
                }
            }
        });
    }

    public void performCleanup(ColumnFamilyStore cfStore, final NodeId.OneShotRenewer renewer) throws InterruptedException, ExecutionException
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
                    if (!task.markSSTablesForCompaction(0, Integer.MAX_VALUE))
                        return;
                    try
                    {
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
                        task.unmarkSSTables();
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

        File directory = new File(ksname);
        String[] filenames = dataFiles.split(",");
        Collection<Descriptor> descriptors = new ArrayList<Descriptor>(filenames.length);

        String cfname = null;
        for (String filename : filenames)
        {
            Pair<Descriptor, String> p = Descriptor.fromFilename(directory, filename.trim());
            if (!p.right.equals(Component.DATA.name()))
            {
                throw new IllegalArgumentException(filename + " does not appear to be a data file");
            }
            if (cfname == null)
            {
                cfname = p.left.cfname;
            }
            else if (!cfname.equals(p.left.cfname))
            {
                throw new IllegalArgumentException("All provided sstables should be for the same column family");
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

                    Collection<SSTableReader> toCompact;
                    try
                    {
                        if (sstables.isEmpty())
                        {
                            logger.info("No file to compact for user defined compaction");
                        }
                        // attempt to schedule the set
                        else if ((toCompact = cfs.getDataTracker().markCompacting(sstables, 1, Integer.MAX_VALUE)) != null)
                        {
                            // success: perform the compaction
                            try
                            {
                                AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();
                                AbstractCompactionTask task = strategy.getUserDefinedTask(toCompact, gcBefore);
                                task.execute(metrics);
                            }
                            finally
                            {
                                cfs.getDataTracker().unmarkCompacting(toCompact);
                            }
                        }
                        else
                        {
                            logger.info("SSTables for user defined compaction are already being compacted.");
                        }
                    }
                    finally
                    {
                        SSTableReader.releaseReferences(sstables);
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
        SSTableReader found = null;
        for (SSTableReader sstable : cfs.markCurrentSSTablesReferenced())
        {
            // .equals() with no other changes won't work because in sstable.descriptor, the directory is an absolute path.
            // We could construct descriptor with an absolute path too but I haven't found any satisfying way to do that
            // (DB.getDataFileLocationForTable() may not return the right path if you have multiple volumes). Hence the
            // endsWith.
            if (sstable.descriptor.toString().endsWith(descriptor.toString()))
                found = sstable;
            else
                sstable.releaseReference();
        }
        return found;
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
    private void doCleanupCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, NodeId.OneShotRenewer renewer) throws IOException
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

            CompactionController controller = new CompactionController(cfs, Collections.singletonList(sstable), getDefaultGcBefore(cfs), false);
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
        if (cfs.table.snapshotExists(validator.request.sessionid))
        {
            // If there is a snapshot created for the session then read from there.
            sstables = cfs.getSnapshotSSTableReader(validator.request.sessionid);
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
        }

        CompactionIterable ci = new ValidationCompactionIterable(cfs, sstables, validator.request.range);
        CloseableIterator<AbstractCompactedRow> iter = ci.iterator();
        metrics.beginCompaction(ci);
        try
        {
            Iterator<AbstractCompactedRow> nni = Iterators.filter(iter, Predicates.notNull());

            // validate the CF as we iterate over it
            validator.prepare(cfs);
            while (nni.hasNext())
            {
                if (ci.isStopRequested())
                    throw new CompactionInterruptedException(ci.getCompactionInfo());
                AbstractCompactedRow row = nni.next();
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
                    main.discardSSTables(truncatedAt);

                    for (SecondaryIndex index : main.indexManager.getIndexes())
                        index.truncate(truncatedAt);
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
        return cfs.isIndex()
               ? GC_ALL
               : (int) (System.currentTimeMillis() / 1000) - cfs.metadata.getGcGraceSeconds();
    }

    private static class ValidationCompactionIterable extends CompactionIterable
    {
        public ValidationCompactionIterable(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, Range<Token> range)
        {
            super(OperationType.VALIDATION,
                  cfs.getCompactionStrategy().getScanners(sstables, range),
                  new ValidationCompactionController(cfs, sstables));
        }
    }

    /*
     * Controller for validation compaction that never purges.
     * Note that we should not call cfs.getOverlappingSSTables on the provided
     * sstables because those sstables are not guaranteed to be active sstables
     * (since we can run repair on a snapshot).
     */
    private static class ValidationCompactionController extends CompactionController
    {
        public ValidationCompactionController(ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
        {
            super(cfs, NO_GC, null);
        }

        @Override
        public boolean shouldPurge(DecoratedKey key)
        {
            return false;
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
