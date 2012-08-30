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
package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import javax.management.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cache.RowCacheSentinel;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.ColumnFamilyMetrics;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.config.CFMetaData.Caching;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    /*
     * maybeSwitchMemtable puts Memtable.getSortedContents on the writer executor.  When the write is complete,
     * we turn the writer into an SSTableReader and add it to ssTables where it is available for reads.
     *
     * There are two other things that maybeSwitchMemtable does.
     * First, it puts the Memtable into memtablesPendingFlush, where it stays until the flush is complete
     * and it's been added as an SSTableReader to ssTables_.  Second, it adds an entry to commitLogUpdater
     * that waits for the flush to complete, then calls onMemtableFlush.  This allows multiple flushes
     * to happen simultaneously on multicore systems, while still calling onMF in the correct order,
     * which is necessary for replay in case of a restart since CommitLog assumes that when onMF is
     * called, all data up to the given context has been persisted to SSTables.
     */
    private static final ExecutorService flushWriter
            = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getFlushWriters(),
                                               StageManager.KEEPALIVE,
                                               TimeUnit.SECONDS,
                                               new LinkedBlockingQueue<Runnable>(DatabaseDescriptor.getFlushQueueSize()),
                                               new NamedThreadFactory("FlushWriter"),
                                               "internal");

    public static final ExecutorService postFlushExecutor = new JMXEnabledThreadPoolExecutor("MemtablePostFlusher");

    static
    {
        // (can block if flush queue fills up, so don't put on scheduledTasks)
        StorageService.optionalTasks.scheduleWithFixedDelay(new MeteredFlusher(), 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public final Table table;
    public final String columnFamily;
    public final CFMetaData metadata;
    public final IPartitioner partitioner;
    private final String mbeanName;
    private volatile boolean valid = true;

    /* Memtables and SSTables on disk for this column family */
    private final DataTracker data;

    /* This is used to generate the next index for a SSTable */
    private final AtomicInteger fileIndexGenerator = new AtomicInteger(0);

    public final SecondaryIndexManager indexManager;

    private static final int INTERN_CUTOFF = 256;
    public final ConcurrentMap<ByteBuffer, ByteBuffer> internedNames = new NonBlockingHashMap<ByteBuffer, ByteBuffer>();

    /* These are locally held copies to be changed from the config during runtime */
    private volatile DefaultInteger minCompactionThreshold;
    private volatile DefaultInteger maxCompactionThreshold;
    private volatile AbstractCompactionStrategy compactionStrategy;

    public final Directories directories;

    /** ratio of in-memory memtable size, to serialized size */
    volatile double liveRatio = 1.0;
    /** ops count last time we computed liveRatio */
    private final AtomicLong liveRatioComputedAt = new AtomicLong(32);

    public final ColumnFamilyMetrics metric;

    public void reload()
    {
        // metadata object has been mutated directly. make all the members jibe with new settings.

        // only update these runtime-modifiable settings if they have not been modified.
        if (!minCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.minCompactionThreshold = new DefaultInteger(metadata.getMinCompactionThreshold());
        if (!maxCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.maxCompactionThreshold = new DefaultInteger(metadata.getMaxCompactionThreshold());

        maybeReloadCompactionStrategy();

        indexManager.reload();
    }

    private void maybeReloadCompactionStrategy()
    {
        // Check if there is a need for reloading
        if (metadata.compactionStrategyClass.equals(compactionStrategy.getClass()) && metadata.compactionStrategyOptions.equals(compactionStrategy.getOptions()))
            return;

        // TODO is there a way to avoid locking here?
        CompactionManager.instance.getCompactionLock().lock();
        try
        {
            compactionStrategy.shutdown();
            compactionStrategy = metadata.createCompactionStrategyInstance(this);
        }
        finally
        {
            CompactionManager.instance.getCompactionLock().unlock();
        }
    }

    public void setCompactionStrategyClass(String compactionStrategyClass) throws ConfigurationException
    {
        metadata.compactionStrategyClass = CFMetaData.createCompactionStrategy(compactionStrategyClass);
        maybeReloadCompactionStrategy();
    }

    public String getCompactionStrategyClass()
    {
        return metadata.compactionStrategyClass.getName();
    }

    public Map<String,String> getCompressionParameters()
    {
        return metadata.compressionParameters().asThriftOptions();
    }

    public void setCompressionParameters(Map<String,String> opts) throws ConfigurationException
    {
        metadata.compressionParameters = CompressionParameters.create(opts);
    }

    private ColumnFamilyStore(Table table,
                              String columnFamilyName,
                              IPartitioner partitioner,
                              int generation,
                              CFMetaData metadata,
                              Directories directories,
                              boolean loadSSTables)
    {
        assert metadata != null : "null metadata for " + table + ":" + columnFamilyName;

        this.table = table;
        columnFamily = columnFamilyName;
        this.metadata = metadata;
        this.minCompactionThreshold = new DefaultInteger(metadata.getMinCompactionThreshold());
        this.maxCompactionThreshold = new DefaultInteger(metadata.getMaxCompactionThreshold());
        this.partitioner = partitioner;
        this.directories = directories;
        this.indexManager = new SecondaryIndexManager(this);
        this.metric = new ColumnFamilyMetrics(this);
        fileIndexGenerator.set(generation);

        Caching caching = metadata.getCaching();

        if (logger.isDebugEnabled())
            logger.debug("Starting CFS {}", columnFamily);

        // scan for sstables corresponding to this cf and load them
        data = new DataTracker(this);

        if (loadSSTables)
        {
            Directories.SSTableLister sstableFiles = directories.sstableLister().skipTemporary(true);
            Collection<SSTableReader> sstables = SSTableReader.batchOpen(sstableFiles.list().entrySet(), metadata, this.partitioner);

            // Filter non-compacted sstables, remove compacted ones
            Set<Integer> compactedSSTables = new HashSet<Integer>();
            for (SSTableReader sstable : sstables)
                compactedSSTables.addAll(sstable.getAncestors());

            Set<SSTableReader> liveSSTables = new HashSet<SSTableReader>();
            for (SSTableReader sstable : sstables)
            {
                if (compactedSSTables.contains(sstable.descriptor.generation))
                    sstable.releaseReference(); // this amount to deleting the sstable
                else
                    liveSSTables.add(sstable);
            }
            data.addInitialSSTables(liveSSTables);
        }

        if (caching == Caching.ALL || caching == Caching.KEYS_ONLY)
            CacheService.instance.keyCache.loadSaved(this);

        // compaction strategy should be created after the CFS has been prepared
        this.compactionStrategy = metadata.createCompactionStrategyInstance(this);

        // create the private ColumnFamilyStores for the secondary column indexes
        for (ColumnDefinition info : metadata.getColumn_metadata().values())
        {
            if (info.getIndexType() != null)
                indexManager.addIndexedColumn(info);
        }

        // register the mbean
        String type = this.partitioner instanceof LocalPartitioner ? "IndexColumnFamilies" : "ColumnFamilies";
        mbeanName = "org.apache.cassandra.db:type=" + type + ",keyspace=" + this.table.name + ",columnfamily=" + columnFamily;
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName nameObj = new ObjectName(mbeanName);
            mbs.registerMBean(this, nameObj);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /** call when dropping or renaming a CF. Performs mbean housekeeping and invalidates CFS to other operations */
    public void invalidate()
    {
        try
        {
            valid = false;
            unregisterMBean();

            data.unreferenceSSTables();
            indexManager.invalidate();
        }
        catch (Exception e)
        {
            // this shouldn't block anything.
            logger.warn("Failed unregistering mbean: " + mbeanName, e);
        }
    }

    /**
     * Removes every SSTable in the directory from the DataTracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    void maybeRemoveUnreadableSSTables(File directory)
    {
        data.removeUnreadableSSTables(directory);
    }

    void unregisterMBean() throws MalformedObjectNameException, InstanceNotFoundException, MBeanRegistrationException
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName nameObj = new ObjectName(mbeanName);
        if (mbs.isRegistered(nameObj))
            mbs.unregisterMBean(nameObj);

        // unregister metrics
        metric.release();
    }

    public long getMinRowSize()
    {
        return metric.minRowSize.value();
    }

    public long getMaxRowSize()
    {
        return metric.maxRowSize.value();
    }

    public long getMeanRowSize()
    {
        return metric.meanRowSize.value();
    }

    public int getMeanColumns()
    {
        return data.getMeanColumns();
    }

    public static ColumnFamilyStore createColumnFamilyStore(Table table, String columnFamily, boolean loadSSTables)
    {
        return createColumnFamilyStore(table, columnFamily, StorageService.getPartitioner(), Schema.instance.getCFMetaData(table.name, columnFamily), loadSSTables);
    }

    public static ColumnFamilyStore createColumnFamilyStore(Table table, String columnFamily, IPartitioner partitioner, CFMetaData metadata)
    {
        return createColumnFamilyStore(table, columnFamily, partitioner, metadata, true);
    }

    private static synchronized ColumnFamilyStore createColumnFamilyStore(Table table,
                                                                         String columnFamily,
                                                                         IPartitioner partitioner,
                                                                         CFMetaData metadata,
                                                                         boolean loadSSTables)
    {
        // get the max generation number, to prevent generation conflicts
        Directories directories = Directories.create(table.name, columnFamily);
        Directories.SSTableLister lister = directories.sstableLister().includeBackups(true);
        List<Integer> generations = new ArrayList<Integer>();
        for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
        {
            Descriptor desc = entry.getKey();
            generations.add(desc.generation);
            if (!desc.isCompatible())
                throw new RuntimeException(String.format("Can't open incompatible SSTable! Current version %s, found file: %s", Descriptor.Version.CURRENT, desc));
        }
        Collections.sort(generations);
        int value = (generations.size() > 0) ? (generations.get(generations.size() - 1)) : 0;

        return new ColumnFamilyStore(table, columnFamily, partitioner, value, metadata, directories, loadSSTables);
    }

    /**
     * Removes unnecessary files from the cf directory at startup: these include temp files, orphans, zero-length files
     * and compacted sstables. Files that cannot be recognized will be ignored.
     */
    public static void scrubDataDirectories(String table, String columnFamily)
    {
        logger.debug("Removing compacted SSTable files from {} (see http://wiki.apache.org/cassandra/MemtableSSTable)", columnFamily);

        Directories directories = Directories.create(table, columnFamily);
        for (Map.Entry<Descriptor,Set<Component>> sstableFiles : directories.sstableLister().list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            Set<Component> components = sstableFiles.getValue();

            if (components.contains(Component.COMPACTED_MARKER) || desc.temporary)
            {
                SSTable.delete(desc, components);
                continue;
            }

            File dataFile = new File(desc.filenameFor(Component.DATA));
            if (components.contains(Component.DATA) && dataFile.length() > 0)
                // everything appears to be in order... moving on.
                continue;

            // missing the DATA file! all components are orphaned
            logger.warn("Removing orphans for {}: {}", desc, components);
            for (Component component : components)
            {
                FileUtils.deleteWithConfirm(desc.filenameFor(component));
            }
        }

        // cleanup incomplete saved caches
        Pattern tmpCacheFilePattern = Pattern.compile(table + "-" + columnFamily + "-(Key|Row)Cache.*\\.tmp$");
        File dir = new File(DatabaseDescriptor.getSavedCachesLocation());

        if (dir.exists())
        {
            assert dir.isDirectory();
            for (File file : dir.listFiles())
                if (tmpCacheFilePattern.matcher(file.getName()).matches())
                    if (!file.delete())
                        logger.warn("could not delete " + file.getAbsolutePath());
        }

        // also clean out any index leftovers.
        CFMetaData cfm = Schema.instance.getCFMetaData(table, columnFamily);
        if (cfm != null) // secondary indexes aren't stored in DD.
        {
            for (ColumnDefinition def : cfm.getColumn_metadata().values())
                scrubDataDirectories(table, cfm.indexColumnFamilyName(def));
        }
    }

    // must be called after all sstables are loaded since row cache merges all row versions
    public void initRowCache()
    {
        if (!isRowCacheEnabled())
            return;

        long start = System.currentTimeMillis();

        int cachedRowsRead = CacheService.instance.rowCache.loadSaved(this);
        if (cachedRowsRead > 0)
            logger.info(String.format("completed loading (%d ms; %d keys) row cache for %s.%s",
                        System.currentTimeMillis() - start,
                        cachedRowsRead,
                        table.name,
                        columnFamily));
    }

    /**
     * See #{@code StorageService.loadNewSSTables(String, String)} for more info
     *
     * @param ksName The keyspace name
     * @param cfName The columnFamily name
     */
    public static synchronized void loadNewSSTables(String ksName, String cfName)
    {
        /** ks/cf existence checks will be done by open and getCFS methods for us */
        Table table = Table.open(ksName);
        table.getColumnFamilyStore(cfName).loadNewSSTables();
    }

    /**
     * #{@inheritDoc}
     */
    public synchronized void loadNewSSTables()
    {
        logger.info("Loading new SSTables for " + table.name + "/" + columnFamily + "...");

        Set<Descriptor> currentDescriptors = new HashSet<Descriptor>();
        for (SSTableReader sstable : data.getView().sstables)
            currentDescriptors.add(sstable.descriptor);
        Set<SSTableReader> newSSTables = new HashSet<SSTableReader>();

        Directories.SSTableLister lister = directories.sstableLister().skipTemporary(true);
        for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
        {
            Descriptor descriptor = entry.getKey();

            if (currentDescriptors.contains(descriptor))
                continue; // old (initialized) SSTable found, skipping
            if (descriptor.temporary) // in the process of being written
                continue;

            if (!descriptor.isCompatible())
                throw new RuntimeException(String.format("Can't open incompatible SSTable! Current version %s, found file: %s",
                                                         Descriptor.Version.CURRENT,
                                                         descriptor));

            Descriptor newDescriptor = new Descriptor(descriptor.version,
                                                      descriptor.directory,
                                                      descriptor.ksname,
                                                      descriptor.cfname,
                                                      fileIndexGenerator.incrementAndGet(),
                                                      false);
            logger.info("Renaming new SSTable {} to {}", descriptor, newDescriptor);
            SSTableWriter.rename(descriptor, newDescriptor, entry.getValue());

            SSTableReader reader;
            try
            {
                reader = SSTableReader.open(newDescriptor, entry.getValue(), metadata, partitioner);
            }
            catch (IOException e)
            {
                SSTableReader.logOpenException(entry.getKey(), e);
                continue;
            }
            newSSTables.add(reader);
        }

        if (newSSTables.isEmpty())
        {
            logger.info("No new SSTables were found for " + table.name + "/" + columnFamily);
            return;
        }

        logger.info("Loading new SSTables and building secondary indexes for " + table.name + "/" + columnFamily + ": " + newSSTables);
        SSTableReader.acquireReferences(newSSTables);
        data.addSSTables(newSSTables);
        try
        {
            indexManager.maybeBuildSecondaryIndexes(newSSTables, indexManager.allIndexesNames());
        }
        finally
        {
            SSTableReader.releaseReferences(newSSTables);
        }

        logger.info("Done loading load new SSTables for " + table.name + "/" + columnFamily);
    }

    public static void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore cfs = Table.open(ksName).getColumnFamilyStore(cfName);

        Set<String> indexes = new HashSet<String>(Arrays.asList(idxNames));

        Collection<SSTableReader> sstables = cfs.getSSTables();
        try
        {
            cfs.indexManager.setIndexRemoved(indexes);
            SSTableReader.acquireReferences(sstables);
            logger.info(String.format("User Requested secondary index re-build for %s/%s indexes", ksName, cfName));
            cfs.indexManager.maybeBuildSecondaryIndexes(sstables, indexes);
            cfs.indexManager.setIndexBuilt(indexes);
        }
        finally
        {
            SSTableReader.releaseReferences(sstables);
        }
    }

    /**
     * @return the name of the column family
     */
    public String getColumnFamilyName()
    {
        return columnFamily;
    }

    public String getTempSSTablePath(File directory)
    {
        return getTempSSTablePath(directory, Descriptor.Version.CURRENT);
    }

    private String getTempSSTablePath(File directory, Descriptor.Version version)
    {
        Descriptor desc = new Descriptor(version,
                                         directory,
                                         table.name,
                                         columnFamily,
                                         fileIndexGenerator.incrementAndGet(),
                                         true);
        return desc.filenameFor(Component.DATA);
    }

    /** flush the given memtable and swap in a new one for its CFS, if it hasn't been frozen already.  threadsafe. */
    public Future<?> maybeSwitchMemtable(Memtable oldMemtable, final boolean writeCommitLog)
    {
        if (oldMemtable.isFrozen())
        {
            logger.debug("memtable is already frozen; another thread must be flushing it");
            return null;
        }

        /*
         * If we can get the writelock, that means no new updates can come in and
         * all ongoing updates to memtables have completed. We can get the tail
         * of the log and use it as the starting position for log replay on recovery.
         *
         * This is why we Table.switchLock needs to be global instead of per-Table:
         * we need to schedule discardCompletedSegments calls in the same order as their
         * contexts (commitlog position) were read, even though the flush executor
         * is multithreaded.
         */
        Table.switchLock.writeLock().lock();
        try
        {
            if (oldMemtable.isFrozen())
            {
                logger.debug("memtable is already frozen; another thread must be flushing it");
                return null;
            }

            assert getMemtableThreadSafe() == oldMemtable;
            final Future<ReplayPosition> ctx = writeCommitLog ? CommitLog.instance.getContext() : Futures.immediateFuture(ReplayPosition.NONE);

            // submit the memtable for any indexed sub-cfses, and our own.
            final List<ColumnFamilyStore> icc = new ArrayList<ColumnFamilyStore>();
            // don't assume that this.memtable is dirty; forceFlush can bring us here during index build even if it is not
            for (ColumnFamilyStore cfs : concatWithIndexes())
            {
                Memtable mt = cfs.getMemtableThreadSafe();
                if (!mt.isClean() && !mt.isFrozen())
                {
                    // We need to freeze indexes too because they can be concurrently flushed too (#3547)
                    mt.freeze();
                    icc.add(cfs);
                }
            }
            final CountDownLatch latch = new CountDownLatch(icc.size());
            for (ColumnFamilyStore cfs : icc)
            {
                Memtable memtable = cfs.data.switchMemtable();
                logger.info("Enqueuing flush of {}", memtable);
                memtable.flushAndSignal(latch, flushWriter, ctx);
            }

            if (metric.memtableSwitchCount.count() == Long.MAX_VALUE)
                metric.memtableSwitchCount.clear();
            metric.memtableSwitchCount.inc();

            // when all the memtables have been written, including for indexes, mark the flush in the commitlog header.
            // a second executor makes sure the onMemtableFlushes get called in the right order,
            // while keeping the wait-for-flush (future.get) out of anything latency-sensitive.
            return postFlushExecutor.submit(new WrappedRunnable()
            {
                public void runMayThrow() throws InterruptedException, ExecutionException
                {
                    latch.await();

                    if (!icc.isEmpty())
                    {
                        //only valid when memtables exist

                        for (SecondaryIndex index : indexManager.getIndexesNotBackedByCfs())
                        {
                            // flush any non-cfs backed indexes
                            logger.info("Flushing SecondaryIndex {}", index);
                            index.forceBlockingFlush();
                        }
                    }

                    if (writeCommitLog)
                    {
                        // if we're not writing to the commit log, we are replaying the log, so marking
                        // the log header with "you can discard anything written before the context" is not valid
                        CommitLog.instance.discardCompletedSegments(metadata.cfId, ctx.get());
                    }
                }
            });
        }
        finally
        {
            Table.switchLock.writeLock().unlock();
        }
    }

    public Future<?> forceFlush()
    {
        // during index build, 2ary index memtables can be dirty even if parent is not.  if so,
        // we want flushLargestMemtables to flush the 2ary index ones too.
        boolean clean = true;
        for (ColumnFamilyStore cfs : concatWithIndexes())
            clean &= cfs.getMemtableThreadSafe().isClean();

        if (clean)
        {
            logger.debug("forceFlush requested but everything is clean in {}", columnFamily);
            return null;
        }

        return maybeSwitchMemtable(getMemtableThreadSafe(), true);
    }

    public void forceBlockingFlush() throws ExecutionException, InterruptedException
    {
        Future<?> future = forceFlush();
        if (future != null)
            future.get();
    }

    public void updateRowCache(DecoratedKey key, ColumnFamily columnFamily)
    {
        if (metadata.cfId == null)
            return; // secondary index

        RowCacheKey cacheKey = new RowCacheKey(metadata.cfId, key);

        // always invalidate a copying cache value
        if (CacheService.instance.rowCache.isPutCopying())
        {
            invalidateCachedRow(cacheKey);
            return;
        }

        // invalidate a normal cache value if it's a sentinel, so the read will retry (and include the new update)
        IRowCacheEntry cachedRow = getCachedRowInternal(cacheKey);
        if (cachedRow != null)
        {
            if (cachedRow instanceof RowCacheSentinel)
                invalidateCachedRow(cacheKey);
            else
                // columnFamily is what is written in the commit log. Because of the PeriodicCommitLog, this can be done in concurrency
                // with this. So columnFamily shouldn't be modified and if it contains super columns, neither should they. So for super
                // columns, we must make sure to clone them when adding to the cache. That's what addAllWithSCCopy does (see #3957)
                ((ColumnFamily) cachedRow).addAllWithSCCopy(columnFamily, HeapAllocator.instance);
        }
    }

    /**
     * Insert/Update the column family for this key.
     * Caller is responsible for acquiring Table.flusherLock!
     * param @ lock - lock that needs to be used.
     * param @ key - key for update/insert
     * param @ columnFamily - columnFamily changes
     */
    public void apply(DecoratedKey key, ColumnFamily columnFamily, SecondaryIndexManager.Updater indexer)
    {
        long start = System.nanoTime();

        Memtable mt = getMemtableThreadSafe();
        mt.put(key, columnFamily, indexer);
        updateRowCache(key, columnFamily);
        metric.writeLatency.addNano(System.nanoTime() - start);

        // recompute liveRatio, if we have doubled the number of ops since last calculated
        while (true)
        {
            long last = liveRatioComputedAt.get();
            long operations = metric.writeLatency.latency.count();
            if (operations < 2 * last)
                break;
            if (liveRatioComputedAt.compareAndSet(last, operations))
            {
                logger.debug("computing liveRatio of {} at {} ops", this, operations);
                mt.updateLiveRatio();
            }
        }
    }

    public static ColumnFamily removeDeletedCF(ColumnFamily cf, int gcBefore)
    {
        cf.maybeResetDeletionTimes(gcBefore);
        return cf.getColumnCount() == 0 && !cf.isMarkedForDelete() ? null : cf;
    }

    public static ColumnFamily removeDeleted(ColumnFamily cf, int gcBefore)
    {
        return removeDeleted(cf, gcBefore, SecondaryIndexManager.nullUpdater);
    }

    /*
     This is complicated because we need to preserve deleted columns, supercolumns, and columnfamilies
     until they have been deleted for at least GC_GRACE_IN_SECONDS.  But, we do not need to preserve
     their contents; just the object itself as a "tombstone" that can be used to repair other
     replicas that do not know about the deletion.
     */
    public static ColumnFamily removeDeleted(ColumnFamily cf, int gcBefore, SecondaryIndexManager.Updater indexer)
    {
        if (cf == null)
        {
            return null;
        }

        removeDeletedColumnsOnly(cf, gcBefore, indexer);
        return removeDeletedCF(cf, gcBefore);
    }

    private static void removeDeletedColumnsOnly(ColumnFamily cf, int gcBefore, SecondaryIndexManager.Updater indexer)
    {
        if (cf.isSuper())
            removeDeletedSuper(cf, gcBefore);
        else
            removeDeletedStandard(cf, gcBefore, indexer);
    }

    public static void removeDeletedColumnsOnly(ColumnFamily cf, int gcBefore)
    {
        removeDeletedColumnsOnly(cf, gcBefore, SecondaryIndexManager.nullUpdater);
    }

    private static void removeDeletedStandard(ColumnFamily cf, int gcBefore, SecondaryIndexManager.Updater indexer)
    {
        Iterator<IColumn> iter = cf.iterator();
        while (iter.hasNext())
        {
            IColumn c = iter.next();
            // remove columns if
            // (a) the column itself is gcable or
            // (b) the column is shadowed by a CF tombstone
            if (c.getLocalDeletionTime() < gcBefore || cf.deletionInfo().isDeleted(c))
            {
                iter.remove();
                indexer.remove(c);
            }
        }
    }

    private static void removeDeletedSuper(ColumnFamily cf, int gcBefore)
    {
        // TODO assume deletion means "most are deleted?" and add to clone, instead of remove from original?
        // this could be improved by having compaction, or possibly even removeDeleted, r/m the tombstone
        // once gcBefore has passed, so if new stuff is added in it doesn't used the wrong algorithm forever
        Iterator<IColumn> iter = cf.iterator();
        while (iter.hasNext())
        {
            SuperColumn c = (SuperColumn)iter.next();
            Iterator<IColumn> subIter = c.getSubColumns().iterator();
            while (subIter.hasNext())
            {
                IColumn subColumn = subIter.next();
                // remove subcolumns if
                // (a) the subcolumn itself is gcable or
                // (b) the supercolumn is shadowed by the CF and the column is not newer
                // (b) the subcolumn is shadowed by the supercolumn
                if (subColumn.getLocalDeletionTime() < gcBefore
                    || cf.deletionInfo().isDeleted(c.name(), subColumn.timestamp())
                    || c.deletionInfo().isDeleted(subColumn))
                {
                    subIter.remove();
                }
            }
            c.maybeResetDeletionTimes(gcBefore);
            if (c.getSubColumns().isEmpty() && !c.isMarkedForDelete())
            {
                iter.remove();
            }
        }
    }

    /**
     * @param sstables
     * @return sstables whose key range overlaps with that of the given sstables, not including itself.
     * (The given sstables may or may not overlap with each other.)
     */
    public Set<SSTableReader> getOverlappingSSTables(Collection<SSTableReader> sstables)
    {
        // a normal compaction won't ever have an empty sstables list, but we create a skeleton
        // compaction controller for streaming, and that passes an empty list.
        if (sstables.isEmpty())
            return ImmutableSet.of();

        DataTracker.SSTableIntervalTree tree = data.getView().intervalTree;

        Set<SSTableReader> results = null;
        for (SSTableReader sstable : sstables)
        {
            Set<SSTableReader> overlaps = ImmutableSet.copyOf(tree.search(Interval.<RowPosition, SSTableReader>create(sstable.first, sstable.last)));
            assert overlaps.contains(sstable);
            results = results == null ? overlaps : Sets.union(results, overlaps);
        }
        results = Sets.difference(results, ImmutableSet.copyOf(sstables));

        return results;
    }

    /*
     * Called after a BinaryMemtable flushes its in-memory data, or we add a file
     * via bootstrap. This information is cached in the ColumnFamilyStore.
     * This is useful for reads because the ColumnFamilyStore first looks in
     * the in-memory store and the into the disk to find the key. If invoked
     * during recoveryMode the onMemtableFlush() need not be invoked.
     *
     * param @ filename - filename just flushed to disk
     */
    public void addSSTable(SSTableReader sstable)
    {
        assert sstable.getColumnFamilyName().equals(columnFamily);
        addSSTables(Arrays.asList(sstable));
    }

    public void addSSTables(Collection<SSTableReader> sstables)
    {
        data.addSSTables(sstables);
        CompactionManager.instance.submitBackground(this);
    }

    /**
     * Calculate expected file size of SSTable after compaction.
     *
     * If operation type is {@code CLEANUP} and we're not dealing with an index sstable,
     * then we calculate expected file size with checking token range to be eliminated.
     *
     * Otherwise, we just add up all the files' size, which is the worst case file
     * size for compaction of all the list of files given.
     *
     * @param sstables SSTables to calculate expected compacted file size
     * @param operation Operation type
     * @return Expected file size of SSTable after compaction
     */
    public long getExpectedCompactedFileSize(Iterable<SSTableReader> sstables, OperationType operation)
    {
        if (operation != OperationType.CLEANUP || isIndex())
        {
            return SSTable.getTotalBytes(sstables);
        }

        // cleanup size estimation only counts bytes for keys local to this node
        long expectedFileSize = 0;
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(table.name);
        for (SSTableReader sstable : sstables)
        {
            List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(ranges);
            for (Pair<Long, Long> position : positions)
                expectedFileSize += position.right - position.left;
        }
        return expectedFileSize;
    }

    /*
     *  Find the maximum size file in the list .
     */
    public SSTableReader getMaxSizeFile(Iterable<SSTableReader> sstables)
    {
        long maxSize = 0L;
        SSTableReader maxFile = null;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.onDiskLength() > maxSize)
            {
                maxSize = sstable.onDiskLength();
                maxFile = sstable;
            }
        }
        return maxFile;
    }

    public void forceCleanup(NodeId.OneShotRenewer renewer) throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.performCleanup(ColumnFamilyStore.this, renewer);
    }

    public void scrub() throws ExecutionException, InterruptedException
    {
        snapshotWithoutFlush("pre-scrub-" + System.currentTimeMillis());
        CompactionManager.instance.performScrub(ColumnFamilyStore.this);
    }

    public void sstablesRewrite() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.performSSTableRewrite(ColumnFamilyStore.this);
    }

    public void markCompacted(Collection<SSTableReader> sstables, OperationType compactionType)
    {
        assert !sstables.isEmpty();
        data.markCompacted(sstables, compactionType);
    }

    public void replaceCompactedSSTables(Collection<SSTableReader> sstables, Iterable<SSTableReader> replacements, OperationType compactionType)
    {
        data.replaceCompactedSSTables(sstables, replacements, compactionType);
    }

    void replaceFlushed(Memtable memtable, SSTableReader sstable)
    {
        data.replaceFlushed(memtable, sstable);
        CompactionManager.instance.submitBackground(this);
    }

    public boolean isValid()
    {
        return valid;
    }

    public long getMemtableColumnsCount()
    {
        return metric.memtableColumnsCount.value();
    }

    public long getMemtableDataSize()
    {
        return metric.memtableDataSize.value();
    }

    public long getTotalMemtableLiveSize()
    {
        return getMemtableDataSize() + indexManager.getTotalLiveSize();
    }

    public int getMemtableSwitchCount()
    {
        return (int) metric.memtableSwitchCount.count();
    }

    /**
     * get the current memtable in a threadsafe fashion.  note that simply "return memtable_" is
     * incorrect; you need to lock to introduce a thread safe happens-before ordering.
     *
     * do NOT use this method to do either a put or get on the memtable object, since it could be
     * flushed in the meantime (and its executor terminated).
     *
     * also do NOT make this method public or it will really get impossible to reason about these things.
     * @return
     */
    private Memtable getMemtableThreadSafe()
    {
        return data.getMemtable();
    }

    /**
     * Package protected for access from the CompactionManager.
     */
    public DataTracker getDataTracker()
    {
        return data;
    }

    public Collection<SSTableReader> getSSTables()
    {
        return data.getSSTables();
    }

    public Set<SSTableReader> getUncompactingSSTables()
    {
        return data.getUncompactingSSTables();
    }

    public long[] getRecentSSTablesPerReadHistogram()
    {
        return metric.recentSSTablesPerRead.getBuckets(true);
    }

    public long[] getSSTablesPerReadHistogram()
    {
        return metric.sstablesPerRead.getBuckets(false);
    }

    public long getReadCount()
    {
        return metric.readLatency.latency.count();
    }

    public double getRecentReadLatencyMicros()
    {
        return metric.readLatency.getRecentLatency();
    }

    public long[] getLifetimeReadLatencyHistogramMicros()
    {
        return metric.readLatency.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentReadLatencyHistogramMicros()
    {
        return metric.readLatency.recentLatencyHistogram.getBuckets(true);
    }

    public long getTotalReadLatencyMicros()
    {
        return metric.readLatency.totalLatency.count();
    }

    public int getPendingTasks()
    {
        return metric.pendingTasks.value();
    }

    public long getWriteCount()
    {
        return metric.writeLatency.latency.count();
    }

    public long getTotalWriteLatencyMicros()
    {
        return metric.writeLatency.totalLatency.count();
    }

    public double getRecentWriteLatencyMicros()
    {
        return metric.writeLatency.getRecentLatency();
    }

    public long[] getLifetimeWriteLatencyHistogramMicros()
    {
        return metric.writeLatency.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentWriteLatencyHistogramMicros()
    {
        return metric.writeLatency.recentLatencyHistogram.getBuckets(true);
    }

    public ColumnFamily getColumnFamily(DecoratedKey key, QueryPath path, ByteBuffer start, ByteBuffer finish, boolean reversed, int limit)
    {
        return getColumnFamily(QueryFilter.getSliceFilter(key, path, start, finish, reversed, limit));
    }

    /**
     * get a list of columns starting from a given column, in a specified order.
     * only the latest version of a column is returned.
     * @return null if there is no data and no tombstones; otherwise a ColumnFamily
     */
    public ColumnFamily getColumnFamily(QueryFilter filter)
    {
        return getColumnFamily(filter, gcBefore());
    }

    public ColumnFamily getColumnFamily(QueryFilter filter, ISortedColumns.Factory factory)
    {
        return getColumnFamily(filter, gcBefore());
    }

    public int gcBefore()
    {
        return (int) (System.currentTimeMillis() / 1000) - metadata.getGcGraceSeconds();
    }

    /**
     * fetch the row given by filter.key if it is in the cache; if not, read it from disk and cache it
     * @param cfId the column family to read the row from
     * @param filter the columns being queried.  Note that we still cache entire rows, but if a row is uncached
     *               and we race to cache it, only the winner will read the entire row
     * @return the entire row for filter.key, if present in the cache (or we can cache it), or just the column
     *         specified by filter otherwise
     */
    private ColumnFamily getThroughCache(UUID cfId, QueryFilter filter)
    {
        assert isRowCacheEnabled()
               : String.format("Row cache is not enabled on column family [" + getColumnFamilyName() + "]");

        RowCacheKey key = new RowCacheKey(cfId, filter.key);

        // attempt a sentinel-read-cache sequence.  if a write invalidates our sentinel, we'll return our
        // (now potentially obsolete) data, but won't cache it. see CASSANDRA-3862
        IRowCacheEntry cached = CacheService.instance.rowCache.get(key);
        if (cached != null)
        {
            if (cached instanceof RowCacheSentinel)
            {
                // Some other read is trying to cache the value, just do a normal non-caching read
                return getTopLevelColumns(filter, Integer.MIN_VALUE, false);
            }
            return (ColumnFamily) cached;
        }

        RowCacheSentinel sentinel = new RowCacheSentinel();
        boolean sentinelSuccess = CacheService.instance.rowCache.putIfAbsent(key, sentinel);

        try
        {
            ColumnFamily data = getTopLevelColumns(QueryFilter.getIdentityFilter(filter.key, new QueryPath(columnFamily)),
                                                   Integer.MIN_VALUE,
                                                   true);
            if (sentinelSuccess && data != null)
                CacheService.instance.rowCache.replace(key, sentinel, data);

            return data;
        }
        finally
        {
            if (sentinelSuccess && data == null)
                CacheService.instance.rowCache.remove(key);
        }
    }

    ColumnFamily getColumnFamily(QueryFilter filter, int gcBefore)
    {
        assert columnFamily.equals(filter.getColumnFamilyName()) : filter.getColumnFamilyName();

        ColumnFamily result = null;

        long start = System.nanoTime();
        try
        {

            if (!isRowCacheEnabled())
            {
                ColumnFamily cf = getTopLevelColumns(filter, gcBefore, false);

                if (cf == null)
                    return null;

                // TODO this is necessary because when we collate supercolumns together, we don't check
                // their subcolumns for relevance, so we need to do a second prune post facto here.
                result = cf.isSuper() ? removeDeleted(cf, gcBefore) : removeDeletedCF(cf, gcBefore);

            }
            else
            {

                UUID cfId = Schema.instance.getId(table.name, columnFamily);
                if (cfId == null)
                {
                    logger.debug("no id found for {}.{}", table.name, columnFamily);
                    return null;
                }

                ColumnFamily cached = getThroughCache(cfId, filter);
                if (cached == null)
                {
                    logger.debug("cached row is empty");
                    return null;
                }

                result = filterColumnFamily(cached, filter, gcBefore);
            }
        }
        finally
        {
            metric.readLatency.addNano(System.nanoTime() - start);
        }

        logger.debug("Read {} columns", result == null ? 0 : result.getColumnCount());
        return result;
    }

    /**
     *  Filter a cached row, which will not be modified by the filter, but may be modified by throwing out
     *  tombstones that are no longer relevant.
     *  The returned column family won't be thread safe.
     */
    ColumnFamily filterColumnFamily(ColumnFamily cached, QueryFilter filter, int gcBefore)
    {
        ColumnFamily cf = cached.cloneMeShallow(ArrayBackedSortedColumns.factory(), filter.filter.isReversed());
        OnDiskAtomIterator ci = filter.getMemtableColumnIterator(cached, null);
        filter.collateOnDiskAtom(cf, Collections.singletonList(ci), gcBefore);
        // TODO this is necessary because when we collate supercolumns together, we don't check
        // their subcolumns for relevance, so we need to do a second prune post facto here.
        return cf.isSuper() ? removeDeleted(cf, gcBefore) : removeDeletedCF(cf, gcBefore);
    }

    /**
     * Get the current view and acquires references on all its sstables.
     * This is a bit tricky because we must ensure that between the time we
     * get the current view and the time we acquire the references the set of
     * sstables hasn't changed. Otherwise we could get a view for which an
     * sstable have been deleted in the meantime.
     *
     * At the end of this method, a reference on all the sstables of the
     * returned view will have been acquired and must thus be released when
     * appropriate.
     */
    private DataTracker.View markCurrentViewReferenced()
    {
        while (true)
        {
            DataTracker.View currentView = data.getView();
            if (SSTableReader.acquireReferences(currentView.sstables))
                return currentView;
        }
    }

    /**
     * Get the current sstables, acquiring references on all of them.
     * The caller is in charge of releasing the references on the sstables.
     *
     * See markCurrentViewReferenced() above.
     */
    public Collection<SSTableReader> markCurrentSSTablesReferenced()
    {
        return markCurrentViewReferenced().sstables;
    }

    /**
     * @return a ViewFragment containing the sstables and memtables that may need to be merged
     * for the given @param key, according to the interval tree
     */
    public ViewFragment markReferenced(DecoratedKey key)
    {
        assert !key.isMinimum();
        DataTracker.View view;
        List<SSTableReader> sstables;
        while (true)
        {
            view = data.getView();
            sstables = view.intervalTree.search(key);
            if (SSTableReader.acquireReferences(sstables))
                break;
            // retry w/ new view
        }
        return new ViewFragment(sstables, Iterables.concat(Collections.singleton(view.memtable), view.memtablesPendingFlush));
    }

    /**
     * @return a ViewFragment containing the sstables and memtables that may need to be merged
     * for rows between @param startWith and @param stopAt, inclusive, according to the interval tree
     */
    public ViewFragment markReferenced(RowPosition startWith, RowPosition stopAt)
    {
        DataTracker.View view;
        List<SSTableReader> sstables;
        while (true)
        {
            view = data.getView();
            // startAt == minimum is ok, but stopAt == minimum is confusing because all IntervalTree deals with
            // is Comparable, so it won't know to special-case that. However max() should not be call if the
            // intervalTree is empty sochecking that first
            //
            if (view.intervalTree.isEmpty())
            {
                sstables = Collections.emptyList();
                break;
            }

            RowPosition stopInTree = stopAt.isMinimum() ? view.intervalTree.max() : stopAt;
            sstables = view.intervalTree.search(Interval.<RowPosition, SSTableReader>create(startWith, stopInTree));
            if (SSTableReader.acquireReferences(sstables))
                break;
            // retry w/ new view
        }
        return new ViewFragment(sstables, Iterables.concat(Collections.singleton(view.memtable), view.memtablesPendingFlush));
    }

    public List<String> getSSTablesForKey(String key)
    {
        DecoratedKey dk = new DecoratedKey(partitioner.getToken(ByteBuffer.wrap(key.getBytes())), ByteBuffer.wrap(key.getBytes()));
        ViewFragment view = markReferenced(dk);
        try
        {
            List<String> files = new ArrayList<String>();
            for (SSTableReader sstr : view.sstables)
            {
                // check if the key actually exists in this sstable, without updating cache and stats
                if (sstr.getPosition(dk, SSTableReader.Operator.EQ, false) != null)
                    files.add(sstr.getFilename());
            }
            return files;
        }
        finally {
            SSTableReader.releaseReferences(view.sstables);
        }
    }

    public ColumnFamily getTopLevelColumns(QueryFilter filter, int gcBefore, boolean forCache)
    {
        CollationController controller = new CollationController(this, forCache, filter, gcBefore);
        ColumnFamily columns = controller.getTopLevelColumns();
        metric.updateSSTableIterated(controller.getSstablesIterated());
        return columns;
    }

    public static abstract class AbstractScanIterator extends AbstractIterator<Row> implements CloseableIterator<Row>
    {
        public boolean needsFiltering()
        {
            return true;
        }
    }

    /**
      * Iterate over a range of rows and columns from memtables/sstables.
      *
      * @param superColumn optional SuperColumn to slice subcolumns of; null to slice top-level columns
      * @param range Either a Bounds, which includes start key, or a Range, which does not.
      * @param columnFilter description of the columns we're interested in for each row
     */
    public AbstractScanIterator getSequentialIterator(ByteBuffer superColumn, final AbstractBounds<RowPosition> range, IFilter columnFilter)
    {
        assert !(range instanceof Range) || !((Range)range).isWrapAround() || range.right.isMinimum() : range;

        final RowPosition startWith = range.left;
        final RowPosition stopAt = range.right;

        QueryFilter filter = new QueryFilter(null, new QueryPath(columnFamily, superColumn, null), columnFilter);

        List<Row> rows;
        final ViewFragment view = markReferenced(startWith, stopAt);
        try
        {
            final CloseableIterator<Row> iterator = RowIteratorFactory.getIterator(view.memtables, view.sstables, startWith, stopAt, filter, this);
            final int gcBefore = (int)(System.currentTimeMillis() / 1000) - metadata.getGcGraceSeconds();

            return new AbstractScanIterator()
            {
                protected Row computeNext()
                {
                    // pull a row out of the iterator
                    if (!iterator.hasNext())
                        return endOfData();

                    Row current = iterator.next();
                    DecoratedKey key = current.key;

                    if (!stopAt.isMinimum() && stopAt.compareTo(key) < 0)
                        return endOfData();

                    // skipping outside of assigned range
                    if (!range.contains(key))
                        return computeNext();

                    if (logger.isDebugEnabled())
                        logger.debug("scanned " + key);

                    // TODO this is necessary because when we collate supercolumns together, we don't check
                    // their subcolumns for relevance, so we need to do a second prune post facto here.
                    return current.cf != null && current.cf.isSuper()
                        ? new Row(current.key, removeDeleted(current.cf, gcBefore))
                        : current;
                }

                public void close() throws IOException
                {
                    SSTableReader.releaseReferences(view.sstables);
                    iterator.close();
                }
            };
        }
        catch (RuntimeException e)
        {
            // In case getIterator() throws, otherwise the iteror close method releases the references.
            SSTableReader.releaseReferences(view.sstables);
            throw e;
        }
    }

    public List<Row> getRangeSlice(ByteBuffer superColumn, final AbstractBounds<RowPosition> range, int maxResults, IFilter columnFilter, List<IndexExpression> rowFilter)
    {
        return getRangeSlice(superColumn, range, maxResults, columnFilter, rowFilter, false, false);
    }

    public List<Row> getRangeSlice(ByteBuffer superColumn, final AbstractBounds<RowPosition> range, int maxResults, IFilter columnFilter, List<IndexExpression> rowFilter, boolean maxIsColumns, boolean isPaging)
    {
        return filter(getSequentialIterator(superColumn, range, columnFilter), ExtendedFilter.create(this, columnFilter, rowFilter, maxResults, maxIsColumns, isPaging));
    }

    public List<Row> search(List<IndexExpression> clause, AbstractBounds<RowPosition> range, int maxResults, IFilter dataFilter)
    {
        return search(clause, range, maxResults, dataFilter, false);
    }

    public List<Row> search(List<IndexExpression> clause, AbstractBounds<RowPosition> range, int maxResults, IFilter dataFilter, boolean maxIsColumns)
    {
        return indexManager.search(clause, range, maxResults, dataFilter, maxIsColumns);
    }

    public List<Row> filter(AbstractScanIterator rowIterator, ExtendedFilter filter)
    {
        if (logger.isDebugEnabled())
            logger.debug("Filtering {} for rows matching {}", rowIterator, filter);
        List<Row> rows = new ArrayList<Row>();
        int columnsCount = 0;
        try
        {
            while (rowIterator.hasNext() && rows.size() < filter.maxRows() && columnsCount < filter.maxColumns())
            {
                // get the raw columns requested, and additional columns for the expressions if necessary
                Row rawRow = rowIterator.next();
                ColumnFamily data = rawRow.cf;

                if (rowIterator.needsFiltering())
                {
                    // roughtly
                    IFilter extraFilter = filter.getExtraFilter(data);
                    if (extraFilter != null)
                    {
                        QueryPath path = new QueryPath(columnFamily);
                        ColumnFamily cf = filter.cfs.getColumnFamily(new QueryFilter(rawRow.key, path, extraFilter));
                        if (cf != null)
                            data.addAll(cf, HeapAllocator.instance);
                    }

                    if (!filter.isSatisfiedBy(data, null))
                        continue;

                    logger.debug("{} satisfies all filter expressions", data);
                    // cut the resultset back to what was requested, if necessary
                    data = filter.prune(data);
                }

                rows.add(new Row(rawRow.key, data));

                if (data != null)
                    columnsCount += filter.lastCounted(data);
                // Update the underlying filter to avoid querying more columns per slice than necessary and to handle paging
                filter.updateFilter(columnsCount);
            }
            return rows;
        }
        finally
        {
            try
            {
                rowIterator.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public AbstractType<?> getComparator()
    {
        return metadata.comparator;
    }

    public void snapshotWithoutFlush(String snapshotName)
    {
        for (ColumnFamilyStore cfs : concatWithIndexes())
        {
            DataTracker.View currentView = cfs.markCurrentViewReferenced();

            try
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, snapshotName);
                    ssTable.createLinks(snapshotDirectory.getPath()); // hard links
                    if (logger.isDebugEnabled())
                        logger.debug("Snapshot for " + table + " keyspace data file " + ssTable.getFilename() +
                                     " created in " + snapshotDirectory);
                }

                if (cfs.compactionStrategy instanceof LeveledCompactionStrategy)
                    cfs.directories.snapshotLeveledManifest(snapshotName);
            }
            finally
            {
                SSTableReader.releaseReferences(currentView.sstables);
            }
        }
    }

    public List<SSTableReader> getSnapshotSSTableReader(String tag) throws IOException
    {
        Map<Descriptor, Set<Component>> snapshots = directories.sstableLister().snapshots(tag).list();
        List<SSTableReader> readers = new ArrayList<SSTableReader>(snapshots.size());
        for (Map.Entry<Descriptor, Set<Component>> entries : snapshots.entrySet())
            readers.add(SSTableReader.open(entries.getKey(), entries.getValue(), metadata, partitioner));
        return readers;
    }

    /**
     * Take a snap shot of this columnfamily store.
     *
     * @param snapshotName the name of the associated with the snapshot
     */
    public void snapshot(String snapshotName)
    {
        try
        {
            forceBlockingFlush();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }

        snapshotWithoutFlush(snapshotName);
    }

    public boolean snapshotExists(String snapshotName)
    {
        return directories.snapshotExists(snapshotName);
    }

    public void clearSnapshot(String snapshotName)
    {
        directories.clearSnapshot(snapshotName);
    }

    public boolean hasUnreclaimedSpace()
    {
        return getLiveDiskSpaceUsed() < getTotalDiskSpaceUsed();
    }

    public long getTotalDiskSpaceUsed()
    {
        return metric.totalDiskSpaceUsed.count();
    }

    public long getLiveDiskSpaceUsed()
    {
        return metric.liveDiskSpaceUsed.count();
    }

    public int getLiveSSTableCount()
    {
        return metric.liveSSTableCount.value();
    }

    /**
     * @return the cached row for @param key if it is already present in the cache.
     * That is, unlike getThroughCache, it will not readAndCache the row if it is not present, nor
     * are these calls counted in cache statistics.
     *
     * Note that this WILL cause deserialization of a SerializingCache row, so if all you
     * need to know is whether a row is present or not, use containsCachedRow instead.
     */
    public ColumnFamily getRawCachedRow(DecoratedKey key)
    {
        if (metadata.cfId == null)
            return null; // secondary index

        IRowCacheEntry cached = getCachedRowInternal(new RowCacheKey(metadata.cfId, key));
        return cached == null || cached instanceof RowCacheSentinel ? null : (ColumnFamily) cached;
    }

    private IRowCacheEntry getCachedRowInternal(RowCacheKey key)
    {
        return CacheService.instance.rowCache.getCapacity() == 0 ? null : CacheService.instance.rowCache.getInternal(key);
    }

    /**
     * @return true if @param key is contained in the row cache
     */
    public boolean containsCachedRow(DecoratedKey key)
    {
        return CacheService.instance.rowCache.getCapacity() != 0 && CacheService.instance.rowCache.containsKey(new RowCacheKey(metadata.cfId, key));
    }

    public void invalidateCachedRow(RowCacheKey key)
    {
        CacheService.instance.rowCache.remove(key);
    }

    public void invalidateCachedRow(DecoratedKey key)
    {
        UUID cfId = Schema.instance.getId(table.name, this.columnFamily);
        if (cfId == null)
            return; // secondary index

        invalidateCachedRow(new RowCacheKey(cfId, key));
    }

    public void forceMajorCompaction() throws InterruptedException, ExecutionException
    {
        CompactionManager.instance.performMaximal(this);
    }

    public static Iterable<ColumnFamilyStore> all()
    {
        List<Iterable<ColumnFamilyStore>> stores = new ArrayList<Iterable<ColumnFamilyStore>>(Schema.instance.getTables().size());
        for (Table table : Table.all())
        {
            stores.add(table.getColumnFamilyStores());
        }
        return Iterables.concat(stores);
    }

    public static List<ColumnFamilyStore> allUserDefined()
    {
        List<ColumnFamilyStore> cfses = new ArrayList<ColumnFamilyStore>();
        for (Table table : Sets.difference(ImmutableSet.copyOf(Table.all()), ImmutableSet.of(Table.open(Table.SYSTEM_KS))))
            cfses.addAll(table.getColumnFamilyStores());
        return cfses;
    }

    public Iterable<DecoratedKey> keySamples(Range<Token> range)
    {
        Collection<SSTableReader> sstables = getSSTables();
        Iterable<DecoratedKey>[] samples = new Iterable[sstables.size()];
        int i = 0;
        for (SSTableReader sstable: sstables)
        {
            samples[i++] = sstable.getKeySamples(range);
        }
        return Iterables.concat(samples);
    }

    /**
     * For testing.  No effort is made to clear historical or even the current memtables, nor for
     * thread safety.  All we do is wipe the sstable containers clean, while leaving the actual
     * data files present on disk.  (This allows tests to easily call loadNewSSTables on them.)
     */
    public void clearUnsafe()
    {
        for (ColumnFamilyStore cfs : concatWithIndexes())
            cfs.data.init();
    }

    /**
     * Waits for flushes started BEFORE THIS METHOD IS CALLED to finish.
     * Does NOT guarantee that no flush is active when it returns.
     */
    private void waitForActiveFlushes()
    {
        Future<?> future;
        Table.switchLock.writeLock().lock();
        try
        {
            future = postFlushExecutor.submit(new Runnable() { public void run() { } });
        }
        finally
        {
            Table.switchLock.writeLock().unlock();
        }

        try
        {
            future.get();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Truncate practically deletes the entire column family's data
     * @return a Future to the delete operation. Call the future's get() to make
     * sure the column family has been deleted
     */
    public Future<?> truncate() throws ExecutionException, InterruptedException
    {
        // We have two goals here:
        // - truncate should delete everything written before truncate was invoked
        // - but not delete anything that isn't part of the snapshot we create.
        // We accomplish this by first flushing manually, then snapshotting, and
        // recording the timestamp IN BETWEEN those actions. Any sstables created
        // with this timestamp or greater time, will not be marked for delete.
        //
        // Bonus complication: since we store replay position in sstable metadata,
        // truncating those sstables means we will replay any CL segments from the
        // beginning if we restart before they are discarded for normal reasons
        // post-truncate.  So we need to (a) force a new segment so the currently
        // active one can be discarded, and (b) flush *all* CFs so that unflushed
        // data in others don't keep any pre-truncate CL segments alive.
        //
        // Bonus bonus: simply forceFlush of all the CF is not enough, because if
        // for a given column family the memtable is clean, forceFlush will return
        // immediately, even though there could be a memtable being flushed at the same
        // time.  So to guarantee that all segments can be cleaned out, we need to
        // "waitForActiveFlushes" after the new segment has been created.
        logger.debug("truncating {}", columnFamily);

        if (DatabaseDescriptor.isAutoSnapshot())
        {
            // flush the CF being truncated before forcing the new segment
            forceBlockingFlush();
        }
        else
        {
            // just nuke the memtable data w/o writing to disk first
            Table.switchLock.writeLock().lock();
            try
            {
                for (ColumnFamilyStore cfs : concatWithIndexes())
                {
                    Memtable mt = cfs.getMemtableThreadSafe();
                    if (!mt.isClean() && !mt.isFrozen())
                    {
                        mt.cfs.data.renewMemtable();
                    }
                }
            }
            finally
            {
                Table.switchLock.writeLock().unlock();
            }
        }

        KSMetaData ksm = Schema.instance.getKSMetaData(this.table.name);
        if (ksm.durableWrites)
        {
            CommitLog.instance.forceNewSegment();
            Future<ReplayPosition> position = CommitLog.instance.getContext();
            // now flush everyone else.  re-flushing ourselves is not necessary, but harmless
            for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
                cfs.forceFlush();
            waitForActiveFlushes();
            // if everything was clean, flush won't have called discard
            CommitLog.instance.discardCompletedSegments(metadata.cfId, position.get());
        }

        // sleep a little to make sure that our truncatedAt comes after any sstable
        // that was part of the flushed we forced; otherwise on a tie, it won't get deleted.
        try
        {
            long starttime = System.currentTimeMillis();
            while ((System.currentTimeMillis() - starttime) < 1)
            {
                Thread.sleep(1);
            }
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        long truncatedAt = System.currentTimeMillis();
        if (DatabaseDescriptor.isAutoSnapshot())
            snapshot(Table.getTimestampedSnapshotName(columnFamily));

        return CompactionManager.instance.submitTruncate(this, truncatedAt);
    }

    public long getBloomFilterFalsePositives()
    {
        return metric.bloomFilterFalsePositives.value();
    }

    public long getRecentBloomFilterFalsePositives()
    {
        return metric.recentBloomFilterFalsePositives.value();
    }

    public double getBloomFilterFalseRatio()
    {
        return metric.bloomFilterFalseRatio.value();
    }

    public double getRecentBloomFilterFalseRatio()
    {
        return metric.recentBloomFilterFalseRatio.value();
    }

    public long getBloomFilterDiskSpaceUsed()
    {
        return metric.bloomFilterDiskSpaceUsed.value();
    }

    @Override
    public String toString()
    {
        return "CFS(" +
               "Keyspace='" + table.name + '\'' +
               ", ColumnFamily='" + columnFamily + '\'' +
               ')';
    }

    public void disableAutoCompaction()
    {
        minCompactionThreshold.set(0);
        maxCompactionThreshold.set(0);
    }

    /*
     JMX getters and setters for the Default<T>s.
       - get/set minCompactionThreshold
       - get/set maxCompactionThreshold
       - get     memsize
       - get     memops
       - get/set memtime
     */

    public AbstractCompactionStrategy getCompactionStrategy()
    {
        return compactionStrategy;
    }

    public void setCompactionThresholds(int minThreshold, int maxThreshold)
    {
        validateCompactionThresholds(minThreshold, maxThreshold);

        minCompactionThreshold.set(minThreshold);
        maxCompactionThreshold.set(maxThreshold);

        // this is called as part of CompactionStrategy constructor; avoid circular dependency by checking for null
        if (compactionStrategy != null)
            CompactionManager.instance.submitBackground(this);
    }

    public int getMinimumCompactionThreshold()
    {
        return minCompactionThreshold.value();
    }

    public void setMinimumCompactionThreshold(int minCompactionThreshold)
    {
        validateCompactionThresholds(minCompactionThreshold, maxCompactionThreshold.value());
        this.minCompactionThreshold.set(minCompactionThreshold);
    }

    public int getMaximumCompactionThreshold()
    {
        return maxCompactionThreshold.value();
    }

    public void setMaximumCompactionThreshold(int maxCompactionThreshold)
    {
        validateCompactionThresholds(minCompactionThreshold.value(), maxCompactionThreshold);
        this.maxCompactionThreshold.set(maxCompactionThreshold);
    }

    private void validateCompactionThresholds(int minThreshold, int maxThreshold)
    {
        if (minThreshold > maxThreshold && maxThreshold != 0)
            throw new RuntimeException(String.format("The min_compaction_threshold cannot be larger than the max_compaction_threshold. " +
                                                     "Min is '%d', Max is '%d'.", minThreshold, maxThreshold));
    }

    public boolean isCompactionDisabled()
    {
        return getMinimumCompactionThreshold() <= 0 || getMaximumCompactionThreshold() <= 0;
    }

    // End JMX get/set.

    public long estimateKeys()
    {
        return data.estimatedKeys();
    }

    public long[] getEstimatedRowSizeHistogram()
    {
        return metric.estimatedRowSizeHistogram.value();
    }

    public long[] getEstimatedColumnCountHistogram()
    {
        return metric.estimatedColumnCountHistogram.value();
    }

    public double getCompressionRatio()
    {
        return metric.compressionRatio.value();
    }

    /** true if this CFS contains secondary index data */
    public boolean isIndex()
    {
        return partitioner instanceof LocalPartitioner;
    }

    private ByteBuffer intern(ByteBuffer name)
    {
        ByteBuffer internedName = internedNames.get(name);
        if (internedName == null)
        {
            internedName = ByteBufferUtil.clone(name);
            ByteBuffer concurrentName = internedNames.putIfAbsent(internedName, internedName);
            if (concurrentName != null)
                internedName = concurrentName;
        }
        return internedName;
    }

    public ByteBuffer internOrCopy(ByteBuffer name, Allocator allocator)
    {
        if (internedNames.size() >= INTERN_CUTOFF)
            return allocator.clone(name);

        return intern(name);
    }

    public ByteBuffer maybeIntern(ByteBuffer name)
    {
        if (internedNames.size() >= INTERN_CUTOFF)
            return null;

        return intern(name);
    }

    public SSTableWriter createCompactionWriter(long estimatedRows, File location, Collection<SSTableReader> sstables)
    {
        ReplayPosition rp = ReplayPosition.getReplayPosition(sstables);
        SSTableMetadata.Collector sstableMetadataCollector = SSTableMetadata.createCollector().replayPosition(rp);

        // Get the max timestamp of the precompacted sstables
        // and adds generation of live ancestors
        for (SSTableReader sstable : sstables)
        {
            sstableMetadataCollector.updateMaxTimestamp(sstable.getMaxTimestamp());
            sstableMetadataCollector.addAncestor(sstable.descriptor.generation);
            for (Integer i : sstable.getAncestors())
            {
                if (new File(sstable.descriptor.withGeneration(i).filenameFor(Component.DATA)).exists())
                    sstableMetadataCollector.addAncestor(i);
            }
        }

        return new SSTableWriter(getTempSSTablePath(location), estimatedRows, metadata, partitioner, sstableMetadataCollector);
    }

    public Iterable<ColumnFamilyStore> concatWithIndexes()
    {
        return Iterables.concat(indexManager.getIndexesBackedByCfs(), Collections.singleton(this));
    }

    public Set<Memtable> getMemtablesPendingFlush()
    {
        return data.getMemtablesPendingFlush();
    }

    public List<String> getBuiltIndexes()
    {
       return indexManager.getBuiltIndexes();
    }

    public int getUnleveledSSTables()
    {
        return this.compactionStrategy instanceof LeveledCompactionStrategy
               ? ((LeveledCompactionStrategy) this.compactionStrategy).getLevelSize(0)
               : 0;
    }

    public static class ViewFragment
    {
        public final List<SSTableReader> sstables;
        public final Iterable<Memtable> memtables;

        public ViewFragment(List<SSTableReader> sstables, Iterable<Memtable> memtables)
        {
            this.sstables = sstables;
            this.memtables = memtables;
        }
    }

    /**
     * Returns the creation time of the oldest memtable not fully flushed yet.
     */
    public long oldestUnflushedMemtable()
    {
        DataTracker.View view = data.getView();
        long oldest = view.memtable.creationTime();
        for (Memtable memtable : view.memtablesPendingFlush)
            oldest = Math.min(oldest, memtable.creationTime());
        return oldest;
    }

    public boolean isEmpty()
    {
        DataTracker.View view = data.getView();
        return view.sstables.isEmpty() && view.memtable.getOperations() == 0 && view.memtablesPendingFlush.isEmpty();
    }

    private boolean isRowCacheEnabled()
    {
        return !(metadata.getCaching() == Caching.NONE
              || metadata.getCaching() == Caching.KEYS_ONLY
              || CacheService.instance.rowCache.getCapacity() == 0);
    }

    /**
     * Discard all SSTables that were created before given timestamp. Caller is responsible to obtain compactionLock.
     *
     * @param truncatedAt The timestamp of the truncation
     *                    (all SSTables before that timestamp are going be marked as compacted)
     */
    public void discardSSTables(long truncatedAt)
    {
        List<SSTableReader> truncatedSSTables = new ArrayList<SSTableReader>();

        for (SSTableReader sstable : getSSTables())
        {
            if (!sstable.newSince(truncatedAt))
                truncatedSSTables.add(sstable);
        }

        if (!truncatedSSTables.isEmpty())
            markCompacted(truncatedSSTables, OperationType.UNKNOWN);
    }
}
