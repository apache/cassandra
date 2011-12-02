/**
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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import javax.management.*;

import com.google.common.collect.Iterables;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.IntervalTree.Interval;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean
{
    private static Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    /*
     * maybeSwitchMemtable puts Memtable.getSortedContents on the writer executor.  When the write is complete,
     * we turn the writer into an SSTableReader and add it to ssTables_ where it is available for reads.
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

    private volatile int memtableSwitchCount = 0;

    /* This is used to generate the next index for a SSTable */
    private AtomicInteger fileIndexGenerator = new AtomicInteger(0);

    public final SecondaryIndexManager indexManager;

    private LatencyTracker readStats = new LatencyTracker();
    private LatencyTracker writeStats = new LatencyTracker();

    // counts of sstables accessed by reads
    private final EstimatedHistogram recentSSTablesPerRead = new EstimatedHistogram(35);
    private final EstimatedHistogram sstablesPerRead = new EstimatedHistogram(35);

    private static final int INTERN_CUTOFF = 256;
    public final ConcurrentMap<ByteBuffer, ByteBuffer> internedNames = new NonBlockingHashMap<ByteBuffer, ByteBuffer>();

    /* These are locally held copies to be changed from the config during runtime */
    private volatile DefaultInteger minCompactionThreshold;
    private volatile DefaultInteger maxCompactionThreshold;
    private volatile AbstractCompactionStrategy compactionStrategy;
    private volatile DefaultInteger rowCacheSaveInSeconds;
    private volatile DefaultInteger keyCacheSaveInSeconds;
    private volatile DefaultInteger rowCacheKeysToSave;

    /** Lock to allow migrations to block all flushing, so we can be sure not to write orphaned data files */
    public final Lock flushLock = new ReentrantLock();

    public static enum CacheType
    {
        KEY_CACHE_TYPE("KeyCache"),
        ROW_CACHE_TYPE("RowCache");

        public final String name;

        private CacheType(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    public final AutoSavingCache<Pair<Descriptor,DecoratedKey>, Long> keyCache;
    public final AutoSavingCache<DecoratedKey, ColumnFamily> rowCache;


    /** ratio of in-memory memtable size, to serialized size */
    volatile double liveRatio = 1.0;
    /** ops count last time we computed liveRatio */
    private final AtomicLong liveRatioComputedAt = new AtomicLong(32);

    public void reload() throws IOException
    {
        // metadata object has been mutated directly. make all the members jibe with new settings.

        // only update these runtime-modifiable settings if they have not been modified.
        if (!minCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.minCompactionThreshold = new DefaultInteger(metadata.getMinCompactionThreshold());
        if (!maxCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.maxCompactionThreshold = new DefaultInteger(metadata.getMaxCompactionThreshold());
        if (!rowCacheSaveInSeconds.isModified())
            rowCacheSaveInSeconds = new DefaultInteger(metadata.getRowCacheSavePeriodInSeconds());
        if (!keyCacheSaveInSeconds.isModified())
            keyCacheSaveInSeconds = new DefaultInteger(metadata.getKeyCacheSavePeriodInSeconds());
        if (!rowCacheKeysToSave.isModified())
            rowCacheKeysToSave = new DefaultInteger(metadata.getRowCacheKeysToSave());

        maybeReloadCompactionStrategy();

        updateCacheSizes();
        scheduleCacheSaving(rowCacheSaveInSeconds.value(), keyCacheSaveInSeconds.value(), rowCacheKeysToSave.value());

        indexManager.reload();
    }

    private void maybeReloadCompactionStrategy()
    {
        // Check if there is a need for reloading
        if (metadata.compactionStrategyClass.equals(compactionStrategy.getClass()) && metadata.compactionStrategyOptions.equals(compactionStrategy.getOptions()))
            return;

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

    private ColumnFamilyStore(Table table, String columnFamilyName, IPartitioner partitioner, int generation, CFMetaData metadata)
    {
        assert metadata != null : "null metadata for " + table + ":" + columnFamilyName;
        this.table = table;
        columnFamily = columnFamilyName;
        this.metadata = metadata;
        this.minCompactionThreshold = new DefaultInteger(metadata.getMinCompactionThreshold());
        this.maxCompactionThreshold = new DefaultInteger(metadata.getMaxCompactionThreshold());
        this.rowCacheSaveInSeconds = new DefaultInteger(metadata.getRowCacheSavePeriodInSeconds());
        this.keyCacheSaveInSeconds = new DefaultInteger(metadata.getKeyCacheSavePeriodInSeconds());
        this.rowCacheKeysToSave = new DefaultInteger(metadata.getRowCacheKeysToSave());
        this.partitioner = partitioner;
        this.indexManager = new SecondaryIndexManager(this);
        fileIndexGenerator.set(generation);

        if (logger.isDebugEnabled())
            logger.debug("Starting CFS {}", columnFamily);

        ICache<Pair<Descriptor, DecoratedKey>, Long> kc = ConcurrentLinkedHashCache.create(0, table.name, columnFamilyName);
        keyCache = new AutoSavingKeyCache<Pair<Descriptor, DecoratedKey>, Long>(kc, table.name, columnFamilyName);
        ICache<DecoratedKey, ColumnFamily> rc = metadata.getRowCacheProvider().create(0, table.name, columnFamilyName);
        rowCache = new AutoSavingRowCache<DecoratedKey, ColumnFamily>(rc, table.name, columnFamilyName);

        // scan for sstables corresponding to this cf and load them
        data = new DataTracker(this);
        Set<DecoratedKey> savedKeys = keyCache.readSaved();
        Set<Map.Entry<Descriptor, Set<Component>>> entries = files(table.name, columnFamilyName, false, false).entrySet();
        data.addInitialSSTables(SSTableReader.batchOpen(entries, savedKeys, data, metadata, this.partitioner));

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
            data.removeAllSSTables();
            indexManager.invalidate();
        }
        catch (Exception e)
        {
            // this shouldn't block anything.
            logger.warn("Failed unregistering mbean: " + mbeanName, e);
        }
    }

    void unregisterMBean() throws MalformedObjectNameException, InstanceNotFoundException, MBeanRegistrationException
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName nameObj = new ObjectName(mbeanName);
        if (mbs.isRegistered(nameObj))
            mbs.unregisterMBean(nameObj);
    }

    public long getMinRowSize()
    {
        return data.getMinRowSize();
    }

    public long getMaxRowSize()
    {
        return data.getMaxRowSize();
    }

    public long getMeanRowSize()
    {
        return data.getMeanRowSize();
    }

    public int getMeanColumns()
    {
        return data.getMeanColumns();
    }

    public static ColumnFamilyStore createColumnFamilyStore(Table table, String columnFamily)
    {
        return createColumnFamilyStore(table, columnFamily, StorageService.getPartitioner(), Schema.instance.getCFMetaData(table.name, columnFamily));
    }

    public static synchronized ColumnFamilyStore createColumnFamilyStore(Table table, String columnFamily, IPartitioner partitioner, CFMetaData metadata)
    {
        // get the max generation number, to prevent generation conflicts
        List<Integer> generations = new ArrayList<Integer>();
        for (String path : DatabaseDescriptor.getAllDataFileLocationsForTable(table.name))
        {
            Iterable<Pair<Descriptor, Component>> pairs = files(new File(path), columnFamily);
            File incrementalsPath = new File(path, "backups");
            if (incrementalsPath.exists())
                pairs = Iterables.concat(pairs, files(incrementalsPath, columnFamily));

            for (Pair<Descriptor, Component> pair : pairs)
            {
                Descriptor desc = pair.left;
                if (!desc.cfname.equals(columnFamily))
                    continue;
                generations.add(desc.generation);
                if (!desc.isCompatible())
                    throw new RuntimeException(String.format("Can't open incompatible SSTable! Current version %s, found file: %s",
                                                             Descriptor.CURRENT_VERSION, desc));
            }
        }
        Collections.sort(generations);
        int value = (generations.size() > 0) ? (generations.get(generations.size() - 1)) : 0;

        return new ColumnFamilyStore(table, columnFamily, partitioner, value, metadata);
    }

    /**
     * Removes unnecessary files from the cf directory at startup: these include temp files, orphans, zero-length files
     * and compacted sstables. Files that cannot be recognized will be ignored.
     */
    public static void scrubDataDirectories(String table, String columnFamily)
    {
        logger.debug("Removing compacted SSTable files from {} (see http://wiki.apache.org/cassandra/MemtableSSTable)", columnFamily);

        for (Map.Entry<Descriptor,Set<Component>> sstableFiles : files(table, columnFamily, true, true).entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            Set<Component> components = sstableFiles.getValue();

            if (components.contains(Component.COMPACTED_MARKER) || desc.temporary)
            {
                try
                {
                    SSTable.delete(desc, components);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
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
                try
                {
                    FileUtils.deleteWithConfirm(desc.filenameFor(component));
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
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
    public void initCaches()
    {
        long start = System.currentTimeMillis();
        // results are sorted on read (via treeset) because there are few reads and many writes and reads only happen at startup
        int cachedRowsRead = 0;
        for (DecoratedKey key : rowCache.readSaved())
        {
            cacheRow(key);
            if (cachedRowsRead++ > rowCache.getCapacity())
            {
                logger.debug(String.format("Stopped loading row cache after capacity %d was reached", rowCache.getCapacity()));
                break;
            }
        }
        if (rowCache.size() > 0)
            logger.info(String.format("completed loading (%d ms; %d keys) row cache for %s.%s",
                                      System.currentTimeMillis()-start,
                                      rowCache.size(),
                                      table.name,
                                      columnFamily));

        scheduleCacheSaving(metadata.getRowCacheSavePeriodInSeconds(), metadata.getKeyCacheSavePeriodInSeconds(), metadata.getRowCacheKeysToSave());
    }

    public void scheduleCacheSaving(int rowCacheSavePeriodInSeconds, int keyCacheSavePeriodInSeconds, int rowCacheKeysToSave)
    {
        keyCache.scheduleSaving(keyCacheSavePeriodInSeconds, Integer.MAX_VALUE);
        rowCache.scheduleSaving(rowCacheSavePeriodInSeconds, rowCacheKeysToSave);
    }

    public AutoSavingCache<Pair<Descriptor,DecoratedKey>, Long> getKeyCache()
    {
        return keyCache;
    }

    /**
     * Collects a map of sstable components.
     */
    private static Map<Descriptor,Set<Component>> files(String keyspace, final String columnFamily, final boolean includeCompacted, final boolean includeTemporary)
    {
        final Map<Descriptor,Set<Component>> sstables = new HashMap<Descriptor,Set<Component>>();
        for (String directory : DatabaseDescriptor.getAllDataFileLocationsForTable(keyspace))
        {
            for (Pair<Descriptor, Component> component : files(new File(directory), columnFamily))
            {
                if (component != null)
                {
                    if ((includeCompacted || !new File(component.left.filenameFor(Component.COMPACTED_MARKER)).exists())
                     && (includeTemporary || !component.left.temporary))
                    {
                        Set<Component> components = sstables.get(component.left);
                        if (components == null)
                        {
                            components = new HashSet<Component>();
                            sstables.put(component.left, components);
                        }
                        components.add(component.right);
                    }
                    else
                        logger.debug("not including compacted sstable " + component.left.cfname + "-" + component.left.generation);
                }
            }
        }
        return sstables;
    }

    private static List<Pair<Descriptor, Component>> files(File path, final String columnFamilyName)
    {
        final List<Pair<Descriptor, Component>> sstables = new ArrayList<Pair<Descriptor, Component>>();
        final String sstableFilePrefix = columnFamilyName + Component.separator;

        // NB: we never "accept" a file in the FilenameFilter sense: they are added to the sstable map
        path.listFiles(new FileFilter()
        {
            public boolean accept(File file)
            {
                // we are only interested in the SSTable files that belong to the specific ColumnFamily
                if (file.isDirectory() || !file.getName().startsWith(sstableFilePrefix))
                    return false;

                Pair<Descriptor, Component> pair = SSTable.tryComponentFromFilename(file.getParentFile(), file.getName());

                if (pair != null)
                    sstables.add(pair);

                return false;
            }
        });

        return sstables;
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

        // current view over ColumnFamilyStore
        DataTracker.View view = data.getView();
        // descriptors of currently registered SSTables
        Set<Descriptor> currentDescriptors = new HashSet<Descriptor>();
        // going to hold new SSTable view of the CFS containing old and new SSTables
        Set<SSTableReader> sstables = new HashSet<SSTableReader>();
        Set<DecoratedKey> savedKeys = keyCache.readSaved();
        // get the max generation number, to prevent generation conflicts
        int generation = 0;

        for (SSTableReader reader : view.sstables)
        {
            sstables.add(reader); // first of all, add old SSTables
            currentDescriptors.add(reader.descriptor);

            if (reader.descriptor.generation > generation)
                generation = reader.descriptor.generation;
        }

        SSTableReader reader;
        // set to true if we have at least one new SSTable to load
        boolean atLeastOneNew = false;

        for (Map.Entry<Descriptor, Set<Component>> rawSSTable : files(table.name, columnFamily, false, false).entrySet())
        {
            Descriptor descriptor = rawSSTable.getKey();

            if (currentDescriptors.contains(descriptor))
                continue; // old (initialized) SSTable found, skipping

            if (!descriptor.cfname.equals(columnFamily))
                continue;

            if (!descriptor.isCompatible())
                throw new RuntimeException(String.format("Can't open incompatible SSTable! Current version %s, found file: %s",
                                                         Descriptor.CURRENT_VERSION,
                                                         descriptor));

            logger.info("Initializing new SSTable {}", rawSSTable);
            try
            {
                reader = SSTableReader.open(rawSSTable.getKey(), rawSSTable.getValue(), savedKeys, data, metadata, partitioner);
            }
            catch (IOException e)
            {
                SSTableReader.logOpenException(rawSSTable.getKey(), e);
                continue;
            }

            sstables.add(reader);

            if (descriptor.generation > generation)
                generation = descriptor.generation;

            if (!atLeastOneNew) // set flag only once
                atLeastOneNew = true;
        }

        if (!atLeastOneNew)
        {
            logger.info("No new SSTables where found for " + table.name + "/" + columnFamily);
            return;
        }

        logger.info("Loading new SSTable Set for " + table.name + "/" + columnFamily + ": " + sstables);
        SSTableReader.acquireReferences(sstables);
        data.addSSTables(sstables); // this will call updateCacheSizes() for us
        logger.info("Requesting a full secondary index re-build for " + table.name + "/" + columnFamily);
        try
        {
            indexManager.maybeBuildSecondaryIndexes(sstables, indexManager.getIndexedColumns());
        }
        catch (IOException e)
        {
           throw new IOError(e);
        }
        finally
        {
            SSTableReader.releaseReferences(sstables);
        }

        logger.info("Setting up new generation: " + generation);
        fileIndexGenerator.set(generation);

        logger.info("Done loading load new SSTables for " + table.name + "/" + columnFamily);
    }

    /**
     * @return the name of the column family
     */
    public String getColumnFamilyName()
    {
        return columnFamily;
    }

    /*
     * @return a temporary file name for an sstable.
     * When the sstable object is closed, it will be renamed to a non-temporary
     * format, so incomplete sstables can be recognized and removed on startup.
     */
    public String getFlushPath(long estimatedSize, String version)
    {
        String location = table.getDataFileLocation(estimatedSize);
        if (location == null)
            throw new RuntimeException("Insufficient disk space to flush " + estimatedSize + " bytes");
        return getTempSSTablePath(location, version);
    }

    public String getTempSSTablePath(String directory, String version)
    {
        Descriptor desc = new Descriptor(version,
                                         new File(directory),
                                         table.name,
                                         columnFamily,
                                         fileIndexGenerator.incrementAndGet(),
                                         true);
        return desc.filenameFor(Component.DATA);
    }

    public String getTempSSTablePath(String directory)
    {
        return getTempSSTablePath(directory, Descriptor.CURRENT_VERSION);
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
         * This is why we Table.flusherLock needs to be global instead of per-Table:
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
            final ReplayPosition ctx = writeCommitLog ? CommitLog.instance.getContext() : ReplayPosition.NONE;
            logger.debug("flush position is {}", ctx);

            // submit the memtable for any indexed sub-cfses, and our own.
            List<ColumnFamilyStore> icc = new ArrayList<ColumnFamilyStore>();
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

            if (memtableSwitchCount == Integer.MAX_VALUE)
                memtableSwitchCount = 0;
            memtableSwitchCount++;

            // when all the memtables have been written, including for indexes, mark the flush in the commitlog header.
            // a second executor makes sure the onMemtableFlushes get called in the right order,
            // while keeping the wait-for-flush (future.get) out of anything latency-sensitive.
            return postFlushExecutor.submit(new WrappedRunnable()
            {
                public void runMayThrow() throws InterruptedException, IOException
                {
                    latch.await();
                    if (writeCommitLog)
                    {
                        // if we're not writing to the commit log, we are replaying the log, so marking
                        // the log header with "you can discard anything written before the context" is not valid
                        CommitLog.instance.discardCompletedSegments(metadata.cfId, ctx);
                    }
                }
            });
        }
        finally
        {
            Table.switchLock.writeLock().unlock();
        }
    }

    public boolean isDropped()
    {
        return isIndex()
               ? Schema.instance.getCFMetaData(table.name, getParentColumnfamily()) == null
               : Schema.instance.getCFMetaData(metadata.cfId) == null;
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
        if (rowCache.isPutCopying())
        {
            invalidateCachedRow(key);
        }
        else
        {
            ColumnFamily cachedRow = getRawCachedRow(key);
            if (cachedRow != null)
                cachedRow.addAll(columnFamily, HeapAllocator.instance);
        }
    }

    /**
     * Insert/Update the column family for this key.
     * Caller is responsible for acquiring Table.flusherLock!
     * param @ lock - lock that needs to be used.
     * param @ key - key for update/insert
     * param @ columnFamily - columnFamily changes
     */
    public void apply(DecoratedKey key, ColumnFamily columnFamily)
    {
        long start = System.nanoTime();

        Memtable mt = getMemtableThreadSafe();
        mt.put(key, columnFamily);
        updateRowCache(key, columnFamily);
        writeStats.addNano(System.nanoTime() - start);

        // recompute liveRatio, if we have doubled the number of ops since last calculated
        while (true)
        {
            long last = liveRatioComputedAt.get();
            long operations = writeStats.getOpCount();
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
        // in case of a timestamp tie, tombstones get priority over non-tombstones.
        // (we want this to be deterministic to avoid confusion.)
        if (cf.getColumnCount() == 0 && cf.getLocalDeletionTime() <= gcBefore)
            return null;

        cf.maybeResetDeletionTimes(gcBefore);
        return cf;
    }

    /*
     This is complicated because we need to preserve deleted columns, supercolumns, and columnfamilies
     until they have been deleted for at least GC_GRACE_IN_SECONDS.  But, we do not need to preserve
     their contents; just the object itself as a "tombstone" that can be used to repair other
     replicas that do not know about the deletion.
     */
    public static ColumnFamily removeDeleted(ColumnFamily cf, int gcBefore)
    {
        if (cf == null)
        {
            return null;
        }

        removeDeletedColumnsOnly(cf, gcBefore);
        return removeDeletedCF(cf, gcBefore);
    }

    public static void removeDeletedColumnsOnly(ColumnFamily cf, int gcBefore)
    {
        if (cf.isSuper())
            removeDeletedSuper(cf, gcBefore);
        else
            removeDeletedStandard(cf, gcBefore);
    }

    private static void removeDeletedStandard(ColumnFamily cf, int gcBefore)
    {
        Iterator<IColumn> iter = cf.iterator();
        while (iter.hasNext())
        {
            IColumn c = iter.next();
            ByteBuffer cname = c.name();
            // remove columns if
            // (a) the column itself is tombstoned or
            // (b) the CF is tombstoned and the column is not newer than it
            if ((c.isMarkedForDelete() && c.getLocalDeletionTime() <= gcBefore)
                || c.timestamp() <= cf.getMarkedForDeleteAt())
            {
                iter.remove();
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
            long minTimestamp = Math.max(c.getMarkedForDeleteAt(), cf.getMarkedForDeleteAt());
            Iterator<IColumn> subIter = c.getSubColumns().iterator();
            while (subIter.hasNext())
            {
                IColumn subColumn = subIter.next();
                // remove subcolumns if
                // (a) the subcolumn itself is tombstoned or
                // (b) the supercolumn is tombstoned and the subcolumn is not newer than it
                if (subColumn.timestamp() <= minTimestamp
                    || (subColumn.isMarkedForDelete() && subColumn.getLocalDeletionTime() <= gcBefore))
                {
                    subIter.remove();
                }
            }
            if (c.getSubColumns().isEmpty() && c.getLocalDeletionTime() <= gcBefore)
            {
                iter.remove();
            }
            else
            {
                c.maybeResetDeletionTimes(gcBefore);
            }
        }
    }

    /**
     * Uses bloom filters to check if key may be present in any sstable in this
     * ColumnFamilyStore, minus a set of provided ones.
     *
     * Because BFs are checked, negative returns ensure that the key is not
     * present in the checked SSTables, but positive ones doesn't ensure key
     * presence.
     */
    public boolean isKeyInRemainingSSTables(DecoratedKey key, Set<? extends SSTable> sstablesToIgnore)
    {
        // we don't need to acquire references here, since the bloom filter is safe to use even post-compaction
        List<SSTableReader> filteredSSTables = data.getView().intervalTree.search(new Interval(key, key));
        for (SSTableReader sstable : filteredSSTables)
        {
            if (!sstablesToIgnore.contains(sstable) && sstable.getBloomFilter().isPresent(key.key))
                return true;
        }
        return false;
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
        data.addSSTables(Arrays.asList(sstable));
        CompactionManager.instance.submitBackground(this);
    }

    /*
     * Add up all the files sizes this is the worst case file
     * size for compaction of all the list of files given.
     */
    public long getExpectedCompactedFileSize(Iterable<SSTableReader> sstables)
    {
        long expectedFileSize = 0;
        for (SSTableReader sstable : sstables)
        {
            long size = sstable.onDiskLength();
            expectedFileSize = expectedFileSize + size;
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

    public void markCompacted(Collection<SSTableReader> sstables)
    {
        assert !sstables.isEmpty();
        data.markCompacted(sstables);
    }

    public void replaceCompactedSSTables(Collection<SSTableReader> sstables, Iterable<SSTableReader> replacements)
    {
        data.replaceCompactedSSTables(sstables, replacements);
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
        return getMemtableThreadSafe().getOperations();
    }

    public long getMemtableDataSize()
    {
        return getMemtableThreadSafe().getLiveSize();
    }

    public long getTotalMemtableLiveSize()
    {
        long total = 0;
        for (ColumnFamilyStore cfs : concatWithIndexes())
            total += cfs.getMemtableThreadSafe().getLiveSize();
        return total;
    }

    public int getMemtableSwitchCount()
    {
        return memtableSwitchCount;
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

    public long[] getRecentSSTablesPerReadHistogram()
    {
        return recentSSTablesPerRead.getBuckets(true);
    }

    public long[] getSSTablesPerReadHistogram()
    {
        return sstablesPerRead.getBuckets(false);
    }

    public long getReadCount()
    {
        return readStats.getOpCount();
    }

    public double getRecentReadLatencyMicros()
    {
        return readStats.getRecentLatencyMicros();
    }

    public long[] getLifetimeReadLatencyHistogramMicros()
    {
        return readStats.getTotalLatencyHistogramMicros();
    }

    public long[] getRecentReadLatencyHistogramMicros()
    {
        return readStats.getRecentLatencyHistogramMicros();
    }

    public long getTotalReadLatencyMicros()
    {
        return readStats.getTotalLatencyMicros();
    }

// TODO this actually isn't a good meature of pending tasks
    public int getPendingTasks()
    {
        return Table.switchLock.getQueueLength();
    }

    public long getWriteCount()
    {
        return writeStats.getOpCount();
    }

    public long getTotalWriteLatencyMicros()
    {
        return writeStats.getTotalLatencyMicros();
    }

    public double getRecentWriteLatencyMicros()
    {
        return writeStats.getRecentLatencyMicros();
    }

    public long[] getLifetimeWriteLatencyHistogramMicros()
    {
        return writeStats.getTotalLatencyHistogramMicros();
    }

    public long[] getRecentWriteLatencyHistogramMicros()
    {
        return writeStats.getRecentLatencyHistogramMicros();
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

    private ColumnFamily cacheRow(DecoratedKey key)
    {
        ColumnFamily cached;
        if ((cached = rowCache.get(key)) == null)
        {
            // We force ThreadSafeSortedColumns because cached row will be accessed concurrently
            cached = getTopLevelColumns(QueryFilter.getIdentityFilter(key, new QueryPath(columnFamily)), Integer.MIN_VALUE, true);
            if (cached == null)
                return null;

            // avoid keeping a permanent reference to the original key buffer
            rowCache.put(new DecoratedKey(key.token, ByteBufferUtil.clone(key.key)), cached);
        }
        return cached;
    }

    private ColumnFamily getColumnFamily(QueryFilter filter, int gcBefore)
    {
        assert columnFamily.equals(filter.getColumnFamilyName()) : filter.getColumnFamilyName();

        long start = System.nanoTime();
        try
        {
            if (rowCache.getCapacity() == 0)
            {
                ColumnFamily cf = getTopLevelColumns(filter, gcBefore, false);

                if (cf == null)
                    return null;

                // TODO this is necessary because when we collate supercolumns together, we don't check
                // their subcolumns for relevance, so we need to do a second prune post facto here.
                return cf.isSuper() ? removeDeleted(cf, gcBefore) : removeDeletedCF(cf, gcBefore);
            }

            ColumnFamily cached = cacheRow(filter.key);
            if (cached == null)
                return null;

            return filterColumnFamily(cached, filter, gcBefore);
        }
        finally
        {
            readStats.addNano(System.nanoTime() - start);
        }
    }

    /**
     *  Filter a cached row, which will not be modified by the filter, but may be modified by throwing out
     *  tombstones that are no longer relevant.
     *  The returned column family won't be thread safe.
     */
    ColumnFamily filterColumnFamily(ColumnFamily cached, QueryFilter filter, int gcBefore)
    {
        ColumnFamily cf = cached.cloneMeShallow(ArrayBackedSortedColumns.factory(), filter.filter.isReversed());
        IColumnIterator ci = filter.getMemtableColumnIterator(cached, null, getComparator());
        filter.collateColumns(cf, Collections.singletonList(ci), getComparator(), gcBefore);
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
        assert !key.isEmpty();
        DataTracker.View view;
        List<SSTableReader> sstables;
        while (true)
        {
            view = data.getView();
            sstables = view.intervalTree.search(new Interval(key, key));
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
    public ViewFragment markReferenced(DecoratedKey startWith, DecoratedKey stopAt)
    {
        DataTracker.View view;
        List<SSTableReader> sstables;
        while (true)
        {
            view = data.getView();
            // startAt == minimum is ok, but stopAt == minimum is confusing because all IntervalTree deals with
            // is Comparable, so it won't know to special-case that.
            Comparable stopInTree = stopAt.isEmpty() ? view.intervalTree.max() : stopAt;
            sstables = view.intervalTree.search(new Interval(startWith, stopInTree));
            if (SSTableReader.acquireReferences(sstables))
                break;
            // retry w/ new view
        }
        return new ViewFragment(sstables, Iterables.concat(Collections.singleton(view.memtable), view.memtablesPendingFlush));
    }

    private ColumnFamily getTopLevelColumns(QueryFilter filter, int gcBefore, boolean forCache)
    {
        CollationController controller = new CollationController(this, forCache, filter, gcBefore);
        ColumnFamily columns = controller.getTopLevelColumns();
        recentSSTablesPerRead.add(controller.getSstablesIterated());
        sstablesPerRead.add(controller.getSstablesIterated());
        return columns;
    }

    /**
      * Fetch a range of rows and columns from memtables/sstables.
      *
      * @param superColumn optional SuperColumn to slice subcolumns of; null to slice top-level columns
      * @param range Either a Bounds, which includes start key, or a Range, which does not.
      * @param maxResults Maximum rows to return
      * @param columnFilter description of the columns we're interested in for each row
      * @return true if we found all keys we were looking for, otherwise false
     */
    public List<Row> getRangeSlice(ByteBuffer superColumn, final AbstractBounds range, int maxResults, IFilter columnFilter)
    throws ExecutionException, InterruptedException
    {
        assert range instanceof Bounds
               || (!((Range)range).isWrapAround() || range.right.equals(StorageService.getPartitioner().getMinimumToken()))
               : range;

        DecoratedKey startWith = new DecoratedKey(range.left, null);
        DecoratedKey stopAt = new DecoratedKey(range.right, null);

        QueryFilter filter = new QueryFilter(null, new QueryPath(columnFamily, superColumn, null), columnFilter);
        int gcBefore = (int)(System.currentTimeMillis() / 1000) - metadata.getGcGraceSeconds();

        List<Row> rows;
        ViewFragment view = markReferenced(startWith, stopAt);
        try
        {
            CloseableIterator<Row> iterator = RowIteratorFactory.getIterator(view.memtables, view.sstables, startWith, stopAt, filter, getComparator(), this);
            rows = new ArrayList<Row>();

            try
            {
                // pull rows out of the iterator
                boolean first = true;
                while (iterator.hasNext())
                {
                    Row current = iterator.next();
                    DecoratedKey key = current.key;

                    if (!stopAt.isEmpty() && stopAt.compareTo(key) < 0)
                        return rows;

                    // skip first one
                    if (range instanceof Bounds || !first || !key.equals(startWith))
                    {
                        // TODO this is necessary because when we collate supercolumns together, we don't check
                        // their subcolumns for relevance, so we need to do a second prune post facto here.
                        rows.add(current.cf != null && current.cf.isSuper()
                                 ? new Row(current.key, ColumnFamilyStore.removeDeleted(current.cf, gcBefore))
                                 : current);
                        if (logger.isDebugEnabled())
                            logger.debug("scanned " + key);
                    }
                    first = false;

                    if (rows.size() >= maxResults)
                        return rows;
                }
            }
            finally
            {
                try
                {
                    iterator.close();
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        }
        finally
        {
            // separate finally block to release references in case getIterator() throws
            SSTableReader.releaseReferences(view.sstables);
        }

        return rows;
    }

    public List<Row> search(IndexClause clause, AbstractBounds range, IFilter dataFilter)
    {
        return indexManager.search(clause, range, dataFilter);
    }

    public AbstractType getComparator()
    {
        return metadata.comparator;
    }

    private void snapshotWithoutFlush(String snapshotName)
    {
        for (ColumnFamilyStore cfs : concatWithIndexes())
        {
            DataTracker.View currentView = cfs.markCurrentViewReferenced();

            try
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    // mkdir
                    File dataDirectory = ssTable.descriptor.directory.getParentFile();
                    String snapshotDirectoryPath = Table.getSnapshotPath(dataDirectory.getAbsolutePath(), table.name, snapshotName);
                    FileUtils.createDirectory(snapshotDirectoryPath);

                    // hard links
                    ssTable.createLinks(snapshotDirectoryPath);
                    if (logger.isDebugEnabled())
                        logger.debug("Snapshot for " + table + " keyspace data file " + ssTable.getFilename() +
                                     " created in " + snapshotDirectoryPath);
                }

                if (compactionStrategy instanceof LeveledCompactionStrategy)
                {
                    File manifest = LeveledManifest.tryGetManifest(cfs);

                    if (manifest != null)
                    {
                        File snapshotDirectory = new File(Table.getSnapshotPath(manifest.getParent(), snapshotName));
                        FileUtils.createDirectory(snapshotDirectory);

                        CLibrary.createHardLink(manifest, new File(snapshotDirectory, manifest.getName()));
                    }
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            finally
            {
                SSTableReader.releaseReferences(currentView.sstables);
            }
        }
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

    public boolean hasUnreclaimedSpace()
    {
        return data.getLiveSize() < data.getTotalSize();
    }

    public long getTotalDiskSpaceUsed()
    {
        return data.getTotalSize();
    }

    public long getLiveDiskSpaceUsed()
    {
        return data.getLiveSize();
    }

    public int getLiveSSTableCount()
    {
        return data.getSSTables().size();
    }

    /** raw cached row -- does not fetch the row if it is not present.  not counted in cache statistics.  */
    public ColumnFamily getRawCachedRow(DecoratedKey key)
    {
        return rowCache.getCapacity() == 0 ? null : rowCache.getInternal(key);
    }

    public void invalidateCachedRow(DecoratedKey key)
    {
        rowCache.remove(key);
    }

    public void forceMajorCompaction() throws InterruptedException, ExecutionException
    {
        CompactionManager.instance.performMaximal(this);
    }

    public void invalidateRowCache()
    {
        rowCache.clear();
    }

    public void invalidateKeyCache()
    {
        keyCache.clear();
    }

    public int getRowCacheCapacity()
    {
        return rowCache.getCapacity();
    }

    public int getKeyCacheCapacity()
    {
        return keyCache.getCapacity();
    }

    public int getRowCacheSize()
    {
        return rowCache.size();
    }

    public int getKeyCacheSize()
    {
        return keyCache.size();
    }

    public static Iterable<ColumnFamilyStore> all()
    {
        Iterable<ColumnFamilyStore>[] stores = new Iterable[Schema.instance.getTables().size()];
        int i = 0;
        for (Table table : Table.all())
        {
            stores[i++] = table.getColumnFamilyStores();
        }
        return Iterables.concat(stores);
    }

    public Iterable<DecoratedKey> allKeySamples()
    {
        Collection<SSTableReader> sstables = getSSTables();
        Iterable<DecoratedKey>[] samples = new Iterable[sstables.size()];
        int i = 0;
        for (SSTableReader sstable: sstables)
        {
            samples[i++] = sstable.getKeySamples();
        }
        return Iterables.concat(samples);
    }

    public Iterable<DecoratedKey> keySamples(Range range)
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
     * For testing.  no effort is made to clear historical memtables, nor for
     * thread safety
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
    public Future<?> truncate() throws IOException, ExecutionException, InterruptedException
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
        // flush the CF being truncated before forcing the new segment
        forceBlockingFlush();
        CommitLog.instance.forceNewSegment();
        ReplayPosition position = CommitLog.instance.getContext();
        // now flush everyone else.  re-flushing ourselves is not necessary, but harmless
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            cfs.forceFlush();
        waitForActiveFlushes();
        // if everything was clean, flush won't have called discard
        CommitLog.instance.discardCompletedSegments(metadata.cfId, position);

        // sleep a little to make sure that our truncatedAt comes after any sstable
        // that was part of the flushed we forced; otherwise on a tie, it won't get deleted.
        try
        {
            Thread.sleep(100);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        long truncatedAt = System.currentTimeMillis();
        snapshot(Table.getTimestampedSnapshotName(columnFamily));

        return CompactionManager.instance.submitTruncate(this, truncatedAt);
    }

    public long getBloomFilterFalsePositives()
    {
        return data.getBloomFilterFalsePositives();
    }

    public long getRecentBloomFilterFalsePositives()
    {
        return data.getRecentBloomFilterFalsePositives();
    }

    public double getBloomFilterFalseRatio()
    {
        return data.getBloomFilterFalseRatio();
    }

    public double getRecentBloomFilterFalseRatio()
    {
        return data.getRecentBloomFilterFalseRatio();
    }

    public long getBloomFilterDiskSpaceUsed()
    {
        long total = 0;
        for (SSTableReader sst : getSSTables())
            total += sst.getBloomFilterSerializedSize();
        return total;
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
       - get/set rowCacheSavePeriodInSeconds
       - get/set keyCacheSavePeriodInSeconds
       - get/set rowCacheKeysToSave
     */

    public AbstractCompactionStrategy getCompactionStrategy()
    {
        return compactionStrategy;
    }

    public int getMinimumCompactionThreshold()
    {
        return minCompactionThreshold.value();
    }

    public void setMinimumCompactionThreshold(int minCompactionThreshold)
    {
        if ((minCompactionThreshold > this.maxCompactionThreshold.value()) && this.maxCompactionThreshold.value() != 0)
        {
            throw new RuntimeException("The min_compaction_threshold cannot be larger than the max.");
        }
        this.minCompactionThreshold.set(minCompactionThreshold);
    }

    public int getMaximumCompactionThreshold()
    {
        return maxCompactionThreshold.value();
    }

    public void setMaximumCompactionThreshold(int maxCompactionThreshold)
    {
        if (maxCompactionThreshold < this.minCompactionThreshold.value())
        {
            throw new RuntimeException("The max_compaction_threshold cannot be smaller than the min.");
        }
        this.maxCompactionThreshold.set(maxCompactionThreshold);
    }

    public boolean isCompactionDisabled()
    {
        return getMinimumCompactionThreshold() <= 0 || getMaximumCompactionThreshold() <= 0;
    }

    public int getRowCacheSavePeriodInSeconds()
    {
        return rowCacheSaveInSeconds.value();
    }
    public void setRowCacheSavePeriodInSeconds(int rcspis)
    {
        if (rcspis < 0)
        {
            throw new RuntimeException("RowCacheSavePeriodInSeconds must be non-negative.");
        }
        this.rowCacheSaveInSeconds.set(rcspis);
        scheduleCacheSaving(rowCacheSaveInSeconds.value(), keyCacheSaveInSeconds.value(), rowCacheKeysToSave.value());
    }

    public int getKeyCacheSavePeriodInSeconds()
    {
        return keyCacheSaveInSeconds.value();
    }
    public void setKeyCacheSavePeriodInSeconds(int kcspis)
    {
        if (kcspis < 0)
        {
            throw new RuntimeException("KeyCacheSavePeriodInSeconds must be non-negative.");
        }
        this.keyCacheSaveInSeconds.set(kcspis);
        scheduleCacheSaving(rowCacheSaveInSeconds.value(), keyCacheSaveInSeconds.value(), rowCacheKeysToSave.value());
    }

    public int getRowCacheKeysToSave()
    {
        return rowCacheKeysToSave.value();
    }

    public void setRowCacheKeysToSave(int keysToSave)
    {
        this.rowCacheKeysToSave.set(keysToSave);
    }
    // End JMX get/set.

    public long estimateKeys()
    {
        return data.estimatedKeys();
    }

    /**
     * Resizes the key and row caches based on the current key estimate.
     */
    public synchronized void updateCacheSizes()
    {
        long keys = estimateKeys();
        keyCache.updateCacheSize(keys);
        rowCache.updateCacheSize(keys);
    }

    public long[] getEstimatedRowSizeHistogram()
    {
        return data.getEstimatedRowSizeHistogram();
    }

    public long[] getEstimatedColumnCountHistogram()
    {
        return data.getEstimatedColumnCountHistogram();
    }

    public double getCompressionRatio()
    {
        return data.getCompressionRatio();
    }
    
    /** true if this CFS contains secondary index data */
    public boolean isIndex()
    {
        return partitioner instanceof LocalPartitioner;
    }

    private String getParentColumnfamily()
    {
        assert isIndex();
        return columnFamily.split("\\.")[0];
    }

    /**
     * sets each cache's maximum capacity to 75% of its current size
     */
    public void reduceCacheSizes()
    {
        rowCache.reduceCacheSize();
        keyCache.reduceCacheSize();
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

    public SSTableWriter createFlushWriter(long estimatedRows, long estimatedSize, ReplayPosition context) throws IOException
    {
        SSTableMetadata.Collector sstableMetadataCollector = SSTableMetadata.createCollector().replayPosition(context);
        return new SSTableWriter(getFlushPath(estimatedSize, Descriptor.CURRENT_VERSION),
                                 estimatedRows,
                                 metadata,
                                 partitioner,
                                 sstableMetadataCollector);
    }

    public SSTableWriter createCompactionWriter(long estimatedRows, String location, Collection<SSTableReader> sstables) throws IOException
    {
        ReplayPosition rp = ReplayPosition.getReplayPosition(sstables);
        SSTableMetadata.Collector sstableMetadataCollector = SSTableMetadata.createCollector().replayPosition(rp);

        // get the max timestamp of the precompacted sstables
        for (SSTableReader sstable : sstables)
            sstableMetadataCollector.updateMaxTimestamp(sstable.getMaxTimestamp());

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
}
