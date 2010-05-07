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

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.ArrayUtils;

import com.google.common.collect.Iterables;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean
{
    private static Logger logger_ = LoggerFactory.getLogger(ColumnFamilyStore.class);

    /*
     * submitFlush first puts [Binary]Memtable.getSortedContents on the flushSorter executor,
     * which then puts the sorted results on the writer executor.  This is because sorting is CPU-bound,
     * and writing is disk-bound; we want to be able to do both at once.  When the write is complete,
     * we turn the writer into an SSTableReader and add it to ssTables_ where it is available for reads.
     *
     * For BinaryMemtable that's about all that happens.  For live Memtables there are two other things
     * that switchMemtable does (which should be the only caller of submitFlush in this case).
     * First, it puts the Memtable into memtablesPendingFlush, where it stays until the flush is complete
     * and it's been added as an SSTableReader to ssTables_.  Second, it adds an entry to commitLogUpdater
     * that waits for the flush to complete, then calls onMemtableFlush.  This allows multiple flushes
     * to happen simultaneously on multicore systems, while still calling onMF in the correct order,
     * which is necessary for replay in case of a restart since CommitLog assumes that when onMF is
     * called, all data up to the given context has been persisted to SSTables.
     */
    private static ExecutorService flushSorter_
            = new JMXEnabledThreadPoolExecutor(1,
                                               Runtime.getRuntime().availableProcessors(),
                                               StageManager.KEEPALIVE,
                                               TimeUnit.SECONDS,
                                               new LinkedBlockingQueue<Runnable>(1 + Runtime.getRuntime().availableProcessors()),
                                               new NamedThreadFactory("FLUSH-SORTER-POOL"));
    private static ExecutorService flushWriter_
            = new JMXEnabledThreadPoolExecutor(1,
                                               DatabaseDescriptor.getAllDataFileLocations().length,
                                               StageManager.KEEPALIVE,
                                               TimeUnit.SECONDS,
                                               new LinkedBlockingQueue<Runnable>(1 + 2 * DatabaseDescriptor.getAllDataFileLocations().length),
                                               new NamedThreadFactory("FLUSH-WRITER-POOL"));
    private static ExecutorService commitLogUpdater_ = new JMXEnabledThreadPoolExecutor("MEMTABLE-POST-FLUSHER");

    private Set<Memtable> memtablesPendingFlush = new ConcurrentSkipListSet<Memtable>();

    private final String table_;
    public final String columnFamily_;

    private volatile Integer memtableSwitchCount = 0;

    /* This is used to generate the next index for a SSTable */
    private AtomicInteger fileIndexGenerator_ = new AtomicInteger(0);

    /* active memtable associated with this ColumnFamilyStore. */
    private Memtable memtable_;

    // TODO binarymemtable ops are not threadsafe (do they need to be?)
    private AtomicReference<BinaryMemtable> binaryMemtable_;

    /* SSTables on disk for this column family */
    private SSTableTracker ssTables_;

    private LatencyTracker readStats_ = new LatencyTracker();
    private LatencyTracker writeStats_ = new LatencyTracker();

    private long minRowCompactedSize = 0L;
    private long maxRowCompactedSize = 0L;
    private long rowsCompactedTotalSize = 0L;
    private long rowsCompactedCount = 0L;
    
    ColumnFamilyStore(String table, String columnFamilyName, int indexValue)
    {
        table_ = table;
        columnFamily_ = columnFamilyName;
        fileIndexGenerator_.set(indexValue);
        memtable_ = new Memtable(this);
        binaryMemtable_ = new AtomicReference<BinaryMemtable>(new BinaryMemtable(this));

        if (logger_.isDebugEnabled())
            logger_.debug("Starting CFS {}", columnFamily_);
        // scan for data files corresponding to this CF
        List<File> sstableFiles = new ArrayList<File>();
        Pattern auxFilePattern = Pattern.compile("(.*)(-Filter\\.db$|-Index\\.db$)");
        for (File file : files())
        {
            String filename = file.getName();

            /* look for and remove orphans. An orphan is a -Filter.db or -Index.db with no corresponding -Data.db. */
            Matcher matcher = auxFilePattern.matcher(file.getAbsolutePath());
            if (matcher.matches())
            {
                String basePath = matcher.group(1);
                if (!new File(basePath + "-Data.db").exists())
                {
                    logger_.info(String.format("Removing orphan %s", file.getAbsolutePath()));
                    try
                    {
                        FileUtils.deleteWithConfirm(file);
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                    continue;
                }
            }

            if (((file.length() == 0 && !filename.endsWith("-Compacted")) || (filename.contains("-" + SSTable.TEMPFILE_MARKER))))
            {
                try
                {
                    FileUtils.deleteWithConfirm(file);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
                continue;
            }

            if (filename.contains("-Data.db"))
            {
                sstableFiles.add(file.getAbsoluteFile());
            }
        }
        Collections.sort(sstableFiles, new FileUtils.FileComparator());

        /* Load the index files and the Bloom Filters associated with them. */
        List<SSTableReader> sstables = new ArrayList<SSTableReader>();
        for (File file : sstableFiles)
        {
            String filename = file.getAbsolutePath();
            if (SSTable.deleteIfCompacted(filename))
                continue;

            SSTableReader sstable;
            try
            {
                sstable = SSTableReader.open(filename);
            }
            catch (IOException ex)
            {
                logger_.error("Corrupt file " + filename + "; skipped", ex);
                continue;
            }
            sstables.add(sstable);
        }
        ssTables_ = new SSTableTracker(table, columnFamilyName);
        ssTables_.add(sstables);
    }

    public void addToCompactedRowStats(Long rowsize)
    {
        if (minRowCompactedSize < 1 || rowsize < minRowCompactedSize)
            minRowCompactedSize = rowsize;
        if (rowsize > maxRowCompactedSize)
            maxRowCompactedSize = rowsize;
        rowsCompactedCount++;
        rowsCompactedTotalSize += rowsize;
    }

    public long getMinRowCompactedSize()
    {
        return minRowCompactedSize;
    }

    public long getMaxRowCompactedSize()
    {
        return maxRowCompactedSize;
    }

    public long getMeanRowCompactedSize()
    {
        if (rowsCompactedCount > 0)
            return rowsCompactedTotalSize / rowsCompactedCount;
        else
            return 0L;
    }

    public static ColumnFamilyStore createColumnFamilyStore(String table, String columnFamily)
    {
        /*
         * Get all data files associated with old Memtables for this table.
         * These files are named as follows <Table>-1.db, ..., <Table>-n.db. Get
         * the max which in this case is n and increment it to use it for next
         * index.
         */
        List<Integer> generations = new ArrayList<Integer>();
        String[] dataFileDirectories = DatabaseDescriptor.getAllDataFileLocationsForTable(table);
        for (String directory : dataFileDirectories)
        {
            File fileDir = new File(directory);
            File[] files = fileDir.listFiles();
            
            for (File file : files)
            {
                if (file.isDirectory())
                    continue;
                String filename = file.getAbsolutePath();
                String cfName = getColumnFamilyFromFileName(filename);

                if (cfName.equals(columnFamily))
                {
                    generations.add(getGenerationFromFileName(filename));
                }
            }
        }
        Collections.sort(generations);
        int value = (generations.size() > 0) ? (generations.get(generations.size() - 1)) : 0;

        ColumnFamilyStore cfs = new ColumnFamilyStore(table, columnFamily, value);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName mbeanName = new ObjectName("org.apache.cassandra.db:type=ColumnFamilyStores,keyspace=" + table + ",columnfamily=" + columnFamily);
            if (mbs.isRegistered(mbeanName))
                mbs.unregisterMBean(mbeanName);
            mbs.registerMBean(cfs, mbeanName);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        return cfs;
    }

    private Set<File> files()
    {
        Set<File> fileSet = new HashSet<File>();
        for (String directory : DatabaseDescriptor.getAllDataFileLocationsForTable(table_))
        {
            File[] files = new File(directory).listFiles();
            for (File file : files)
            {
                if (file.isDirectory())
                    continue;
                String cfName = getColumnFamilyFromFileName(file.getAbsolutePath());
                if (cfName.equals(columnFamily_))
                    fileSet.add(file);
            }
        }
        return fileSet;
    }

    /**
     * @return the name of the column family
     */
    public String getColumnFamilyName()
    {
        return columnFamily_;
    }

    private static String getColumnFamilyFromFileName(String filename)
    {
        return SSTable.Descriptor.fromFilename(filename).cfname;
    }

    public static int getGenerationFromFileName(String filename)
    {
        return SSTable.Descriptor.fromFilename(filename).generation;
    }

    /*
     * @return a temporary file name for an sstable.
     * When the sstable object is closed, it will be renamed to a non-temporary
     * format, so incomplete sstables can be recognized and removed on startup.
     */
    public String getFlushPath()
    {
        long guessedSize = 2 * DatabaseDescriptor.getMemtableThroughput() * 1024*1024; // 2* adds room for keys, column indexes
        String location = DatabaseDescriptor.getDataFileLocationForTable(table_, guessedSize);
        if (location == null)
            throw new RuntimeException("Insufficient disk space to flush");
        return getTempSSTablePath(location);
    }

    public String getTempSSTablePath(String directory)
    {
        SSTable.Descriptor desc = new SSTable.Descriptor(new File(directory),
                                                         table_,
                                                         columnFamily_,
                                                         fileIndexGenerator_.incrementAndGet(),
                                                         true);
        return desc.filenameFor("Data.db");
    }

    /** flush the given memtable and swap in a new one for its CFS, if it hasn't been frozen already.  threadsafe. */
    Future<?> maybeSwitchMemtable(Memtable oldMemtable, final boolean writeCommitLog)
    {
        /**
         *  If we can get the writelock, that means no new updates can come in and 
         *  all ongoing updates to memtables have completed. We can get the tail
         *  of the log and use it as the starting position for log replay on recovery.
         */
        Table.flusherLock.writeLock().lock();
        try
        {
            if (oldMemtable.isFrozen())
            {
                return null;
            }
            oldMemtable.freeze();

            final CommitLogSegment.CommitLogContext ctx = writeCommitLog ? CommitLog.instance().getContext() : null;
            logger_.info(columnFamily_ + " has reached its threshold; switching in a fresh Memtable at " + ctx);
            final Condition condition = submitFlush(oldMemtable);
            memtable_ = new Memtable(this);
            // a second executor that makes sure the onMemtableFlushes get called in the right order,
            // while keeping the wait-for-flush (future.get) out of anything latency-sensitive.
            return commitLogUpdater_.submit(new WrappedRunnable()
            {
                public void runMayThrow() throws InterruptedException, IOException
                {
                    condition.await();
                    if (writeCommitLog)
                    {
                        // if we're not writing to the commit log, we are replaying the log, so marking
                        // the log header with "you can discard anything written before the context" is not valid
                        final int cfId = DatabaseDescriptor.getTableMetaData(table_).get(columnFamily_).cfId;
                        CommitLog.instance().discardCompletedSegments(cfId, ctx);
                    }
                }
            });
        }
        finally
        {
            Table.flusherLock.writeLock().unlock();
            if (memtableSwitchCount == Integer.MAX_VALUE)
            {
                memtableSwitchCount = 0;
            }
            memtableSwitchCount++;
        }
    }

    void switchBinaryMemtable(DecoratedKey key, byte[] buffer)
    {
        binaryMemtable_.set(new BinaryMemtable(this));
        binaryMemtable_.get().put(key, buffer);
    }

    public void forceFlushIfExpired()
    {
        if (memtable_.isExpired())
            forceFlush();
    }

    public Future<?> forceFlush()
    {
        if (memtable_.isClean())
            return null;

        return maybeSwitchMemtable(memtable_, true);
    }

    public void forceBlockingFlush() throws ExecutionException, InterruptedException
    {
        Future<?> future = forceFlush();
        if (future != null)
            future.get();
    }

    public void forceFlushBinary()
    {
        if (binaryMemtable_.get().isClean())
            return;

        submitFlush(binaryMemtable_.get());
    }

    /**
     * Insert/Update the column family for this key.
     * Caller is responsible for acquiring Table.flusherLock!
     * param @ lock - lock that needs to be used.
     * param @ key - key for update/insert
     * param @ columnFamily - columnFamily changes
     */
    Memtable apply(DecoratedKey key, ColumnFamily columnFamily)
    {
        long start = System.nanoTime();

        boolean flushRequested = memtable_.isThresholdViolated();
        memtable_.put(key, columnFamily);
        writeStats_.addNano(System.nanoTime() - start);
        
        return flushRequested ? memtable_ : null;
    }

    /*
     * Insert/Update the column family for this key. param @ lock - lock that
     * needs to be used. param @ key - key for update/insert param @
     * columnFamily - columnFamily changes
     */
    void applyBinary(DecoratedKey key, byte[] buffer)
    {
        long start = System.nanoTime();
        binaryMemtable_.get().put(key, buffer);
        writeStats_.addNano(System.nanoTime() - start);
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

        if (cf.isSuper())
            removeDeletedSuper(cf, gcBefore);
        else
            removeDeletedStandard(cf, gcBefore);

        // in case of a timestamp tie, tombstones get priority over non-tombstones.
        // (we want this to be deterministic to avoid confusion.)
        if (cf.getColumnCount() == 0 && cf.getLocalDeletionTime() <= gcBefore)
        {
            return null;
        }
        return cf;
    }

    private static void removeDeletedStandard(ColumnFamily cf, int gcBefore)
    {
        for (byte[] cname : cf.getColumnNames())
        {
            IColumn c = cf.getColumnsMap().get(cname);
            if ((c.isMarkedForDelete() && c.getLocalDeletionTime() <= gcBefore)
                || c.timestamp() <= cf.getMarkedForDeleteAt())
            {
                cf.remove(cname);
            }
        }
    }

    private static void removeDeletedSuper(ColumnFamily cf, int gcBefore)
    {
        // TODO assume deletion means "most are deleted?" and add to clone, instead of remove from original?
        // this could be improved by having compaction, or possibly even removeDeleted, r/m the tombstone
        // once gcBefore has passed, so if new stuff is added in it doesn't used the wrong algorithm forever
        for (byte[] cname : cf.getColumnNames())
        {
            IColumn c = cf.getColumnsMap().get(cname);
            long minTimestamp = Math.max(c.getMarkedForDeleteAt(), cf.getMarkedForDeleteAt());
            for (IColumn subColumn : c.getSubColumns())
            {
                if (subColumn.timestamp() <= minTimestamp
                    || (subColumn.isMarkedForDelete() && subColumn.getLocalDeletionTime() <= gcBefore))
                {
                    ((SuperColumn)c).remove(subColumn.name());
                }
            }
            if (c.getSubColumns().isEmpty() && c.getLocalDeletionTime() <= gcBefore)
            {
                cf.remove(c.name());
            }
        }
    }

    /*
     * Called after the Memtable flushes its in-memory data, or we add a file
     * via bootstrap. This information is
     * cached in the ColumnFamilyStore. This is useful for reads because the
     * ColumnFamilyStore first looks in the in-memory store and the into the
     * disk to find the key. If invoked during recoveryMode the
     * onMemtableFlush() need not be invoked.
     *
     * param @ filename - filename just flushed to disk
     */
    public void addSSTable(SSTableReader sstable)
    {
        ssTables_.add(Arrays.asList(sstable));
        CompactionManager.instance.submitMinorIfNeeded(this);
    }

    /*
     * Add up all the files sizes this is the worst case file
     * size for compaction of all the list of files given.
     */
    long getExpectedCompactedFileSize(Iterable<SSTableReader> sstables)
    {
        long expectedFileSize = 0;
        for (SSTableReader sstable : sstables)
        {
            long size = sstable.length();
            expectedFileSize = expectedFileSize + size;
        }
        return expectedFileSize;
    }

    /*
     *  Find the maximum size file in the list .
     */
    SSTableReader getMaxSizeFile(Iterable<SSTableReader> sstables)
    {
        long maxSize = 0L;
        SSTableReader maxFile = null;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.length() > maxSize)
            {
                maxSize = sstable.length();
                maxFile = sstable;
            }
        }
        return maxFile;
    }

    void forceCleanup()
    {
        CompactionManager.instance.submitCleanup(ColumnFamilyStore.this);
    }

    public Table getTable()
    {
        return Table.open(table_);
    }

    void markCompacted(Collection<SSTableReader> sstables)
    {
        ssTables_.markCompacted(sstables);
    }

    boolean isCompleteSSTables(Collection<SSTableReader> sstables)
    {
        return ssTables_.getSSTables().equals(new HashSet<SSTableReader>(sstables));
    }

    void replaceCompactedSSTables(Collection<SSTableReader> sstables, Iterable<SSTableReader> replacements)
    {
        ssTables_.replace(sstables, replacements);
    }

    /**
     * submits flush sort on the flushSorter executor, which will in turn submit to flushWriter when sorted.
     * TODO because our executors use CallerRunsPolicy, when flushSorter fills up, no writes will proceed
     * because the next flush will start executing on the caller, mutation-stage thread that has the
     * flush write lock held.  (writes aquire this as a read lock before proceeding.)
     * This is good, because it backpressures flushes, but bad, because we can't write until that last
     * flushing thread finishes sorting, which will almost always be longer than any of the flushSorter threads proper
     * (since, by definition, it started last).
     */
    Condition submitFlush(IFlushable flushable)
    {
        logger_.info("Enqueuing flush of {}", flushable);
        final Condition condition = new SimpleCondition();
        flushable.flushAndSignal(condition, flushSorter_, flushWriter_);
        return condition;
    }

    public int getMemtableColumnsCount()
    {
        return getMemtableThreadSafe().getCurrentOperations();
    }

    public int getMemtableDataSize()
    {
        return getMemtableThreadSafe().getCurrentThroughput();
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
        Table.flusherLock.readLock().lock();
        try
        {
            return memtable_;
        }
        finally
        {
            Table.flusherLock.readLock().unlock();
        }
    }

    public Collection<SSTableReader> getSSTables()
    {
        return ssTables_.getSSTables();
    }

    public long getReadCount()
    {
        return readStats_.getOpCount();
    }

    public double getRecentReadLatencyMicros()
    {
        return readStats_.getRecentLatencyMicros();
    }

    public long[] getLifetimeReadLatencyHistogramMicros()
    {
        return readStats_.getTotalLatencyHistogramMicros();
    }

    public long[] getRecentReadLatencyHistogramMicros()
    {
        return readStats_.getRecentLatencyHistogramMicros();
    }

    public long getTotalReadLatencyMicros()
    {
        return readStats_.getTotalLatencyMicros();
    }

// TODO this actually isn't a good meature of pending tasks
    public int getPendingTasks()
    {
        return Table.flusherLock.getQueueLength();
    }

    public long getWriteCount()
    {
        return writeStats_.getOpCount();
    }

    public long getTotalWriteLatencyMicros()
    {
        return writeStats_.getTotalLatencyMicros();
    }

    public double getRecentWriteLatencyMicros()
    {
        return writeStats_.getRecentLatencyMicros();
    }

    public long[] getLifetimeWriteLatencyHistogramMicros()
    {
        return writeStats_.getTotalLatencyHistogramMicros();
    }

    public long[] getRecentWriteLatencyHistogramMicros()
    {
        return writeStats_.getRecentLatencyHistogramMicros();
    }

    public ColumnFamily getColumnFamily(DecoratedKey key, QueryPath path, byte[] start, byte[] finish, List<byte[]> bitmasks, boolean reversed, int limit)
    {
        return getColumnFamily(QueryFilter.getSliceFilter(key, path, start, finish, bitmasks, reversed, limit));
    }

    public ColumnFamily getColumnFamily(DecoratedKey key, QueryPath path, byte[] start, byte[] finish, boolean reversed, int limit)
    {
        return getColumnFamily(QueryFilter.getSliceFilter(key, path, start, finish, null, reversed, limit));
    }

    public ColumnFamily getColumnFamily(QueryFilter filter)
    {
        return getColumnFamily(filter, CompactionManager.getDefaultGCBefore());
    }

    private ColumnFamily cacheRow(DecoratedKey key)
    {
        ColumnFamily cached;
        if ((cached = ssTables_.getRowCache().get(key)) == null)
        {
            cached = getTopLevelColumns(QueryFilter.getIdentityFilter(key, new QueryPath(columnFamily_)), Integer.MIN_VALUE);
            if (cached == null)
                return null;
            ssTables_.getRowCache().put(key, cached);
        }
        return cached;
    }

    /**
     * get a list of columns starting from a given column, in a specified order.
     * only the latest version of a column is returned.
     * @return null if there is no data and no tombstones; otherwise a ColumnFamily
     */
    public ColumnFamily getColumnFamily(QueryFilter filter, int gcBefore)
    {
        assert columnFamily_.equals(filter.getColumnFamilyName());

        long start = System.nanoTime();
        try
        {
            if (ssTables_.getRowCache().getCapacity() == 0)
                return removeDeleted(getTopLevelColumns(filter, gcBefore), gcBefore);

            ColumnFamily cached = cacheRow(filter.key);
            if (cached == null)
                return null;
            IColumnIterator ci = filter.getMemtableColumnIterator(cached, null, getComparator());
            ColumnFamily returnCF = ci.getColumnFamily().cloneMeShallow();
            filter.collectCollatedColumns(returnCF, ci, gcBefore);
            // TODO this is necessary because when we collate supercolumns together, we don't check
            // their subcolumns for relevance, so we need to do a second prune post facto here.
            return removeDeleted(returnCF, gcBefore);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            readStats_.addNano(System.nanoTime() - start);
        }
    }

    private ColumnFamily getTopLevelColumns(QueryFilter filter, int gcBefore)
    {
        // we are querying top-level columns, do a merging fetch with indexes.
        List<IColumnIterator> iterators = new ArrayList<IColumnIterator>();
        final ColumnFamily returnCF = ColumnFamily.create(table_, columnFamily_);
        try
        {
            IColumnIterator iter;

            /* add the current memtable */
            iter = filter.getMemtableColumnIterator(getMemtableThreadSafe(), getComparator());
            if (iter != null)
            {
                returnCF.delete(iter.getColumnFamily());
                iterators.add(iter);
            }

            /* add the memtables being flushed */
            for (Memtable memtable : memtablesPendingFlush)
            {
                iter = filter.getMemtableColumnIterator(memtable, getComparator());
                if (iter != null)
                {
                    returnCF.delete(iter.getColumnFamily());
                    iterators.add(iter);
                }
            }

            /* add the SSTables on disk */
            for (SSTableReader sstable : ssTables_)
            {
                iter = filter.getSSTableColumnIterator(sstable);
                if (iter.getColumnFamily() != null)
                {
                    returnCF.delete(iter.getColumnFamily());
                    iterators.add(iter);
                }
            }

            Comparator<IColumn> comparator = QueryFilter.getColumnComparator(getComparator());
            Iterator collated = IteratorUtils.collatedIterator(comparator, iterators);
            filter.collectCollatedColumns(returnCF, collated, gcBefore);
            return returnCF; // caller is responsible for final removeDeleted
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            /* close all cursors */
            for (IColumnIterator ci : iterators)
            {
                try
                {
                    ci.close();
                }
                catch (Throwable th)
                {
                    logger_.error("error closing " + ci, th);
                }
            }
        }
    }

    /**
      * Fetch a range of rows and columns from memtables/sstables.
      * 
      * @param rows The resulting rows fetched during this operation 
      * @param superColumn Super column to filter by
      * @param range Either a Bounds, which includes start key, or a Range, which does not.
      * @param maxResults Maximum rows to return
      * @param sliceRange Information on how to slice columns
      * @param columnNames Column names to filter by 
      * @return true if we found all keys we were looking for, otherwise false
     */
    private boolean getRangeRows(List<Row> rows, byte[] superColumn, final AbstractBounds range, int maxResults, SliceRange sliceRange, List<byte[]> columnNames)
    throws ExecutionException, InterruptedException
    {
        final DecoratedKey startWith = new DecoratedKey(range.left, (byte[])null);
        final DecoratedKey stopAt = new DecoratedKey(range.right, (byte[])null);
        
        final int gcBefore = CompactionManager.getDefaultGCBefore();

        final QueryPath queryPath =  new QueryPath(columnFamily_, superColumn, null);
        final SortedSet<byte[]> columnNameSet = new TreeSet<byte[]>(getComparator());
        if (columnNames != null)
            columnNameSet.addAll(columnNames);

        final QueryFilter filter = sliceRange == null ? QueryFilter.getNamesFilter(null, queryPath, columnNameSet)
                                                      : QueryFilter.getSliceFilter(null, queryPath, sliceRange.start, sliceRange.finish, sliceRange.bitmasks, sliceRange.reversed, sliceRange.count);

        Collection<Memtable> memtables = new ArrayList<Memtable>();
        memtables.add(getMemtableThreadSafe());
        memtables.addAll(memtablesPendingFlush);

        Collection<SSTableReader> sstables = new ArrayList<SSTableReader>();
        Iterables.addAll(sstables, ssTables_);

        RowIterator iterator = RowIteratorFactory.getIterator(memtables, sstables, startWith, stopAt, filter, getComparator(), gcBefore);

        try
        {
            // pull rows out of the iterator
            boolean first = true; 
            while(iterator.hasNext())
            {
                Row current = iterator.next();
                DecoratedKey key = current.key;

                if (!stopAt.isEmpty() && stopAt.compareTo(key) < 0)
                    return true;

                // skip first one
                if(range instanceof Bounds || !first || !key.equals(startWith))
                    rows.add(current);
                first = false;

                if (rows.size() >= maxResults)
                    return true;
            }
            return false;
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

    /**
     *
     * @param super_column
     * @param range: either a Bounds, which includes start key, or a Range, which does not.
     * @param keyMax maximum number of keys to process, regardless of startKey/finishKey
     * @param sliceRange may be null if columnNames is specified. specifies contiguous columns to return in what order.
     * @param columnNames may be null if sliceRange is specified. specifies which columns to return in what order.      @return list of key->list<column> tuples.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public RangeSliceReply getRangeSlice(byte[] super_column, final AbstractBounds range, int keyMax, SliceRange sliceRange, List<byte[]> columnNames)
    throws ExecutionException, InterruptedException
    {
        List<Row> rows = new ArrayList<Row>();
        boolean completed;
        if ((range instanceof Bounds || !((Range)range).isWrapAround()))
        {
            completed = getRangeRows(rows, super_column, range, keyMax, sliceRange, columnNames);
        }
        else
        {
            // wrapped range
            Token min = StorageService.getPartitioner().getMinimumToken();
            Range first = new Range(range.left, min);
            completed = getRangeRows(rows, super_column, first, keyMax, sliceRange, columnNames);
            if (!completed && min.compareTo(range.right) < 0)
            {
                Range second = new Range(min, range.right);
                getRangeRows(rows, super_column, second, keyMax, sliceRange, columnNames);
            }
        }

        return new RangeSliceReply(rows);
    }

    public AbstractType getComparator()
    {
        return DatabaseDescriptor.getComparator(table_, columnFamily_);
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

        for (SSTableReader ssTable : ssTables_)
        {
            try
            {
                // mkdir
                File sourceFile = new File(ssTable.getFilename());
                File dataDirectory = sourceFile.getParentFile().getParentFile();
                String snapshotDirectoryPath = Table.getSnapshotPath(dataDirectory.getAbsolutePath(), table_, snapshotName);
                FileUtils.createDirectory(snapshotDirectoryPath);

                // hard links
                File targetLink = new File(snapshotDirectoryPath, sourceFile.getName());
                FileUtils.createHardLink(sourceFile, targetLink);

                sourceFile = new File(ssTable.indexFilename());
                targetLink = new File(snapshotDirectoryPath, sourceFile.getName());
                FileUtils.createHardLink(sourceFile, targetLink);

                sourceFile = new File(ssTable.filterFilename());
                targetLink = new File(snapshotDirectoryPath, sourceFile.getName());
                FileUtils.createHardLink(sourceFile, targetLink);
                if (logger_.isDebugEnabled())
                    logger_.debug("Snapshot for " + table_ + " table data file " + sourceFile.getAbsolutePath() +
                        " created as " + targetLink.getAbsolutePath());
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }

        }
    }

    public void loadRowCache()
    {
        CFMetaData metadata = DatabaseDescriptor.getTableMetaData(table_).get(columnFamily_);
        assert metadata != null;
        if (metadata.preloadRowCache)
        {
            logger_.debug(String.format("Loading cache for keyspace/columnfamily %s/%s", table_, columnFamily_));
            int ROWS = 4096;
            Token min = StorageService.getPartitioner().getMinimumToken();
            Token start = min;
            long i = 0;
            while (i < ssTables_.getRowCache().getCapacity())
            {
                RangeSliceReply result;
                try
                {
                    SliceRange range = new SliceRange(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, ROWS);
                    result = getRangeSlice(null, new Bounds(start, min), ROWS, range, null);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }

                for (Row row : result.rows)
                    ssTables_.getRowCache().put(row.key, row.cf);
                i += result.rows.size();
                if (result.rows.size() < ROWS)
                    break;

                start = DatabaseDescriptor.getPartitioner().getToken(result.rows.get(ROWS - 1).key.key);
            }
            logger_.info(String.format("Loaded %s rows into the %s cache", i, columnFamily_));
        }
    }


    public boolean hasUnreclaimedSpace()
    {
        return ssTables_.getLiveSize() < ssTables_.getTotalSize();
    }

    public long getTotalDiskSpaceUsed()
    {
        return ssTables_.getTotalSize();
    }

    public long getLiveDiskSpaceUsed()
    {
        return ssTables_.getLiveSize();
    }

    public int getLiveSSTableCount()
    {
        return ssTables_.size();
    }

    /** raw cached row -- does not fetch the row if it is not present.  not counted in cache statistics.  */
    public ColumnFamily getRawCachedRow(DecoratedKey key)
    {
        return ssTables_.getRowCache().getCapacity() == 0 ? null : ssTables_.getRowCache().getInternal(key);
    }

    void invalidateCachedRow(DecoratedKey key)
    {
        ssTables_.getRowCache().remove(key);
    }

    public void forceMajorCompaction()
    {
        CompactionManager.instance.submitMajor(this);
    }

    public void invalidateRowCache()
    {
        ssTables_.getRowCache().clear();
    }

    public static Iterable<ColumnFamilyStore> all()
    {
        Iterable<ColumnFamilyStore>[] stores = new Iterable[DatabaseDescriptor.getTables().size()];
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

    /**
     * for testing.  no effort is made to clear historical memtables.
     */
    void clearUnsafe()
    {
        memtable_.clearUnsafe();
        ssTables_.clearUnsafe();
    }


    public Set<Memtable> getMemtablesPendingFlush()
    {
        return memtablesPendingFlush;
    }
}
