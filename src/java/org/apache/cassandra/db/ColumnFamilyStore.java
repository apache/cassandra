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
import java.io.IOException;
import java.io.Closeable;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import org.apache.cassandra.cache.IAggregatableCacheProvider;
import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.cache.JMXAggregatingCache;
import org.apache.cassandra.cache.JMXInstrumentedCache;
import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.*;
import org.apache.cassandra.io.util.FileUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;

import org.apache.commons.collections.IteratorUtils;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import com.google.common.collect.Iterators;
import com.google.common.base.Predicate;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean
{
    private static Logger logger_ = Logger.getLogger(ColumnFamilyStore.class);

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
    private static NonBlockingHashMap<String, Set<Memtable>> memtablesPendingFlush = new NonBlockingHashMap<String, Set<Memtable>>();
    private static ExecutorService flushSorter_
            = new JMXEnabledThreadPoolExecutor(1,
                                               Runtime.getRuntime().availableProcessors(),
                                               Integer.MAX_VALUE,
                                               TimeUnit.SECONDS,
                                               new LinkedBlockingQueue<Runnable>(1 + 2 * Runtime.getRuntime().availableProcessors()),
                                               new NamedThreadFactory("FLUSH-SORTER-POOL"));
    private static ExecutorService flushWriter_
            = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getAllDataFileLocations().length,
                                               DatabaseDescriptor.getAllDataFileLocations().length,
                                               Integer.MAX_VALUE,
                                               TimeUnit.SECONDS,
                                               new LinkedBlockingQueue<Runnable>(),
                                               new NamedThreadFactory("FLUSH-WRITER-POOL"));
    private static ExecutorService commitLogUpdater_ = new JMXEnabledThreadPoolExecutor("MEMTABLE-POST-FLUSHER");

    private static final int KEY_RANGE_FILE_BUFFER_SIZE = 256 * 1024;

    private final String table_;
    public final String columnFamily_;
    private final boolean isSuper_;

    private volatile Integer memtableSwitchCount = 0;

    /* This is used to generate the next index for a SSTable */
    private AtomicInteger fileIndexGenerator_ = new AtomicInteger(0);

    /* active memtable associated with this ColumnFamilyStore. */
    private Memtable memtable_;

    private final JMXInstrumentedCache<String, ColumnFamily> rowCache;

    // TODO binarymemtable ops are not threadsafe (do they need to be?)
    private AtomicReference<BinaryMemtable> binaryMemtable_;

    /* SSTables on disk for this column family */
    private SSTableTracker ssTables_;

    private LatencyTracker readStats_ = new LatencyTracker();
    private LatencyTracker writeStats_ = new LatencyTracker();
    
    ColumnFamilyStore(String table, String columnFamilyName, boolean isSuper, int indexValue) throws IOException
    {
        table_ = table;
        columnFamily_ = columnFamilyName;
        isSuper_ = isSuper;
        fileIndexGenerator_.set(indexValue);
        memtable_ = new Memtable(table_, columnFamily_);
        binaryMemtable_ = new AtomicReference<BinaryMemtable>(new BinaryMemtable(table_, columnFamily_));

        if (logger_.isDebugEnabled())
            logger_.debug("Starting CFS " + columnFamily_);
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
                    FileUtils.deleteWithConfirm(file);
                    continue;
                }
            }

            if (((file.length() == 0 && !filename.endsWith("-Compacted")) || (filename.contains("-" + SSTable.TEMPFILE_MARKER))))
            {
                FileUtils.deleteWithConfirm(file);
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
        ssTables_ = new SSTableTracker(sstables);

        double v = DatabaseDescriptor.getRowsCachedFraction(table, columnFamilyName);
        int cacheSize;
        if (0 < v && v < 1)
            cacheSize = Math.max(1, (int)(v * ssTables_.estimatedKeys()));
        else
            cacheSize = (int)v;
        if (logger_.isDebugEnabled())
            logger_.debug("row cache capacity for " + columnFamilyName + " is " + cacheSize);
        rowCache = new JMXInstrumentedCache<String, ColumnFamily>(table, columnFamilyName + "RowCache", cacheSize);

        // we don't need to keep a reference to the key cache aggregator, just create it so it registers itself w/ JMX
        new JMXAggregatingCache(new Iterable<IAggregatableCacheProvider>()
        {
            public Iterator<IAggregatableCacheProvider> iterator()
            {
                final Iterator<SSTableReader> iter = ssTables_.iterator();
                return new AbstractIterator<IAggregatableCacheProvider>()
                {
                    @Override
                    protected IAggregatableCacheProvider computeNext()
                    {
                        if (!iter.hasNext())
                            return endOfData();

                        return new IAggregatableCacheProvider()
                        {
                            SSTableReader sstable = iter.next();

                            public InstrumentedCache getCache()
                            {
                                return sstable.getKeyCache();
                            }

                            public long getObjectCount()
                            {
                                return sstable.getIndexPositions().size() * SSTableReader.indexInterval();
                            }
                        };
                    }
                };
            }
        }, table, columnFamilyName + "KeyCache");
    }

    public static ColumnFamilyStore createColumnFamilyStore(String table, String columnFamily) throws IOException
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
                String filename = file.getName();
                String cfName = getColumnFamilyFromFileName(filename);

                if (cfName.equals(columnFamily))
                {
                    generations.add(getGenerationFromFileName(filename));
                }
            }
        }
        Collections.sort(generations);
        int value = (generations.size() > 0) ? (generations.get(generations.size() - 1)) : 0;

        ColumnFamilyStore cfs = new ColumnFamilyStore(table, columnFamily, "Super".equals(DatabaseDescriptor.getColumnType(table, columnFamily)), value);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            String mbeanName = "org.apache.cassandra.db:type=ColumnFamilyStores,keyspace=" + table + ",columnfamily=" + columnFamily;
            mbs.registerMBean(cfs, new ObjectName(mbeanName));
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
                String cfName = getColumnFamilyFromFileName(file.getName());
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
        return filename.split("-")[0];
    }

    public static int getGenerationFromFileName(String filename)
    {
        /*
         * File name is of the form <table>-<column family>-<index>-Data.db.
         * This tokenizer will strip the .db portion.
         */
        StringTokenizer st = new StringTokenizer(filename, "-");
        /*
         * Now I want to get the index portion of the filename. We accumulate
         * the indices and then sort them to get the max index.
         */
        int count = st.countTokens();
        int i = 0;
        String index = null;
        while (st.hasMoreElements())
        {
            index = (String) st.nextElement();
            if (i == (count - 2))
            {
                break;
            }
            ++i;
        }
        return Integer.parseInt(index);
    }

    /*
     * @return a temporary file name for an sstable.
     * When the sstable object is closed, it will be renamed to a non-temporary
     * format, so incomplete sstables can be recognized and removed on startup.
     */
    synchronized String getTempSSTablePath()
    {
        String fname = getTempSSTableFileName();
        return new File(DatabaseDescriptor.getNextAvailableDataLocation() + File.separator + table_, fname).getAbsolutePath();
    }

    public String getTempSSTableFileName()
    {
        return String.format("%s-%s-%s-Data.db",
                             columnFamily_, SSTable.TEMPFILE_MARKER, fileIndexGenerator_.incrementAndGet());
    }

    Future<?> switchMemtable(Memtable oldMemtable, final boolean writeCommitLog) throws IOException
    {
        /**
         *  If we can get the writelock, that means no new updates can come in and 
         *  all ongoing updates to memtables have completed. We can get the tail
         *  of the log and use it as the starting position for log replay on recovery.
         */
        Table.flusherLock.writeLock().lock();
        try
        {
            final CommitLog.CommitLogContext ctx = CommitLog.open().getContext(); // this is harmless if !writeCommitLog

            if (oldMemtable.isFrozen())
            {
                return null;
            }
            logger_.info(columnFamily_ + " has reached its threshold; switching in a fresh Memtable");
            oldMemtable.freeze();
            final Condition condition = submitFlush(oldMemtable);
            memtable_ = new Memtable(table_, columnFamily_);
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
                        onMemtableFlush(ctx);
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

    void switchBinaryMemtable(String key, byte[] buffer) throws IOException
    {
        binaryMemtable_.set(new BinaryMemtable(table_, columnFamily_));
        binaryMemtable_.get().put(key, buffer);
    }

    public void forceFlushIfExpired() throws IOException
    {
        if (memtable_.isExpired())
            forceFlush();
    }

    public Future<?> forceFlush() throws IOException
    {
        if (memtable_.isClean())
            return null;

        return switchMemtable(memtable_, true);
    }

    void forceBlockingFlush() throws IOException, ExecutionException, InterruptedException
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
    Memtable apply(String key, ColumnFamily columnFamily) throws IOException
    {
        long start = System.currentTimeMillis();

        boolean flushRequested = memtable_.isThresholdViolated();
        memtable_.put(key, columnFamily);
        writeStats_.add(System.currentTimeMillis() - start);
        
        return flushRequested ? memtable_ : null;
    }

    /*
     * Insert/Update the column family for this key. param @ lock - lock that
     * needs to be used. param @ key - key for update/insert param @
     * columnFamily - columnFamily changes
     */
    void applyBinary(String key, byte[] buffer) throws IOException
    {
        long start = System.currentTimeMillis();
        binaryMemtable_.get().put(key, buffer);
        writeStats_.add(System.currentTimeMillis() - start);
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
     * This method is called when the Memtable is frozen and ready to be flushed
     * to disk. This method informs the CommitLog that a particular ColumnFamily
     * is being flushed to disk.
     */
    void onMemtableFlush(CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        if (cLogCtx.isValidContext())
        {
            CommitLog.open().onMemtableFlush(table_, columnFamily_, cLogCtx);
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
        ssTables_.add(sstable);
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
        try
        {
            return Table.open(table_);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    void markCompacted(Collection<SSTableReader> sstables) throws IOException
    {
        ssTables_.markCompacted(sstables);
    }

    boolean isCompleteSSTables(Collection<SSTableReader> sstables)
    {
        return ssTables_.getSSTables().equals(new HashSet<SSTableReader>(sstables));
    }

    void replaceCompactedSSTables(Collection<SSTableReader> sstables, Iterable<SSTableReader> replacements)
            throws IOException
    {
        ssTables_.replace(sstables, replacements);
    }

    public static List<Memtable> getUnflushedMemtables(String cfName)
    {
        return new ArrayList<Memtable>(getMemtablesPendingFlushNotNull(cfName));
    }

    static Set<Memtable> getMemtablesPendingFlushNotNull(String columnFamilyName)
    {
        Set<Memtable> memtables = memtablesPendingFlush.get(columnFamilyName);
        if (memtables == null)
        {
            memtablesPendingFlush.putIfAbsent(columnFamilyName, new ConcurrentSkipListSet<Memtable>());
            memtables = memtablesPendingFlush.get(columnFamilyName); // might not be the object we just put, if there was a race!
        }
        return memtables;
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
    Condition submitFlush(final IFlushable flushable)
    {
        logger_.info("Enqueuing flush of " + flushable);
        if (flushable instanceof Memtable)
        {
            // special-casing Memtable here is a bit messy, but it's best to keep the flush-related happenings in one place
            // since they're a little complicated.  (We dont' want to move the remove back to switchMemtable, which is
            // the other sane option, since that could mean keeping a flushed memtable in the Historical set unnecessarily
            // while earlier flushes finish.)
            getMemtablesPendingFlushNotNull(columnFamily_).add((Memtable) flushable); // it's ok for the MT to briefly be both active and pendingFlush
        }
        final Condition condition = new SimpleCondition();
        flushSorter_.submit(new Runnable()
        {
            public void run()
            {
                final List sortedKeys = flushable.getSortedKeys();
                flushWriter_.submit(new WrappedRunnable()
                {
                    public void runMayThrow() throws IOException
                    {
                        addSSTable(flushable.writeSortedContents(sortedKeys));
                        if (flushable instanceof Memtable)
                        {
                            getMemtablesPendingFlushNotNull(columnFamily_).remove(flushable);
                        }
                        condition.signalAll();
                    }
                });
            }
        });
        return condition;
    }

    public boolean isSuper()
    {
        return isSuper_;
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

    public Iterator<DecoratedKey> memtableKeyIterator() throws ExecutionException, InterruptedException
    {
        Table.flusherLock.readLock().lock();
        try
        {
             return memtable_.getKeyIterator();
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

    public double getRecentReadLatency()
    {
        return readStats_.getRecentLatency();
    }

    public long getTotalReadLatency()
    {
        return readStats_.getTotalLatency();
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

    public long getTotalWriteLatency()
    {
        return writeStats_.getTotalLatency();
    }

    public double getRecentWriteLatency()
    {
        return writeStats_.getRecentLatency();
    }

    public ColumnFamily getColumnFamily(String key, QueryPath path, byte[] start, byte[] finish, boolean reversed, int limit) throws IOException
    {
        return getColumnFamily(new SliceQueryFilter(key, path, start, finish, reversed, limit));
    }

    public ColumnFamily getColumnFamily(QueryFilter filter) throws IOException
    {
        return getColumnFamily(filter, CompactionManager.getDefaultGCBefore());
    }

    private ColumnFamily cacheRow(String key) throws IOException
    {
        ColumnFamily cached;
        if ((cached = rowCache.get(key)) == null)
        {
            cached = getTopLevelColumns(new IdentityQueryFilter(key, new QueryPath(columnFamily_)), Integer.MIN_VALUE);
            if (cached == null)
                return null;
            rowCache.put(key, cached);
        }
        return cached;
    }

    /**
     * get a list of columns starting from a given column, in a specified order.
     * only the latest version of a column is returned.
     * @return null if there is no data and no tombstones; otherwise a ColumnFamily
     */
    public ColumnFamily getColumnFamily(QueryFilter filter, int gcBefore) throws IOException
    {
        assert columnFamily_.equals(filter.getColumnFamilyName());

        long start = System.currentTimeMillis();
        try
        {
            if (filter.path.superColumnName == null)
            {
                if (rowCache.getCapacity() == 0)
                    return removeDeleted(getTopLevelColumns(filter, gcBefore), gcBefore);

                ColumnFamily cached = cacheRow(filter.key);
                ColumnIterator ci = filter.getMemColumnIterator(memtable_, cached, getComparator()); // TODO passing memtable here is confusing since it's almost entirely unused
                ColumnFamily returnCF = ci.getColumnFamily();
                filter.collectCollatedColumns(returnCF, ci, gcBefore);
                return removeDeleted(returnCF, gcBefore);
            }

            // we are querying subcolumns of a supercolumn: fetch the supercolumn with NQF, then filter in-memory.
            ColumnFamily cf;
            SuperColumn sc;
            if (rowCache.getCapacity() == 0)
            {
                QueryFilter nameFilter = new NamesQueryFilter(filter.key, new QueryPath(columnFamily_), filter.path.superColumnName);
                cf = getTopLevelColumns(nameFilter, gcBefore);
                if (cf == null || cf.getColumnCount() == 0)
                    return cf;

                assert cf.getSortedColumns().size() == 1;
                sc = (SuperColumn)cf.getSortedColumns().iterator().next();
            }
            else
            {
                cf = cacheRow(filter.key);
                if (cf == null)
                    return null;
                sc = (SuperColumn)cf.getColumn(filter.path.superColumnName);
                if (sc == null)
                    return null;
                sc = (SuperColumn)sc.cloneMe();
            }
            
            SuperColumn scFiltered = filter.filterSuperColumn(sc, gcBefore);
            ColumnFamily cfFiltered = cf.cloneMeShallow();
            cfFiltered.addColumn(scFiltered);
            return removeDeleted(cfFiltered, gcBefore);
        }
        finally
        {
            readStats_.add(System.currentTimeMillis() - start);
        }
    }

    private ColumnFamily getTopLevelColumns(QueryFilter filter, int gcBefore) throws IOException
    {
        // we are querying top-level columns, do a merging fetch with indexes.
        List<ColumnIterator> iterators = new ArrayList<ColumnIterator>();
        try
        {
            final ColumnFamily returnCF;
            ColumnIterator iter;

            /* add the current memtable */
            Table.flusherLock.readLock().lock();
            try
            {
                iter = filter.getMemColumnIterator(memtable_, getComparator());
                // TODO this is a little subtle: the Memtable ColumnIterator has to be a shallow clone of the source CF,
                // with deletion times set correctly, so we can use it as the "base" CF to add query results to.
                // (for sstable ColumnIterators we do not care if it is a shallow clone or not.)
                returnCF = iter.getColumnFamily();
            }
            finally
            {
                Table.flusherLock.readLock().unlock();
            }
            iterators.add(iter);

            /* add the memtables being flushed */
            List<Memtable> memtables = getUnflushedMemtables(filter.getColumnFamilyName());
            for (Memtable memtable:memtables)
            {
                iter = filter.getMemColumnIterator(memtable, getComparator());
                returnCF.delete(iter.getColumnFamily());
                iterators.add(iter);
            }

            /* add the SSTables on disk */
            for (SSTableReader sstable : ssTables_)
            {
                iter = filter.getSSTableColumnIterator(sstable);
                if (iter.hasNext()) // initializes iter.CF
                {
                    returnCF.delete(iter.getColumnFamily());
                }
                iterators.add(iter);
            }

            Comparator<IColumn> comparator = filter.getColumnComparator(getComparator());
            Iterator collated = IteratorUtils.collatedIterator(comparator, iterators);
            if (!collated.hasNext())
                return null;

            filter.collectCollatedColumns(returnCF, collated, gcBefore);
            return returnCF;
        }
        finally
        {
            /* close all cursors */
            for (ColumnIterator ci : iterators)
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
     * @param startWith key to start with, inclusive.  empty string = start at beginning.
     * @param stopAt key to stop at, inclusive.  empty string = stop only when keys are exhausted.
     * @param maxResults
     * @return list of keys between startWith and stopAt

       TODO refactor better.  this is just getKeyRange w/o the deletion check, for the benefit of
       range_slice.  still opens one randomaccessfile per key, which sucks.  something like compactioniterator
       would be better.
     */
    private RangeReply getKeyRange(final DecoratedKey startWith, final DecoratedKey stopAt, int maxResults)
    throws IOException, ExecutionException, InterruptedException
    {
        // create a CollatedIterator that will return unique keys from different sources
        // (current memtable, historical memtables, and SSTables) in the correct order.
        List<Iterator<DecoratedKey>> iterators = new ArrayList<Iterator<DecoratedKey>>();

        // we iterate through memtables with a priority queue to avoid more sorting than necessary.
        // this predicate throws out the keys before the start of our range.
        Predicate<DecoratedKey> p = new Predicate<DecoratedKey>()
        {
            public boolean apply(DecoratedKey key)
            {
                return startWith.compareTo(key) <= 0
                       && (stopAt.isEmpty() || key.compareTo(stopAt) <= 0);
            }
        };

        // current memtable keys.  have to go through the CFS api for locking.
        iterators.add(Iterators.filter(memtableKeyIterator(), p));
        // historical memtables
        for (Memtable memtable : ColumnFamilyStore.getUnflushedMemtables(columnFamily_))
        {
            iterators.add(Iterators.filter(memtable.getKeyIterator(), p));
        }

        // sstables
        for (SSTableReader sstable : ssTables_)
        {
            final SSTableScanner scanner = sstable.getScanner(KEY_RANGE_FILE_BUFFER_SIZE);
            scanner.seekTo(startWith);
            Iterator<DecoratedKey> iter = new CloseableIterator<DecoratedKey>()
            {
                public boolean hasNext()
                {
                    return scanner.hasNext();
                }
                public DecoratedKey next()
                {
                    return scanner.next().getKey();
                }
                public void remove()
                {
                    throw new UnsupportedOperationException();
                }
                public void close() throws IOException
                {
                    scanner.close();
                }
            };
            assert iter instanceof Closeable; // otherwise we leak FDs
            iterators.add(iter);
        }

        Iterator<DecoratedKey> collated = IteratorUtils.collatedIterator(DecoratedKey.comparator, iterators);
        Iterable<DecoratedKey> reduced = new ReducingIterator<DecoratedKey, DecoratedKey>(collated) {
            DecoratedKey current;

            public void reduce(DecoratedKey current)
            {
                 this.current = current;
            }

            protected DecoratedKey getReduced()
            {
                return current;
            }
        };

        try
        {
            // pull keys out of the CollatedIterator
            List<String> keys = new ArrayList<String>();
            boolean rangeCompletedLocally = false;
            for (DecoratedKey current : reduced)
            {
                if (!stopAt.isEmpty() && stopAt.compareTo(current) < 0)
                {
                    rangeCompletedLocally = true;
                    break;
                }
                keys.add(current.key);
                if (keys.size() >= maxResults)
                {
                    rangeCompletedLocally = true;
                    break;
                }
            }
            return new RangeReply(keys, rangeCompletedLocally);
        }
        finally
        {
            for (Iterator iter : iterators)
            {
                if (iter instanceof Closeable)
                {
                    ((Closeable)iter).close();
                }
            }
        }
    }

    /**
     *
     * @param super_column
     * @param startKey key to start at (inclusive). empty string = start at the beginning.
     * @param finishKey key to stop at (inclusive). empty string = stop at the end.
     * @param keyMax maximum number of keys to process, regardless of startKey/finishKey
     * @param sliceRange may be null if columnNames is specified. specifies contiguous columns to return in what order.
     * @param columnNames may be null if sliceRange is specified. specifies which columns to return in what order.      @return list of key->list<column> tuples.
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public RangeSliceReply getRangeSlice(byte[] super_column, final DecoratedKey startKey, final DecoratedKey finishKey, int keyMax, SliceRange sliceRange, List<byte[]> columnNames)
    throws IOException, ExecutionException, InterruptedException
    {
        RangeReply rr = getKeyRange(startKey, finishKey, keyMax);
        List<Row> rows = new ArrayList<Row>(rr.keys.size());
        final QueryPath queryPath =  new QueryPath(columnFamily_, super_column, null);
        final SortedSet<byte[]> columnNameSet = new TreeSet<byte[]>(getComparator());
        if (columnNames != null)
            columnNameSet.addAll(columnNames);
        for (String key : rr.keys)
        {
            QueryFilter filter = sliceRange == null ? new NamesQueryFilter(key, queryPath, columnNameSet) : new SliceQueryFilter(key, queryPath, sliceRange.start, sliceRange.finish, sliceRange.reversed, sliceRange.count);
            rows.add(new Row(key, getColumnFamily(filter)));
        }

        return new RangeSliceReply(rows, rr.rangeCompletedLocally);
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
    public void snapshot(String snapshotName) throws IOException
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
    }

    public long getTotalDiskSpaceUsed()
    {
        long n = 0;
        for (File file : files())
        {
            n += file.length();
        }
        return n;
    }

    public long getLiveDiskSpaceUsed()
    {
        long n = 0;
        for (SSTableReader sstable : ssTables_)
        {
            n += sstable.bytesOnDisk();
        }
        return n;
    }

    public int getLiveSSTableCount()
    {
        return ssTables_.size();
    }

    /** raw cached row -- does not fetch the row if it is not present */
    public ColumnFamily getRawCachedRow(String key)
    {
        return rowCache.getCapacity() == 0 ? null : rowCache.get(key);
    }

    void invalidateCachedRow(String key)
    {
        if (rowCache != null)
            rowCache.remove(key);
    }

    public void forceMajorCompaction()
    {
        CompactionManager.instance.submitMajor(this);
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

    public Iterable<SSTable.KeyPosition> allIndexPositions()
    {
        Collection<SSTableReader> sstables = getSSTables();
        Iterable<SSTable.KeyPosition>[] positions = new Iterable[sstables.size()];
        int i = 0;
        for (SSTableReader sstable: sstables)
        {
            positions[i++] = sstable.getIndexPositions();
        }
        return Iterables.concat(positions);
    }

    /**
     * for testing.  no effort is made to clear historical memtables.
     */
    void clearUnsafe()
    {
        memtable_.clearUnsafe();
        ssTables_.clearUnsafe();
    }
}
