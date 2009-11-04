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

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.*;
import java.net.InetAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.collections.iterators.FilterIterator;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public final class ColumnFamilyStore implements ColumnFamilyStoreMBean
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
            = new DebuggableThreadPoolExecutor(1,
                                               Runtime.getRuntime().availableProcessors(),
                                               Integer.MAX_VALUE,
                                               TimeUnit.SECONDS,
                                               new LinkedBlockingQueue<Runnable>(2 * Runtime.getRuntime().availableProcessors()),
                                               new NamedThreadFactory("FLUSH-SORTER-POOL"));
    private static ExecutorService flushWriter_
            = new DebuggableThreadPoolExecutor(DatabaseDescriptor.getAllDataFileLocations().length,
                                               DatabaseDescriptor.getAllDataFileLocations().length,
                                               Integer.MAX_VALUE,
                                               TimeUnit.SECONDS,
                                               new LinkedBlockingQueue<Runnable>(),
                                               new NamedThreadFactory("FLUSH-WRITER-POOL"));
    private static ExecutorService commitLogUpdater_ = new DebuggableThreadPoolExecutor("MEMTABLE-POST-FLUSHER");

    private final String table_;
    public final String columnFamily_;
    private final boolean isSuper_;

    private volatile Integer memtableSwitchCount = 0;

    /* This is used to generate the next index for a SSTable */
    private AtomicInteger fileIndexGenerator_ = new AtomicInteger(0);

    /* active memtable associated with this ColumnFamilyStore. */
    private Memtable memtable_;

    // TODO binarymemtable ops are not threadsafe (do they need to be?)
    private AtomicReference<BinaryMemtable> binaryMemtable_;

    /* SSTables on disk for this column family */
    private SSTableTracker ssTables_ = new SSTableTracker();

    private TimedStatsDeque readStats_ = new TimedStatsDeque(60000);
    private TimedStatsDeque writeStats_ = new TimedStatsDeque(60000);
    
    private final IPartitioner partitioner = StorageService.getPartitioner();

    ColumnFamilyStore(String table, String columnFamilyName, boolean isSuper, int indexValue) throws IOException
    {
        table_ = table;
        columnFamily_ = columnFamilyName;
        isSuper_ = isSuper;
        fileIndexGenerator_.set(indexValue);
        memtable_ = new Memtable(table_, columnFamily_);
        binaryMemtable_ = new AtomicReference<BinaryMemtable>(new BinaryMemtable(table_, columnFamily_));
    }

    public static ColumnFamilyStore getColumnFamilyStore(String table, String columnFamily) throws IOException
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
            mbs.registerMBean(cfs, new ObjectName(
                    "org.apache.cassandra.db:type=ColumnFamilyStores,name=" + table + ",columnfamily=" + columnFamily));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        return cfs;
    }

    void onStart() throws IOException
    {
        if (logger_.isDebugEnabled())
            logger_.debug("Starting CFS " + columnFamily_);
        // scan for data files corresponding to this CF
        List<File> sstableFiles = new ArrayList<File>();
        Pattern auxFilePattern = Pattern.compile("(.*)(-Filter\\.db$|-Index\\.db$)");
        String[] dataFileDirectories = DatabaseDescriptor.getAllDataFileLocationsForTable(table_);
        for (String directory : dataFileDirectories)
        {
            File fileDir = new File(directory);
            File[] files = fileDir.listFiles();
            for (File file : files)
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

                if (((file.length() == 0 && !filename.endsWith("-Compacted")) || (filename.contains("-" + SSTable.TEMPFILE_MARKER))) && (filename.contains(columnFamily_)))
                {
                    FileUtils.deleteWithConfirm(file);
                    continue;
                }

                String cfName = getColumnFamilyFromFileName(filename);
                if (cfName.equals(columnFamily_) && filename.contains("-Data.db"))
                {
                    sstableFiles.add(file.getAbsoluteFile());
                }
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
        ssTables_.onStart(sstables);

        // submit initial check-for-compaction request
        CompactionManager.instance().submit(ColumnFamilyStore.this);

        // schedule hinted handoff
        if (table_.equals(Table.SYSTEM_TABLE) && columnFamily_.equals(HintedHandOffManager.HINTS_CF))
        {
            HintedHandOffManager.instance().scheduleHandoffsFor(this);
        }
    }

    /*
     * This method is called to obtain statistics about
     * the Column Family represented by this Column Family
     * Store. It will report the total number of files on
     * disk and the total space oocupied by the data files
     * associated with this Column Family.
    */
    public String cfStats(String newLineSeparator)
    {
        StringBuilder sb = new StringBuilder();
        /*
         * We want to do this so that if there are
         * no files on disk we do not want to display
         * something ugly on the admin page.
        */
        if (ssTables_.size() == 0)
        {
            return sb.toString();
        }
        sb.append(columnFamily_ + " statistics :");
        sb.append(newLineSeparator);
        sb.append("Number of files on disk : " + ssTables_.size());
        sb.append(newLineSeparator);
        double totalSpace = 0d;
        for (SSTableReader sstable: ssTables_)
        {
            File f = new File(sstable.getFilename());
            totalSpace += f.length();
        }
        String diskSpace = FileUtils.stringifyFileSize(totalSpace);
        sb.append("Total disk space : " + diskSpace);
        sb.append(newLineSeparator);
        sb.append("--------------------------------------");
        sb.append(newLineSeparator);
        return sb.toString();
    }
    
    /*
     * This method forces a compaction of the SSTables on disk. We wait
     * for the process to complete by waiting on a future pointer.
    */
    List<SSTableReader> forceAntiCompaction(Collection<Range> ranges, InetAddress target)
    {
        assert ranges != null;
        Future<List<SSTableReader>> futurePtr = CompactionManager.instance().submit(ColumnFamilyStore.this, ranges, target);

        List<SSTableReader> result;
        try
        {
            /* Waiting for the compaction to complete. */
            result = futurePtr.get();
            if (logger_.isDebugEnabled())
              logger_.debug("Done forcing compaction ...");
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
        return result;
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
        return new File(DatabaseDescriptor.getDataFileLocationForTable(table_), fname).getAbsolutePath();
    }

    public String getTempSSTableFileName()
    {
        return String.format("%s-%s-%s-Data.db",
                             columnFamily_, SSTable.TEMPFILE_MARKER, fileIndexGenerator_.incrementAndGet());
    }

    Future<?> switchMemtable(Memtable oldMemtable) throws IOException
    {
        /**
         *  If we can get the writelock, that means no new updates can come in and 
         *  all ongoing updates to memtables have completed. We can get the tail
         *  of the log and use it as the starting position for log replay on recovery.
         */
        Table.flusherLock_.writeLock().lock();
        try
        {
            final CommitLog.CommitLogContext ctx = CommitLog.open().getContext();

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
            return commitLogUpdater_.submit(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        condition.await();
                        onMemtableFlush(ctx);
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        finally
        {
            Table.flusherLock_.writeLock().unlock();
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

        return switchMemtable(memtable_);
    }

    void forceBlockingFlush() throws IOException, ExecutionException, InterruptedException
    {
        Future<?> future = forceFlush();
        if (future != null)
            future.get();
    }

    public void forceFlushBinary()
    {
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
    static ColumnFamily removeDeleted(ColumnFamily cf)
    {
        return removeDeleted(cf, getDefaultGCBefore());
    }

    public static int getDefaultGCBefore()
    {
        return (int)(System.currentTimeMillis() / 1000) - DatabaseDescriptor.getGcGraceInSeconds();
    }

    public static ColumnFamily removeDeleted(ColumnFamily cf, int gcBefore)
    {
        if (cf == null)
        {
            return null;
        }

        // in case of a timestamp tie, tombstones get priority over non-tombstones.
        // (we want this to be deterministic to avoid confusion.)
        for (byte[] cname : cf.getColumnNames())
        {
            IColumn c = cf.getColumnsMap().get(cname);
            if (c instanceof SuperColumn)
            {
                long minTimestamp = Math.max(c.getMarkedForDeleteAt(), cf.getMarkedForDeleteAt());
                // don't operate directly on the supercolumn, it could be the one in the memtable.
                // instead, create a new SC and add in the subcolumns that qualify.
                cf.remove(cname);
                SuperColumn sc = ((SuperColumn)c).cloneMeShallow();
                for (IColumn subColumn : c.getSubColumns())
                {
                    if (subColumn.timestamp() > minTimestamp)
                    {
                        if (!subColumn.isMarkedForDelete() || subColumn.getLocalDeletionTime() > gcBefore)
                        {
                            sc.addColumn(subColumn);
                        }
                    }
                }
                if (sc.getSubColumns().size() > 0 || sc.getLocalDeletionTime() > gcBefore)
                {
                    cf.addColumn(sc);
                }
            }
            else if ((c.isMarkedForDelete() && c.getLocalDeletionTime() <= gcBefore)
                     || c.timestamp() <= cf.getMarkedForDeleteAt())
            {
                cf.remove(cname);
            }
        }

        if (cf.getColumnCount() == 0 && cf.getLocalDeletionTime() <= gcBefore)
        {
            return null;
        }
        return cf;
    }

    /*
     * This version is used only on start up when we are recovering from logs.
     * Hence no locking is required since we process logs on the main thread. In
     * the future we may want to parellelize the log processing for a table by
     * having a thread per log file present for recovery. Re-visit at that time.
     */
    void applyNow(String key, ColumnFamily columnFamily) throws IOException
    {
        getMemtableThreadSafe().put(key, columnFamily);
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
     * param @ bf - bloom filter which indicates the keys that are in this file.
    */
    public void addSSTable(SSTableReader sstable)
    {
        ssTables_.add(sstable);
        CompactionManager.instance().submit(this);
    }

    /*
     * Group files of similar size into buckets.
     */
    static Set<List<SSTableReader>> getCompactionBuckets(Iterable<SSTableReader> files, long min)
    {
        Map<List<SSTableReader>, Long> buckets = new HashMap<List<SSTableReader>, Long>();
        for (SSTableReader sstable : files)
        {
            long size = sstable.length();

            boolean bFound = false;
            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than `min`)
            for (List<SSTableReader> bucket : buckets.keySet())
            {
                long averageSize = buckets.get(bucket);
                if ((size > averageSize / 2 && size < 3 * averageSize / 2)
                    || (size < min && averageSize < min))
                {
                    // remove and re-add because adding changes the hash
                    buckets.remove(bucket);
                    averageSize = (averageSize + size) / 2;
                    bucket.add(sstable);
                    buckets.put(bucket, averageSize);
                    bFound = true;
                    break;
                }
            }
            // no similar bucket found; put it in a new one
            if (!bFound)
            {
                ArrayList<SSTableReader> bucket = new ArrayList<SSTableReader>();
                bucket.add(sstable);
                buckets.put(bucket, size);
            }
        }

        return buckets.keySet();
    }

    /*
     * Break the files into buckets and then compact.
     */
    int doCompaction(int minThreshold, int maxThreshold) throws IOException
    {
        int filesCompacted = 0;
        if (minThreshold > 0 && maxThreshold > 0)
        {
            logger_.debug("Checking to see if compaction of " + columnFamily_ + " would be useful");
            for (List<SSTableReader> sstables : getCompactionBuckets(ssTables_, 50L * 1024L * 1024L))
            {
                if (sstables.size() < minThreshold)
                {
                    continue;
                }
                // if we have too many to compact all at once, compact older ones first -- this avoids
                // re-compacting files we just created.
                Collections.sort(sstables);
                filesCompacted += doFileCompaction(sstables.subList(0, Math.min(sstables.size(), maxThreshold)));
            }
            logger_.debug(filesCompacted + " files compacted");
        }
        else
        {
            logger_.debug("Compaction is currently disabled.");
        }
        return filesCompacted;
    }

    void doMajorCompaction(long skip) throws IOException
    {
        doMajorCompactionInternal(skip);
    }

    /*
     * Compact all the files irrespective of the size.
     * skip : is the amount in GB of the files to be skipped
     * all files greater than skip GB are skipped for this compaction.
     * Except if skip is 0 , in that case this is ignored and all files are taken.
     */
    void doMajorCompactionInternal(long skip) throws IOException
    {
        Collection<SSTableReader> sstables;
        if (skip > 0L)
        {
            sstables = new ArrayList<SSTableReader>();
            for (SSTableReader sstable : ssTables_)
            {
                if (sstable.length() < skip * 1024L * 1024L * 1024L)
                {
                    sstables.add(sstable);
                }
            }
        }
        else
        {
            sstables = ssTables_.getSSTables();
        }

        doFileCompaction(sstables);
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

    List<SSTableReader> doAntiCompaction(Collection<Range> ranges, InetAddress target) throws IOException
    {
        return doFileAntiCompaction(ssTables_.getSSTables(), ranges, target);
    }

    void forceCleanup()
    {
        CompactionManager.instance().submitCleanup(ColumnFamilyStore.this);
    }

    /**
     * This function goes over each file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    void doCleanupCompaction() throws IOException
    {
        for (SSTableReader sstable : ssTables_)
        {
            doCleanup(sstable);
        }
    }

    /**
     * cleans up one particular file by removing keys that this node is not responsible for.
     * @throws IOException
     */
    /* TODO: Take care of the comments later. */
    void doCleanup(SSTableReader sstable) throws IOException
    {
        assert sstable != null;
        List<SSTableReader> sstables = doFileAntiCompaction(Arrays.asList(sstable), StorageService.instance().getLocalRanges(), null);
        if (!sstables.isEmpty())
        {
            assert sstables.size() == 1;
            addSSTable(sstables.get(0));
        }
        if (logger_.isDebugEnabled())
          logger_.debug("Original file : " + sstable + " of size " + sstable.length());
        ssTables_.markCompacted(Arrays.asList(sstable));
    }

    /**
     * This function is used to do the anti compaction process , it spits out the file which has keys that belong to a given range
     * If the target is not specified it spits out the file as a compacted file with the unecessary ranges wiped out.
     *
     * @param sstables
     * @param ranges
     * @param target
     * @return
     * @throws IOException
     */
    List<SSTableReader> doFileAntiCompaction(Collection<SSTableReader> sstables, Collection<Range> ranges, InetAddress target) throws IOException
    {
        logger_.info("AntiCompacting [" + StringUtils.join(sstables, ",") + "]");
        // Calculate the expected compacted filesize
        long expectedRangeFileSize = getExpectedCompactedFileSize(sstables) / 2;
        String compactionFileLocation = DatabaseDescriptor.getDataFileLocationForTable(table_, expectedRangeFileSize);
        if (compactionFileLocation == null)
        {
            throw new UnsupportedOperationException("disk full");
        }
        List<SSTableReader> results = new ArrayList<SSTableReader>();

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        int expectedBloomFilterSize = Math.max(SSTableReader.indexInterval(), (int)(SSTableReader.getApproximateKeyCount(sstables) / 2));
        if (logger_.isDebugEnabled())
          logger_.debug("Expected bloom filter size : " + expectedBloomFilterSize);

        SSTableWriter writer = null;
        CompactionIterator ci = new CompactionIterator(sstables, getDefaultGCBefore());

        try
        {
            if (!ci.hasNext())
            {
                logger_.warn("Nothing to compact (all files empty or corrupt). This should not happen.");
                return results;
            }

            while (ci.hasNext())
            {
                CompactionIterator.CompactedRow row = ci.next();
                if (Range.isTokenInRanges(row.key.token, ranges))
                {
                    if (writer == null)
                    {
                        if (target != null)
                        {
                            compactionFileLocation = compactionFileLocation + File.separator + "bootstrap";
                        }
                        FileUtils.createDirectory(compactionFileLocation);
                        String newFilename = new File(compactionFileLocation, getTempSSTableFileName()).getAbsolutePath();
                        writer = new SSTableWriter(newFilename, expectedBloomFilterSize, StorageService.getPartitioner());
                    }
                    writer.append(row.key, row.buffer);
                    totalkeysWritten++;
                }
            }
        }
        finally
        {
            ci.close();
        }

        if (writer != null)
        {
            results.add(writer.closeAndOpenReader(DatabaseDescriptor.getKeysCachedFraction(table_)));
            String format = "AntiCompacted to %s.  %d/%d bytes for %d keys.  Time: %dms.";
            long dTime = System.currentTimeMillis() - startTime;
            logger_.info(String.format(format, writer.getFilename(), getTotalBytes(sstables), results.get(0).length(), totalkeysWritten, dTime));
        }

        return results;
    }

    private int doFileCompaction(Collection<SSTableReader> sstables) throws IOException
    {
        return doFileCompaction(sstables, getDefaultGCBefore());
    }

    /*
    * This function does the actual compaction for files.
    * It maintains a priority queue of with the first key from each file
    * and then removes the top of the queue and adds it to the SStable and
    * repeats this process while reading the next from each file until its
    * done with all the files . The SStable to which the keys are written
    * represents the new compacted file. Before writing if there are keys
    * that occur in multiple files and are the same then a resolution is done
    * to get the latest data.
    *
    * The collection of sstables passed may be empty (but not null); even if
    * it is not empty, it may compact down to nothing if all rows are deleted.
    */
    int doFileCompaction(Collection<SSTableReader> sstables, int gcBefore) throws IOException
    {
        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            Table.open(table_).snapshot("compact-" + columnFamily_);
        logger_.info("Compacting [" + StringUtils.join(sstables, ",") + "]");
        String compactionFileLocation = DatabaseDescriptor.getDataFileLocationForTable(table_, getExpectedCompactedFileSize(sstables));
        // If the compaction file path is null that means we have no space left for this compaction.
        // try again w/o the largest one.
        if (compactionFileLocation == null)
        {
            SSTableReader maxFile = getMaxSizeFile(sstables);
            List<SSTableReader> smallerSSTables = new ArrayList<SSTableReader>(sstables);
            smallerSSTables.remove(maxFile);
            return doFileCompaction(smallerSSTables);
        }

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        // TODO the int cast here is potentially buggy
        int expectedBloomFilterSize = Math.max(SSTableReader.indexInterval(), (int)SSTableReader.getApproximateKeyCount(sstables));
        if (logger_.isDebugEnabled())
          logger_.debug("Expected bloom filter size : " + expectedBloomFilterSize);

        SSTableWriter writer;
        CompactionIterator ci = new CompactionIterator(sstables, gcBefore); // retain a handle so we can call close()
        Iterator nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());

        try
        {
            if (!nni.hasNext())
            {
                // don't mark compacted in the finally block, since if there _is_ nondeleted data,
                // we need to sync it (via closeAndOpen) first, so there is no period during which
                // a crash could cause data loss.
                ssTables_.markCompacted(sstables);
                return 0;
            }

            String newFilename = new File(compactionFileLocation, getTempSSTableFileName()).getAbsolutePath();
            writer = new SSTableWriter(newFilename, expectedBloomFilterSize, StorageService.getPartitioner());

            while (nni.hasNext())
            {
                CompactionIterator.CompactedRow row = (CompactionIterator.CompactedRow) nni.next();
                writer.append(row.key, row.buffer);
                totalkeysWritten++;
            }
        }
        finally
        {
            ci.close();
        }

        SSTableReader ssTable = writer.closeAndOpenReader(DatabaseDescriptor.getKeysCachedFraction(table_));
        ssTables_.add(ssTable);
        ssTables_.markCompacted(sstables);
        CompactionManager.instance().submit(ColumnFamilyStore.this);

        String format = "Compacted to %s.  %d/%d bytes for %d keys.  Time: %dms.";
        long dTime = System.currentTimeMillis() - startTime;
        logger_.info(String.format(format, writer.getFilename(), getTotalBytes(sstables), ssTable.length(), totalkeysWritten, dTime));
        return sstables.size();
    }

    private long getTotalBytes(Iterable<SSTableReader> sstables)
    {
        long sum = 0;
        for (SSTableReader sstable : sstables)
        {
            sum += sstable.length();
        }
        return sum;
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
                flushWriter_.submit(new Runnable()
                {
                    public void run()
                    {
                        try
                        {
                            addSSTable(flushable.writeSortedContents(sortedKeys));
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException(e);
                        }
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

    public void flushMemtableOnRecovery() throws IOException
    {
        getMemtableThreadSafe().flushOnRecovery();
    }

    public int getMemtableColumnsCount()
    {
        return getMemtableThreadSafe().getCurrentObjectCount();
    }

    public int getMemtableDataSize()
    {
        return getMemtableThreadSafe().getCurrentSize();
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
        Table.flusherLock_.readLock().lock();
        try
        {
            return memtable_;
        }
        finally
        {
            Table.flusherLock_.readLock().unlock();
        }
    }

    public Iterator<DecoratedKey> memtableKeyIterator() throws ExecutionException, InterruptedException
    {
        Table.flusherLock_.readLock().lock();
        try
        {
             return memtable_.getKeyIterator();
        }
        finally
        {
            Table.flusherLock_.readLock().unlock();
        }
    }

    public Collection<SSTableReader> getSSTables()
    {
        return ssTables_.getSSTables();
    }

    public int getReadCount()
    {
        return readStats_.size();
    }

    public double getReadLatency()
    {
        return readStats_.mean();
    }

    // TODO this actually isn't a good meature of pending tasks
    public int getPendingTasks()
    {
        return Table.flusherLock_.getQueueLength();
    }

    /**
     * @return the number of write operations on this column family in the last minute
     */
    public int getWriteCount() {
        return writeStats_.size();
    }

    /**
     * @return average latency per write operation in the last minute
     */
    public double getWriteLatency() {
        return writeStats_.mean();
    }

    public ColumnFamily getColumnFamily(String key, QueryPath path, byte[] start, byte[] finish, boolean reversed, int limit) throws IOException
    {
        return getColumnFamily(new SliceQueryFilter(key, path, start, finish, reversed, limit));
    }

    public ColumnFamily getColumnFamily(QueryFilter filter) throws IOException
    {
        return getColumnFamily(filter, getDefaultGCBefore());
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
            // if we are querying subcolumns of a supercolumn, fetch the supercolumn with NQF, then filter in-memory.
            if (filter.path.superColumnName != null)
            {
                QueryFilter nameFilter = new NamesQueryFilter(filter.key, new QueryPath(columnFamily_), filter.path.superColumnName);
                ColumnFamily cf = getColumnFamilyInternal(nameFilter, getDefaultGCBefore());
                if (cf == null || cf.getColumnCount() == 0)
                    return cf;

                assert cf.getSortedColumns().size() == 1;
                SuperColumn sc = (SuperColumn)cf.getSortedColumns().iterator().next();
                SuperColumn scFiltered = filter.filterSuperColumn(sc, gcBefore);
                ColumnFamily cfFiltered = cf.cloneMeShallow();
                cfFiltered.addColumn(scFiltered);
                return removeDeleted(cfFiltered, gcBefore);
            }

            return removeDeleted(getColumnFamilyInternal(filter, gcBefore), gcBefore);
        }
        finally
        {
            readStats_.add(System.currentTimeMillis() - start);
        }
    }

    private ColumnFamily getColumnFamilyInternal(QueryFilter filter, int gcBefore) throws IOException
    {
        // we are querying top-level columns, do a merging fetch with indexes.
        List<ColumnIterator> iterators = new ArrayList<ColumnIterator>();
        try
        {
            final ColumnFamily returnCF;
            ColumnIterator iter;

            /* add the current memtable */
            Table.flusherLock_.readLock().lock();
            try
            {
                iter = filter.getMemColumnIterator(memtable_, getComparator());
                returnCF = iter.getColumnFamily();
            }
            finally
            {
                Table.flusherLock_.readLock().unlock();
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
     */
    public RangeReply getKeyRange(final String startWith, final String stopAt, int maxResults)
    throws IOException, ExecutionException, InterruptedException
    {
        final DecoratedKey startWithDK = partitioner.decorateKey(startWith);
        final DecoratedKey stopAtDK = partitioner.decorateKey(stopAt);
        // (OPP key decoration is a no-op so using the "decorated" comparator against raw keys is fine)
        final Comparator<DecoratedKey> comparator = partitioner.getDecoratedKeyComparator();

        // create a CollatedIterator that will return unique keys from different sources
        // (current memtable, historical memtables, and SSTables) in the correct order.
        List<Iterator<DecoratedKey>> iterators = new ArrayList<Iterator<DecoratedKey>>();

        // we iterate through memtables with a priority queue to avoid more sorting than necessary.
        // this predicate throws out the keys before the start of our range.
        Predicate<DecoratedKey> p = new Predicate<DecoratedKey>()
        {
            public boolean apply(DecoratedKey key)
            {
                return comparator.compare(startWithDK, key) <= 0
                       && (stopAt.isEmpty() || comparator.compare(key, stopAtDK) <= 0);
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
            final SSTableScanner scanner = sstable.getScanner();
            scanner.seekTo(startWithDK);
            Iterator<DecoratedKey> iter = new Iterator<DecoratedKey>()
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
            };
            iterators.add(iter);
        }

        Iterator<DecoratedKey> collated = IteratorUtils.collatedIterator(comparator, iterators);
        
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
            // pull keys out of the CollatedIterator.  checking tombstone status is expensive,
            // so we set an arbitrary limit on how many we'll do at once.
            List<String> keys = new ArrayList<String>();
            boolean rangeCompletedLocally = false;
            for (DecoratedKey current : reduced)
            {
                if (!stopAt.isEmpty() && comparator.compare(stopAtDK, current) < 0)
                {
                    rangeCompletedLocally = true;
                    break;
                }
                // make sure there is actually non-tombstone content associated w/ this key
                // TODO record the key source(s) somehow and only check that source (e.g., memtable or sstable)
                QueryFilter filter = new SliceQueryFilter(current.key, new QueryPath(columnFamily_), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, 1);
                if (getColumnFamily(filter, Integer.MAX_VALUE) != null)
                {
                    keys.add(current.key);
                }
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

    /**
     * for testing.  no effort is made to clear historical memtables.
     */
    void clearUnsafe()
    {
        memtable_.clearUnsafe();
        ssTables_.clearUnsafe();
    }

}
