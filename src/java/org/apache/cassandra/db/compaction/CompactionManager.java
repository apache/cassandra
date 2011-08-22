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

package org.apache.cassandra.db.compaction;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.MapMaker;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.AntiEntropyService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.*;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

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
    // acquire as read to perform a compaction, and as write to prevent compactions
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

    private CompactionExecutor executor = new CompactionExecutor();
    private CompactionExecutor validationExecutor = new ValidationExecutor();
    private Map<ColumnFamilyStore, Integer> estimatedCompactions = new MapMaker().concurrencyLevel(1).weakKeys().makeMap();

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
    public Future<Integer> submitMinorIfNeeded(final ColumnFamilyStore cfs)
    {
        Callable<Integer> callable = new Callable<Integer>()
        {
            public Integer call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    if (cfs.isInvalid())
                        return 0;
                    Integer minThreshold = cfs.getMinimumCompactionThreshold();
                    Integer maxThreshold = cfs.getMaximumCompactionThreshold();
    
                    if (minThreshold == 0 || maxThreshold == 0)
                    {
                        logger.debug("Compaction is currently disabled.");
                        return 0;
                    }
                    logger.debug("Checking to see if compaction of " + cfs.columnFamily + " would be useful");
                    Set<List<SSTableReader>> buckets = getBuckets(convertSSTablesToPairs(cfs.getSSTables()), 50L * 1024L * 1024L);
                    updateEstimateFor(cfs, buckets);
                    int gcBefore = getDefaultGcBefore(cfs);
                    
                    for (List<SSTableReader> sstables : buckets)
                    {
                        if (sstables.size() < minThreshold)
                            continue;
                        // if we have too many to compact all at once, compact older ones first -- this avoids
                        // re-compacting files we just created.
                        Collections.sort(sstables);
                        Collection<SSTableReader> tocompact = cfs.getDataTracker().markCompacting(sstables, minThreshold, maxThreshold);
                        if (tocompact == null)
                            // enough threads are busy in this bucket
                            continue;
                        try
                        {
                            return doCompaction(cfs, tocompact, gcBefore);
                        }
                        finally
                        {
                            cfs.getDataTracker().unmarkCompacting(tocompact);
                        }
                    }
                }
                finally 
                {
                    compactionLock.readLock().unlock();
                }
                return 0;
            }
        };
        return executor.submit(callable);
    }

    private void updateEstimateFor(ColumnFamilyStore cfs, Set<List<SSTableReader>> buckets)
    {
        Integer minThreshold = cfs.getMinimumCompactionThreshold();
        Integer maxThreshold = cfs.getMaximumCompactionThreshold();

        if (minThreshold > 0 && maxThreshold > 0)
        {
            int n = 0;
            for (List<SSTableReader> sstables : buckets)
            {
                if (sstables.size() >= minThreshold)
                {
                    n += Math.ceil((double)sstables.size() / maxThreshold);
                }
            }
            estimatedCompactions.put(cfs, n);
        }
        else
        {
            logger.debug("Compaction is currently disabled.");
        }
    }

    public void performCleanup(final ColumnFamilyStore cfStore, final NodeId.OneShotRenewer renewer) throws InterruptedException, ExecutionException
    {
        Callable<Object> runnable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                // acquire the write lock to schedule all sstables
                compactionLock.writeLock().lock();
                try 
                {
                    if (cfStore.isInvalid())
                        return this;
                    Collection<SSTableReader> tocleanup = cfStore.getDataTracker().markCompacting(cfStore.getSSTables(), 1, Integer.MAX_VALUE);
                    if (tocleanup == null || tocleanup.isEmpty())
                        return this;
                    try
                    {
                        // downgrade the lock acquisition
                        compactionLock.readLock().lock();
                        compactionLock.writeLock().unlock();
                        try
                        {
                            doCleanupCompaction(cfStore, tocleanup, renewer);
                        }
                        finally
                        {
                            compactionLock.readLock().unlock();
                        }
                    }
                    finally
                    {
                        cfStore.getDataTracker().unmarkCompacting(tocleanup);
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

    public void performScrub(final ColumnFamilyStore cfStore) throws InterruptedException, ExecutionException
    {
        Callable<Object> runnable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                // acquire the write lock to schedule all sstables
                compactionLock.writeLock().lock();
                try
                {
                    if (cfStore.isInvalid())
                        return this;

                    Collection<SSTableReader> toscrub = cfStore.getDataTracker().markCompacting(cfStore.getSSTables(), 1, Integer.MAX_VALUE);
                    if (toscrub == null || toscrub.isEmpty())
                        return this;
                    try
                    {
                        // downgrade the lock acquisition
                        compactionLock.readLock().lock();
                        compactionLock.writeLock().unlock();
                        try
                        {
                            doScrub(cfStore, toscrub);
                        }
                        finally
                        {
                            compactionLock.readLock().unlock();
                        }
                    }
                    finally
                    {
                        cfStore.getDataTracker().unmarkCompacting(toscrub);
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

    public void performMajor(final ColumnFamilyStore cfStore) throws InterruptedException, ExecutionException
    {
        submitMajor(cfStore, 0, getDefaultGcBefore(cfStore)).get();
    }

    public Future<Object> submitMajor(final ColumnFamilyStore cfStore, final long skip, final int gcBefore)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                // acquire the write lock long enough to schedule all sstables
                compactionLock.writeLock().lock();
                try
                {
                    if (cfStore.isInvalid())
                        return this;
                    Collection<SSTableReader> sstables;
                    if (skip > 0)
                    {
                        sstables = new ArrayList<SSTableReader>();
                        for (SSTableReader sstable : cfStore.getSSTables())
                        {
                            if (sstable.length() < skip * 1024L * 1024L * 1024L)
                            {
                                sstables.add(sstable);
                            }
                        }
                    }
                    else
                    {
                        sstables = cfStore.getSSTables();
                    }

                    Collection<SSTableReader> tocompact = cfStore.getDataTracker().markCompacting(sstables, 0, Integer.MAX_VALUE);
                    if (tocompact == null || tocompact.isEmpty())
                        return this;
                    try
                    {
                        // downgrade the lock acquisition
                        compactionLock.readLock().lock();
                        compactionLock.writeLock().unlock();
                        try
                        {
                            doCompaction(cfStore, tocompact, gcBefore);
                        }
                        finally
                        {
                            compactionLock.readLock().unlock();
                        }
                    }
                    finally
                    {
                        cfStore.getDataTracker().unmarkCompacting(tocompact);
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
        return executor.submit(callable);
    }

    public void forceUserDefinedCompaction(String ksname, String dataFiles)
    {
        if (!DatabaseDescriptor.getTables().contains(ksname))
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

    public Future<Object> submitUserDefined(final ColumnFamilyStore cfs, final Collection<Descriptor> dataFiles, final int gcBefore)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    if (cfs.isInvalid())
                        return this;

                    // look up the sstables now that we're on the compaction executor, so we don't try to re-compact
                    // something that was already being compacted earlier.
                    Collection<SSTableReader> sstables = new ArrayList<SSTableReader>();
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
                        logger.error("No file to compact for user defined compaction");
                    }
                    // attempt to schedule the set
                    else if ((sstables = cfs.getDataTracker().markCompacting(sstables, 1, Integer.MAX_VALUE)) != null)
                    {
                        String location = cfs.table.getDataFileLocation(1);
                        // success: perform the compaction
                        try
                        {
                            // Forcing deserialization because in case the user wants expired columns to be transformed to tombstones
                            doCompactionWithoutSizeEstimation(cfs, sstables, gcBefore, location, true);
                        }
                        finally
                        {
                            cfs.getDataTracker().unmarkCompacting(sstables);
                        }
                    }
                    else
                    {
                        logger.error("SSTables for user defined compaction are already being compacted.");
                    }

                    return this;
                }
                finally
                {
                    compactionLock.readLock().unlock();
                }
            }
        };
        return executor.submit(callable);
    }

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
                    if (!cfStore.isInvalid())
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
        for (String ksname : DatabaseDescriptor.getNonSystemTables())
        {
            for (ColumnFamilyStore cfs : Table.open(ksname).getColumnFamilyStores())
                cfs.disableAutoCompaction();
        }
    }

    int doCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore) throws IOException
    {
        if (sstables.size() < 2)
        {
            logger.info("Nothing to compact in " + cfs.getColumnFamilyName() + "; use forceUserDefinedCompaction if you wish to force compaction of single sstables (e.g. for tombstone collection)");
            return 0;
        }

        Table table = cfs.table;

        // If the compaction file path is null that means we have no space left for this compaction.
        // try again w/o the largest one.
        Set<SSTableReader> smallerSSTables = new HashSet<SSTableReader>(sstables);
        while (smallerSSTables.size() > 1)
        {
            String compactionFileLocation = table.getDataFileLocation(cfs.getExpectedCompactedFileSize(smallerSSTables));
            if (compactionFileLocation != null)
                return doCompactionWithoutSizeEstimation(cfs, smallerSSTables, gcBefore, compactionFileLocation, false);

            logger.warn("insufficient space to compact all requested files " + StringUtils.join(smallerSSTables, ", "));
            smallerSSTables.remove(cfs.getMaxSizeFile(smallerSSTables));
        }

        logger.error("insufficient space to compact even the two smallest files, aborting");
        return 0;
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     */
    int doCompactionWithoutSizeEstimation(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore, String compactionFileLocation, boolean forceDeserialize) throws IOException
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert sstables != null;

        Table table = cfs.table;
        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            table.snapshot(System.currentTimeMillis() + "-" + "compact-" + cfs.columnFamily);

        // sanity check: all sstables must belong to the same cfs
        for (SSTableReader sstable : sstables)
            assert sstable.descriptor.cfname.equals(cfs.columnFamily);

        CompactionController controller = new CompactionController(cfs, sstables, gcBefore, forceDeserialize);
        // new sstables from flush can be added during a compaction, but only the compaction can remove them,
        // so in our single-threaded compaction world this is a valid way of determining if we're compacting
        // all the sstables (that existed when we started)
        CompactionType type = controller.isMajor()
                            ? CompactionType.MAJOR
                            : CompactionType.MINOR;
        logger.info("Compacting {}: {}", type, sstables);

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        // TODO the int cast here is potentially buggy
        int expectedBloomFilterSize = Math.max(DatabaseDescriptor.getIndexInterval(), (int)SSTableReader.getApproximateKeyCount(sstables));
        if (logger.isDebugEnabled())
          logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

        SSTableWriter writer;
        CompactionIterator ci = new CompactionIterator(type, sstables, controller); // retain a handle so we can call close()
        Iterator<AbstractCompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());
        Map<DecoratedKey, Long> cachedKeys = new HashMap<DecoratedKey, Long>();

        executor.beginCompaction(ci);
        try
        {
            if (!nni.hasNext())
            {
                // don't mark compacted in the finally block, since if there _is_ nondeleted data,
                // we need to sync it (via closeAndOpen) first, so there is no period during which
                // a crash could cause data loss.
                cfs.markCompacted(sstables);
                return 0;
            }

            writer = cfs.createCompactionWriter(expectedBloomFilterSize, compactionFileLocation, sstables);
            while (nni.hasNext())
            {
                AbstractCompactedRow row = nni.next();
                if (row.isEmpty())
                    continue;

                long position = writer.append(row);
                totalkeysWritten++;

                if (DatabaseDescriptor.getPreheatKeyCache())
                {
                    for (SSTableReader sstable : sstables)
                    {
                        if (sstable.getCachedPosition(row.key) != null)
                        {
                            cachedKeys.put(row.key, position);
                            break;
                        }
                    }
                }
            }
        }
        finally
        {
            ci.close();
            executor.finishCompaction(ci);
        }

        SSTableReader ssTable = writer.closeAndOpenReader(getMaxDataAge(sstables));
        cfs.replaceCompactedSSTables(sstables, Arrays.asList(ssTable));
        for (Entry<DecoratedKey, Long> entry : cachedKeys.entrySet()) // empty if preheat is off
            ssTable.cacheKey(entry.getKey(), entry.getValue());
        submitMinorIfNeeded(cfs);

        long dTime = System.currentTimeMillis() - startTime;
        long startsize = SSTable.getTotalBytes(sstables);
        long endsize = ssTable.length();
        double ratio = (double)endsize / (double)startsize;
        logger.info(String.format("Compacted to %s.  %,d to %,d (~%d%% of original) bytes for %,d keys.  Time: %,dms.",
                                  writer.getFilename(), startsize, endsize, (int) (ratio * 100), totalkeysWritten, dTime));
        return sstables.size();
    }

    private static long getMaxDataAge(Collection<SSTableReader> sstables)
    {
        long max = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.maxDataAge > max)
                max = sstable.maxDataAge;
        }
        return max;
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
        logger.info("Scrubbing " + sstable);
        CompactionController controller = new CompactionController(cfs, Collections.singletonList(sstable), getDefaultGcBefore(cfs), true);
        boolean isCommutative = cfs.metadata.getDefaultValidator().isCommutative();

        // Calculate the expected compacted filesize
        String compactionFileLocation = cfs.table.getDataFileLocation(sstable.length());
        if (compactionFileLocation == null)
            throw new IOException("disk full");
        int expectedBloomFilterSize = Math.max(DatabaseDescriptor.getIndexInterval(),
                                               (int)(SSTableReader.getApproximateKeyCount(Arrays.asList(sstable))));

        // loop through each row, deserializing to check for damage.
        // we'll also loop through the index at the same time, using the position from the index to recover if the
        // row header (key or data size) is corrupt. (This means our position in the index file will be one row
        // "ahead" of the data file.)
        final BufferedRandomAccessFile dataFile = BufferedRandomAccessFile.getUncachingReader(sstable.getFilename());
        String indexFilename = sstable.descriptor.filenameFor(Component.PRIMARY_INDEX);
        BufferedRandomAccessFile indexFile = BufferedRandomAccessFile.getUncachingReader(indexFilename);
        try
        {
            ByteBuffer nextIndexKey = ByteBufferUtil.readWithShortLength(indexFile);
            {
                // throw away variable so we don't have a side effect in the assert
                long firstRowPositionFromIndex = indexFile.readLong();
                assert firstRowPositionFromIndex == 0 : firstRowPositionFromIndex;
            }

            SSTableWriter writer = maybeCreateWriter(cfs, compactionFileLocation, expectedBloomFilterSize, null, Collections.singletonList(sstable));
            executor.beginCompaction(new ScrubInfo(dataFile, sstable));
            int goodRows = 0, badRows = 0, emptyRows = 0;

            while (!dataFile.isEOF())
            {
                long rowStart = dataFile.getFilePointer();
                if (logger.isDebugEnabled())
                    logger.debug("Reading row at " + rowStart);

                DecoratedKey key = null;
                long dataSize = -1;
                try
                {
                    key = SSTableReader.decodeKey(sstable.partitioner, sstable.descriptor, ByteBufferUtil.readWithShortLength(dataFile));
                    dataSize = sstable.descriptor.hasIntRowSize ? dataFile.readInt() : dataFile.readLong();
                    if (logger.isDebugEnabled())
                        logger.debug(String.format("row %s is %s bytes", ByteBufferUtil.bytesToHex(key.key), dataSize));
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    // check for null key below
                }

                ByteBuffer currentIndexKey = nextIndexKey;
                long nextRowPositionFromIndex;
                try
                {
                    nextIndexKey = indexFile.isEOF() ? null : ByteBufferUtil.readWithShortLength(indexFile);
                    nextRowPositionFromIndex = indexFile.isEOF() ? dataFile.length() : indexFile.readLong();
                }
                catch (Throwable th)
                {
                    logger.warn("Error reading index file", th);
                    nextIndexKey = null;
                    nextRowPositionFromIndex = dataFile.length();
                }

                long dataStart = dataFile.getFilePointer();
                long dataStartFromIndex = currentIndexKey == null
                                        ? -1
                                        : rowStart + 2 + currentIndexKey.remaining() + (sstable.descriptor.hasIntRowSize ? 4 : 8);
                long dataSizeFromIndex = nextRowPositionFromIndex - dataStartFromIndex;
                assert currentIndexKey != null || indexFile.isEOF();
                if (logger.isDebugEnabled() && currentIndexKey != null)
                    logger.debug(String.format("Index doublecheck: row %s is %s bytes", ByteBufferUtil.bytesToHex(currentIndexKey),  dataSizeFromIndex));

                writer.mark();
                try
                {
                    if (key == null)
                        throw new IOError(new IOException("Unable to read row key from data file"));
                    if (dataSize > dataFile.length())
                        throw new IOError(new IOException("Impossible row size " + dataSize));
                    SSTableIdentityIterator row = new SSTableIdentityIterator(sstable, dataFile, key, dataStart, dataSize, true);
                    AbstractCompactedRow compactedRow = controller.getCompactedRow(row);
                    if (compactedRow.isEmpty())
                    {
                        emptyRows++;
                    }
                    else
                    {
                        writer.append(compactedRow);
                        goodRows++;
                    }
                    if (!key.key.equals(currentIndexKey) || dataStart != dataStartFromIndex)
                        logger.warn("Index file contained a different key or row size; using key from data file");
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    logger.warn("Non-fatal error reading row (stacktrace follows)", th);
                    writer.reset();

                    if (currentIndexKey != null
                        && (key == null || !key.key.equals(currentIndexKey) || dataStart != dataStartFromIndex || dataSize != dataSizeFromIndex))
                    {
                        logger.info(String.format("Retrying from row index; data is %s bytes starting at %s",
                                                  dataSizeFromIndex, dataStartFromIndex));
                        key = SSTableReader.decodeKey(sstable.partitioner, sstable.descriptor, currentIndexKey);
                        try
                        {
                            SSTableIdentityIterator row = new SSTableIdentityIterator(sstable, dataFile, key, dataStartFromIndex, dataSizeFromIndex, true);
                            AbstractCompactedRow compactedRow = controller.getCompactedRow(row);
                            if (compactedRow.isEmpty())
                            {
                                emptyRows++;
                            }
                            else
                            {
                                writer.append(compactedRow);
                                goodRows++;
                            }
                        }
                        catch (Throwable th2)
                        {
                            throwIfFatal(th2);
                            // Skipping rows is dangerous for counters (see CASSANDRA-2759)
                            if (isCommutative)
                                throw new IOError(th2);

                            logger.warn("Retry failed too.  Skipping to next row (retry's stacktrace follows)", th2);
                            writer.reset();
                            dataFile.seek(nextRowPositionFromIndex);
                            badRows++;
                        }
                    }
                    else
                    {
                        // Skipping rows is dangerous for counters (see CASSANDRA-2759)
                        if (isCommutative)
                            throw new IOError(th);

                        logger.warn("Row at " + dataStart + " is unreadable; skipping to next");
                        if (currentIndexKey != null)
                            dataFile.seek(nextRowPositionFromIndex);
                        badRows++;
                    }
                }
            }

            if (writer.getFilePointer() > 0)
            {
                SSTableReader newSstable = writer.closeAndOpenReader(sstable.maxDataAge);
                cfs.replaceCompactedSSTables(Arrays.asList(sstable), Arrays.asList(newSstable));
                logger.info("Scrub of " + sstable + " complete: " + goodRows + " rows in new sstable and " + emptyRows + " empty (tombstoned) rows dropped");
                if (badRows > 0)
                    logger.warn("Unable to recover " + badRows + " rows that were skipped.  You can attempt manual recovery from the pre-scrub snapshot.  You can also run nodetool repair to transfer the data from a healthy replica, if any");
            }
            else
            {
                cfs.markCompacted(Arrays.asList(sstable));
                if (badRows > 0)
                    logger.warn("No valid rows found while scrubbing " + sstable + "; it is marked for deletion now. If you want to attempt manual recovery, you can find a copy in the pre-scrub snapshot");
                else
                    logger.info("Scrub of " + sstable + " complete; looks like all " + emptyRows + " rows were tombstoned");
            }
        }
        finally
        {
            FileUtils.closeQuietly(dataFile);
            FileUtils.closeQuietly(indexFile);
        }
    }

    private void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
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
        Collection<Range> ranges = StorageService.instance.getLocalRanges(table.name);
        boolean isCommutative = cfs.metadata.getDefaultValidator().isCommutative();
        if (ranges.isEmpty())
        {
            logger.info("Cleanup cannot run before a node has joined the ring");
            return;
        }

        for (SSTableReader sstable : sstables)
        {
            CompactionController controller = new CompactionController(cfs, Collections.singletonList(sstable), getDefaultGcBefore(cfs), false);
            long startTime = System.currentTimeMillis();

            long totalkeysWritten = 0;

            int expectedBloomFilterSize = Math.max(DatabaseDescriptor.getIndexInterval(),
                                                   (int)(SSTableReader.getApproximateKeyCount(Arrays.asList(sstable))));
            if (logger.isDebugEnabled())
              logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

            SSTableWriter writer = null;

            logger.info("Cleaning up " + sstable);
            // Calculate the expected compacted filesize
            long expectedRangeFileSize = cfs.getExpectedCompactedFileSize(Arrays.asList(sstable)) / 2;
            String compactionFileLocation = table.getDataFileLocation(expectedRangeFileSize);
            if (compactionFileLocation == null)
                throw new IOException("disk full");

            SSTableScanner scanner = sstable.getDirectScanner(CompactionIterator.FILE_BUFFER_SIZE);
            SortedSet<ByteBuffer> indexedColumns = cfs.getIndexedColumns();
            CleanupInfo ci = new CleanupInfo(sstable, scanner);
            executor.beginCompaction(ci);
            try
            {
                while (scanner.hasNext())
                {
                    SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                    if (Range.isTokenInRanges(row.getKey().token, ranges))
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
                        if (!indexedColumns.isEmpty() || isCommutative)
                        {
                            while (row.hasNext())
                            {
                                IColumn column = row.next();
                                if (column instanceof CounterColumn)
                                    renewer.maybeRenew((CounterColumn) column);
                                if (indexedColumns.contains(column.name()))
                                    Table.cleanupIndexEntry(cfs, row.getKey().key, column);
                            }
                        }
                    }
                }
            }
            finally
            {
                scanner.close();
                executor.finishCompaction(ci);
            }

            List<SSTableReader> results = new ArrayList<SSTableReader>();
            if (writer != null)
            {
                SSTableReader newSstable = writer.closeAndOpenReader(sstable.maxDataAge);
                results.add(newSstable);

                String format = "Cleaned up to %s.  %,d to %,d (~%d%% of original) bytes for %,d keys.  Time: %,dms.";
                long dTime = System.currentTimeMillis() - startTime;
                long startsize = sstable.length();
                long endsize = newSstable.length();
                double ratio = (double)endsize / (double)startsize;
                logger.info(String.format(format, writer.getFilename(), startsize, endsize, (int)(ratio*100), totalkeysWritten, dTime));
            }

            // flush to ensure we don't lose the tombstones on a restart, since they are not commitlog'd
            for (ByteBuffer columnName : cfs.getIndexedColumns())
            {
                try
                {
                    cfs.getIndexedColumnFamilyStore(columnName).forceBlockingFlush();
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
            cfs.replaceCompactedSSTables(Arrays.asList(sstable), results);
        }
    }

    private SSTableWriter maybeCreateWriter(ColumnFamilyStore cfs, String compactionFileLocation, int expectedBloomFilterSize, SSTableWriter writer, Collection<SSTableReader> sstables)
            throws IOException
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

        CompactionIterator ci = new ValidationCompactionIterator(cfs, validator.request.range);
        validationExecutor.beginCompaction(ci);
        try
        {
            Iterator<AbstractCompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());

            // validate the CF as we iterate over it
            validator.prepare(cfs);
            while (nni.hasNext())
            {
                AbstractCompactedRow row = nni.next();
                validator.add(row);
            }
            validator.complete();
        }
        finally
        {
            ci.close();
            validationExecutor.finishCompaction(ci);
        }
    }

    /*
    * Group files of similar size into buckets.
    */
    static <T> Set<List<T>> getBuckets(Collection<Pair<T, Long>> files, long min)
    {
        // Sort the list in order to get deterministic results during the grouping below
        List<Pair<T, Long>> sortedFiles = new ArrayList<Pair<T, Long>>(files);
        Collections.sort(sortedFiles, new Comparator<Pair<T, Long>>()
        {
            public int compare(Pair<T, Long> p1, Pair<T, Long> p2)
            {
                return p1.right.compareTo(p2.right);
            }
        });

        Map<List<T>, Long> buckets = new HashMap<List<T>, Long>();

        for (Pair<T, Long> pair: sortedFiles)
        {
            long size = pair.right;

            boolean bFound = false;
            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than `min`)
            for (Entry<List<T>, Long> entry : buckets.entrySet())
            {
                List<T> bucket = entry.getKey();
                long averageSize = entry.getValue();
                if ((size > (averageSize / 2) && size < (3 * averageSize) / 2)
                    || (size < min && averageSize < min))
                {
                    // remove and re-add because adding changes the hash
                    buckets.remove(bucket);
                    long totalSize = bucket.size() * averageSize;
                    averageSize = (totalSize + size) / (bucket.size() + 1);
                    bucket.add(pair.left);
                    buckets.put(bucket, averageSize);
                    bFound = true;
                    break;
                }
            }
            // no similar bucket found; put it in a new one
            if (!bFound)
            {
                ArrayList<T> bucket = new ArrayList<T>();
                bucket.add(pair.left);
                buckets.put(bucket, size);
            }
        }

        return buckets.keySet();
    }

    private static Collection<Pair<SSTableReader, Long>> convertSSTablesToPairs(Collection<SSTableReader> collection)
    {
        Collection<Pair<SSTableReader, Long>> tablePairs = new ArrayList<Pair<SSTableReader, Long>>();
        for(SSTableReader table: collection)
        {
            tablePairs.add(new Pair<SSTableReader, Long>(table, table.length()));
        }
        return tablePairs;
    }
    
    /**
     * Is not scheduled, because it is performing disjoint work from sstable compaction.
     */
    public Future submitIndexBuild(final ColumnFamilyStore cfs, final Table.IndexBuilder builder)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                compactionLock.readLock().lock();
                try
                {
                    if (cfs.isInvalid())
                        return;
                    executor.beginCompaction(builder);
                    try
                    {
                        builder.build();
                    }
                    finally
                    {
                        executor.finishCompaction(builder);
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

    /**
     * Submits an sstable to be rebuilt: is not scheduled, since the sstable must not exist.
     */
    public Future<SSTableReader> submitSSTableBuild(final Descriptor desc, OperationType type)
    {
        // invalid descriptions due to missing or dropped CFS are handled by SSTW and StreamInSession.
        final SSTableWriter.Builder builder = SSTableWriter.createBuilder(desc, type);
        Callable<SSTableReader> callable = new Callable<SSTableReader>()
        {
            public SSTableReader call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    executor.beginCompaction(builder);
                    try
                    {
                        return builder.build();
                    }
                    finally
                    {
                        executor.finishCompaction(builder);
                    }
                }
                finally
                {
                    compactionLock.readLock().unlock();
                }
            }
        };
        return executor.submit(callable);
    }

    public Future<?> submitCacheWrite(final AutoSavingCache.Writer writer)
    {
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                if (!AutoSavingCache.flushInProgress.compareAndSet(false, true))
                {
                    logger.debug("Cache flushing was already in progress: skipping {}", writer.getCompactionInfo());
                    return;
                }
                try
                {
                    executor.beginCompaction(writer);
                    try
                    {
                        writer.saveCache();
                    }
                    finally
                    {
                        executor.finishCompaction(writer);
                    }
                }
                finally
                {
                    AutoSavingCache.flushInProgress.set(false);
                }
            }
        };
        return executor.submit(runnable);
    }

    public Future<?> submitTruncate(final ColumnFamilyStore main, final long truncatedAt)
    {
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws InterruptedException, IOException
            {
                for (ColumnFamilyStore cfs : main.concatWithIndexes())
                {
                    List<SSTableReader> truncatedSSTables = new ArrayList<SSTableReader>();
                    for (SSTableReader sstable : cfs.getSSTables())
                    {
                        if (!sstable.newSince(truncatedAt))
                            truncatedSSTables.add(sstable);
                    }
                    cfs.markCompacted(truncatedSSTables);
                }

                main.invalidateRowCache();
            }
        };

        return executor.submit(runnable);
    }

    private static int getDefaultGcBefore(ColumnFamilyStore cfs)
    {
        return cfs.isIndex()
               ? Integer.MAX_VALUE
               : (int) (System.currentTimeMillis() / 1000) - cfs.metadata.getGcGraceSeconds();
    }

    private static class ValidationCompactionIterator extends CompactionIterator
    {
        public ValidationCompactionIterator(ColumnFamilyStore cfs, Range range) throws IOException
        {
            super(CompactionType.VALIDATION,
                  getCollatingIterator(cfs.getSSTables(), range),
                  new CompactionController(cfs, cfs.getSSTables(), getDefaultGcBefore(cfs), true));
        }

        protected static CollatingIterator getCollatingIterator(Iterable<SSTableReader> sstables, Range range) throws IOException
        {
            CollatingIterator iter = FBUtilities.getCollatingIterator();
            for (SSTableReader sstable : sstables)
            {
                iter.addIterator(sstable.getDirectScanner(FILE_BUFFER_SIZE, range));
            }
            return iter;
        }
    }

    public int getActiveCompactions()
    {
        return executor.getActiveCount() + validationExecutor.getActiveCount();
    }

    private static class CompactionExecutor extends DebuggableThreadPoolExecutor
    {
        // a synchronized identity set of running tasks to their compaction info
        private static final Set<CompactionInfo.Holder> compactions = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<CompactionInfo.Holder, Boolean>()));

        protected CompactionExecutor(int minThreads, int maxThreads, String name, BlockingQueue<Runnable> queue)
        {
            super(minThreads,
                  maxThreads,
                  60,
                  TimeUnit.SECONDS,
                  queue,
                  new NamedThreadFactory(name, DatabaseDescriptor.getCompactionThreadPriority()));
        }

        private CompactionExecutor(int threadCount, String name)
        {
            this(threadCount, threadCount, name, new LinkedBlockingQueue<Runnable>());
        }

        public CompactionExecutor()
        {
            this(Math.max(1, DatabaseDescriptor.getConcurrentCompactors()), "CompactionExecutor");
        }

        void beginCompaction(CompactionInfo.Holder ci)
        {
            compactions.add(ci);
        }

        void finishCompaction(CompactionInfo.Holder ci)
        {
            compactions.remove(ci);
        }

        public static List<CompactionInfo.Holder> getCompactions()
        {
            return new ArrayList<CompactionInfo.Holder>(compactions);
        }
    }

    private static class ValidationExecutor extends CompactionExecutor
    {
        public ValidationExecutor()
        {
            super(1, Integer.MAX_VALUE, "ValidationExecutor", new SynchronousQueue<Runnable>());
        }
    }

    public List<CompactionInfo> getCompactions()
    {
        List<CompactionInfo> out = new ArrayList<CompactionInfo>();
        for (CompactionInfo.Holder ci : CompactionExecutor.getCompactions())
            out.add(ci.getCompactionInfo());
        return out;
    }

    public List<String> getCompactionSummary()
    {
        List<String> out = new ArrayList<String>();
        for (CompactionInfo.Holder ci : CompactionExecutor.getCompactions())
            out.add(ci.getCompactionInfo().toString());
        return out;
    }

    public int getPendingTasks()
    {
        int n = 0;
        for (Integer i : estimatedCompactions.values())
            n += i;
        return (int) (executor.getTaskCount() + validationExecutor.getTaskCount() - executor.getCompletedTaskCount() - validationExecutor.getCompletedTaskCount()) + n;
    }

    public long getCompletedTasks()
    {
        return executor.getCompletedTaskCount() + validationExecutor.getCompletedTaskCount();
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

    private static class CleanupInfo implements CompactionInfo.Holder
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
                return new CompactionInfo(sstable.descriptor.ksname,
                                          sstable.descriptor.cfname,
                                          CompactionType.CLEANUP,
                                          scanner.getFilePointer(),
                                          scanner.getFileLength());
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
        }
    }

    private static class ScrubInfo implements CompactionInfo.Holder
    {
        private final BufferedRandomAccessFile dataFile;
        private final SSTableReader sstable;
        public ScrubInfo(BufferedRandomAccessFile dataFile, SSTableReader sstable)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
        }

        public CompactionInfo getCompactionInfo()
        {
            try
            {
                return new CompactionInfo(sstable.descriptor.ksname,
                                          sstable.descriptor.cfname,
                                          CompactionType.SCRUB,
                                          dataFile.getFilePointer(),
                                          dataFile.length());
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
        }
    }
}
