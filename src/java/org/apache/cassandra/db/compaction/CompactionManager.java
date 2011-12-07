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
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexBuilder;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.AntiEntropyService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

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

    /**
     * compactionLock has two purposes:
     * - Compaction acquires its readLock so that multiple compactions can happen simultaneously,
     *   but the KS/CF migtations acquire its writeLock, so they can be sure no new SSTables will
     *   be created for a dropped CF posthumously.  (Thus, compaction checks CFS.isValid while the
     *   lock is acquired.)
     * - "Special" compactions will acquire writelock instead of readlock to make sure that all
     *   other compaction activity is quiesced and they can grab ALL the sstables to do something.
     *   TODO this is too big a hammer -- we should only care about quiescing all for the given CFS.
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

    private CompactionExecutor executor = new CompactionExecutor();
    private CompactionExecutor validationExecutor = new ValidationExecutor();

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
    public Future<Integer> submitBackground(final ColumnFamilyStore cfs)
    {
        Callable<Integer> callable = new Callable<Integer>()
        {
            public Integer call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    if (!cfs.isValid())
                        return 0;

                    boolean taskExecuted = false;
                    AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();
                    List<AbstractCompactionTask> tasks = strategy.getBackgroundTasks(getDefaultGcBefore(cfs));
                    for (AbstractCompactionTask task : tasks)
                    {
                        if (!task.markSSTablesForCompaction())
                            continue;

                        taskExecuted = true;
                        try
                        {
                            task.execute(executor);
                        }
                        finally
                        {
                            task.unmarkSSTables();
                        }
                    }

                    // newly created sstables might have made other compactions eligible
                    if (taskExecuted)
                        submitBackground(cfs);
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
                    if (!cfStore.isValid())
                        return this;
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
            public void perform(ColumnFamilyStore cfs, Collection<SSTableReader> sstables) throws IOException
            {
                assert !cfs.isIndex();
                for (final SSTableReader sstable : sstables)
                {
                    // SSTables are marked by the caller
                    CompactionTask task = new CompactionTask(cfs, Collections.singletonList(sstable), Integer.MAX_VALUE);
                    task.isUserDefined(true);
                    task.execute(executor);
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
                doCleanupCompaction(store, sstables, renewer);
            }
        });
    }

    public void performMaximal(final ColumnFamilyStore cfStore) throws InterruptedException, ExecutionException
    {
        submitMaximal(cfStore, getDefaultGcBefore(cfStore)).get();
    }

    public Future<Object> submitMaximal(final ColumnFamilyStore cfStore, final int gcBefore)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                // acquire the write lock long enough to schedule all sstables
                compactionLock.writeLock().lock();
                try
                {
                    if (!cfStore.isValid())
                        return this;
                    AbstractCompactionStrategy strategy = cfStore.getCompactionStrategy();
                    for (AbstractCompactionTask task : strategy.getMaximalTasks(gcBefore))
                    {
                        if (!task.markSSTablesForCompaction(0, Integer.MAX_VALUE))
                            return this;
                        try
                        {
                            // downgrade the lock acquisition
                            compactionLock.readLock().lock();
                            compactionLock.writeLock().unlock();
                            try
                            {
                                return task.execute(executor);
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
                }
                finally
                {
                    // we probably already downgraded
                    if (compactionLock.writeLock().isHeldByCurrentThread())
                        compactionLock.writeLock().unlock();
                }
                return this;
            }
        };
        return executor.submit(callable);
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

    public Future<Object> submitUserDefined(final ColumnFamilyStore cfs, final Collection<Descriptor> dataFiles, final int gcBefore)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    if (!cfs.isValid())
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

                    Collection<SSTableReader> toCompact;
                    try
                    {
                        if (sstables.isEmpty())
                        {
                            logger.error("No file to compact for user defined compaction");
                        }
                        // attempt to schedule the set
                        else if ((toCompact = cfs.getDataTracker().markCompacting(sstables, 1, Integer.MAX_VALUE)) != null)
                        {
                            // success: perform the compaction
                            try
                            {
                                AbstractCompactionStrategy strategy = cfs.getCompactionStrategy();
                                AbstractCompactionTask task = strategy.getUserDefinedTask(toCompact, gcBefore);
                                task.execute(executor);
                            }
                            finally
                            {
                                cfs.getDataTracker().unmarkCompacting(toCompact);
                            }
                        }
                        else
                        {
                            logger.error("SSTables for user defined compaction are already being compacted.");
                        }
                    }
                    finally
                    {
                        SSTableReader.releaseReferences(sstables);
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
                    if (cfStore.isValid())
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
        logger.info("Scrubbing " + sstable);
        CompactionController controller = new CompactionController(cfs, Collections.singletonList(sstable), getDefaultGcBefore(cfs), true);
        boolean isCommutative = cfs.metadata.getDefaultValidator().isCommutative();

        // Calculate the expected compacted filesize
        String compactionFileLocation = cfs.table.getDataFileLocation(sstable.onDiskLength());
        if (compactionFileLocation == null)
            throw new IOException("disk full");
        int expectedBloomFilterSize = Math.max(DatabaseDescriptor.getIndexInterval(),
                                               (int)(SSTableReader.getApproximateKeyCount(Arrays.asList(sstable))));

        // loop through each row, deserializing to check for damage.
        // we'll also loop through the index at the same time, using the position from the index to recover if the
        // row header (key or data size) is corrupt. (This means our position in the index file will be one row
        // "ahead" of the data file.)
        final RandomAccessReader dataFile = sstable.openDataReader(true);
        RandomAccessReader indexFile = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX)), true);
        ScrubInfo scrubInfo = new ScrubInfo(dataFile, sstable);
        executor.beginCompaction(scrubInfo);

        SSTableWriter writer = null;
        SSTableReader newSstable = null;
        int goodRows = 0, badRows = 0, emptyRows = 0;

        try
        {
            ByteBuffer nextIndexKey = ByteBufferUtil.readWithShortLength(indexFile);
            {
                // throw away variable so we don't have a side effect in the assert
                long firstRowPositionFromIndex = indexFile.readLong();
                assert firstRowPositionFromIndex == 0 : firstRowPositionFromIndex;
            }

            // TODO errors when creating the writer may leave empty temp files.
            writer = maybeCreateWriter(cfs, compactionFileLocation, expectedBloomFilterSize, null, Collections.singletonList(sstable));

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
                    writer.resetAndTruncate();

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
                            writer.resetAndTruncate();
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
                newSstable = writer.closeAndOpenReader(sstable.maxDataAge);
        }
        catch (Exception e)
        {
            if (writer != null)
                writer.abort();
            throw FBUtilities.unchecked(e);
        }
        finally
        {
            FileUtils.closeQuietly(dataFile);
            FileUtils.closeQuietly(indexFile);

            executor.finishCompaction(scrubInfo);
        }

        if (newSstable == null)
        {
            cfs.markCompacted(Arrays.asList(sstable));
            if (badRows > 0)
                logger.warn("No valid rows found while scrubbing " + sstable + "; it is marked for deletion now. If you want to attempt manual recovery, you can find a copy in the pre-scrub snapshot");
            else
                logger.info("Scrub of " + sstable + " complete; looks like all " + emptyRows + " rows were tombstoned");
        }
        else
        {
            cfs.replaceCompactedSSTables(Arrays.asList(sstable), Arrays.asList(newSstable));
            logger.info("Scrub of " + sstable + " complete: " + goodRows + " rows in new sstable and " + emptyRows + " empty (tombstoned) rows dropped");
            if (badRows > 0)
                logger.warn("Unable to recover " + badRows + " rows that were skipped.  You can attempt manual recovery from the pre-scrub snapshot.  You can also run nodetool repair to transfer the data from a healthy replica, if any");
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
            SSTableReader newSstable = null;

            logger.info("Cleaning up " + sstable);
            // Calculate the expected compacted filesize
            long expectedRangeFileSize = cfs.getExpectedCompactedFileSize(Arrays.asList(sstable)) / 2;
            String compactionFileLocation = table.getDataFileLocation(expectedRangeFileSize);
            if (compactionFileLocation == null)
                throw new IOException("disk full");

            SSTableScanner scanner = sstable.getDirectScanner();
            Collection<ByteBuffer> indexedColumns = cfs.indexManager.getIndexedColumns();
            List<IColumn> indexedColumnsInRow = null;

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
                            if (indexedColumnsInRow != null)
                                indexedColumnsInRow.clear();
                            
                            while (row.hasNext())
                            {
                                IColumn column = row.next();
                                if (column instanceof CounterColumn)
                                    renewer.maybeRenew((CounterColumn) column);
                                if (indexedColumns.contains(column.name()))
                                {
                                    if (indexedColumnsInRow == null)
                                        indexedColumnsInRow = new ArrayList<IColumn>();
                                    
                                    indexedColumnsInRow.add(column);
                                }
                            }
                            
                            if (indexedColumnsInRow != null && !indexedColumnsInRow.isEmpty())
                                cfs.indexManager.deleteFromIndexes(row.getKey(), indexedColumnsInRow);
                        }
                    }
                }
                if (writer != null)
                    newSstable = writer.closeAndOpenReader(sstable.maxDataAge);
            }
            catch (Exception e)
            {
                if (writer != null)
                    writer.abort();
                throw FBUtilities.unchecked(e);
            }
            finally
            {
                scanner.close();
                executor.finishCompaction(ci);
                executor.finishCompaction(ci);
            }

            List<SSTableReader> results = new ArrayList<SSTableReader>();
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

        // we don't mark validating sstables as compacting in DataTracker, so we have to mark them referenced
        // instead so they won't be cleaned up if they do get compacted during the validation
        Collection<SSTableReader> sstables = cfs.markCurrentSSTablesReferenced();
        CompactionIterable ci = new ValidationCompactionIterable(cfs, sstables, validator.request.range);
        CloseableIterator<AbstractCompactedRow> iter = ci.iterator();
        validationExecutor.beginCompaction(ci);
        try
        {
            Iterator<AbstractCompactedRow> nni = Iterators.filter(iter, Predicates.notNull());

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
            SSTableReader.releaseReferences(sstables);
            iter.close();
            validationExecutor.finishCompaction(ci);
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
                compactionLock.writeLock().lock();
                try
                {
                    for (ColumnFamilyStore cfs : main.concatWithIndexes())
                    {
                        List<SSTableReader> truncatedSSTables = new ArrayList<SSTableReader>();
                        for (SSTableReader sstable : cfs.getSSTables())
                        {
                            if (!sstable.newSince(truncatedAt))
                                truncatedSSTables.add(sstable);
                        }
                        if (!truncatedSSTables.isEmpty())
                            cfs.markCompacted(truncatedSSTables);
                    }
                }
                finally
                {
                    compactionLock.writeLock().unlock();
                }

                main.invalidateRowCache();
            }
        };

        return executor.submit(runnable);
    }

    static int getDefaultGcBefore(ColumnFamilyStore cfs)
    {
        return cfs.isIndex()
               ? Integer.MAX_VALUE
               : (int) (System.currentTimeMillis() / 1000) - cfs.metadata.getGcGraceSeconds();
    }

    private static class ValidationCompactionIterable extends CompactionIterable
    {
        public ValidationCompactionIterable(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, Range range) throws IOException
        {
            super(OperationType.VALIDATION,
                  getScanners(sstables, range),
                  new CompactionController(cfs, sstables, getDefaultGcBefore(cfs), true));
        }

        protected static List<SSTableScanner> getScanners(Iterable<SSTableReader> sstables, Range range) throws IOException
        {
            ArrayList<SSTableScanner> scanners = new ArrayList<SSTableScanner>();
            for (SSTableReader sstable : sstables)
                scanners.add(sstable.getDirectScanner(range));
            return scanners;
        }
    }

    public int getActiveCompactions()
    {
        return CompactionExecutor.compactions.size();
    }

    private static class CompactionExecutor extends DebuggableThreadPoolExecutor implements CompactionExecutorStatsCollector
    {
        // a synchronized identity set of running tasks to their compaction info
        private static final Set<CompactionInfo.Holder> compactions = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<CompactionInfo.Holder, Boolean>()));

        protected CompactionExecutor(int minThreads, int maxThreads, String name, BlockingQueue<Runnable> queue)
        {
            super(minThreads, maxThreads, 60, TimeUnit.SECONDS, queue, new NamedThreadFactory(name, Thread.MIN_PRIORITY));
        }

        private CompactionExecutor(int threadCount, String name)
        {
            this(threadCount, threadCount, name, new LinkedBlockingQueue<Runnable>());
        }

        public CompactionExecutor()
        {
            this(Math.max(1, DatabaseDescriptor.getConcurrentCompactors()), "CompactionExecutor");
        }

        public void beginCompaction(CompactionInfo.Holder ci)
        {
            compactions.add(ci);
        }

        public void finishCompaction(CompactionInfo.Holder ci)
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

    public interface CompactionExecutorStatsCollector
    {
        void beginCompaction(CompactionInfo.Holder ci);
        void finishCompaction(CompactionInfo.Holder ci);
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
        for (String tableName : Schema.instance.getTables())
        {
            for (ColumnFamilyStore cfs : Table.open(tableName).getColumnFamilyStores())
            {
                n += cfs.getCompactionStrategy().getEstimatedRemainingTasks();
            }
        }
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
                return new CompactionInfo(this.hashCode(),
                                          sstable.descriptor.ksname,
                                          sstable.descriptor.cfname,
                                          OperationType.CLEANUP,
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
        private final RandomAccessReader dataFile;
        private final SSTableReader sstable;
        public ScrubInfo(RandomAccessReader dataFile, SSTableReader sstable)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
        }

        public CompactionInfo getCompactionInfo()
        {
            try
            {
                return new CompactionInfo(this.hashCode(),
                                          sstable.descriptor.ksname,
                                          sstable.descriptor.cfname,
                                          OperationType.SCRUB,
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
