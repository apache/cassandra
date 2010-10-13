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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.LocalToken;
import org.apache.cassandra.io.ICompactionInfo;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.SSTableDeletingReference;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class Table
{
    public static final String SYSTEM_TABLE = "system";

    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    private static final String SNAPSHOT_SUBDIR_NAME = "snapshots";

    /**
     * accesses to CFS.memtable should acquire this for thread safety.
     * only Table.maybeSwitchMemtable should aquire the writeLock; see that method for the full explanation.
     */
    static final ReentrantReadWriteLock flusherLock = new ReentrantReadWriteLock(true);

    // This is a result of pushing down the point in time when storage directories get created.  It used to happen in
    // CassandraDaemon, but it is possible to call Table.open without a running daemon, so it made sense to ensure
    // proper directories here.
    static
    {
        try
        {
            DatabaseDescriptor.createAllDirectories();
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /** Table objects, one per keyspace.  only one instance should ever exist for any given keyspace. */
    private static final Map<String, Table> instances = new NonBlockingHashMap<String, Table>();

    /* Table name. */
    public final String name;
    /* ColumnFamilyStore per column family */
    public final Map<Integer, ColumnFamilyStore> columnFamilyStores = new HashMap<Integer, ColumnFamilyStore>(); // TODO make private again
    private final Object[] indexLocks;
    private ScheduledFuture<?> flushTask;

    public static Table open(String table)
    {
        Table tableInstance = instances.get(table);
        if (tableInstance == null)
        {
            // instantiate the Table.  we could use putIfAbsent but it's important to making sure it is only done once
            // per keyspace, so we synchronize and re-check before doing it.
            synchronized (Table.class)
            {
                tableInstance = instances.get(table);
                if (tableInstance == null)
                {
                    // open and store the table
                    tableInstance = new Table(table);
                    instances.put(table, tableInstance);

                    //table has to be constructed and in the cache before cacheRow can be called
                    for (ColumnFamilyStore cfs : tableInstance.getColumnFamilyStores())
                        cfs.initRowCache();
                }
            }
        }
        return tableInstance;
    }

    public static Table clear(String table) throws IOException
    {
        synchronized (Table.class)
        {
            Table t = instances.remove(table);
            if (t != null)
            {
                t.flushTask.cancel(false);
                for (ColumnFamilyStore cfs : t.getColumnFamilyStores())
                    t.unloadCf(cfs);
            }
            return t;
        }
    }
    
    public Collection<ColumnFamilyStore> getColumnFamilyStores()
    {
        return Collections.unmodifiableCollection(columnFamilyStores.values());
    }

    public ColumnFamilyStore getColumnFamilyStore(String cfName)
    {
        Integer id = CFMetaData.getId(name, cfName);
        if (id == null)
            throw new IllegalArgumentException(String.format("Unknown table/cf pair (%s.%s)", name, cfName));
        return columnFamilyStores.get(id);
    }

    /**
     * Do a cleanup of keys that do not belong locally.
     */
    public void forceCleanup() throws IOException, ExecutionException, InterruptedException
    {
        if (name.equals(SYSTEM_TABLE))
            throw new RuntimeException("Cleanup of the system table is neither necessary nor wise");

        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            cfStore.forceCleanup();
    }

    /**
     * Take a snapshot of the entire set of column families with a given timestamp.
     * 
     * @param clientSuppliedName the tag associated with the name of the snapshot.  This
     *                           value can be null.
     */
    public void snapshot(String clientSuppliedName)
    {
        String snapshotName = getTimestampedSnapshotName(clientSuppliedName);

        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            cfStore.snapshot(snapshotName);
        }
    }

    /**
     * @param clientSuppliedName; may be null.
     * @return
     */
    public static String getTimestampedSnapshotName(String clientSuppliedName)
    {
        String snapshotName = Long.toString(System.currentTimeMillis());
        if (clientSuppliedName != null && !clientSuppliedName.equals(""))
        {
            snapshotName = snapshotName + "-" + clientSuppliedName;
        }
        return snapshotName;
    }


    /**
     * Clear all the snapshots for a given table.
     */
    public void clearSnapshot() throws IOException
    {
        for (String dataDirPath : DatabaseDescriptor.getAllDataFileLocations())
        {
            String snapshotPath = dataDirPath + File.separator + name + File.separator + SNAPSHOT_SUBDIR_NAME;
            File snapshotDir = new File(snapshotPath);
            if (snapshotDir.exists())
            {
                if (logger.isDebugEnabled())
                    logger.debug("Removing snapshot directory " + snapshotPath);
                FileUtils.deleteRecursive(snapshotDir);
            }
        }
    }
    
    /*
     * This method is an ADMIN operation to force compaction
     * of all SSTables on disk. 
     */
    public void forceCompaction() throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            CompactionManager.instance.performMajor(cfStore);
    }

    /**
     * @return A list of open SSTableReaders (TODO: ensure that the caller doesn't modify these).
     */
    public List<SSTableReader> getAllSSTables()
    {
        List<SSTableReader> list = new ArrayList<SSTableReader>();
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            list.addAll(cfStore.getSSTables());
        return list;
    }

    private Table(String table)
    {
        name = table;
        indexLocks = new Object[DatabaseDescriptor.getConcurrentWriters() * 8];
        for (int i = 0; i < indexLocks.length; i++)
            indexLocks[i] = new Object();
        // create data directories.
        for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
        {
            try
            {
                String keyspaceDir = dataDir + File.separator + table;
                FileUtils.createDirectory(keyspaceDir);
    
                // remove the deprecated streaming directory.
                File streamingDir = new File(keyspaceDir, "stream");
                if (streamingDir.exists())
                    FileUtils.deleteRecursive(streamingDir);
            }
            catch (IOException ex)
            {
                throw new IOError(ex);
            }
        }

        for (CFMetaData cfm : new ArrayList<CFMetaData>(DatabaseDescriptor.getTableDefinition(table).cfMetaData().values()))
        {
            logger.debug("Initializing {}.{}", name, cfm.cfName);
            initCf(cfm.cfId, cfm.cfName);
        }

        // check 10x as often as the lifetime, so we can exceed lifetime by 10% at most
        int checkMs = DatabaseDescriptor.getMemtableLifetimeMS() / 10;
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                for (ColumnFamilyStore cfs : columnFamilyStores.values())
                {
                    cfs.forceFlushIfExpired();
                }
            }
        };
        flushTask = StorageService.scheduledTasks.scheduleWithFixedDelay(runnable, checkMs, checkMs, TimeUnit.MILLISECONDS);
    }
    
    public void dropCf(Integer cfId) throws IOException
    {
        assert columnFamilyStores.containsKey(cfId);
        ColumnFamilyStore cfs = columnFamilyStores.remove(cfId);
        if (cfs == null)
            return;
        
        unloadCf(cfs);
        cfs.removeAllSSTables();
    }
    
    // disassociate a cfs from this table instance.
    private void unloadCf(ColumnFamilyStore cfs) throws IOException
    {
        try
        {
            cfs.forceBlockingFlush();
        }
        catch (ExecutionException e)
        {
            throw new IOException(e);
        }
        catch (InterruptedException e)
        {
            throw new IOException(e);
        }
        cfs.unregisterMBean();
    }
    
    /** adds a cf to internal structures, ends up creating disk files). */
    public void initCf(Integer cfId, String cfName)
    {
        assert !columnFamilyStores.containsKey(cfId) : String.format("tried to init %s as %s, but already used by %s",
                                                                     cfName, cfId, columnFamilyStores.get(cfId));
        columnFamilyStores.put(cfId, ColumnFamilyStore.createColumnFamilyStore(this, cfName));
    }
    
    public void reloadCf(Integer cfId) throws IOException
    {
        ColumnFamilyStore cfs = columnFamilyStores.remove(cfId);
        assert cfs != null;
        unloadCf(cfs);
        initCf(cfId, cfs.getColumnFamilyName());
    }
    
    /** basically a combined drop and add */
    public void renameCf(Integer cfId, String newName) throws IOException
    {
        assert columnFamilyStores.containsKey(cfId);
        ColumnFamilyStore cfs = columnFamilyStores.remove(cfId);
        unloadCf(cfs);
        cfs.renameSSTables(newName);
        initCf(cfId, newName);
    }

    public Row getRow(QueryFilter filter) throws IOException
    {
        ColumnFamilyStore cfStore = getColumnFamilyStore(filter.getColumnFamilyName());
        ColumnFamily columnFamily = cfStore.getColumnFamily(filter);
        return new Row(filter.key, columnFamily);
    }

    /**
     * This method adds the row to the Commit Log associated with this table.
     * Once this happens the data associated with the individual column families
     * is also written to the column family store's memtable.
    */
    public void apply(RowMutation mutation, Object serializedMutation, boolean writeCommitLog) throws IOException
    {
        List<Memtable> memtablesToFlush = Collections.emptyList();

        // write the mutation to the commitlog and memtables
        flusherLock.readLock().lock();
        try
        {
            if (writeCommitLog)
                CommitLog.instance().add(mutation, serializedMutation);
        
            DecoratedKey key = StorageService.getPartitioner().decorateKey(mutation.key());
            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                ColumnFamilyStore cfs = columnFamilyStores.get(cf.id());
                if (cfs == null)
                {
                    logger.error("Attempting to mutate non-existant column family " + cf.id());
                    continue;
                }

                SortedSet<byte[]> mutatedIndexedColumns = null;
                for (byte[] column : cfs.getIndexedColumns())
                {
                    if (cf.getColumnNames().contains(column) || cf.isMarkedForDelete())
                    {
                        if (mutatedIndexedColumns == null)
                            mutatedIndexedColumns = new TreeSet<byte[]>(FBUtilities.byteArrayComparator);
                        mutatedIndexedColumns.add(column);
                    }
                }

                synchronized (indexLockFor(mutation.key()))
                {
                    ColumnFamily oldIndexedColumns = null;
                    if (mutatedIndexedColumns != null)
                    {
                        // with the raw data CF, we can just apply every update in any order and let
                        // read-time resolution throw out obsolete versions, thus avoiding read-before-write.
                        // but for indexed data we need to make sure that we're not creating index entries
                        // for obsolete writes.
                        oldIndexedColumns = readCurrentIndexedColumns(key, cfs, mutatedIndexedColumns);
                        ignoreObsoleteMutations(cf, mutatedIndexedColumns, oldIndexedColumns);
                    }

                    Memtable fullMemtable = cfs.apply(key, cf);
                    if (fullMemtable != null)
                        memtablesToFlush = addFullMemtable(memtablesToFlush, fullMemtable);

                    if (mutatedIndexedColumns != null)
                    {
                        // ignore full index memtables -- we flush those when the "master" one is full
                        applyIndexUpdates(mutation.key(), cf, cfs, mutatedIndexedColumns, oldIndexedColumns);
                    }
                }
            }
        }
        finally
        {
            flusherLock.readLock().unlock();
        }

        // flush memtables that got filled up outside the readlock (maybeSwitchMemtable acquires writeLock).
        // usually mTF will be empty and this will be a no-op.
        for (Memtable memtable : memtablesToFlush)
            memtable.cfs.maybeSwitchMemtable(memtable, writeCommitLog);
    }

    private static List<Memtable> addFullMemtable(List<Memtable> memtablesToFlush, Memtable fullMemtable)
    {
        if (memtablesToFlush.isEmpty())
            memtablesToFlush = new ArrayList<Memtable>(2);
        memtablesToFlush.add(fullMemtable);
        return memtablesToFlush;
    }

    private static void ignoreObsoleteMutations(ColumnFamily cf, SortedSet<byte[]> mutatedIndexedColumns, ColumnFamily oldIndexedColumns)
    {
        if (oldIndexedColumns == null)
            return;

        ColumnFamily cf2 = cf.cloneMe();
        for (IColumn oldColumn : oldIndexedColumns)
        {
            cf2.addColumn(oldColumn);
        }
        ColumnFamily resolved = ColumnFamilyStore.removeDeleted(cf2, Integer.MAX_VALUE);

        for (IColumn oldColumn : oldIndexedColumns)
        {
            IColumn resolvedColumn = resolved == null ? null : resolved.getColumn(oldColumn.name());
            if (resolvedColumn != null && resolvedColumn.equals(oldColumn))
            {
                cf.remove(oldColumn.name());
                mutatedIndexedColumns.remove(oldColumn.name());
                oldIndexedColumns.remove(oldColumn.name());
            }
        }
    }

    private static ColumnFamily readCurrentIndexedColumns(DecoratedKey key, ColumnFamilyStore cfs, SortedSet<byte[]> mutatedIndexedColumns)
    {
        QueryFilter filter = QueryFilter.getNamesFilter(key, new QueryPath(cfs.getColumnFamilyName()), mutatedIndexedColumns);
        return cfs.getColumnFamily(filter);
    }

    /**
     * removes obsolete index entries and creates new ones for the given row key and mutated columns.
     * @return list of full (index CF) memtables
     */
    private static List<Memtable> applyIndexUpdates(byte[] key,
                                                    ColumnFamily cf,
                                                    ColumnFamilyStore cfs,
                                                    SortedSet<byte[]> mutatedIndexedColumns,
                                                    ColumnFamily oldIndexedColumns)
    {
        List<Memtable> fullMemtables = Collections.emptyList();

        // add new index entries
        for (byte[] columnName : mutatedIndexedColumns)
        {
            IColumn column = cf.getColumn(columnName);
            if (column == null || column.isMarkedForDelete())
                continue; // null column == row deletion

            DecoratedKey<LocalToken> valueKey = cfs.getIndexKeyFor(columnName, column.value());
            ColumnFamily cfi = cfs.newIndexedColumnFamily(columnName);
            if (column instanceof ExpiringColumn)
            {
                ExpiringColumn ec = (ExpiringColumn)column;
                cfi.addColumn(new ExpiringColumn(key, ArrayUtils.EMPTY_BYTE_ARRAY, ec.timestamp, ec.getTimeToLive(), ec.getLocalDeletionTime()));
            }
            else
            {
                cfi.addColumn(new Column(key, ArrayUtils.EMPTY_BYTE_ARRAY, column.timestamp()));
            }
            Memtable fullMemtable = cfs.getIndexedColumnFamilyStore(columnName).apply(valueKey, cfi);
            if (fullMemtable != null)
                fullMemtables = addFullMemtable(fullMemtables, fullMemtable);
        }

        // remove the old index entries
        if (oldIndexedColumns != null)
        {
            int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
            for (Map.Entry<byte[], IColumn> entry : oldIndexedColumns.getColumnsMap().entrySet())
            {
                byte[] columnName = entry.getKey();
                IColumn column = entry.getValue();
                if (column.isMarkedForDelete())
                    continue;
                DecoratedKey<LocalToken> valueKey = cfs.getIndexKeyFor(columnName, column.value());
                ColumnFamily cfi = cfs.newIndexedColumnFamily(columnName);
                cfi.addTombstone(key, localDeletionTime, column.timestamp());
                Memtable fullMemtable = cfs.getIndexedColumnFamilyStore(columnName).apply(valueKey, cfi);
                if (fullMemtable != null)
                    fullMemtables = addFullMemtable(fullMemtables, fullMemtable);
            }
        }

        return fullMemtables;
    }

    public IndexBuilder createIndexBuilder(ColumnFamilyStore cfs, SortedSet<byte[]> columns, ReducingKeyIterator iter)
    {
        return new IndexBuilder(cfs, columns, iter);
    }

    public class IndexBuilder implements ICompactionInfo
    {
        private final ColumnFamilyStore cfs;
        private final SortedSet<byte[]> columns;
        private final ReducingKeyIterator iter;

        public IndexBuilder(ColumnFamilyStore cfs, SortedSet<byte[]> columns, ReducingKeyIterator iter)
        {
            this.cfs = cfs;
            this.columns = columns;
            this.iter = iter;
        }

        public void build()
        {
            while (iter.hasNext())
            {
                DecoratedKey key = iter.next();
                logger.debug("Indexing row {} ", key);
                List<Memtable> memtablesToFlush = Collections.emptyList();
                flusherLock.readLock().lock();
                try
                {
                    synchronized (indexLockFor(key.key))
                    {
                        ColumnFamily cf = readCurrentIndexedColumns(key, cfs, columns);
                        if (cf != null)
                            memtablesToFlush = applyIndexUpdates(key.key, cf, cfs, cf.getColumnNames(), null);
                    }
                }
                finally
                {
                    flusherLock.readLock().unlock();
                }

                // during index build, we do flush index memtables separately from master; otherwise we could OOM
                for (Memtable memtable : memtablesToFlush)
                    memtable.cfs.maybeSwitchMemtable(memtable, false);
            }

            try
            {
                iter.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public long getTotalBytes()
        {
            return iter.getTotalBytes();
        }

        public long getBytesRead()
        {
            return iter.getBytesRead();
        }

        public String getTaskType()
        {
            return "Secondary index build";
        }
    }

    private Object indexLockFor(byte[] key)
    {
        return indexLocks[Math.abs(Arrays.hashCode(key) % indexLocks.length)];
    }

    public List<Future<?>> flush() throws IOException
    {
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (Integer cfId : columnFamilyStores.keySet())
        {
            Future<?> future = columnFamilyStores.get(cfId).forceFlush();
            if (future != null)
                futures.add(future);
        }
        return futures;
    }

    // for binary load path.  skips commitlog.
    void load(RowMutation rowMutation) throws IOException
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(rowMutation.key());
        for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
        {
            Collection<IColumn> columns = columnFamily.getSortedColumns();
            for (IColumn column : columns)
            {
                ColumnFamilyStore cfStore = columnFamilyStores.get(FBUtilities.byteArrayToInt(column.name()));
                cfStore.applyBinary(key, column.value());
            }
        }
    }

    public String getDataFileLocation(long expectedCompactedFileSize)
    {
        String path = DatabaseDescriptor.getDataFileLocationForTable(name, expectedCompactedFileSize);
        if (path == null)
        {
            // retry after GCing to force unmap of compacted SSTables so they can be deleted
            StorageService.instance.requestGC();
            try
            {
                Thread.sleep(SSTableDeletingReference.RETRY_DELAY * 2);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            path = DatabaseDescriptor.getDataFileLocationForTable(name, expectedCompactedFileSize);
        }
        return path;
    }

    public static String getSnapshotPath(String dataDirPath, String tableName, String snapshotName)
    {
        return dataDirPath + File.separator + tableName + File.separator + SNAPSHOT_SUBDIR_NAME + File.separator + snapshotName;
    }

    public static Iterable<Table> all()
    {
        Function<String, Table> transformer = new Function<String, Table>()
        {
            public Table apply(String tableName)
            {
                return Table.open(tableName);
            }
        };
        return Iterables.transform(DatabaseDescriptor.getTables(), transformer);
    }

    /**
     * Performs a synchronous truncate operation, effectively deleting all data
     * from the column family cfname
     * @param cfname
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void truncate(String cfname) throws InterruptedException, ExecutionException, IOException
    {
        logger.debug("Truncating...");
        ColumnFamilyStore cfs = getColumnFamilyStore(cfname);
        // truncate, blocking
        cfs.truncate().get();
        logger.debug("Truncation done.");
    }
}
