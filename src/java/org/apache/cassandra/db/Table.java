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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionType;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.LocalToken;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.SSTableDeletingReference;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NodeId;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class Table
{
    public static final String SYSTEM_TABLE = "system";

    public static final String SNAPSHOT_SUBDIR_NAME = "snapshots";

    private static final Logger logger = LoggerFactory.getLogger(Table.class);

    /**
     * accesses to CFS.memtable should acquire this for thread safety.
     * Table.maybeSwitchMemtable should aquire the writeLock; see that method for the full explanation.
     *
     * (Enabling fairness in the RRWL is observed to decrease throughput, so we leave it off.)
     */
    static final ReentrantReadWriteLock switchLock = new ReentrantReadWriteLock();

    // It is possible to call Table.open without a running daemon, so it makes sense to ensure
    // proper directories here as well as in CassandraDaemon.
    static 
    {
        if (!StorageService.instance.isClientMode()) 
        {
            try
            {
                DatabaseDescriptor.createAllDirectories();
            }
            catch (IOException ex)
            {
                throw new IOError(ex);
            }
        }
    }

    /** Table objects, one per keyspace.  only one instance should ever exist for any given keyspace. */
    private static final Map<String, Table> instances = new NonBlockingHashMap<String, Table>();

    /* Table name. */
    public final String name;
    /* ColumnFamilyStore per column family */
    private final Map<Integer, ColumnFamilyStore> columnFamilyStores = new ConcurrentHashMap<Integer, ColumnFamilyStore>();
    private final Object[] indexLocks;
    private ScheduledFuture<?> flushTask;
    private volatile AbstractReplicationStrategy replicationStrategy;

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
                        cfs.initCaches();
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
        return getColumnFamilyStore(id);
    }

    public ColumnFamilyStore getColumnFamilyStore(Integer id)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(id);
        if (cfs == null)
            throw new IllegalArgumentException("Unknown CF " + id);
        return cfs;
    }

    /**
     * Do a cleanup of keys that do not belong locally.
     */
    public void forceCleanup(NodeId.OneShotRenewer renewer) throws IOException, ExecutionException, InterruptedException
    {
        if (name.equals(SYSTEM_TABLE))
            throw new UnsupportedOperationException("Cleanup of the system table is neither necessary nor wise");

        // Sort the column families in order of SSTable size, so cleanup of smaller CFs
        // can free up space for larger ones
        List<ColumnFamilyStore> sortedColumnFamilies = new ArrayList<ColumnFamilyStore>(columnFamilyStores.values());
        Collections.sort(sortedColumnFamilies, new Comparator<ColumnFamilyStore>()
        {
            // Compare first on size and, if equal, sort by name (arbitrary & deterministic).
            public int compare(ColumnFamilyStore cf1, ColumnFamilyStore cf2)
            {
                long diff = (cf1.getTotalDiskSpaceUsed() - cf2.getTotalDiskSpaceUsed());
                if (diff > 0)
                    return 1;
                if (diff < 0)
                    return -1;
                return cf1.columnFamily.compareTo(cf2.columnFamily);
            }
        });

        // Cleanup in sorted order to free up space for the larger ones
        for (ColumnFamilyStore cfs : sortedColumnFamilies)
            cfs.forceCleanup(renewer);
    }

    /**
     * Take a snapshot of the entire set of column families with a given timestamp
     * 
     * @param snapshotName the tag associated with the name of the snapshot.  This value may not be null
     */
    public void snapshot(String snapshotName)
    {
        assert snapshotName != null;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            cfStore.snapshot(snapshotName);
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

    /**?
     * Clear snapshots for this table. If no tag is given we will clear all
     * snapshots
     *
     * @param snapshotName the user supplied snapshot name
     * @return true if the snapshot exists
     */
    public boolean snapshotExists(String snapshotName)
    {
        for (String dataDirPath : DatabaseDescriptor.getAllDataFileLocations())
        {
            String snapshotPath = dataDirPath + File.separator + name + File.separator + SNAPSHOT_SUBDIR_NAME + File.separator + snapshotName;
            File snapshot = new File(snapshotPath);
            if (snapshot.exists())
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Clear all the snapshots for a given table.
     */
    public void clearSnapshot(String tag) throws IOException
    {
        for (String dataDirPath : DatabaseDescriptor.getAllDataFileLocations())
        {
            // If tag is empty we will delete the entire snapshot directory
            String snapshotPath = dataDirPath + File.separator + name + File.separator + SNAPSHOT_SUBDIR_NAME + File.separator + tag;
            File snapshotDir = new File(snapshotPath);
            if (snapshotDir.exists())
            {
                if (logger.isDebugEnabled())
                    logger.debug("Removing snapshot directory " + snapshotPath);
                FileUtils.deleteRecursive(snapshotDir);
            }
        }
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
        KSMetaData ksm = DatabaseDescriptor.getKSMetaData(table);
        assert ksm != null : "Unknown keyspace " + table;
        try
        {
            createReplicationStrategy(ksm);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }

        indexLocks = new Object[DatabaseDescriptor.getConcurrentWriters() * 128];
        for (int i = 0; i < indexLocks.length; i++)
            indexLocks[i] = new Object();
        // create data directories.
        for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
        {
            try
            {
                String keyspaceDir = dataDir + File.separator + table;
                if (!StorageService.instance.isClientMode())
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
        flushTask = StorageService.tasks.scheduleWithFixedDelay(runnable, 10, 10, TimeUnit.SECONDS);
    }

    public void createReplicationStrategy(KSMetaData ksm) throws ConfigurationException
    {
        if (replicationStrategy != null)
            StorageService.instance.getTokenMetadata().unregister(replicationStrategy);
            
        replicationStrategy = AbstractReplicationStrategy.createReplicationStrategy(ksm.name,
                                                                                    ksm.strategyClass,
                                                                                    StorageService.instance.getTokenMetadata(),
                                                                                    DatabaseDescriptor.getEndpointSnitch(),
                                                                                    ksm.strategyOptions);
    }

    // best invoked on the compaction mananger.
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
    public void apply(RowMutation mutation, boolean writeCommitLog) throws IOException
    {
        List<Memtable> memtablesToFlush = Collections.emptyList();
        if (logger.isDebugEnabled())
            logger.debug("applying mutation of row {}", ByteBufferUtil.bytesToHex(mutation.key()));

        // write the mutation to the commitlog and memtables
        switchLock.readLock().lock();
        try
        {
            if (writeCommitLog)
                CommitLog.instance.add(mutation);
        
            DecoratedKey<?> key = StorageService.getPartitioner().decorateKey(mutation.key());
            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                ColumnFamilyStore cfs = columnFamilyStores.get(cf.id());
                if (cfs == null)
                {
                    logger.error("Attempting to mutate non-existant column family " + cf.id());
                    continue;
                }

                SortedSet<ByteBuffer> mutatedIndexedColumns = null;
                for (ByteBuffer column : cfs.getIndexedColumns())
                {
                    if (cf.getColumnNames().contains(column) || cf.isMarkedForDelete())
                    {
                        if (mutatedIndexedColumns == null)
                            mutatedIndexedColumns = new TreeSet<ByteBuffer>();
                        mutatedIndexedColumns.add(column);
                        if (logger.isDebugEnabled())
                        {
                            // can't actually use validator to print value here, because we overload value
                            // for deletion timestamp as well (which may not be a well-formed value for the column type)
                            ByteBuffer value = cf.getColumn(column) == null ? null : cf.getColumn(column).value(); // may be null on row-level deletion
                            logger.debug(String.format("mutating indexed column %s value %s",
                                                       cf.getComparator().getString(column),
                                                       value == null ? "null" : ByteBufferUtil.bytesToHex(value)));
                        }
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
                        logger.debug("Pre-mutation index row is {}", oldIndexedColumns);
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
            switchLock.readLock().unlock();
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

    private static void ignoreObsoleteMutations(ColumnFamily cf, SortedSet<ByteBuffer> mutatedIndexedColumns, ColumnFamily oldIndexedColumns)
    {
        // DO NOT modify the cf object here, it can race w/ the CL write (see https://issues.apache.org/jira/browse/CASSANDRA-2604)

        if (oldIndexedColumns == null)
            return;

        for (Iterator<ByteBuffer> iter = mutatedIndexedColumns.iterator(); iter.hasNext(); )
        {
            ByteBuffer name = iter.next();
            IColumn newColumn = cf.getColumn(name); // null == row delete or it wouldn't be marked Mutated
            if (newColumn != null && cf.isMarkedForDelete())
            {
                // row is marked for delete, but column was also updated.  if column is timestamped less than
                // the row tombstone, treat it as if it didn't exist.  Otherwise we don't care about row
                // tombstone for the purpose of the index update and we can proceed as usual.
                if (newColumn.timestamp() <= cf.getMarkedForDeleteAt())
                {
                    // don't remove from the cf object; that can race w/ CommitLog write.  Leaving it is harmless.
                    newColumn = null;
                }
            }
            IColumn oldColumn = oldIndexedColumns.getColumn(name);

            // deletions are irrelevant to the index unless we're changing state from live -> deleted, i.e.,
            // just updating w/ a newer tombstone doesn't matter
            boolean bothDeleted = (newColumn == null || newColumn.isMarkedForDelete())
                                  && (oldColumn == null || oldColumn.isMarkedForDelete());
            // obsolete means either the row or the column timestamp we're applying is older than existing data
            boolean obsoleteRowTombstone = newColumn == null && oldColumn != null && cf.getMarkedForDeleteAt() < oldColumn.timestamp();
            boolean obsoleteColumn = newColumn != null && (newColumn.timestamp() <= oldIndexedColumns.getMarkedForDeleteAt()
                                                           || (oldColumn != null && oldColumn.reconcile(newColumn) == oldColumn));
            if (bothDeleted || obsoleteRowTombstone || obsoleteColumn)
            {
                if (logger.isDebugEnabled())
                    logger.debug("skipping index update for obsolete mutation of " + cf.getComparator().getString(name));
                iter.remove();
                oldIndexedColumns.remove(name);
            }
        }
    }

    private static ColumnFamily readCurrentIndexedColumns(DecoratedKey<?> key, ColumnFamilyStore cfs, SortedSet<ByteBuffer> mutatedIndexedColumns)
    {
        QueryFilter filter = QueryFilter.getNamesFilter(key, new QueryPath(cfs.getColumnFamilyName()), mutatedIndexedColumns);
        return cfs.getColumnFamily(filter);
    }

    /**
     * removes obsolete index entries and creates new ones for the given row key and mutated columns.
     * @return list of full (index CF) memtables
     */
    private static List<Memtable> applyIndexUpdates(ByteBuffer key,
                                                    ColumnFamily cf,
                                                    ColumnFamilyStore cfs,
                                                    SortedSet<ByteBuffer> mutatedIndexedColumns,
                                                    ColumnFamily oldIndexedColumns)
    {
        List<Memtable> fullMemtables = Collections.emptyList();

        // add new index entries
        for (ByteBuffer columnName : mutatedIndexedColumns)
        {
            IColumn column = cf.getColumn(columnName);
            if (column == null || column.isMarkedForDelete())
                continue; // null column == row deletion

            DecoratedKey<LocalToken> valueKey = cfs.getIndexKeyFor(columnName, column.value());
            ColumnFamily cfi = cfs.newIndexedColumnFamily(columnName);
            if (column instanceof ExpiringColumn)
            {
                ExpiringColumn ec = (ExpiringColumn)column;
                cfi.addColumn(new ExpiringColumn(key, ByteBufferUtil.EMPTY_BYTE_BUFFER, ec.timestamp, ec.getTimeToLive(), ec.getLocalDeletionTime()));
            }
            else
            {
                cfi.addColumn(new Column(key, ByteBufferUtil.EMPTY_BYTE_BUFFER, column.timestamp()));
            }
            if (logger.isDebugEnabled())
                logger.debug("applying index row {}:{}", valueKey, cfi);
            Memtable fullMemtable = cfs.getIndexedColumnFamilyStore(columnName).apply(valueKey, cfi);
            if (fullMemtable != null)
                fullMemtables = addFullMemtable(fullMemtables, fullMemtable);
        }

        // remove the old index entries
        if (oldIndexedColumns != null)
        {
            int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
            for (Map.Entry<ByteBuffer, IColumn> entry : oldIndexedColumns.getColumnsMap().entrySet())
            {
                ByteBuffer columnName = entry.getKey();
                IColumn column = entry.getValue();
                if (column.isMarkedForDelete())
                    continue;
                DecoratedKey<LocalToken> valueKey = cfs.getIndexKeyFor(columnName, column.value());
                ColumnFamily cfi = cfs.newIndexedColumnFamily(columnName);
                cfi.addTombstone(key, localDeletionTime, column.timestamp());
                Memtable fullMemtable = cfs.getIndexedColumnFamilyStore(columnName).apply(valueKey, cfi);
                if (logger.isDebugEnabled())
                    logger.debug("applying index tombstones {}:{}", valueKey, cfi);
                if (fullMemtable != null)
                    fullMemtables = addFullMemtable(fullMemtables, fullMemtable);
            }
        }

        return fullMemtables;
    }

    public static void cleanupIndexEntry(ColumnFamilyStore cfs, ByteBuffer key, IColumn column)
    {
        if (column.isMarkedForDelete())
            return;
        int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
        DecoratedKey<LocalToken> valueKey = cfs.getIndexKeyFor(column.name(), column.value());
        ColumnFamily cfi = cfs.newIndexedColumnFamily(column.name());
        cfi.addTombstone(key, localDeletionTime, column.timestamp());
        Memtable fullMemtable = cfs.getIndexedColumnFamilyStore(column.name()).apply(valueKey, cfi);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", valueKey, cfi);
        if (fullMemtable != null)
            fullMemtable.cfs.maybeSwitchMemtable(fullMemtable, false);
    }

    public IndexBuilder createIndexBuilder(ColumnFamilyStore cfs, SortedSet<ByteBuffer> columns, ReducingKeyIterator iter)
    {
        return new IndexBuilder(cfs, columns, iter);
    }

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy;
    }

    public class IndexBuilder implements CompactionInfo.Holder
    {
        private final ColumnFamilyStore cfs;
        private final SortedSet<ByteBuffer> columns;
        private final ReducingKeyIterator iter;

        public IndexBuilder(ColumnFamilyStore cfs, SortedSet<ByteBuffer> columns, ReducingKeyIterator iter)
        {
            this.cfs = cfs;
            this.columns = columns;
            this.iter = iter;
        }

        public CompactionInfo getCompactionInfo()
        {
            return new CompactionInfo(cfs.table.name,
                                      cfs.columnFamily,
                                      CompactionType.INDEX_BUILD,
                                      iter.getBytesRead(),
                                      iter.getTotalBytes());
        }

        public void build()
        {
            while (iter.hasNext())
            {
                DecoratedKey<?> key = iter.next();
                logger.debug("Indexing row {} ", key);
                List<Memtable> memtablesToFlush = Collections.emptyList();
                switchLock.readLock().lock();
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
                    switchLock.readLock().unlock();
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
    }

    private Object indexLockFor(ByteBuffer key)
    {
        return indexLocks[Math.abs(key.hashCode() % indexLocks.length)];
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
        DecoratedKey<?> key = StorageService.getPartitioner().decorateKey(rowMutation.key());
        for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
        {
            Collection<IColumn> columns = columnFamily.getSortedColumns();
            for (IColumn column : columns)
            {
                ColumnFamilyStore cfStore = columnFamilyStores.get(ByteBufferUtil.toInt(column.name()));
                cfStore.applyBinary(key, column.value());
            }
        }
    }

    public String getDataFileLocation(long expectedSize)
    {
        String path = DatabaseDescriptor.getDataFileLocationForTable(name, expectedSize);
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
            path = DatabaseDescriptor.getDataFileLocationForTable(name, expectedSize);
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

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(name='" + name + "')";
    }
}
