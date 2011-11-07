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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.SSTableDeletingTask;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MmappedSegmentedFile;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * It represents a Keyspace.
 */
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

    /* Table name. */
    public final String name;
    /* ColumnFamilyStore per column family */
    private final Map<Integer, ColumnFamilyStore> columnFamilyStores = new ConcurrentHashMap<Integer, ColumnFamilyStore>();
    private final Object[] indexLocks;
    private volatile AbstractReplicationStrategy replicationStrategy;

    public static Table open(String table)
    {
        return open(table, Schema.instance);
    }

    public static Table open(String table, Schema schema)
    {
        Table tableInstance = schema.getTableInstance(table);

        if (tableInstance == null)
        {
            // instantiate the Table.  we could use putIfAbsent but it's important to making sure it is only done once
            // per keyspace, so we synchronize and re-check before doing it.
            synchronized (Table.class)
            {
                tableInstance = schema.getTableInstance(table);
                if (tableInstance == null)
                {
                    // open and store the table
                    tableInstance = new Table(table);
                    schema.storeTableInstance(tableInstance);

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
        return clear(table, Schema.instance);
    }

    public static Table clear(String table, Schema schema) throws IOException
    {
        synchronized (Table.class)
        {
            Table t = schema.removeTableInstance(table);
            if (t != null)
            {
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
        Integer id = Schema.instance.getId(name, cfName);
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
     * @param clientSuppliedName may be null.
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
     * @return A list of open SSTableReaders
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
        KSMetaData ksm = Schema.instance.getKSMetaData(table);
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

        for (CFMetaData cfm : new ArrayList<CFMetaData>(Schema.instance.getTableDefinition(table).cfMetaData().values()))
        {
            logger.debug("Initializing {}.{}", name, cfm.cfName);
            initCf(cfm.cfId, cfm.cfName);
        }

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
        cfs.invalidate();
    }
    
    /** adds a cf to internal structures, ends up creating disk files). */
    public void initCf(Integer cfId, String cfName)
    {
        assert !columnFamilyStores.containsKey(cfId) : String.format("tried to init %s as %s, but already used by %s",
                                                                     cfName, cfId, columnFamilyStores.get(cfId));
        columnFamilyStores.put(cfId, ColumnFamilyStore.createColumnFamilyStore(this, cfName));
    }

    public Row getRow(QueryFilter filter) throws IOException
    {
        ColumnFamilyStore cfStore = getColumnFamilyStore(filter.getColumnFamilyName());
        ColumnFamily columnFamily = cfStore.getColumnFamily(filter, ArrayBackedSortedColumns.factory());
        return new Row(filter.key, columnFamily);
    }

    /**
     * This method adds the row to the Commit Log associated with this table.
     * Once this happens the data associated with the individual column families
     * is also written to the column family store's memtable.
    */
    public void apply(RowMutation mutation, boolean writeCommitLog) throws IOException
    {
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
                for (ByteBuffer column : cfs.indexManager.getIndexedColumns())
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

                // Sharding the lock is insufficient to avoid contention when there is a "hot" row, e.g., for
                // hint writes when a node is down (keyed by target IP).  So it is worth special-casing the
                // no-index case to avoid the synchronization.
                if (mutatedIndexedColumns == null)
                {
                    cfs.apply(key, cf);
                    continue;
                }
                // else mutatedIndexedColumns != null
                synchronized (indexLockFor(mutation.key()))
                {
                    // with the raw data CF, we can just apply every update in any order and let
                    // read-time resolution throw out obsolete versions, thus avoiding read-before-write.
                    // but for indexed data we need to make sure that we're not creating index entries
                    // for obsolete writes.
                    ColumnFamily oldIndexedColumns = readCurrentIndexedColumns(key, cfs, mutatedIndexedColumns);
                    logger.debug("Pre-mutation index row is {}", oldIndexedColumns);
                    ignoreObsoleteMutations(cf, mutatedIndexedColumns, oldIndexedColumns);

                    cfs.apply(key, cf);

                    // ignore full index memtables -- we flush those when the "master" one is full
                    cfs.indexManager.applyIndexUpdates(mutation.key(), cf, mutatedIndexedColumns, oldIndexedColumns);
                }
            }
        }
        finally
        {
            switchLock.readLock().unlock();
        }

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

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy;
    }

    public static void indexRow(DecoratedKey<?> key, ColumnFamilyStore cfs, SortedSet<ByteBuffer> indexedColumns)
    {
        if (logger.isDebugEnabled())
            logger.debug("Indexing row {} ", cfs.metadata.getKeyValidator().getString(key.key));

        switchLock.readLock().lock();
        try
        {
            synchronized (cfs.table.indexLockFor(key.key))
            {
                ColumnFamily cf = readCurrentIndexedColumns(key, cfs, indexedColumns);
                if (cf != null)
                    try
                    {
                        cfs.indexManager.applyIndexUpdates(key.key, cf, cf.getColumnNames(), null);
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
            }
        }
        finally
        {
            switchLock.readLock().unlock();
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

    public String getDataFileLocation(long expectedSize)
    {
        String path = DatabaseDescriptor.getDataFileLocationForTable(name, expectedSize);
        // Requesting GC has a chance to free space only if we're using mmap and a non SUN jvm
        if (path == null
         && (DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap || DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.mmap)
         && !MmappedSegmentedFile.isCleanerAvailable())
        {
            StorageService.instance.requestGC();
            // retry after GCing has forced unmap of compacted SSTables so they can be deleted
            // Note: GCInspector will do this already, but only sun JVM supports GCInspector so far
            SSTableDeletingTask.rescheduleFailedTasks();
            try
            {
                Thread.sleep(10000);
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
        return getSnapshotPath(dataDirPath + File.separator + tableName, snapshotName);
    }

    public static String getSnapshotPath(String tableDirectory, String snapshotName)
    {
        return tableDirectory + File.separator + SNAPSHOT_SUBDIR_NAME + File.separator + snapshotName;
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
        return Iterables.transform(Schema.instance.getTables(), transformer);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(name='" + name + "')";
    }
}
