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

import java.io.IOError;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.io.IOException;
import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.Future;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.sstable.SSTableDeletingReference;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Table
{
    public static final String SYSTEM_TABLE = "system";

    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    private static final String SNAPSHOT_SUBDIR_NAME = "snapshots";
    /* accesses to CFS.memtable should acquire this for thread safety.  only switchMemtable should aquire the writeLock. */
    static final ReentrantReadWriteLock flusherLock = new ReentrantReadWriteLock(true);

    private static Timer flushTimer = new Timer("FLUSH-TIMER");

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
    // cache application CFs since Range queries ask for them a _lot_
    private SortedSet<String> applicationColumnFamilies;
    private final TimerTask flushTask;
    private final Object[] indexLocks;
    
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
                    tableInstance = new Table(table);
                    instances.put(table, tableInstance);
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
                t.flushTask.cancel();
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
    public void forceCleanup()
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
    public void forceCompaction()
    {
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            CompactionManager.instance.submitMajor(cfStore);
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

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        for (CFMetaData cfm : new ArrayList<CFMetaData>(DatabaseDescriptor.getTableDefinition(table).cfMetaData().values()))
        {
            ColumnFamilyStore cfs = ColumnFamilyStore.createColumnFamilyStore(table, cfm.cfName);
            columnFamilyStores.put(cfm.cfId, cfs);
            try
            {
                ObjectName mbeanName = new ObjectName("org.apache.cassandra.db:type=ColumnFamilyStores,keyspace=" + table + ",columnfamily=" + cfm.cfName);
                if (mbs.isRegistered(mbeanName))
                    mbs.unregisterMBean(mbeanName);
                mbs.registerMBean(cfs, mbeanName);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }

        }

        // check 10x as often as the lifetime, so we can exceed lifetime by 10% at most
        int checkMs = DatabaseDescriptor.getMemtableLifetimeMS() / 10;
        flushTask = new TimerTask()
        {
            public void run()
            {
                for (ColumnFamilyStore cfs : columnFamilyStores.values())
                {
                    cfs.forceFlushIfExpired();
                }
            }
        };
        flushTimer.schedule(flushTask, checkMs, checkMs);
    }
    
    /** removes a cf from internal structures (doesn't change disk files). */
    public void dropCf(Integer cfId) throws IOException
    {
        assert columnFamilyStores.containsKey(cfId);
        ColumnFamilyStore cfs = columnFamilyStores.remove(cfId);
        if (cfs != null)
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
        }
    }
    
    /** adds a cf to internal structures, ends up creating disk files). */
    public void initCf(Integer cfId, String cfName)
    {
        assert !columnFamilyStores.containsKey(cfId) : String.format("tried to init %s as %s, but already used by %s",
                                                                     cfName, cfId, columnFamilyStores.get(cfId));
        columnFamilyStores.put(cfId, ColumnFamilyStore.createColumnFamilyStore(name, cfName));
    }
    
    /** basically a combined drop and add */
    public void renameCf(Integer cfId, String newName) throws IOException
    {
        dropCf(cfId);
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
        HashMap<ColumnFamilyStore,Memtable> memtablesToFlush = new HashMap<ColumnFamilyStore, Memtable>(2);

        // write the mutation to the commitlog and memtables
        flusherLock.readLock().lock();
        try
        {
            if (writeCommitLog)
                CommitLog.instance().add(mutation, serializedMutation);
        
            DecoratedKey key = StorageService.getPartitioner().decorateKey(mutation.key());
            for (ColumnFamily columnFamily : mutation.getColumnFamilies())
            {
                ColumnFamilyStore cfs = columnFamilyStores.get(columnFamily.id());
                if (cfs == null)
                {
                    logger.error("Attempting to mutate non-existant column family " + columnFamily.id());
                    continue;
                }

                ColumnFamily oldIndexedColumns;
                SortedSet<byte[]> mutatedIndexedColumns = null;
                for (byte[] column : cfs.getIndexedColumns())
                {
                    if (columnFamily.getColumnNames().contains(column))
                    {
                        if (mutatedIndexedColumns == null)
                            mutatedIndexedColumns = new TreeSet<byte[]>(FBUtilities.byteArrayComparator);
                        mutatedIndexedColumns.add(column);
                    }
                }

                if (mutatedIndexedColumns == null)
                {
                    // just update the actual value, no extra synchronization
                    applyCF(cfs, key, columnFamily, memtablesToFlush);
                }
                else
                {
                    synchronized (indexLocks[Arrays.hashCode(mutation.key()) % indexLocks.length])
                    {
                        // read old indexed values
                        QueryFilter filter = QueryFilter.getNamesFilter(key, new QueryPath(cfs.getColumnFamilyName()), mutatedIndexedColumns);
                        oldIndexedColumns = cfs.getColumnFamily(filter);

                        // apply the mutation
                        applyCF(cfs, key, columnFamily, memtablesToFlush);

                        // add new index entries
                        for (byte[] columnName : mutatedIndexedColumns)
                        {
                            IColumn column = columnFamily.getColumn(columnName);
                            DecoratedKey valueKey = cfs.getIndexKeyFor(columnName, column.value());
                            ColumnFamily cf = cfs.newIndexedColumnFamily(columnName);
                            cf.addColumn(new Column(mutation.key(), ArrayUtils.EMPTY_BYTE_ARRAY, column.clock()));
                            applyCF(cfs.getIndexedColumnFamilyStore(columnName), valueKey, cf, memtablesToFlush);
                        }

                        // remove the old index entries
                        if (oldIndexedColumns != null)
                        {
                            int localDeletionTime = (int)(System.currentTimeMillis() / 1000);
                            for (Map.Entry<byte[], IColumn> entry : oldIndexedColumns.getColumnsMap().entrySet())
                            {
                                byte[] columnName = entry.getKey();
                                IColumn column = entry.getValue();
                                DecoratedKey valueKey = cfs.getIndexKeyFor(columnName, column.value());
                                ColumnFamily cf = cfs.newIndexedColumnFamily(columnName);
                                cf.deleteColumn(mutation.key(), localDeletionTime, column.clock());
                                applyCF(cfs, valueKey, cf, memtablesToFlush);
                            }
                        }
                    }
                }

                ColumnFamily cachedRow = cfs.getRawCachedRow(key);
                if (cachedRow != null)
                    cachedRow.addAll(columnFamily);
            }
        }
        finally
        {
            flusherLock.readLock().unlock();
        }

        // flush memtables that got filled up.  usually mTF will be empty and this will be a no-op
        for (Map.Entry<ColumnFamilyStore, Memtable> entry : memtablesToFlush.entrySet())
            entry.getKey().maybeSwitchMemtable(entry.getValue(), writeCommitLog);
    }

    public void applyIndexedCF(ColumnFamilyStore indexedCfs, DecoratedKey rowKey, DecoratedKey indexedKey, ColumnFamily indexedColumnFamily) 
    {
        Memtable memtableToFlush;
        flusherLock.readLock().lock();
        try
        {
            synchronized (indexLocks[Arrays.hashCode(rowKey.key) % indexLocks.length])
            {
                memtableToFlush = indexedCfs.apply(indexedKey, indexedColumnFamily);
            }
        }
        finally 
        {
            flusherLock.readLock().unlock();
        }

        if (memtableToFlush != null)
            indexedCfs.maybeSwitchMemtable(memtableToFlush, false);
    }
    
    private static void applyCF(ColumnFamilyStore cfs, DecoratedKey key, ColumnFamily columnFamily, HashMap<ColumnFamilyStore, Memtable> memtablesToFlush)
    {
        Memtable memtableToFlush = cfs.apply(key, columnFamily);
        if (memtableToFlush != null)
            memtablesToFlush.put(cfs, memtableToFlush);
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
