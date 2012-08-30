/*
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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;

/**
 * It represents a Keyspace.
 */
public class Table
{
    public static final String SYSTEM_KS = "system";

    private static final Logger logger = LoggerFactory.getLogger(Table.class);

    /**
     * accesses to CFS.memtable should acquire this for thread safety.
     * CFS.maybeSwitchMemtable should aquire the writeLock; see that method for the full explanation.
     * <p/>
     * (Enabling fairness in the RRWL is observed to decrease throughput, so we leave it off.)
     */
    public static final ReentrantReadWriteLock switchLock = new ReentrantReadWriteLock();

    // It is possible to call Table.open without a running daemon, so it makes sense to ensure
    // proper directories here as well as in CassandraDaemon.
    static
    {
        if (!StorageService.instance.isClientMode())
            DatabaseDescriptor.createAllDirectories();
    }

    /* Table name. */
    public final String name;
    /* ColumnFamilyStore per column family */
    private final Map<UUID, ColumnFamilyStore> columnFamilyStores = new ConcurrentHashMap<UUID, ColumnFamilyStore>();
    private final Object[] indexLocks;
    private volatile AbstractReplicationStrategy replicationStrategy;

    public static Table open(String table)
    {
        return open(table, Schema.instance, true);
    }

    public static Table openWithoutSSTables(String table)
    {
        return open(table, Schema.instance, false);
    }

    private static Table open(String table, Schema schema, boolean loadSSTables)
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
                    tableInstance = new Table(table, loadSSTables);
                    schema.storeTableInstance(tableInstance);

                    // table has to be constructed and in the cache before cacheRow can be called
                    for (ColumnFamilyStore cfs : tableInstance.getColumnFamilyStores())
                        cfs.initRowCache();
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

    /**
     * Removes every SSTable in the directory from the appropriate DataTracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    public static void removeUnreadableSSTables(File directory)
    {
        for (Table table : Table.all())
        {
            for (ColumnFamilyStore baseCfs : table.getColumnFamilyStores())
            {
                for (ColumnFamilyStore cfs : baseCfs.concatWithIndexes())
                    cfs.maybeRemoveUnreadableSSTables(directory);
            }
        }
    }

    public Collection<ColumnFamilyStore> getColumnFamilyStores()
    {
        return Collections.unmodifiableCollection(columnFamilyStores.values());
    }

    public ColumnFamilyStore getColumnFamilyStore(String cfName)
    {
        UUID id = Schema.instance.getId(name, cfName);
        if (id == null)
            throw new IllegalArgumentException(String.format("Unknown table/cf pair (%s.%s)", name, cfName));
        return getColumnFamilyStore(id);
    }

    public ColumnFamilyStore getColumnFamilyStore(UUID id)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(id);
        if (cfs == null)
            throw new IllegalArgumentException("Unknown CF " + id);
        return cfs;
    }

    /**
     * Take a snapshot of the specific column family, or the entire set of column families
     * if columnFamily is null with a given timestamp
     *
     * @param snapshotName     the tag associated with the name of the snapshot.  This value may not be null
     * @param columnFamilyName the column family to snapshot or all on null
     * @throws IOException if the column family doesn't exist
     */
    public void snapshot(String snapshotName, String columnFamilyName) throws IOException
    {
        assert snapshotName != null;
        boolean tookSnapShot = false;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (columnFamilyName == null || cfStore.columnFamily.equals(columnFamilyName))
            {
                tookSnapShot = true;
                cfStore.snapshot(snapshotName);
            }
        }

        if ((columnFamilyName != null) && !tookSnapShot)
            throw new IOException("Failed taking snapshot. Column family " + columnFamilyName + " does not exist.");
    }

    /**
     * @param clientSuppliedName may be null.
     * @return the name of the snapshot
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
     * Check whether snapshots already exists for a given name.
     *
     * @param snapshotName the user supplied snapshot name
     * @return true if the snapshot exists
     */
    public boolean snapshotExists(String snapshotName)
    {
        assert snapshotName != null;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (cfStore.snapshotExists(snapshotName))
                return true;
        }
        return false;
    }

    /**
     * Clear all the snapshots for a given table.
     *
     * @param snapshotName the user supplied snapshot name. It empty or null,
     *                     all the snapshots will be cleaned
     */
    public void clearSnapshot(String snapshotName)
    {
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            cfStore.clearSnapshot(snapshotName);
        }
    }

    /**
     * @return A list of open SSTableReaders
     */
    public List<SSTableReader> getAllSSTables()
    {
        List<SSTableReader> list = new ArrayList<SSTableReader>(columnFamilyStores.size());
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            list.addAll(cfStore.getSSTables());
        return list;
    }

    private Table(String table, boolean loadSSTables)
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

        for (CFMetaData cfm : new ArrayList<CFMetaData>(Schema.instance.getTableDefinition(table).cfMetaData().values()))
        {
            logger.debug("Initializing {}.{}", name, cfm.cfName);
            initCf(cfm.cfId, cfm.cfName, loadSSTables);
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
    public void dropCf(UUID cfId) throws IOException
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

    /**
     * adds a cf to internal structures, ends up creating disk files).
     */
    public void initCf(UUID cfId, String cfName, boolean loadSSTables)
    {
        if (columnFamilyStores.containsKey(cfId))
        {
            // this is the case when you reset local schema
            // just reload metadata
            ColumnFamilyStore cfs = columnFamilyStores.get(cfId);
            assert cfs.getColumnFamilyName().equals(cfName);

            cfs.metadata.reload();
            cfs.reload();
        }
        else
        {
            columnFamilyStores.put(cfId, ColumnFamilyStore.createColumnFamilyStore(this, cfName, loadSSTables));
        }
    }

    public Row getRow(QueryFilter filter)
    {
        ColumnFamilyStore cfStore = getColumnFamilyStore(filter.getColumnFamilyName());
        ColumnFamily columnFamily = cfStore.getColumnFamily(filter, ArrayBackedSortedColumns.factory());
        return new Row(filter.key, columnFamily);
    }

    public void apply(RowMutation mutation, boolean writeCommitLog)
    {
        apply(mutation, writeCommitLog, true);
    }

    /**
     * This method appends a row to the global CommitLog, then updates memtables and indexes.
     *
     * @param mutation       the row to write.  Must not be modified after calling apply, since commitlog append
     *                       may happen concurrently, depending on the CL Executor type.
     * @param writeCommitLog false to disable commitlog append entirely
     * @param updateIndexes  false to disable index updates (used by CollationController "defragmenting")
     */
    public void apply(RowMutation mutation, boolean writeCommitLog, boolean updateIndexes)
    {
        if (!mutation.getTable().equals(Tracing.TRACE_KS))
            logger.debug("applying mutation");

        // write the mutation to the commitlog and memtables
        switchLock.readLock().lock();
        try
        {
            if (writeCommitLog)
                CommitLog.instance.add(mutation);

            DecoratedKey key = StorageService.getPartitioner().decorateKey(mutation.key());
            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                ColumnFamilyStore cfs = columnFamilyStores.get(cf.id());
                if (cfs == null)
                {
                    logger.error("Attempting to mutate non-existant column family " + cf.id());
                    continue;
                }

                cfs.apply(key, cf, updateIndexes ? cfs.indexManager.updaterFor(key, true) : SecondaryIndexManager.nullUpdater);
            }
        }
        finally
        {
            switchLock.readLock().unlock();
        }
    }

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy;
    }

    /**
     * @param key row to index
     * @param cfs ColumnFamily to index row in
     * @param idxNames columns to index, in comparator order
     */
    public static void indexRow(DecoratedKey key, ColumnFamilyStore cfs, Set<String> idxNames)
    {
        if (logger.isDebugEnabled())
            logger.debug("Indexing row {} ", cfs.metadata.getKeyValidator().getString(key.key));

        Collection<SecondaryIndex> indexes = cfs.indexManager.getIndexesByNames(idxNames);

        switchLock.readLock().lock();
        try
        {
            // Our index lock is per-row, but we don't want to hold writes for too long, so for large rows
            // we release the lock between pages
            SliceQueryPager pager = new SliceQueryPager(cfs, key, ColumnSlice.ALL_COLUMNS_ARRAY);
            while (pager.hasNext())
            {
                synchronized (cfs.table.indexLockFor(key.key))
                {
                    ColumnFamily cf = pager.next();
                    ColumnFamily cf2 = cf.cloneMeShallow();
                    for (IColumn column : cf)
                    {
                        if (cfs.indexManager.indexes(column.name(), indexes))
                            cf2.addColumn(column);
                    }
                    cfs.indexManager.indexRow(key.key, cf2);
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

    public List<Future<?>> flush()
    {
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (UUID cfId : columnFamilyStores.keySet())
        {
            Future<?> future = columnFamilyStores.get(cfId).forceFlush();
            if (future != null)
                futures.add(future);
        }
        return futures;
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
