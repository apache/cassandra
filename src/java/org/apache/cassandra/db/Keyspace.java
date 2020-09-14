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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.repair.CassandraKeyspaceRepairManager;
import org.apache.cassandra.db.view.ViewManager;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.repair.KeyspaceRepairManager;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * It represents a Keyspace.
 */
public class Keyspace
{
    private static final Logger logger = LoggerFactory.getLogger(Keyspace.class);

    private static final String TEST_FAIL_WRITES_KS = System.getProperty("cassandra.test.fail_writes_ks", "");
    private static final boolean TEST_FAIL_WRITES = !TEST_FAIL_WRITES_KS.isEmpty();
    private static int TEST_FAIL_MV_LOCKS_COUNT = Integer.getInteger("cassandra.test.fail_mv_locks_count", 0);

    public final KeyspaceMetrics metric;

    // It is possible to call Keyspace.open without a running daemon, so it makes sense to ensure
    // proper directories here as well as in CassandraDaemon.
    static
    {
        if (DatabaseDescriptor.isDaemonInitialized() || DatabaseDescriptor.isToolInitialized())
            DatabaseDescriptor.createAllDirectories();
    }

    private volatile KeyspaceMetadata metadata;

    //OpOrder is defined globally since we need to order writes across
    //Keyspaces in the case of Views (batchlog of view mutations)
    public static final OpOrder writeOrder = new OpOrder();

    /* ColumnFamilyStore per column family */
    private final ConcurrentMap<TableId, ColumnFamilyStore> columnFamilyStores = new ConcurrentHashMap<>();

    private volatile AbstractReplicationStrategy replicationStrategy;
    public final ViewManager viewManager;
    private final KeyspaceWriteHandler writeHandler;
    private volatile ReplicationParams replicationParams;
    private final KeyspaceRepairManager repairManager;
    private final SchemaProvider schema;

    private static volatile boolean initialized = false;

    public static void setInitialized()
    {
        initialized = true;
    }

    public static Keyspace open(String keyspaceName)
    {
        assert initialized || SchemaConstants.isLocalSystemKeyspace(keyspaceName);
        return open(keyspaceName, Schema.instance, true);
    }

    // to only be used by org.apache.cassandra.tools.Standalone* classes
    public static Keyspace openWithoutSSTables(String keyspaceName)
    {
        return open(keyspaceName, Schema.instance, false);
    }

    @VisibleForTesting
    static Keyspace open(String keyspaceName, SchemaProvider schema, boolean loadSSTables)
    {
        Keyspace keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);

        if (keyspaceInstance == null)
        {
            // Instantiate the Keyspace while holding the Schema lock. This both ensures we only do it once per
            // keyspace, and also ensures that Keyspace construction sees a consistent view of the schema.
            synchronized (schema)
            {
                keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);
                if (keyspaceInstance == null)
                {
                    // open and store the keyspace
                    keyspaceInstance = new Keyspace(keyspaceName, schema, loadSSTables);
                    schema.storeKeyspaceInstance(keyspaceInstance);
                }
            }
        }
        return keyspaceInstance;
    }

    public static Keyspace clear(String keyspaceName)
    {
        return clear(keyspaceName, Schema.instance);
    }

    public static Keyspace clear(String keyspaceName, Schema schema)
    {
        synchronized (schema)
        {
            Keyspace t = schema.removeKeyspaceInstance(keyspaceName);
            if (t != null)
            {
                for (ColumnFamilyStore cfs : t.getColumnFamilyStores())
                    t.unloadCf(cfs);
                t.metric.release();
            }
            return t;
        }
    }

    public static ColumnFamilyStore openAndGetStore(TableMetadataRef tableRef)
    {
        return open(tableRef.keyspace).getColumnFamilyStore(tableRef.id);
    }

    public static ColumnFamilyStore openAndGetStore(TableMetadata table)
    {
        return open(table.keyspace).getColumnFamilyStore(table.id);
    }

    /**
     * Removes every SSTable in the directory from the appropriate Tracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    public static void removeUnreadableSSTables(File directory)
    {
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore baseCfs : keyspace.getColumnFamilyStores())
            {
                for (ColumnFamilyStore cfs : baseCfs.concatWithIndexes())
                    cfs.maybeRemoveUnreadableSSTables(directory);
            }
        }
    }

    public void setMetadata(KeyspaceMetadata metadata)
    {
        this.metadata = metadata;
        createReplicationStrategy(metadata);
    }

    public KeyspaceMetadata getMetadata()
    {
        return metadata;
    }

    public Collection<ColumnFamilyStore> getColumnFamilyStores()
    {
        return Collections.unmodifiableCollection(columnFamilyStores.values());
    }

    public ColumnFamilyStore getColumnFamilyStore(String cfName)
    {
        TableMetadata table = schema.getTableMetadata(getName(), cfName);
        if (table == null)
            throw new IllegalArgumentException(String.format("Unknown keyspace/cf pair (%s.%s)", getName(), cfName));
        return getColumnFamilyStore(table.id);
    }

    public ColumnFamilyStore getColumnFamilyStore(TableId id)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(id);
        if (cfs == null)
            throw new IllegalArgumentException("Unknown CF " + id);
        return cfs;
    }

    public boolean hasColumnFamilyStore(TableId id)
    {
        return columnFamilyStores.containsKey(id);
    }

    /**
     * Take a snapshot of the specific column family, or the entire set of column families
     * if columnFamily is null with a given timestamp
     *
     * @param snapshotName     the tag associated with the name of the snapshot.  This value may not be null
     * @param columnFamilyName the column family to snapshot or all on null
     * @param skipFlush Skip blocking flush of memtable
     * @throws IOException if the column family doesn't exist
     */
    public void snapshot(String snapshotName, String columnFamilyName, boolean skipFlush) throws IOException
    {
        assert snapshotName != null;
        boolean tookSnapShot = false;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (columnFamilyName == null || cfStore.name.equals(columnFamilyName))
            {
                tookSnapShot = true;
                cfStore.snapshot(snapshotName, skipFlush);
            }
        }

        if ((columnFamilyName != null) && !tookSnapShot)
            throw new IOException("Failed taking snapshot. Table " + columnFamilyName + " does not exist.");
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
        snapshot(snapshotName, columnFamilyName, false);
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

    public static String getTimestampedSnapshotNameWithPrefix(String clientSuppliedName, String prefix)
    {
        return prefix + "-" + getTimestampedSnapshotName(clientSuppliedName);
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
     * Clear all the snapshots for a given keyspace.
     *
     * @param snapshotName the user supplied snapshot name. It empty or null,
     *                     all the snapshots will be cleaned
     */
    public static void clearSnapshot(String snapshotName, String keyspace)
    {
        List<File> snapshotDirs = Directories.getKSChildDirectories(keyspace);
        Directories.clearSnapshot(snapshotName, snapshotDirs);
    }

    /**
     * @return A list of open SSTableReaders
     */
    public List<SSTableReader> getAllSSTables(SSTableSet sstableSet)
    {
        List<SSTableReader> list = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            Iterables.addAll(list, cfStore.getSSTables(sstableSet));
        return list;
    }

    private Keyspace(String keyspaceName, SchemaProvider schema, boolean loadSSTables)
    {
        this.schema = schema;
        metadata = schema.getKeyspaceMetadata(keyspaceName);
        assert metadata != null : "Unknown keyspace " + keyspaceName;
        
        if (metadata.isVirtual())
            throw new IllegalStateException("Cannot initialize Keyspace with virtual metadata " + keyspaceName);
        createReplicationStrategy(metadata);

        this.metric = new KeyspaceMetrics(this);
        this.viewManager = new ViewManager(this);
        for (TableMetadata cfm : metadata.tablesAndViews())
        {
            logger.trace("Initializing {}.{}", getName(), cfm.name);
            initCf(schema.getTableMetadataRef(cfm.id), loadSSTables);
        }
        this.viewManager.reload(false);

        this.repairManager = new CassandraKeyspaceRepairManager(this);
        this.writeHandler = new CassandraKeyspaceWriteHandler(this);
    }

    private Keyspace(KeyspaceMetadata metadata)
    {
        this.schema = Schema.instance;
        this.metadata = metadata;
        createReplicationStrategy(metadata);
        this.metric = new KeyspaceMetrics(this);
        this.viewManager = new ViewManager(this);
        this.repairManager = new CassandraKeyspaceRepairManager(this);
        this.writeHandler = new CassandraKeyspaceWriteHandler(this);
    }

    public KeyspaceRepairManager getRepairManager()
    {
        return repairManager;
    }

    public static Keyspace mockKS(KeyspaceMetadata metadata)
    {
        return new Keyspace(metadata);
    }

    private void createReplicationStrategy(KeyspaceMetadata ksm)
    {
        logger.info("Creating replication strategy " + ksm.name + " params " + ksm.params);
        replicationStrategy = ksm.createReplicationStrategy();
        if (!ksm.params.replication.equals(replicationParams))
        {
            logger.debug("New replication settings for keyspace {} - invalidating disk boundary caches", ksm.name);
            columnFamilyStores.values().forEach(ColumnFamilyStore::invalidateDiskBoundaries);
        }
        replicationParams = ksm.params.replication;
    }

    // best invoked on the compaction mananger.
    public void dropCf(TableId tableId)
    {
        assert columnFamilyStores.containsKey(tableId);
        ColumnFamilyStore cfs = columnFamilyStores.remove(tableId);
        if (cfs == null)
            return;

        cfs.getCompactionStrategyManager().shutdown();
        CompactionManager.instance.interruptCompactionForCFs(cfs.concatWithIndexes(), (sstable) -> true, true);
        // wait for any outstanding reads/writes that might affect the CFS
        cfs.keyspace.writeOrder.awaitNewBarrier();
        cfs.readOrdering.awaitNewBarrier();

        unloadCf(cfs);
    }

    // disassociate a cfs from this keyspace instance.
    private void unloadCf(ColumnFamilyStore cfs)
    {
        cfs.forceBlockingFlush();
        cfs.invalidate();
    }

    /**
     * Registers a custom cf instance with this keyspace.
     * This is required for offline tools what use non-standard directories.
     */
    public void initCfCustom(ColumnFamilyStore newCfs)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(newCfs.metadata.id);

        if (cfs == null)
        {
            // CFS being created for the first time, either on server startup or new CF being added.
            // We don't worry about races here; startup is safe, and adding multiple idential CFs
            // simultaneously is a "don't do that" scenario.
            ColumnFamilyStore oldCfs = columnFamilyStores.putIfAbsent(newCfs.metadata.id, newCfs);
            // CFS mbean instantiation will error out before we hit this, but in case that changes...
            if (oldCfs != null)
                throw new IllegalStateException("added multiple mappings for cf id " + newCfs.metadata.id);
        }
        else
        {
            throw new IllegalStateException("CFS is already initialized: " + cfs.name);
        }
    }

    public KeyspaceWriteHandler getWriteHandler()
    {
        return writeHandler;
    }

    /**
     * adds a cf to internal structures, ends up creating disk files).
     */
    public void initCf(TableMetadataRef metadata, boolean loadSSTables)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(metadata.id);

        if (cfs == null)
        {
            // CFS being created for the first time, either on server startup or new CF being added.
            // We don't worry about races here; startup is safe, and adding multiple idential CFs
            // simultaneously is a "don't do that" scenario.
            ColumnFamilyStore oldCfs = columnFamilyStores.putIfAbsent(metadata.id, ColumnFamilyStore.createColumnFamilyStore(this, metadata, loadSSTables));
            // CFS mbean instantiation will error out before we hit this, but in case that changes...
            if (oldCfs != null)
                throw new IllegalStateException("added multiple mappings for cf id " + metadata.id);
        }
        else
        {
            // re-initializing an existing CF.  This will happen if you cleared the schema
            // on this node and it's getting repopulated from the rest of the cluster.
            assert cfs.name.equals(metadata.name);
            cfs.reload();
        }
    }

    public CompletableFuture<?> applyFuture(Mutation mutation, boolean writeCommitLog, boolean updateIndexes)
    {
        return applyInternal(mutation, writeCommitLog, updateIndexes, true, true, new CompletableFuture<>());
    }

    public CompletableFuture<?> applyFuture(Mutation mutation, boolean writeCommitLog, boolean updateIndexes, boolean isDroppable,
                                            boolean isDeferrable)
    {
        return applyInternal(mutation, writeCommitLog, updateIndexes, isDroppable, isDeferrable, new CompletableFuture<>());
    }

    public void apply(Mutation mutation, boolean writeCommitLog, boolean updateIndexes)
    {
        apply(mutation, writeCommitLog, updateIndexes, true);
    }

    public void apply(final Mutation mutation,
                      final boolean writeCommitLog)
    {
        apply(mutation, writeCommitLog, true, true);
    }

    /**
     * If apply is blocking, apply must not be deferred
     * Otherwise there is a race condition where ALL mutation workers are beeing blocked ending
     * in a complete deadlock of the mutation stage. See CASSANDRA-12689.
     *
     * @param mutation       the row to write.  Must not be modified after calling apply, since commitlog append
     *                       may happen concurrently, depending on the CL Executor type.
     * @param makeDurable    if true, don't return unless write has been made durable
     * @param updateIndexes  false to disable index updates (used by CollationController "defragmenting")
     * @param isDroppable    true if this should throw WriteTimeoutException if it does not acquire lock within write_request_timeout_in_ms
     */
    public void apply(final Mutation mutation,
                      final boolean makeDurable,
                      boolean updateIndexes,
                      boolean isDroppable)
    {
        applyInternal(mutation, makeDurable, updateIndexes, isDroppable, false, null);
    }

    /**
     * This method appends a row to the global CommitLog, then updates memtables and indexes.
     *
     * @param mutation       the row to write.  Must not be modified after calling apply, since commitlog append
     *                       may happen concurrently, depending on the CL Executor type.
     * @param makeDurable    if true, don't return unless write has been made durable
     * @param updateIndexes  false to disable index updates (used by CollationController "defragmenting")
     * @param isDroppable    true if this should throw WriteTimeoutException if it does not acquire lock within write_request_timeout_in_ms
     * @param isDeferrable   true if caller is not waiting for future to complete, so that future may be deferred
     */
    private CompletableFuture<?> applyInternal(final Mutation mutation,
                                               final boolean makeDurable,
                                               boolean updateIndexes,
                                               boolean isDroppable,
                                               boolean isDeferrable,
                                               CompletableFuture<?> future)
    {
        if (TEST_FAIL_WRITES && metadata.name.equals(TEST_FAIL_WRITES_KS))
            throw new RuntimeException("Testing write failures");

        Lock[] locks = null;

        boolean requiresViewUpdate = updateIndexes && viewManager.updatesAffectView(Collections.singleton(mutation), false);

        if (requiresViewUpdate)
        {
            mutation.viewLockAcquireStart.compareAndSet(0L, System.currentTimeMillis());

            // the order of lock acquisition doesn't matter (from a deadlock perspective) because we only use tryLock()
            Collection<TableId> tableIds = mutation.getTableIds();
            Iterator<TableId> idIterator = tableIds.iterator();

            locks = new Lock[tableIds.size()];
            for (int i = 0; i < tableIds.size(); i++)
            {
                TableId tableId = idIterator.next();
                int lockKey = Objects.hash(mutation.key().getKey(), tableId);
                while (true)
                {
                    Lock lock = null;

                    if (TEST_FAIL_MV_LOCKS_COUNT == 0)
                        lock = ViewManager.acquireLockFor(lockKey);
                    else
                        TEST_FAIL_MV_LOCKS_COUNT--;

                    if (lock == null)
                    {
                        //throw WTE only if request is droppable
                        if (isDroppable && (approxTime.isAfter(mutation.approxCreatedAtNanos + DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS))))
                        {
                            for (int j = 0; j < i; j++)
                                locks[j].unlock();

                            if (logger.isTraceEnabled())
                                logger.trace("Could not acquire lock for {} and table {}", ByteBufferUtil.bytesToHex(mutation.key().getKey()), columnFamilyStores.get(tableId).name);
                            Tracing.trace("Could not acquire MV lock");
                            if (future != null)
                            {
                                future.completeExceptionally(new WriteTimeoutException(WriteType.VIEW, ConsistencyLevel.LOCAL_ONE, 0, 1));
                                return future;
                            }
                            else
                                throw new WriteTimeoutException(WriteType.VIEW, ConsistencyLevel.LOCAL_ONE, 0, 1);
                        }
                        else if (isDeferrable)
                        {
                            for (int j = 0; j < i; j++)
                                locks[j].unlock();

                            // This view update can't happen right now. so rather than keep this thread busy
                            // we will re-apply ourself to the queue and try again later
                            final CompletableFuture<?> mark = future;
                            Stage.MUTATION.execute(() ->
                                                   applyInternal(mutation, makeDurable, true, isDroppable, true, mark)
                            );
                            return future;
                        }
                        else
                        {
                            // Retry lock on same thread, if mutation is not deferrable.
                            // Mutation is not deferrable, if applied from MutationStage and caller is waiting for future to finish
                            // If blocking caller defers future, this may lead to deadlock situation with all MutationStage workers
                            // being blocked by waiting for futures which will never be processed as all workers are blocked
                            try
                            {
                                // Wait a little bit before retrying to lock
                                Thread.sleep(10);
                            }
                            catch (InterruptedException e)
                            {
                                // Just continue
                            }
                            continue;
                        }
                    }
                    else
                    {
                        locks[i] = lock;
                    }
                    break;
                }
            }

            long acquireTime = System.currentTimeMillis() - mutation.viewLockAcquireStart.get();
            // Metrics are only collected for droppable write operations
            // Bulk non-droppable operations (e.g. commitlog replay, hint delivery) are not measured
            if (isDroppable)
            {
                for(TableId tableId : tableIds)
                    columnFamilyStores.get(tableId).metric.viewLockAcquireTime.update(acquireTime, MILLISECONDS);
            }
        }
        int nowInSec = FBUtilities.nowInSeconds();
        try (WriteContext ctx = getWriteHandler().beginWrite(mutation, makeDurable))
        {
            for (PartitionUpdate upd : mutation.getPartitionUpdates())
            {
                ColumnFamilyStore cfs = columnFamilyStores.get(upd.metadata().id);
                if (cfs == null)
                {
                    logger.error("Attempting to mutate non-existant table {} ({}.{})", upd.metadata().id, upd.metadata().keyspace, upd.metadata().name);
                    continue;
                }
                AtomicLong baseComplete = new AtomicLong(Long.MAX_VALUE);

                if (requiresViewUpdate)
                {
                    try
                    {
                        Tracing.trace("Creating materialized view mutations from base table replica");
                        viewManager.forTable(upd.metadata().id).pushViewReplicaUpdates(upd, makeDurable, baseComplete);
                    }
                    catch (Throwable t)
                    {
                        JVMStabilityInspector.inspectThrowable(t);
                        logger.error(String.format("Unknown exception caught while attempting to update MaterializedView! %s",
                                                   upd.metadata().toString()), t);
                        throw t;
                    }
                }

                UpdateTransaction indexTransaction = updateIndexes
                                                     ? cfs.indexManager.newUpdateTransaction(upd, ctx, nowInSec)
                                                     : UpdateTransaction.NO_OP;
                cfs.getWriteHandler().write(upd, ctx, indexTransaction);

                if (requiresViewUpdate)
                    baseComplete.set(System.currentTimeMillis());
            }

            if (future != null) {
                future.complete(null);
            }
            return future;
        }
        finally
        {
            if (locks != null)
            {
                for (Lock lock : locks)
                    if (lock != null)
                        lock.unlock();
            }
        }
    }

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy;
    }

    public List<Future<?>> flush()
    {
        List<Future<?>> futures = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores.values())
            futures.add(cfs.forceFlush());
        return futures;
    }

    public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes,
                                                              boolean autoAddIndexes,
                                                              String... cfNames) throws IOException
    {
        Set<ColumnFamilyStore> valid = new HashSet<>();

        if (cfNames.length == 0)
        {
            // all stores are interesting
            for (ColumnFamilyStore cfStore : getColumnFamilyStores())
            {
                valid.add(cfStore);
                if (autoAddIndexes)
                    valid.addAll(getIndexColumnFamilyStores(cfStore));
            }
            return valid;
        }

        // include the specified stores and possibly the stores of any of their indexes
        for (String cfName : cfNames)
        {
            if (SecondaryIndexManager.isIndexColumnFamily(cfName))
            {
                if (!allowIndexes)
                {
                    logger.warn("Operation not allowed on secondary Index table ({})", cfName);
                    continue;
                }
                String baseName = SecondaryIndexManager.getParentCfsName(cfName);
                String indexName = SecondaryIndexManager.getIndexName(cfName);

                ColumnFamilyStore baseCfs = getColumnFamilyStore(baseName);
                Index index = baseCfs.indexManager.getIndexByName(indexName);
                if (index == null)
                    throw new IllegalArgumentException(String.format("Invalid index specified: %s/%s.",
                                                                     baseCfs.metadata.name,
                                                                     indexName));

                if (index.getBackingTable().isPresent())
                    valid.add(index.getBackingTable().get());
            }
            else
            {
                ColumnFamilyStore cfStore = getColumnFamilyStore(cfName);
                valid.add(cfStore);
                if (autoAddIndexes)
                    valid.addAll(getIndexColumnFamilyStores(cfStore));
            }
        }

        return valid;
    }

    private Set<ColumnFamilyStore> getIndexColumnFamilyStores(ColumnFamilyStore baseCfs)
    {
        Set<ColumnFamilyStore> stores = new HashSet<>();
        for (ColumnFamilyStore indexCfs : baseCfs.indexManager.getAllIndexColumnFamilyStores())
        {
            logger.info("adding secondary index table {} to operation", indexCfs.metadata.name);
            stores.add(indexCfs);
        }
        return stores;
    }

    public static Iterable<Keyspace> all()
    {
        return Iterables.transform(Schema.instance.getKeyspaces(), Keyspace::open);
    }

    /**
     * @return a {@link Stream} of all existing/open {@link Keyspace} instances
     */
    public static Stream<Keyspace> allExisting()
    {
        return Schema.instance.getKeyspaces().stream().map(Schema.instance::getKeyspaceInstance).filter(Objects::nonNull);
    }

    public static Iterable<Keyspace> nonSystem()
    {
        return Iterables.transform(Schema.instance.getNonSystemKeyspaces(), Keyspace::open);
    }

    public static Iterable<Keyspace> nonLocalStrategy()
    {
        return Iterables.transform(Schema.instance.getNonLocalStrategyKeyspaces(), Keyspace::open);
    }

    public static Iterable<Keyspace> system()
    {
        return Iterables.transform(SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES, Keyspace::open);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(name='" + getName() + "')";
    }

    public String getName()
    {
        return metadata.name;
    }
}
