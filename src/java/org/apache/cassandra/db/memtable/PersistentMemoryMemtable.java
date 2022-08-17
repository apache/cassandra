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

package org.apache.cassandra.db.memtable;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.pmem.llpl.util.AutoCloseableIterator;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.memtable.pmem.PmemIndexBuilder;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import com.intel.pmem.llpl.util.LongART;
import com.intel.pmem.llpl.util.ConcurrentLongART;
import com.intel.pmem.llpl.Transaction;
import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.util.LongLinkedList;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.pmem.PmemRowMap;
import org.apache.cassandra.db.memtable.pmem.PmemTableInfo;
import org.apache.cassandra.db.memtable.pmem.PmemUnfilteredPartitionIterator;
import org.apache.cassandra.db.memtable.pmem.PmemPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class PersistentMemoryMemtable extends AbstractMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(PersistentMemoryMemtable.class);
    public static final TransactionalHeap heap;
    private static final LongLinkedList tablesLinkedList;
    private static final int CORES = FBUtilities.getAvailableProcessors();
    private final Owner owner;
    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS42;
    protected StatsCollector statsCollector = new StatsCollector();
    private static final Map<TableId, PmemTableInfo> tablesMetadataMap = new ConcurrentHashMap<>();
    private PmemTableInfo pmemTableInfo;

    static
    {
        String path = System.getProperty("pmem_path");
        if (path == null)
        {
            logger.error("Failed to open pool. System property \"pmem_path\" in \"conf/jvm.options\" is unset!");
            System.exit(1);
        }
        long size = 0;
        try
        {
            size = Long.parseLong(System.getProperty("pool_size"));
        }
        catch (NumberFormatException e)
        {
            logger.error("Failed to open pool. System property \"pool_size\" in \"conf/jvm.options\" is invalid!");
            System.exit(1);
        }
        heap = TransactionalHeap.exists(path) ?
               TransactionalHeap.openHeap(path) :
               TransactionalHeap.createHeap(path, size);
        long rootHandle = heap.getRoot();
        if (rootHandle == 0)
        {
            //Holds CART address for each table
            tablesLinkedList = new LongLinkedList(heap);
            heap.setRoot(tablesLinkedList.handle());
        }
        else
        {
            tablesLinkedList = LongLinkedList.fromHandle(heap, rootHandle);
            if (tablesMetadataMap.size() == 0)
            {
                reloadTablesMap(tablesLinkedList);
            }
        }
    }

    private static void reloadTablesMap(LongLinkedList tablesLinkedList)
    {
        Iterator<Long> tablesIterator = tablesLinkedList.iterator();
        while (tablesIterator.hasNext())
        {
            PmemTableInfo pmemTableInfo = PmemTableInfo.fromHandle(heap, tablesIterator.next());
            tablesMetadataMap.putIfAbsent(pmemTableInfo.getTableID(), pmemTableInfo);
        }
        logger.debug("Reloaded tables\n");
    }

    //ConcurrentLongART holds all memtable Partitions which is thread-safe
    public PersistentMemoryMemtable(TableMetadataRef metadataRef, Owner owner)
    {
        super(updateMetadataRef(metadataRef));
        this.owner = owner;

        if (tablesMetadataMap.get(metadata.get().id) == null)
            addToTablesMetadataMap(metadata);
        else
        {
            pmemTableInfo = tablesMetadataMap.get(metadata.get().id);
            if (!pmemTableInfo.isLoaded())
            {   //On cassandra restart we reload the metadata and SerializationHeaderList
                pmemTableInfo.reload(metadata.get());
            }
            else
            {   //On Table alter, Update the metadata and  add altered Serialization header in PmemTableInfo
                pmemTableInfo.update(metadata.get());
            }
        }
    }

    // This function assumes it is being called from within an outer transaction
    private long merge(Object update, long partitionHandle)
    {
        PmemRowUpdater updater = (PmemRowUpdater) update;
        PmemPartition pMemPartition = new PmemPartition(heap, updater.getUpdate().partitionKey(), null, updater.getIndexer(), pmemTableInfo);
        try
        {
            if (partitionHandle == 0)
            {
                pMemPartition.initialize(updater.getUpdate(), heap);
            }
            else
            {
                pMemPartition.load(heap, partitionHandle, statsCollector.get());
                pMemPartition.update(updater.getUpdate(), heap);
            }
            return (pMemPartition.getAddress());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e.getMessage()
                                       + " Failed to update partition ", e);
        }
    }

    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        long colUpdateTimeDelta = Long.MAX_VALUE;
        Transaction tx = Transaction.create(heap);
        tx.run(() -> {
            ByteSource partitionByteSource = update.partitionKey().asComparableBytes(ByteComparable.Version.OSS42);
            byte[] partitionKeyBytes = ByteSourceInverse.readBytes(partitionByteSource);
            PmemRowUpdater updater = new PmemRowUpdater(update, indexer);
            ConcurrentLongART memtableCart = tablesMetadataMap.get(metadata.get().id).getMemtableCart();
            memtableCart.put(partitionKeyBytes, updater, this::merge);
        });
        long[] pair = new long[]{ update.dataSize(), colUpdateTimeDelta };
        statsCollector.update(update.stats());
        currentOperations.addAndGet(update.operationCount());
        return pair[1];
    }


    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter columnFilter, boolean reversed, SSTableReadsListener listener)
    {
        Partition p = getPartition(key);
        if (p == null)
            return null;
        else
            return p.unfilteredIterator(columnFilter, slices, reversed);
    }

    @SuppressWarnings({ "resource" })
    public UnfilteredPartitionIterator partitionIterator(ColumnFilter columnFilter, DataRange dataRange, SSTableReadsListener listener)
    {
            AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();
            boolean startIsMin = keyRange.left.isMinimum();
            boolean stopIsMin = keyRange.right.isMinimum();
            boolean isBound = keyRange instanceof Bounds;
            boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
            boolean includeStop = isBound || keyRange instanceof Range;

            AutoCloseableIterator<LongART.Entry> entryIterator;
            ConcurrentLongART memtableCart = tablesMetadataMap.get(metadata.get().id).getMemtableCart();
            if (startIsMin)
            {
                if (stopIsMin)
                {
                    entryIterator = memtableCart.getEntryIterator();
                }
                else
                {
                    ByteSource partitionByteSource = keyRange.right.asComparableBytes(ByteComparable.Version.OSS42);
                    byte[] partitionKeyBytesRight = ByteSourceInverse.readBytes(partitionByteSource);
                    entryIterator = memtableCart.getHeadEntryIterator(partitionKeyBytesRight, includeStop);
                }
            }
            else
            {
                if (stopIsMin)
                {
                    ByteSource partitionByteSource = keyRange.left.asComparableBytes(ByteComparable.Version.OSS42);
                    byte[] partitionKeyBytesLeft = ByteSourceInverse.readBytes(partitionByteSource);
                    entryIterator = memtableCart.getTailEntryIterator(partitionKeyBytesLeft, includeStart);
                }
                else
                {
                    ByteSource partitionByteSource = keyRange.left.asComparableBytes(ByteComparable.Version.OSS42);
                    byte[] partitionKeyBytesLeft = ByteSourceInverse.readBytes(partitionByteSource);
                    partitionByteSource = keyRange.right.asComparableBytes(ByteComparable.Version.OSS42);
                    byte[] partitionKeyBytesRight = ByteSourceInverse.readBytes(partitionByteSource);
                    entryIterator = memtableCart.getEntryIterator(partitionKeyBytesLeft, includeStart, partitionKeyBytesRight, includeStop);
                }
            }
            return new PmemUnfilteredPartitionIterator(heap, entryIterator, columnFilter, dataRange, pmemTableInfo);
            }

    @SuppressWarnings({ "resource" })
    public Partition getPartition(DecoratedKey key)
    {
        PmemPartition pMemPartition = null;
        ByteSource partitionByteSource = key.asComparableBytes(BYTE_COMPARABLE_VERSION);
        byte[] partitionKeyBytes = ByteSourceInverse.readBytes(partitionByteSource);
        ConcurrentLongART memtableCart = tablesMetadataMap.get(metadata.get().id).getMemtableCart();

        if (memtableCart.size() != 0)
        {
            //The cartIterator below is an autoCloseable resource that locks a portion of the ConcurrentLongART.
            // The lock is expected to be held  for the duration of all operations on the current  partition and release afterwards.
            // The cartIterator is then embedded in any row iterator created from this partition and will be closed as part of close action on said row iterator.
            //TODO
            //There is one known scenario where the cartIterator is not closed,
            // on a conditional row update, specifically if there is no ClusteringIndexNamesFilter the returned row Iterator is not closed. This needs to be fixed.
            AutoCloseableIterator<LongART.Entry> cartIterator = memtableCart.getEntryIterator(partitionKeyBytes, true, partitionKeyBytes, true);
            long rowHandle;
            if (cartIterator.hasNext())
            {
                rowHandle = cartIterator.next().getValue();
                pMemPartition = new PmemPartition(heap, key, cartIterator, UpdateTransaction.NO_OP, pmemTableInfo);
                try
                {
                    pMemPartition.load(heap, rowHandle, statsCollector.get());
                }
                catch (IllegalArgumentException e)
                {
                    throw new RuntimeException(e.getMessage()
                                               + " Failed to reload partition ", e);
                }
            }
        }
        return pMemPartition;
    }

    public long partitionCount()
    {

        PmemTableInfo tableInfo = tablesMetadataMap.get(metadata.get().id);
        ConcurrentLongART memtableCart = tableInfo != null ? pmemTableInfo.getMemtableCart() : null;
        if (memtableCart == null)
            return 0;
        return memtableCart.size();
    }

    @Override
    public long getLiveDataSize()
    {
        return 0;
    }

    public FlushablePartitionSet<?> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        return new AbstractFlushablePartitionSet<PmemPartition>()
        {

            @Override
            public Iterator<PmemPartition> iterator()
            {
                return Collections.emptyIterator();
            }

            @Override
            public Memtable memtable()
            {
                return PersistentMemoryMemtable.this;
            }

            @Override
            public PartitionPosition from()
            {
                return from;
            }

            @Override
            public PartitionPosition to()
            {
                return to;
            }

            @Override
            public long partitionCount()
            {
                return 0;
            }

            @Override
            public long partitionKeysSize()
            {
                return 0;
            }
        };
    }

    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason)
    {
        // We want to avoid all flushing.
        switch (reason)
        {
            case STARTUP: // Called after reading and replaying the commit log.
            case INTERNALLY_FORCED: // Called to ensure ordering and persistence of system table events.
                return false;
            case MEMTABLE_PERIOD_EXPIRED: // The specified memtable expiration time elapsed.
            case INDEX_TABLE_FLUSH: // Flush requested on index table because main table is flushing.
            case STREAMS_RECEIVED: // Flush to save streamed data that was written to memtable.
                return false;   // do not do anything

            case INDEX_BUILD_COMPLETED:
            case VIEW_BUILD_STARTED:
            case USER_FORCED:
            case UNIT_TESTS:
            case OWNED_RANGES_CHANGE:
                return false;
            case INDEX_REMOVED:
                dropTable();
                return false;
            case INDEX_BUILD_STARTED:
                if (!metadata().indexes.isEmpty())
                {
                    getInitializationTask(metadata());
                }
                return false;

            case SCHEMA_CHANGE:
                if (!(metadata().params.memtable.factory() instanceof Factory))
                    return true;    // User has switched to a different memtable class. Flush and release all held data.
                // Otherwise, assuming we can handle the change, don't switch.
                // TODO: Handle
                return false;

            case STREAMING: // Called to flush data so it can be streamed. TODO: How dow we stream?
            case SNAPSHOT:
                // We don't flush for this. Returning false will trigger a performSnapshot call.
                return false;
            case DROP: // Called when a table is dropped. This memtable is no longer necessary.
                return false;
            case TRUNCATE: // The data is being deleted, but the table remains.
                // Returning true asks the ColumnFamilyStore to replace this memtable object without flushing.
                truncateTable();
                return false;
            case MEMTABLE_LIMIT: // The memtable size limit is reached, and this table was selected for flushing.
                // Also passed if we call owner.signalLimitReached()
            case COMMITLOG_DIRTY: // Commitlog thinks it needs to keep data from this table.
                // Neither of the above should happen as we specify writesAreDurable and don't use an allocator/cleaner.
                throw new AssertionError();
            default:
                throw new AssertionError();
        }
    }

    /**
     * Called when the table's metadata is updated. The memtable's metadata references to the new version.
     * This method is called only when, we configure memtable property at table level
     */
    public void metadataUpdated()
    {
        //Multiple threads are not supposed to be able to alter at the same time.
        //On Table alter, Update the metadata and  store new Serialization header in PmemTableInfo
        pmemTableInfo.update(metadata.get());
    }

    @Override
    public void localRangesUpdated()
    {

    }

    public void performSnapshot(String snapshotName)
    {
        // TODO: implement. Figure out how to restore snapshot (with external tools).
    }
    @Override
    public void performGarbageCollect()
    {
        logger.info("Starting {} from memtable for {}.{}.", OperationType.GARBAGE_COLLECT, metadata().keyspace, metadata().name);
        ConcurrentLongART memtableCart = tablesMetadataMap.get(metadata().id).getMemtableCart();
        long removedTombstones = 0;
        try (AutoCloseableIterator<LongART.Entry> cartIterator = memtableCart.getEntryIterator())
        {
            long rowHandle;
            while (cartIterator.hasNext())
            {
                rowHandle = cartIterator.next().getValue();
                PmemPartition pMemPartition = new PmemPartition(heap, UpdateTransaction.NO_OP, pmemTableInfo);
                pMemPartition.load(heap, rowHandle, statsCollector.get());
                removedTombstones += pMemPartition.vaccumTombstones();
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        logger.info("Removed {} expired Tombstones from memtable for {}.{} successfully.", removedTombstones, metadata().keyspace, metadata().name);

    }

    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        // This can prepare the memtable data for deletion; it will still be used while the flush is proceeding.
        // A setDiscarded call will follow.
    }

    //Deletes rows from PmemRowMap and cleans up memory.
    private void freeRowMemoryBlock(Long artHandle)
    {
        PmemPartition pmemPartition = new PmemPartition(heap, UpdateTransaction.NO_OP, pmemTableInfo);
        pmemPartition.load(heap, artHandle, statsCollector.get());
        PmemRowMap rm = PmemRowMap.loadFromAddress(heap, pmemPartition.getRowMapAddress(), UpdateTransaction.NO_OP, pmemTableInfo);
        rm.deleteRowMapTree();
    }

    //Frees the Persistent Memory which is associated with CFS
    private void dropTable()
    {
        ConcurrentLongART memtableCart = tablesMetadataMap.get(metadata.get().id).getMemtableCart();
        memtableCart.clear(this::freeRowMemoryBlock);
        memtableCart.free();
        Iterator<Long> tablesIterator = tablesLinkedList.iterator();
        int i = 0;
        while (tablesIterator.hasNext())
        {
            if (pmemTableInfo.handle() == tablesIterator.next().longValue())
            {
                tablesLinkedList.remove(i);
                pmemTableInfo.cleanUp();
                tablesMetadataMap.remove(metadata.get().id);
                break;
            }
            i++;
        }
    }

    //Truncates base and index tables
    private void truncateTable()
    {
        clearMemtableData(metadata.get().id);
        if (!metadata.get().indexes.isEmpty())
        {
            Indexes tableIndexes = metadata().indexes;
            for (IndexMetadata indexTable : tableIndexes)
            {
                TableId indexId = TableId.fromUUID(indexTable.id);
                clearMemtableData(indexId);
            }
        }
    }

    //Clears ConcurrentLongART content
    private void clearMemtableData(TableId id)
    {
        PmemTableInfo pmemIndexTableInfo = tablesMetadataMap.get(id);
        if (pmemIndexTableInfo != null)
        {
            ConcurrentLongART memtableCartForIndex = pmemIndexTableInfo.getMemtableCart();
            memtableCartForIndex.clear(this::freeRowMemoryBlock);
            memtableCartForIndex.free();
            memtableCartForIndex = new ConcurrentLongART(heap, CORES);
            pmemIndexTableInfo.updateMemtableCart(memtableCartForIndex);
        }
        else
            logger.info("Cannot truncate {} index table ",id);
    }

    public void discard()
    {
        //Deletes all memtable data from pmem.
        //dropTable();
        truncateTable();
    }

    @Override
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        return true;
    }

    public CommitLogPosition getApproximateCommitLogLowerBound()
    {
        // We don't maintain commit log positions
        return CommitLogPosition.NONE;
    }

    public CommitLogPosition getCommitLogLowerBound()
    {
        // We don't maintain commit log positions
        return CommitLogPosition.NONE;
    }

    @Override
    public LastCommitLogPosition getFinalCommitLogUpperBound()
    {
       // We don't maintain commit log positions
        return  new LastCommitLogPosition(CommitLogPosition.NONE);
    }

    public boolean isClean()
    {
        return partitionCount() == 0;
    }

    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        // We don't track commit log positions, so if we are dirty, we may.
        return !isClean();
    }

    public void addMemoryUsageTo(MemoryUsage stats)
    {
        // our memory usage is not counted
    }

    @Override
    public void markExtraOnHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {
        // we don't track this
    }

    @Override
    public void markExtraOffHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {
        // we don't track this
    }

    public static Factory factory(Map<String, String> furtherOptions)
    {
        Boolean skipOption = Boolean.parseBoolean(furtherOptions.remove("skipCommitLog"));
        return skipOption ? commitLogSkippingFactory : commitLogWritingFactory;
    }

    private static final Factory commitLogSkippingFactory = new Factory(true);
    private static final Factory commitLogWritingFactory = new Factory(false);

    public static class Factory implements Memtable.Factory
    {
        private final boolean skipCommitLog;

        public Factory(boolean skipCommitLog)
        {
            this.skipCommitLog = skipCommitLog;
        }

        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadataRef,
                               Owner owner)
        {
            return new PersistentMemoryMemtable(metadataRef, owner);
        }

        public boolean writesShouldSkipCommitLog()
        {
            return skipCommitLog;
        }

        public boolean writesAreDurable()
        {
            return true;
        }

        public boolean streamToMemtable()
        {
            return true;
        }

        public boolean streamFromMemtable()
        {
            return true;
        }
    }

    /**
     * @param metadata
     * @return ConcurrentLongART based on TableId
     */
    public static ConcurrentLongART getMemtableCart(TableMetadata metadata)
    {
        return tablesMetadataMap.get(metadata.id).getMemtableCart();
    }

    private void addToTablesMetadataMap(TableMetadataRef metadataRef)
    {
        pmemTableInfo = Transaction.create(heap, () -> {
            ConcurrentLongART memtableCart = new ConcurrentLongART(heap, CORES);
            PmemTableInfo tmpPmemTableInfo = new PmemTableInfo(heap, memtableCart, metadataRef.get());
            tablesLinkedList.addFirst(tmpPmemTableInfo.handle());
            return tmpPmemTableInfo;
        });
        tablesMetadataMap.putIfAbsent(metadata.get().id, pmemTableInfo);
    }

    //This class will provide PartitionUpdate and  UpdateTransaction object
    public class PmemRowUpdater
    {
        private final PartitionUpdate update;
        private final UpdateTransaction indexer;

        PmemRowUpdater(PartitionUpdate update, UpdateTransaction indexer)
        {
            this.update = update;
            this.indexer = indexer;
        }

        public PartitionUpdate getUpdate()
        {
            return update;
        }

        public UpdateTransaction getIndexer()
        {
            return indexer;
        }
    }

    //Intializes task to build index on Exsisting data.
    private void getInitializationTask(TableMetadata metadata)
    {
        ColumnFamilyStore baseCfs = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.id);
        Indexes tableIndexes = metadata().indexes;
        for (IndexMetadata tableIndex : tableIndexes)
        {
            if (!(isBuilt(baseCfs, tableIndex.name) || baseCfs.isEmpty()))
            {
                Index index = baseCfs.indexManager.getIndexByName(tableIndex.name);
                PmemIndexBuilder.buildBlocking(baseCfs, tableIndex, Collections.singleton(index), owner.getCurrentMemtable());
            }
        }
    }

    private static TableMetadataRef updateMetadataRef(TableMetadataRef metadataRef)
    {
        //Since Index TableMetadata will contain base table Id ,
        //converting Index name to Index table ID, which is required to differentiate Base and Index table.
        String indexName = metadataRef.get().indexName().orElse(null);
        if (indexName !=null)
        {
            TableMetadata tableMetadata = metadataRef.get().unbuild().id(TableId.fromUUID(UUID.nameUUIDFromBytes(indexName.getBytes()))).build();
            return TableMetadataRef.forOfflineTools(tableMetadata);
        }
        return metadataRef;
    }

    // Returns true if the given index is built else  returns false
    private boolean isBuilt(ColumnFamilyStore baseCfs, String indexName)
    {
        return SystemKeyspace.isIndexBuilt(baseCfs.keyspace.getName(), indexName);
    }

    private static class StatsCollector
    {
        private final AtomicReference<EncodingStats> stats = new AtomicReference<>(EncodingStats.NO_STATS);

        public void update(EncodingStats newStats)
        {
            while (true)
            {
                EncodingStats current = stats.get();
                EncodingStats updated = current.mergeWith(newStats);
                if (stats.compareAndSet(current, updated))
                    return;
            }
        }

        public EncodingStats get()
        {
            return stats.get();
        }
    }
}