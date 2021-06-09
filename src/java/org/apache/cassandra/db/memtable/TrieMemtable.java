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

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.EnsureOnHeap;

public class TrieMemtable extends AbstractAllocatorMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemtable.class);

    public static final Factory FACTORY = TrieMemtable::new;

    /** Buffer type to use for memtable tries (on- vs off-heap) */
    public static final BufferType BUFFER_TYPE;
    static
    {
        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
        case unslabbed_heap_buffers:
        case heap_buffers:
            BUFFER_TYPE = BufferType.ON_HEAP;
            break;
        case offheap_buffers:
        case offheap_objects:
            BUFFER_TYPE = BufferType.OFF_HEAP;
            break;
        default:
            throw new AssertionError();
        }
    }

    /** If keys is below this length, we will use a recursive procedure for inserting data in the memtable trie. */
    @VisibleForTesting
    public static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    /** The byte-ordering conversion version to use for memtables. */
    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS41;

    // Set to true when the memtable requests a switch (e.g. for trie size limit being reached) to ensure only one
    // thread calls cfs.switchMemtableIfCurrent.
    private AtomicBoolean switchRequested = new AtomicBoolean(false);

    // We index the memtable by PartitionPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final MemtableTrie<BTreePartitionData> partitions = new MemtableTrie<>(BUFFER_TYPE);

    // only to be used by init(), to setup the very first memtable for the cfs
    TrieMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        super(commitLogLowerBound, metadataRef, owner);
    }

    protected Factory factory()
    {
        return FACTORY;
    }

    public boolean isClean()
    {
        return partitions.isEmpty();
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        Cloner cloner = allocator.cloner(opGroup);
        BTreePartitionUpdater updater = new BTreePartitionUpdater(allocator, cloner, opGroup, indexer);
        DecoratedKey key = update.partitionKey();

        // TODO: Improve locking.
        synchronized (this)
        {
            long onHeap = partitions.sizeOnHeap();
            long offHeap = partitions.sizeOffHeap();

            try
            {
                partitions.putSingleton(key, update, updater::mergePartitions, key.getKeyLength() < MAX_RECURSIVE_KEY_LENGTH);
            }
            catch (MemtableTrie.SpaceExhaustedException e)
            {
                // This should never really happen as a flush would be triggered long before this limit is reached.
                throw Throwables.propagate(e);
            }

            allocator.offHeap().adjust(partitions.sizeOffHeap() - offHeap, opGroup);
            allocator.onHeap().adjust(partitions.sizeOnHeap() - onHeap, opGroup);
            updateMin(minTimestamp, update.stats().minTimestamp);
            liveDataSize.addAndGet(updater.dataSize);
            columnsCollector.update(update.columns());
            statsCollector.update(update.stats());
            currentOperations.addAndGet(update.operationCount());
        }

        if (partitions.reachedAllocatedSizeThreshold() && !switchRequested.getAndSet(true))
        {
            logger.info("Scheduling flush due to trie size limit reached.");
            owner.signalFlushRequired(this, ColumnFamilyStore.FlushReason.MEMTABLE_LIMIT);
        }

        return updater.colUpdateTimeDelta;
    }

    public long partitionCount()
    {
        return partitions.valuesCount();
    }

    public MemtableUnfilteredPartitionIterator makePartitionIterator(final ColumnFilter columnFilter, final DataRange dataRange)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        PartitionPosition left = keyRange.left;
        PartitionPosition right = keyRange.right;
        if (left.isMinimum())
            left = null;
        if (right.isMinimum())
            right = null;

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        Trie<BTreePartitionData> subMap = partitions.subtrie(left, includeStart, right, includeStop);

        return new MemtableUnfilteredPartitionIterator(metadata(),
                                                       allocator.ensureOnHeap(),
                                                       subMap,
                                                       columnFilter,
                                                       dataRange);
    }

    public Partition getPartition(DecoratedKey key)
    {
        BTreePartitionData data = partitions.get(key);
        if (data != null)
            return createPartition(metadata(), allocator.ensureOnHeap(), key, data);
        else
            return null;
    }

    private static MemtablePartition createPartition(TableMetadata metadata, EnsureOnHeap ensureOnHeap, DecoratedKey key, BTreePartitionData data)
    {
        return new MemtablePartition(metadata, ensureOnHeap, key, data);
    }

    private static MemtablePartition getPartitionFromTrieEntry(TableMetadata metadata, EnsureOnHeap ensureOnHeap, Map.Entry<ByteComparable, BTreePartitionData> en)
    {
        DecoratedKey key = BufferDecoratedKey.fromByteComparable(en.getKey(),
                                                                 BYTE_COMPARABLE_VERSION,
                                                                 metadata.partitioner);
        return createPartition(metadata, ensureOnHeap, key, en.getValue());
    }


    public FlushCollection<MemtablePartition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Trie<BTreePartitionData> toFlush = partitions.subtrie(from, true, to, false);
        long keySize = 0;
        int keyCount = 0;

        for (Iterator<Map.Entry<ByteComparable, BTreePartitionData>> it = toFlush.entryIterator(); it.hasNext(); )
        {
            Map.Entry<ByteComparable, BTreePartitionData> en = it.next();
            ByteComparable byteComparable = v -> en.getKey().asPeekableBytes(BYTE_COMPARABLE_VERSION);
            byte[] keyBytes = DecoratedKey.keyFromByteComparable(byteComparable, BYTE_COMPARABLE_VERSION, metadata().partitioner);
            keySize += keyBytes.length;
            keyCount++;
        }
        long partitionKeySize = keySize;
        int partitionCount = keyCount;

        return new AbstractFlushCollection<MemtablePartition>()
        {
            public Memtable memtable()
            {
                return TrieMemtable.this;
            }

            public PartitionPosition from()
            {
                return from;
            }

            public PartitionPosition to()
            {
                return to;
            }

            public long partitionCount()
            {
                return partitionCount;
            }

            public Iterator<MemtablePartition> iterator()
            {
                return Iterators.transform(toFlush.entryIterator(),
                                           // TODO: During flushing we shouldn't need to copy partition data on heap because the memtable can't
                                           // disappear until we are done with the flush. Figure out why EnsureOnHeap.NOOP doesn't work.
                                           entry -> getPartitionFromTrieEntry(metadata(), allocator.ensureOnHeap(), entry));
            }

            public long partitionKeySize()
            {
                return partitionKeySize;
            }
        };
    }

    static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator implements Memtable.MemtableUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final EnsureOnHeap ensureOnHeap;
        private final Trie<BTreePartitionData> source;
        private final Iterator<Map.Entry<ByteComparable, BTreePartitionData>> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                                   EnsureOnHeap ensureOnHeap,
                                                   Trie<BTreePartitionData> source,
                                                   ColumnFilter columnFilter,
                                                   DataRange dataRange)
        {
            this.metadata = metadata;
            this.ensureOnHeap = ensureOnHeap;
            this.iter = source.entryIterator();
            this.source = source;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public int getMinLocalDeletionTime()
        {
            int minLocalDeletionTime = Integer.MAX_VALUE;
            for (BTreePartitionData partition : source.values())
                minLocalDeletionTime = Math.min(minLocalDeletionTime, partition.stats.minLocalDeletionTime);

            return minLocalDeletionTime;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            Partition partition = getPartitionFromTrieEntry(metadata(), ensureOnHeap, iter.next());
            DecoratedKey key = partition.partitionKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, partition);
        }
    }

    static class MemtablePartition extends ImmutableBTreePartition
    {

        private final EnsureOnHeap ensureOnHeap;

        private MemtablePartition(TableMetadata table, EnsureOnHeap ensureOnHeap, DecoratedKey key, BTreePartitionData data)
        {
            super(table, key, data);
            this.ensureOnHeap = ensureOnHeap;
        }

        @Override
        protected boolean canHaveShadowedData()
        {
            // The BtreePartitionData we store in the memtable are build iteratively by BTreePartitionData.add(), which
            // doesn't make sure there isn't shadowed data, so we'll need to eliminate any.
            return true;
        }


        @Override
        public DeletionInfo deletionInfo()
        {
            return ensureOnHeap.applyToDeletionInfo(super.deletionInfo());
        }

        @Override
        public Row staticRow()
        {
            return ensureOnHeap.applyToStatic(super.staticRow());
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return ensureOnHeap.applyToPartitionKey(super.partitionKey());
        }

        @Override
        public Row getRow(Clustering<?> clustering)
        {
            return ensureOnHeap.applyToRow(super.getRow(clustering));
        }

        @Override
        public Row lastRow()
        {
            return ensureOnHeap.applyToRow(super.lastRow());
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
        {
            return ensureOnHeap.applyToPartition(super.unfilteredIterator(selection, slices, reversed));
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
        {
            return ensureOnHeap
                            .applyToPartition(super.unfilteredIterator(selection, clusteringsInQueryOrder, reversed));
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator()
        {
            return ensureOnHeap.applyToPartition(super.unfilteredIterator());
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(BTreePartitionData current, ColumnFilter selection, Slices slices, boolean reversed)
        {
            return ensureOnHeap
                            .applyToPartition(super.unfilteredIterator(current, selection, slices, reversed));
        }

        @Override
        public Iterator<Row> iterator()
        {
            return ensureOnHeap.applyToPartition(super.iterator());
        }
    }
}
