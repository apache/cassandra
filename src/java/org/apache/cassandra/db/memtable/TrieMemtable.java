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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;

import org.agrona.concurrent.UnsafeBuffer;
import org.github.jamm.Unmetered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdater;
import org.apache.cassandra.db.partitions.TriePartitionUpdaterLegacyIndex;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.db.tries.ContentManagerPojo;
import org.apache.cassandra.db.tries.ContentSerializer;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.TrieDumperWithPath;
import org.apache.cassandra.db.tries.TrieEntriesWalker;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.db.tries.TrieTailsIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtableBufferAllocator;
import org.apache.cassandra.utils.memory.NativeAllocator;

/// Trie memtable implementation. Improves memory usage, garbage collection efficiency and lookup performance.
/// The implementation is described in detail in [TrieMemtable.md](./TrieMemtable.md) and the paper
/// https://www.vldb.org/pvldb/vol15/p3359-lambov.pdf.
///
/// The configuration takes a single parameter:
/// - shards: the number of shards to split into, defaulting to the number of CPU cores.
///
/// Also see [Memtable_API.md](./Memtable_API.md).
public class TrieMemtable extends AbstractShardedMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemtable.class);

    /// Buffer type to use for memtable tries (on- vs off-heap)
    public static final BufferType BUFFER_TYPE = DatabaseDescriptor.getMemtableAllocationType().toBufferType();

    /// The byte-ordering conversion version to use for memtables.
    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = TrieBackedPartition.BYTE_COMPARABLE_VERSION;

    /// Force copy checker (see [org.apache.cassandra.db.tries.InMemoryTrie#apply]) ensuring all modifications apply
    /// atomically and consistently to the whole partition.
    public static final Predicate<InMemoryBaseTrie.NodeFeatures<Object>> FORCE_COPY_PARTITION_BOUNDARY =
        features -> TrieBackedPartition.isPartitionBoundary(features.content());

    // Set to true when the memtable requests a switch (e.g. for trie size limit being reached) to ensure only one
    // thread calls cfs.switchMemtableIfCurrent.
    private final AtomicBoolean switchRequested = new AtomicBoolean(false);

    /// Sharded memtable sections. Each is responsible for a contiguous range of the token space (between `boundaries[i]`
    /// and `boundaries[i+1]`) and is written to by one thread at a time, while reads are carried out concurrently
    /// (including with any write).
    private final MemtableShard[] shards;

    /// A merged view of the memtable map. Used for partition range queries and flush.
    /// For efficiency, we serve single partition requests off the shard which offers more direct
    /// [org.apache.cassandra.db.tries.InMemoryTrie] methods.
    @VisibleForTesting
    final DeletionAwareTrie<Object, TrieTombstoneMarker> mergedTrie;

    @Unmetered
    private final TrieMemtableMetricsView metrics;

    /// The table metadata at the time of the memtable's creation. Rows in the memtable identify their columns by
    /// their index in the table's column set, thus this memtable can only be used with this exact set of columns, and
    /// must use this metadata to interpret its data. Any change to the columns forces a memtable switch (see
    /// [#shouldSwitch]).
    @Unmetered
    private final TableMetadata actualMetadata;

    TrieMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner, Integer shardCountOption)
    {
        super(commitLogLowerBound, metadataRef, owner, shardCountOption);
        this.metrics = new TrieMemtableMetricsView(metadataRef.keyspace, metadataRef.name);
        this.actualMetadata = metadataRef.get();
        this.shards = generatePartitionShards(boundaries.shardCount(), actualMetadata, allocator, metrics, owner.readOrdering());
        this.mergedTrie = makeMergedTrie(shards);
    }

    private static MemtableShard[] generatePartitionShards(int splits,
                                                           TableMetadata metadata,
                                                           MemtableAllocator allocator,
                                                           TrieMemtableMetricsView metrics,
                                                           OpOrder readOrder)
    {
        MemtableShard[] partitionMapContainer = new MemtableShard[splits];
        for (int i = 0; i < splits; i++)
            partitionMapContainer[i] = new MemtableShard(metadata, allocator, metrics, readOrder);

        return partitionMapContainer;
    }

    private static DeletionAwareTrie<Object, TrieTombstoneMarker> makeMergedTrie(MemtableShard[] shards)
    {
        List<DeletionAwareTrie<Object, TrieTombstoneMarker>> tries = new ArrayList<>(shards.length);
        for (MemtableShard shard : shards)
            tries.add(shard.data);
        return DeletionAwareTrie.mergeDistinct(tries);
    }

    @Override
    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason, TableMetadata latest)
    {
        if (super.shouldSwitch(reason, latest))
            return true;
        if (reason != ColumnFamilyStore.FlushReason.SCHEMA_CHANGE)
            return false;
        // If the columns definition changes, we need to flush as we would need to remap column indexes.
        return !actualMetadata.regularAndStaticColumns().equals(latest.regularAndStaticColumns());
    }

    @Override
    public boolean isClean()
    {
        for (MemtableShard shard : shards)
            if (!shard.isClean())
                return false;
        return true;
    }

    @Override
    public void discard()
    {
        super.discard();
        // metrics here are not thread safe, but I think we can live with that
        metrics.lastFlushShardDataSizes.reset();
        for (MemtableShard shard : shards)
        {
            metrics.lastFlushShardDataSizes.update(shard.liveDataSize());
        }
        // the buffer release is a longer-running process, do it in a separate loop to not make the metrics update wait
        for (MemtableShard shard : shards)
        {
            shard.data.discardBuffers();
        }
    }

    /// For testing only. Overwrite all buffers that this memtable released on discard, to make sure any attempt to
    /// access the data after discard fails.
    @Override
    @VisibleForTesting
    public void overwriteAllData()
    {
        super.overwriteAllData();
        for (MemtableShard shard : shards)
            shard.data.overwriteAllBuffers();
    }

    @Override
    public boolean limitsConcurrentWritesTo(int maxWriters)
    {
        // each shard applies one update at a time under its write lock
        return boundaries.shardCount() <= maxWriters;
    }

    /// Should only be called by [ColumnFamilyStore#apply] via `Keyspace#apply`, which supplies the appropriate
    /// [OpOrder.Group].
    ///
    /// `commitLogSegmentPosition` should only be null if this is a secondary index, in which case it is *expected* to
    /// be null.
    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, boolean assumeMissing)
    {
        RegularAndStaticColumns updateColumns = update.metadata().regularAndStaticColumns();
        RegularAndStaticColumns memtableColumns = actualMetadata.regularAndStaticColumns();
        if (updateColumns != memtableColumns && !updateColumns.equals(memtableColumns))
            update = withMemtableColumns(update);

        DecoratedKey key = update.partitionKey();
        MemtableShard shard = shards[boundaries.getShardForKey(key)];
        long colUpdateTimeDelta = shard.put(update, indexer, opGroup);

        if (shard.data.reachedAllocatedSizeThreshold() && !switchRequested.getAndSet(true))
        {
            logger.info("Scheduling flush due to trie size limit reached.");
            owner.signalFlushRequired(this, ColumnFamilyStore.FlushReason.MEMTABLE_LIMIT);
        }

        return colUpdateTimeDelta;
    }

    /// Rows in the memtable identify their columns by index in the memtable's column set, and encode cell paths using
    /// the memtable's column types. If an update was prepared using a different version of the table metadata (e.g. by
    /// a write that raced with a schema change), convert it to use the memtable's columns.
    ///
    /// Data for columns that have been dropped is discarded. If the update contains a column that the memtable does
    /// not know about, the write fails.
    private PartitionUpdate withMemtableColumns(PartitionUpdate update)
    {
        RegularAndStaticColumns.Builder columnsBuilder = RegularAndStaticColumns.builder();
        for (ColumnMetadata column : update.columns())
        {
            ColumnMetadata memtableColumn = memtableColumn(column);
            if (memtableColumn != null)
                columnsBuilder.add(memtableColumn);
        }
        RegularAndStaticColumns columns = columnsBuilder.build();

        Transformation<UnfilteredRowIterator> convertRows = new Transformation<>()
        {
            @Override
            protected Row applyToStatic(Row row)
            {
                Row converted = withMemtableColumns(row);
                return converted.isEmpty() ? Rows.EMPTY_STATIC_ROW : converted;
            }

            @Override
            protected Row applyToRow(Row row)
            {
                Row converted = withMemtableColumns(row);
                return converted.isEmpty() ? null : converted;
            }
        };

        UnfilteredRowIterator converted = Transformation.apply(update.unfilteredIterator(), convertRows);
        // Note: closing this closes the wrapped iterator.
        try (UnfilteredRowIterator withMetadata = new WrappingUnfilteredRowIterator()
             {
                 @Override
                 public UnfilteredRowIterator wrapped()
                 {
                     return converted;
                 }

                 @Override
                 public TableMetadata metadata()
                 {
                     return actualMetadata;
                 }

                 @Override
                 public RegularAndStaticColumns columns()
                 {
                     return columns;
                 }
             })
        {
            return TriePartitionUpdate.fromIterator(withMetadata);
        }
    }

    private Row withMemtableColumns(Row row)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(row.clustering());
        builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
        builder.addRowDeletion(row.deletion());
        for (ColumnData cd : row)
        {
            ColumnMetadata column = memtableColumn(cd.column());
            if (column == null)
                continue;   // dropped column
            if (column.isSimple())
                builder.addCell(((Cell<?>) cd).withUpdatedColumn(column));
            else
            {
                ComplexColumnData ccd = (ComplexColumnData) cd;
                if (!ccd.complexDeletion().isLive())
                    builder.addComplexDeletion(column, ccd.complexDeletion());
                for (Cell<?> cell : ccd)
                    builder.addCell(cell.withUpdatedColumn(column));
            }
        }
        return builder.build();
    }

    private ColumnMetadata memtableColumn(ColumnMetadata column)
    {
        ColumnMetadata memtableColumn = actualMetadata.getColumn(column.name);
        if (memtableColumn != null && memtableColumn.isStatic() == column.isStatic())
            return memtableColumn;
        if (actualMetadata.droppedColumns.containsKey(column.name.bytes))
            return null;
        throw new IllegalStateException(String.format("Column %s of update is not present in the metadata of memtable %s",
                                                      column, this));
    }

    /// Technically we should scatter gather on all the core threads because the size in following calls are not
    /// using volatile variables, but for metrics purpose this should be good enough.
    @Override
    public long getLiveDataSize()
    {
        long total = 0L;
        for (MemtableShard shard : shards)
            total += shard.liveDataSize();
        return total;
    }

    @Override
    public long operationCount()
    {
        long total = 0L;
        for (MemtableShard shard : shards)
            total += shard.currentOperations();
        return total;
    }

    @Override
    public long partitionCount()
    {
        int total = 0;
        for (MemtableShard shard : shards)
            total += shard.partitionCount();
        return total;
    }

    public long partitionKeysTotalSize()
    {
        long total = 0;
        for (MemtableShard shard : shards)
            total += shard.partitionKeysSize();
        return total;
    }

    /// Returns the minimum timestamp if one available, otherwise `NO_MIN_TIMESTAMP`.
    /// [EncodingStats] uses a synthetic epoch TS at 2015. We don't want to leak that (CASSANDRA-18118) so we return
    /// `NO_MIN_TIMESTAMP` instead.
    ///
    /// @return The minTS or `NO_MIN_TIMESTAMP` if none available
    @Override
    public long getMinTimestamp()
    {
        long min = Long.MAX_VALUE;
        for (MemtableShard shard : shards)
            min =  Long.min(min, shard.minTimestamp());
        return min != EncodingStats.NO_STATS.minTimestamp ? min : NO_MIN_TIMESTAMP;
    }

    @Override
    public long getMinLocalDeletionTime()
    {
        long min = Long.MAX_VALUE;
        for (MemtableShard shard : shards)
            min =  Long.min(min, shard.minLocalDeletionTime());
        return min;
    }

    @Override
    RegularAndStaticColumns columns()
    {
        for (MemtableShard shard : shards)
            columnsCollector.update(shard.columns);
        return columnsCollector.get();
    }

    @Override
    EncodingStats encodingStats()
    {
        for (MemtableShard shard : shards)
            statsCollector.update(shard.stats);
        return statsCollector.get();
    }

    @Override
    public UnfilteredPartitionIterator partitionIterator(final ColumnFilter columnFilter,
                                                         final DataRange dataRange,
                                                         SSTableReadsListener readsListener)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        DeletionAwareTrie<Object, TrieTombstoneMarker> subMap =
            mergedTrie.subtrie(toComparableBound(keyRange.left, includeStart),
                               toComparableBound(keyRange.right, !includeStop));

        return new MemtableUnfilteredPartitionIterator(actualMetadata,
                                                       metadata.get(),
                                                       true, // even if our data is on-heap, we can recycle
                                                             // cells as soon as the opOrder is closed
                                                       subMap,
                                                       columnFilter,
                                                       dataRange);
        // readsListener is ignored as it only accepts sstable signals
    }

    private static ByteComparable toComparableBound(PartitionPosition position, boolean before)
    {
        return position == null || position.isMinimum() ? null : position.asComparableBound(before);
    }

    private Partition getPartition(DecoratedKey key)
    {
        int shardIndex = boundaries.getShardForKey(key);
        DeletionAwareTrie<Object, TrieTombstoneMarker> trie = shards[shardIndex].data.tailTrie(key);
        // Always copy to heap. Even if our data is on-heap, we can recycle cells as soon as the opOrder is closed.
        return createPartition(actualMetadata, metadata.get(), true, key, trie);
    }

    private static TrieBackedPartition createPartition(TableMetadata metadata, TableMetadata droppedColumnsSource, boolean copyToHeap, DecoratedKey key, DeletionAwareTrie<Object, TrieTombstoneMarker> trie)
    {
        if (trie == null)
            return null;
        PartitionData holder = (PartitionData) trie.get(ByteComparable.EMPTY);
        // If we found a matching path in the trie, it must be the root of this partition (because partition keys are
        // prefix-free, it can't be a prefix for a different path, or have another partition key as prefix) and contain
        // PartitionData (because the attachment of a new or modified partition to the trie is atomic).
        assert holder != null : "Entry for " + key + " without associated PartitionData";

        return TrieBackedPartition.create(key,
                                          holder.columns(),
                                          holder.stats(),
                                          holder.rowCountIncludingStatic(),
                                          holder.tombstoneCount(),
                                          trie,
                                          metadata,
                                          droppedColumnsSource,
                                          copyToHeap);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        Partition p = getPartition(key);
        if (p == null)
            return null;
        else
            return p.unfilteredIterator(selectedColumns, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        Partition p = getPartition(key);
        return p != null ? p.unfilteredIterator() : null;
    }

    private static DecoratedKey getPartitionKeyFromPath(TableMetadata metadata, ByteComparable path)
    {
        return BufferDecoratedKey.fromByteComparable(path,
                                                     BYTE_COMPARABLE_VERSION,
                                                     metadata.partitioner);
    }

    /// Make a textual representation of the trie, linking content with its type and key. See
    /// [TrieMemtable.md](TrieMemtable.md) for an example of the output.
    public String dump()
    {
        return mergedTrie.process(Direction.FORWARD, new Dumper(actualMetadata));
    }

    static final int PARTITIONDATA_OFFSET_ROW_COUNT = 0;
    static final int PARTITIONDATA_OFFSET_TOMBSTONE_COUNT = 4;

    /// Metadata object signifying the root node of a partition. Stores row and tombstone counts in a trie cell,
    /// as well as a link to the owning subrange, which is used for compiling encoding statistics and column sets.
    ///
    /// Descends from [TrieBackedPartition.PartitionMarker] to permit tail tries to be passed directly to
    /// [TrieBackedPartition].
    public static class PartitionData implements TrieBackedPartition.PartitionMarker
    {
        public final MemtableShard owner;

        UnsafeBuffer buffer;
        int inBufferPos;

        public PartitionData(MemtableShard owner)
        {
            this(owner, null, 0);
        }

        public PartitionData(MemtableShard owner, UnsafeBuffer buffer, int inBufferPos)
        {
            this.owner = owner;
            this.buffer = buffer;
            this.inBufferPos = inBufferPos;
        }

        public RegularAndStaticColumns columns()
        {
            return owner.columns;
        }

        public EncodingStats stats()
        {
            return owner.stats;
        }

        public int rowCountIncludingStatic()
        {
            return buffer.getInt(inBufferPos + PARTITIONDATA_OFFSET_ROW_COUNT);
        }

        public int tombstoneCount()
        {
            return buffer.getInt(inBufferPos + PARTITIONDATA_OFFSET_TOMBSTONE_COUNT);
        }

        public void markInsertedRows(int howMany)
        {
            buffer.addIntOrdered(inBufferPos + PARTITIONDATA_OFFSET_ROW_COUNT, howMany);
        }

        public void markAddedTombstones(int howMany)
        {
            buffer.addIntOrdered(inBufferPos + PARTITIONDATA_OFFSET_TOMBSTONE_COUNT, howMany);
        }

        @Override
        public String toString()
        {
            return String.format("partition with %d rows and %d tombstones", rowCountIncludingStatic(), tombstoneCount());
        }

        public long unsharedHeapSize()
        {
            return 0;
        }
    }

    /// Walker collecting the number and total size of the keys of the partitions in a trie.
    /// Must be used with [DeletionAwareTrie#processSkippingBranches] to only see the partition roots.
    class KeySizeAndCountCollector extends TrieEntriesWalker<Object, Void>
    {
        long keySize = 0;
        int keyCount = 0;

        @Override
        public Void complete()
        {
            return null;
        }

        @Override
        protected void content(Object content, byte[] bytes, int byteLength)
        {
            // This is used with processSkippingBranches which should ensure that we only see the partition roots.
            assert content instanceof PartitionData;
            ++keyCount;
            byte[] keyBytes = DecoratedKey.keyFromByteSource(ByteSource.preencoded(bytes, 0, byteLength),
                                                             BYTE_COMPARABLE_VERSION,
                                                             metadata().partitioner);
            keySize += keyBytes.length;
        }
    }

    @Override
    public FlushablePartitionSet<TrieBackedPartition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        boolean allPositionsToFlush = from == null && to == null
                                      || from != null && to != null
                                         && from.isMinimum()
                                         && to.isMaximum();

        // to avoid an unnecessary wrapping of mergedTrie into an intersection
        DeletionAwareTrie<Object, TrieTombstoneMarker> toFlush = allPositionsToFlush ? mergedTrie :
                                                                 mergedTrie.subtrie(toComparableBound(from, true), toComparableBound(to, true));

        /*
         In total, we have boundaries.shardCount() shards, with indexes from 0 to boundaries.shardCount() - 1
         We start with 0 or 1 partial shards, then we may have several full shards, and we finish with 0 or 1 partial shards
         (token_m .......... token_m+1](token_m+1 ...  ... ...  ... token_n-1](token_n ........... token_n+1]
                   ↑                  ↑                                      ↑                   ↑
                   [---partial shard--|---full shard---  ... ---full shard---|---partial shard---)

         */
        int fromShardFull;
        int fromShardPartial;
        int toShardFull;
        int toShardPartial;

        if (from == null || from.isMinimum())
            fromShardFull = fromShardPartial = 0;
        else
        {
            fromShardPartial = boundaries.getShardForToken(from.getToken());
            // not symmetric to "to" logic because the start part of shard ranges is exclusive
            Token fromEndBoundaryToken = boundaries.getShardEndBoundary(fromShardPartial);
            if (fromEndBoundaryToken != null && from.equals(fromEndBoundaryToken.maxKeyBound()))
            {
                fromShardPartial++;
                fromShardFull = fromShardPartial;
            }
            else
                fromShardFull = fromShardPartial + 1;
        }

        if  (to == null || to.isMaximum())
            toShardFull = toShardPartial = boundaries.shardCount() - 1;
        else
        {
            toShardPartial = boundaries.getShardForToken(to.getToken());
            Token toEndBoundaryToken = boundaries.getShardEndBoundary(toShardPartial);
            if (toEndBoundaryToken != null && to.equals(toEndBoundaryToken.maxKeyBound()))
                toShardFull = toShardPartial;
            else
                toShardFull = toShardPartial - 1;
        }

        List<DeletionAwareTrie<Object, TrieTombstoneMarker>> partialShardTries = new ArrayList<>(2);
        boolean fromShardAdded = false;
        if (fromShardPartial != fromShardFull)
        {
            partialShardTries.add(shards[fromShardPartial].data);
            fromShardAdded = true;
        }
        if (toShardPartial != toShardFull && (toShardPartial != fromShardPartial || !fromShardAdded) )
        {
            partialShardTries.add(shards[toShardPartial].data);
        }

        KeySizeAndCountCollector counter = new KeySizeAndCountCollector();
        if (!partialShardTries.isEmpty())
        {
            // need to jump over tails keys
            DeletionAwareTrie.mergeDistinct(partialShardTries)
                             .subtrie(toComparableBound(from, true), toComparableBound(to, true))
                             .processSkippingBranches(Direction.FORWARD, counter);
        }

        long keySize = counter.keySize;
        int keyCount = counter.keyCount;
        for (int i = fromShardFull; i <= toShardFull; i++)
        {
            keySize += shards[i].partitionKeysSize();
            keyCount += shards[i].partitionCount();
        }

        long partitionKeySize = keySize;
        long partitionCount = keyCount;

        return new AbstractFlushablePartitionSet<TrieBackedPartition>()
        {
            private final TableMetadata tableMetadata = TrieMemtable.this.metadata();

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

            public Iterator<TrieBackedPartition> iterator()
            {
                // During flushing we are certain the memtable will remain at least until the flush completes and
                // that no further modifications to it will happen. No copying to heap is necessary.
                return new PartitionIterator(toFlush, actualMetadata, metadata.get(), false);
            }

            public long partitionKeysSize()
            {
                return partitionKeySize;
            }

            @Override
            public TableMetadata metadata()
            {
                return tableMetadata;
            }
        };
    }

    public static class MemtableShard
    {
        // The following fields are volatile as we have to make sure that when we
        // collect results from all sub-ranges, the thread accessing the value
        // is guaranteed to see the changes to the values.

        // The smallest timestamp for all partitions stored in this shard
        private volatile long minTimestamp = Long.MAX_VALUE;

        private volatile long minLocalDeletionTime = Long.MAX_VALUE;

        private volatile long liveDataSize = 0;

        private volatile long currentOperations = 0;

        private volatile long partitionKeysSize = 0;

        private volatile int partitionCount = 0;

        @Unmetered
        private final ReentrantLock writeLock = new ReentrantLock();

        /// Content map for the given shard. This is implemented as an in-memory trie which uses the prefix-free
        /// byte-comparable [ByteSource] representations of keys to address partitions and individual rows within
        /// partitions.
        ///
        /// This map is used in a single-producer, multi-consumer fashion: only one thread will insert items but
        /// several threads may read from it and iterate over it. Iterators (especially partition range iterators)
        /// may operate for a long period of time and thus iterators should not throw `ConcurrentModificationException`s
        /// if the underlying map is modified during iteration, they should provide a weakly consistent view of the map
        /// instead.
        ///
        /// Also, this data is backed by memtable memory. When accessing it callers must specify if it can be accessed
        /// unsafely, meaning that the memtable will not be discarded as long as the data is used, or whether the data
        /// should be copied on heap for off-heap allocators.
        @VisibleForTesting
        final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data;

        volatile RegularAndStaticColumns columns;

        volatile EncodingStats stats;

        @Unmetered  // total pool size should not be included in memtable's deep size
        private final MemtableAllocator allocator;

        private final CellDataBufferManager cellDataBufferManager;

        @Unmetered
        private final TrieMemtableMetricsView metrics;

        private final TableMetadata metadata;

        private TriePartitionUpdater noIndexUpdater;
        private TriePartitionUpdaterLegacyIndex legacyIndexUpdater;

        @VisibleForTesting
        MemtableShard(TableMetadata metadata, MemtableAllocator allocator, TrieMemtableMetricsView metrics, OpOrder readOrder)
        {
            this.metadata = metadata;
            this.allocator = allocator;
            if (this.allocator instanceof NativeAllocator)
                this.cellDataBufferManager = new NativeBufferManager((NativeAllocator) allocator);
            else
                this.cellDataBufferManager = new SlabBufferManager((MemtableBufferAllocator) allocator,
                                                                   readOrder,
                                                                   BUFFER_TYPE.onHeapSizeWithoutData());

            this.data = InMemoryDeletionAwareTrie.longLived(BYTE_COMPARABLE_VERSION, BUFFER_TYPE, readOrder,
                                                            new TrieSerializer(cellDataBufferManager, this));
            this.columns = RegularAndStaticColumns.NONE;
            this.stats = EncodingStats.NO_STATS;
            this.metrics = metrics;
        }

        private TriePartitionUpdater getAndConfigureUpdater(UpdateTransaction indexer)
        {
            if (indexer == UpdateTransaction.NO_OP)
            {
                if (noIndexUpdater == null)
                    noIndexUpdater = new TriePartitionUpdater(this, data);
                return noIndexUpdater;
            }
            else
            {
                if (legacyIndexUpdater == null)
                    legacyIndexUpdater = new TriePartitionUpdaterLegacyIndex(this, data, metadata);
                legacyIndexUpdater.setIndexContext(indexer);
                return legacyIndexUpdater;
            }
        }

        public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            // If the update is not TriePartitionUpdate, convert it to one before taking the lock.
            DeletionAwareTrie<Object, TrieTombstoneMarker> mergableTrie = TriePartitionUpdate.asMergableTrie(update);

            boolean locked = writeLock.tryLock();
            if (locked)
            {
                metrics.uncontendedPuts.inc();
            }
            else
            {
                metrics.contendedPuts.inc();
                long lockStartTime = Clock.Global.nanoTime();
                writeLock.lock();
                metrics.contentionTime.addNano(Clock.Global.nanoTime() - lockStartTime);
            }

            TriePartitionUpdater updater = getAndConfigureUpdater(indexer);
            try
            {
                try
                {
                    this.cellDataBufferManager.opOrderGroup = opGroup;
                    int partitionsAdded = mergeUpdate(data,
                                                      allocator,
                                                      mergableTrie,
                                                      indexer,
                                                      updater);
                    if (partitionsAdded > 0)
                    {
                        partitionCount += partitionsAdded;
                        partitionKeysSize += update.partitionKey().getKeyLength();
                    }
                }
                finally
                {
                    indexer.commit();
                    minTimestamp = Math.min(minTimestamp, update.stats().minTimestamp);
                    minLocalDeletionTime = Math.min(minLocalDeletionTime, update.stats().minLocalDeletionTime);
                    liveDataSize += updater.dataSize;
                    currentOperations += update.operationCount();

                    columns = columns.mergeTo(update.columns());
                    stats = stats.mergeWith(update.stats());
                }
            }
            finally
            {
                writeLock.unlock();
            }
            return updater.colUpdateTimeDelta;
        }

        public boolean isClean()
        {
            return data.isEmpty();
        }

        public int partitionCount()
        {
            return partitionCount;
        }

        long minTimestamp()
        {
            return minTimestamp;
        }

        long liveDataSize()
        {
            return liveDataSize;
        }

        long currentOperations()
        {
            return currentOperations;
        }

        long partitionKeysSize()
        {
            return partitionKeysSize;
        }

        long minLocalDeletionTime()
        {
            return minLocalDeletionTime;
        }
    }

    /// Merge an update into the given data trie using the given helpers. Extracted to separate method for testing.
    static int mergeUpdate(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> dataTrie,
                           MemtableAllocator allocator,
                           DeletionAwareTrie<Object, TrieTombstoneMarker> updateTrie,
                           UpdateTransaction indexer,
                           TriePartitionUpdater updater)
    {
        indexer.start();
        // Add the initial trie size on the first operation. This technically isn't correct (other shards
        // do take their memory share even if they are empty) but doing it during construction may cause
        // the allocator to block while we are trying to flush a memtable and become a deadlock.
        long onHeap = dataTrie.isEmpty() ? 0 : dataTrie.usedSizeOnHeap();
        long offHeap = dataTrie.isEmpty() ? 0 : dataTrie.usedSizeOffHeap();
        try
        {
            updater.mergeUpdate(updateTrie);
        }
        catch (TrieSpaceExhaustedException e)
        {
            // This should never happen as {@link InMemoryTrie#reachedAllocatedSizeThreshold} should become
            // true and trigger a memtable switch long before this limit is reached.
            throw new IllegalStateException(e);
        }
        finally
        {
            allocator.offHeap().adjust(dataTrie.usedSizeOffHeap() - offHeap);
            allocator.onHeap().adjust(dataTrie.usedSizeOnHeap() - onHeap);
        }
        return updater.partitionsAdded;
    }

    /// Iterator over partitions of the given trie. Looks for partition markers and presents the branch of each
    /// partition marker as a [TrieBackedPartition].
    static class PartitionIterator extends TrieTailsIterator.DeletionAwareWithoutCoveringDeletions<Object, TrieTombstoneMarker, TrieBackedPartition>
    {
        final TableMetadata metadata;
        final TableMetadata droppedColumnsSource;
        final boolean copyToHeap;

        PartitionIterator(DeletionAwareTrie<Object, TrieTombstoneMarker> source, TableMetadata metadata, TableMetadata droppedColumnsSource, boolean copyToHeap)
        {
            super(source, Direction.FORWARD, TrieBackedPartition.IS_PARTITION_BOUNDARY);
            this.metadata = metadata;
            this.droppedColumnsSource = droppedColumnsSource;
            this.copyToHeap = copyToHeap;
        }

        @Override
        protected TrieBackedPartition mapContent(Object content, DeletionAwareTrie<Object, TrieTombstoneMarker> tailTrie, byte[] bytes, int byteLength)
        {
            PartitionData pd = (PartitionData) content;
            DecoratedKey key = getPartitionKeyFromPath(metadata,
                                                       ByteComparable.preencoded(BYTE_COMPARABLE_VERSION,
                                                                                 bytes, 0, byteLength));
            return TrieBackedPartition.create(key,
                                              pd.columns(),
                                              pd.stats(),
                                              pd.rowCountIncludingStatic(),
                                              pd.tombstoneCount(),
                                              tailTrie,
                                              metadata,
                                              droppedColumnsSource,
                                              copyToHeap);
        }
    }

    /// The implementation of [UnfilteredPartitionIterator] used to walk partition ranges.
    static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator implements UnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<TrieBackedPartition> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                                   TableMetadata droppedColumnsSource,
                                                   boolean copyToHeap,
                                                   DeletionAwareTrie<Object, TrieTombstoneMarker> source,
                                                   ColumnFilter columnFilter,
                                                   DataRange dataRange)
        {
            this.iter = new PartitionIterator(source, metadata, droppedColumnsSource, copyToHeap);
            this.metadata = metadata;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
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
            Partition partition = iter.next();
            DecoratedKey key = partition.partitionKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, partition);
        }
    }

    public static Factory factory(Map<String, String> optionsCopy)
    {
        String shardsString = optionsCopy.remove(SHARDS_OPTION);
        Integer shardCount = shardsString != null ? Integer.parseInt(shardsString) : null;
        return new Factory(shardCount);
    }

    static class Factory implements Memtable.Factory
    {
        final Integer shardCount;

        Factory(Integer shardCount)
        {
            this.shardCount = shardCount;
        }

        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadaRef,
                               Owner owner)
        {
            return new TrieMemtable(commitLogLowerBound, metadaRef, owner, shardCount);
        }

        @Override
        public Runnable createMemtableMetricsReleaser(TableMetadataRef metadataRef)
        {
            // Metrics are the same for all shards, so we can release them all at once.
            return () -> TrieMemtableMetricsView.release(metadataRef.keyspace, metadataRef.name);
        }

        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Factory factory = (Factory) o;
            return Objects.equals(shardCount, factory.shardCount);
        }

        public int hashCode()
        {
            return Objects.hash(shardCount);
        }
    }

    @VisibleForTesting
    public long unusedReservedMemory()
    {
        long size = 0;
        for (MemtableShard shard : shards)
        {
            size += shard.data.unusedReservedOnHeapMemory();
            size += shard.cellDataBufferManager.unusedReservedOnHeapMemory();
        }
        return size;
    }

    /// Release all recycled content references, including the ones waiting in still incomplete recycling lists.
    /// This is a test method and can cause null pointer exceptions if used on a live trie.
    @VisibleForTesting
    void releaseReferencesUnsafe()
    {
        for (MemtableShard shard : shards)
            shard.data.releaseReferencesUnsafe();
    }

    /// Trie serializer, used for mapping trie data cells to and from the various database objects.
    static class TrieSerializer implements ContentSerializer<Object>
    {
        final CellDataBufferManager manager;
        final MemtableShard owner;

        // Singletons mapped to special trie values (i.e. negative trie pointers)

        /// [TrieBackedRow#COMPLEX_COLUMN_MARKER]
        static final int COMPLEX_COLUMN_ID = 0;
        /// [LivenessInfo#EMPTY]
        static final int EMPTY_LIVENESS_ID = 1;
        /// [TrieTombstoneMarker.LevelMarker#ROW] on the left.
        static final int TOMBSTONE_ROW_MARKER_BEFORE_BRANCH = 2;
        /// [TrieTombstoneMarker.LevelMarker#ROW] on the right.
        static final int TOMBSTONE_ROW_MARKER_AFTER_BRANCH = 3;

        // Offset values

        // Offsets 0 to 24 are cells where the length is given in the offset. Lengths above 16 mean cell has no TTL.

        /// Counter cell whose length is stored in byte 15.
        static final byte TYPE_CELL_COUNTER = 0x19;

        /// Cell whose value does not fit and is stored externally.
        static final byte TYPE_CELL_EXTERNAL_VALUE = 0x1A;

        /// Content is [LivenessInfo]
        static final byte TYPE_LIVENESS_INFO = 0x1B;
        /// Content is [TrieTombstoneMarker], to be presented _before_ branch
        static final byte TYPE_TOMBSTONE_MARKER_BEFORE_BRANCH = 0x1C;
        /// Content is [TrieTombstoneMarker], to be presented _after_ branch
        static final byte TYPE_TOMBSTONE_MARKER_AFTER_BRANCH = 0x1D;
        /// Content is [PartitionData]
        static final byte TYPE_PARTITION_DATA = 0x1E;
        // 0x1F for OFFSET_SPECIAL

        // Tombstone flags

        // The three values below match the ones in [TrieCellData], but they don't necessarily have to
        // (the equivalence is only used in [#dumpContent]).
        static final int OFFSET_TIMESTAMP = 0x18;
        static final int OFFSET_LOCAL_DELETION_TIME = 0x14;
        static final int OFFSET_TTL = 0x10;
        static final int OFFSET_TOMBSTONE_KIND = 0x13;

        /// Offset to add to the above for the left side of a tombstone marker
        static final int OFFSET_TOMBSTONE_LEFT = -0x00;
        /// Offset to add to the above for the right side of a tombstone marker
        static final int OFFSET_TOMBSTONE_RIGHT = -0x10;

        /// Byte that stores whether the tombstone marker is also a row marker
        static final int OFFSET_TOMBSTONE_IS_ROW_MARKER = 0x00;

        @VisibleForTesting
        public TrieSerializer(CellDataBufferManager manager, MemtableShard owner)
        {
            this.manager = manager;
            this.owner = owner;
        }

        @Override
        public int idIfSpecial(Object content, boolean shouldPresentAfterBranch)
        {
            if (content == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            {
                assert !shouldPresentAfterBranch;
                return COMPLEX_COLUMN_ID;
            }
            if (content == LivenessInfo.EMPTY || content instanceof LivenessInfo && LivenessInfo.EMPTY.equals(content))
            {
                assert !shouldPresentAfterBranch;
                return EMPTY_LIVENESS_ID;
            }
            if (content == TrieTombstoneMarker.LevelMarker.ROW)
                return shouldPresentAfterBranch ? TOMBSTONE_ROW_MARKER_AFTER_BRANCH : TOMBSTONE_ROW_MARKER_BEFORE_BRANCH;

            // Everything else takes a trie cell.
            return -1;
        }

        @Override
        public Object special(int id)
        {
            switch (id)
            {
                case COMPLEX_COLUMN_ID:
                    return TrieBackedRow.COMPLEX_COLUMN_MARKER;
                case EMPTY_LIVENESS_ID:
                    return LivenessInfo.EMPTY;
                case TOMBSTONE_ROW_MARKER_BEFORE_BRANCH:
                case TOMBSTONE_ROW_MARKER_AFTER_BRANCH:
                    return TrieTombstoneMarker.LevelMarker.ROW;
                default:
                    throw new AssertionError("Unknown special ID " + id);
            }
        }

        @Override
        public boolean shouldPresentSpecialAfterBranch(int id)
        {
            return id == TOMBSTONE_ROW_MARKER_AFTER_BRANCH;
        }

        @Override
        public boolean shouldPreserveSpecialWithoutChildren(int id)
        {
            // All our specials are level markers that should not survive if the branch becomes empty.
            return false;
        }

        @Override
        public boolean shouldPreserveWithoutChildren(int offset)
        {
            // Row markers that fall under a deletion turn into x->x markers with level id. If there is no
            // substructure (e.g. a complex column deletion), these should disappear.
            return offset != TYPE_TOMBSTONE_MARKER_BEFORE_BRANCH && offset != TYPE_TOMBSTONE_MARKER_AFTER_BRANCH;
        }

        @Override
        public boolean shouldPreserveWithoutChildren(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            if (buffer.getByte(inBufferPos + OFFSET_TOMBSTONE_IS_ROW_MARKER) == 0)
                return true;
            if (buffer.getLong(inBufferPos + OFFSET_TOMBSTONE_LEFT + OFFSET_TIMESTAMP) !=
                buffer.getLong(inBufferPos + OFFSET_TOMBSTONE_RIGHT + OFFSET_TIMESTAMP))
                return true;
            if (buffer.getInt(inBufferPos + OFFSET_TOMBSTONE_LEFT + OFFSET_LOCAL_DELETION_TIME) !=
                buffer.getInt(inBufferPos + OFFSET_TOMBSTONE_RIGHT + OFFSET_LOCAL_DELETION_TIME))
                return true;
            if (buffer.getByte(inBufferPos + OFFSET_TOMBSTONE_LEFT + OFFSET_TOMBSTONE_KIND) !=
                buffer.getByte(inBufferPos + OFFSET_TOMBSTONE_RIGHT + OFFSET_TOMBSTONE_KIND))
                return true;
            return false;
        }

        @Override
        public int serialize(Object content, boolean shouldPresentAfterBranch, UnsafeBuffer buffer, int inBufferPos)
        throws TrieSpaceExhaustedException
        {
            assert !shouldPresentAfterBranch || content instanceof TrieTombstoneMarker;
            // most common first
            if (content instanceof CellData)
                return TrieCellData.serialize((CellData<?, ?>) content, buffer, inBufferPos, manager);
            else if (content instanceof LivenessInfo)
                return serializeLivenessInfo((LivenessInfo) content, buffer, inBufferPos);
            else if (content instanceof TrieTombstoneMarker)
                return serializeTombstoneMarker((TrieTombstoneMarker) content, shouldPresentAfterBranch, buffer, inBufferPos);
            else if (content instanceof PartitionData)
                return serializePartitionData((PartitionData) content, buffer, inBufferPos);
            else
                throw new AssertionError("Unknown trie content type: " + content);
        }

        private int serializeLivenessInfo(LivenessInfo livenessInfo, UnsafeBuffer buffer, int inBufferPos)
        {
            buffer.putLongOrdered(inBufferPos + OFFSET_TIMESTAMP, livenessInfo.timestamp());
            buffer.putIntOrdered(inBufferPos + OFFSET_LOCAL_DELETION_TIME, CellData.deletionTimeLongToUnsignedInteger(livenessInfo.localExpirationTime()));
            buffer.putIntOrdered(inBufferPos + OFFSET_TTL, livenessInfo.ttl());
            return TYPE_LIVENESS_INFO;
        }

        private int serializeTombstoneMarker(TrieTombstoneMarker marker, boolean shouldPresentAfterBranch, UnsafeBuffer buffer, int inBufferPos)
        {
            assert marker.isBoundary();
            TrieTombstoneMarker.Covering left = marker.leftDeletion();
            TrieTombstoneMarker.Covering right = marker.rightDeletion();
            serializeTombstoneSide(buffer, inBufferPos + OFFSET_TOMBSTONE_LEFT, left);
            serializeTombstoneSide(buffer, inBufferPos + OFFSET_TOMBSTONE_RIGHT, right);
            buffer.putByte(inBufferPos + OFFSET_TOMBSTONE_IS_ROW_MARKER, (byte) (marker.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW) ? 1 : 0));
            return shouldPresentAfterBranch ? TYPE_TOMBSTONE_MARKER_AFTER_BRANCH : TYPE_TOMBSTONE_MARKER_BEFORE_BRANCH;
        }

        private void serializeTombstoneSide(UnsafeBuffer buffer, int inBufferPos, TrieTombstoneMarker.Covering markerSide)
        {
            if (markerSide != null)
            {
                buffer.putLongOrdered(inBufferPos + OFFSET_TIMESTAMP, markerSide.markedForDeleteAt());
                buffer.putIntOrdered(inBufferPos + OFFSET_LOCAL_DELETION_TIME, CellData.deletionTimeLongToUnsignedInteger(markerSide.localDeletionTime()));
                buffer.putByte(inBufferPos + OFFSET_TOMBSTONE_KIND, (byte) markerSide.deletionKind().ordinal());
            }
            else
            {
                buffer.putByte(inBufferPos + OFFSET_TOMBSTONE_KIND, (byte) -1);
            }
        }

        private int serializePartitionData(PartitionData partitionData, UnsafeBuffer buffer, int inBufferPos)
        {
            if (partitionData.buffer == null)
            {
                // We are creating a new partition. Link this buffer/inBufferPos with the argument, so that we can add
                // statistics as we descend into the partition.
                partitionData.buffer = buffer;
                partitionData.inBufferPos = inBufferPos;
                // we don't need to set anything else as the buffer is filled with 0s when allocated
            }
            else
            {
                // We are making a copy of another PartitionData object.
                buffer.putIntOrdered(inBufferPos + PARTITIONDATA_OFFSET_ROW_COUNT, partitionData.rowCountIncludingStatic());
                buffer.putIntOrdered(inBufferPos + PARTITIONDATA_OFFSET_TOMBSTONE_COUNT, partitionData.tombstoneCount());
            }
            return TYPE_PARTITION_DATA;
        }

        @Override
        public int updateInPlace(UnsafeBuffer buffer, int inBufferPos, int offsetBits, Object newContent) throws TrieSpaceExhaustedException
        {
            // We can always set in place, but we may need to release previously held buffer.
            if (releaseNeeded(offsetBits))
                release(buffer, inBufferPos, offsetBits);

            return serialize(newContent, shouldPresentAfterBranch(offsetBits), buffer, inBufferPos);
        }

        @Override
        public void releaseSpecial(int id)
        {
            // nothing to do, our specials are fixed
        }

        @Override
        public Object deserialize(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            switch (offsetBits)
            {
                case TYPE_CELL_COUNTER:
                    return new TrieCellData.Counter(buffer, inBufferPos);
                case TYPE_CELL_EXTERNAL_VALUE:
                    return new TrieCellData.External(buffer, inBufferPos, manager);
                case TYPE_LIVENESS_INFO:
                    return deserializeLivenessInfo(buffer, inBufferPos);
                case TYPE_TOMBSTONE_MARKER_BEFORE_BRANCH:
                case TYPE_TOMBSTONE_MARKER_AFTER_BRANCH:
                    return deserializeTombstoneMarker(buffer, inBufferPos);
                case TYPE_PARTITION_DATA:
                    return new PartitionData(owner, buffer, inBufferPos);
                default:
                    return TrieCellData.embedded(buffer, inBufferPos, offsetBits);
            }
        }

        private LivenessInfo deserializeLivenessInfo(UnsafeBuffer buffer, int inBufferPos)
        {
            long timestamp = buffer.getLong(inBufferPos + OFFSET_TIMESTAMP);
            long localExpirationTime = CellData.deletionTimeUnsignedIntegerToLong(buffer.getInt(inBufferPos + OFFSET_LOCAL_DELETION_TIME));
            int ttl = buffer.getInt(inBufferPos + OFFSET_TTL);
            return LivenessInfo.withExpirationTime(timestamp, ttl, localExpirationTime);
        }

        private TrieTombstoneMarker deserializeTombstoneMarker(UnsafeBuffer buffer, int inBufferPos)
        {
            TrieTombstoneMarker.Covering left = deserializeTombstoneSide(buffer, inBufferPos + OFFSET_TOMBSTONE_LEFT);
            TrieTombstoneMarker.Covering right = deserializeTombstoneSide(buffer, inBufferPos + OFFSET_TOMBSTONE_RIGHT);
            TrieTombstoneMarker.LevelMarker levelMarker = buffer.getByte(inBufferPos + OFFSET_TOMBSTONE_IS_ROW_MARKER) != 0
                                                          ? TrieTombstoneMarker.LevelMarker.ROW
                                                          : null;
            return TrieTombstoneMarker.make(left, right, levelMarker);
        }

        private TrieTombstoneMarker.Covering deserializeTombstoneSide(UnsafeBuffer buffer, int inBufferPos)
        {
            byte kind = buffer.getByte(inBufferPos + OFFSET_TOMBSTONE_KIND);
            if (kind < 0)
                return null;
            return TrieTombstoneMarker.covering(buffer.getLong(inBufferPos + OFFSET_TIMESTAMP),
                                                CellData.deletionTimeUnsignedIntegerToLong(buffer.getInt(inBufferPos + OFFSET_LOCAL_DELETION_TIME)),
                                                TrieTombstoneMarker.Kind.values()[kind]);
        }

        @Override
        public boolean shouldPresentAfterBranch(int offsetBits)
        {
            // only markers can be after branch
            return offsetBits == TYPE_TOMBSTONE_MARKER_AFTER_BRANCH;
        }

        @Override
        public boolean releaseNeeded(int offsetBits)
        {
            return manager.releaseNeeded() && offsetBits == TYPE_CELL_EXTERNAL_VALUE;
        }

        @Override
        public void release(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            TrieCellData.External.release(buffer, inBufferPos, manager);
        }

        @Override
        public void completeMutation()
        {
            manager.completeMutation();
        }

        @Override
        public void abortMutation()
        {
            manager.abortMutation();
        }

        @Override
        public long usedSizeOnHeap()
        {
            return manager.onHeapSize();
        }

        @Override
        public long usedSizeOffHeap()
        {
            // managed separately in allocator
            return 0;
        }

        @Override
        public long unusedReservedOnHeapMemory()
        {
            return manager.unusedReservedOnHeapMemory();
        }

        @Override
        public void releaseReferencesUnsafe()
        {
            manager.releaseReferencesUnsafe();
        }

        @Override
        public String dumpSpecial(int id)
        {
            return "Payload: " + special(id).toString();
        }

        @Override
        public String dumpContent(UnsafeBuffer buffer, int inBufferPos, int offsetBits)
        {
            // This method dumps the content of the cell using their most common interpretation. While the labels may be
            // incorrect for some types, the data is still presented.
            return String.format("Payload: length/type %02x data %s ttl %08x ldt %08x timestamp %016x",
                                 offsetBits,
                                 ByteBufferUtil.bytesToHex(buffer.byteBuffer()
                                                                 .duplicate()
                                                                 .position(inBufferPos + 0)
                                                                 .limit(inBufferPos + 16)),
                                 buffer.getInt(inBufferPos + OFFSET_TTL),
                                 buffer.getInt(inBufferPos + OFFSET_LOCAL_DELETION_TIME),
                                 buffer.getLong(inBufferPos + OFFSET_TIMESTAMP)
            );
        }
    }

    /// Buffer manager for cell data, used to store data that does not fit the 15 bytes for value in the trie block.
    @VisibleForTesting
    public static abstract class CellDataBufferManager implements TrieCellData.ExternalBufferHandler
    {
        OpOrder.Group opOrderGroup;

        /// On-heap size of any additional structures used to store the references to data
        abstract long onHeapSize();

        /// If true, the release method will be called when a value is no longer in use
        abstract boolean releaseNeeded();

        /// See [org.apache.cassandra.db.tries.MemoryManager#completeMutation]
        abstract void completeMutation();
        /// See [org.apache.cassandra.db.tries.MemoryManager#abortMutation]
        abstract void abortMutation();

        /// See [org.apache.cassandra.db.tries.MemoryManager#unusedReservedOnHeapMemory]
        @VisibleForTesting
        abstract long unusedReservedOnHeapMemory();

        /// See [org.apache.cassandra.db.tries.ContentManager#releaseReferencesUnsafe]
        @VisibleForTesting
        abstract void releaseReferencesUnsafe();
    }

    /// Buffer manager for cell data, used to store data that does not fit the 15 bytes for value in the trie block.
    ///
    /// This option stores data in ByteBuffers allocated by the given [MemtableBufferAllocator] and keeps a list of the
    /// ByteBuffers it returned in a long-lived [ContentManagerPojo].
    /// It has on-heap presence that is proportional to the number of large data values.
    static class SlabBufferManager extends CellDataBufferManager
    {
        final MemtableBufferAllocator allocator;
        final long bufferSizeOnHeap;
        final ContentManagerPojo<ByteBuffer> buffers;

        @VisibleForTesting
        public SlabBufferManager(MemtableBufferAllocator allocator, OpOrder opOrder, long bufferSizeOnHeap)
        {
            this.allocator = allocator;
            this.bufferSizeOnHeap = bufferSizeOnHeap;
            this.buffers = new ContentManagerPojo<>(Predicates.alwaysTrue(), InMemoryBaseTrie.ExpectedLifetime.LONG,
                                                    opOrder);
        }

        @Override
        public long store(ByteBuffer buffer, int length) throws TrieSpaceExhaustedException
        {
            ByteBuffer cloned = allocator.allocate(length, opOrderGroup);
            FastByteOperations.copy(buffer, buffer.position(), cloned, cloned.position(), length);
            return buffers.addContent(cloned, false);
        }

        @Override
        public ByteBuffer load(long handle, int length)
        {
            return buffers.getContent((int) handle);
        }

        @Override
        long onHeapSize()
        {
            return buffers.usedSizeOnHeap() + buffers.valuesCount() * bufferSizeOnHeap;
        }

        @Override
        public boolean releaseNeeded()
        {
            return true;
        }

        @Override
        public void release(long handle, int length)
        {
            buffers.releaseContent((int) handle);
        }

        @Override
        public void completeMutation()
        {
            buffers.completeMutation();
        }

        @Override
        public void abortMutation()
        {
            buffers.abortMutation();
        }

        @Override
        @VisibleForTesting
        long unusedReservedOnHeapMemory()
        {
            return buffers.unusedReservedOnHeapMemory();
        }

        @Override
        @VisibleForTesting
        void releaseReferencesUnsafe()
        {
            buffers.releaseReferencesUnsafe();
        }
    }

    /// Buffer manager for cell data, used to store data that does not fit the 15 bytes for value in the trie block.
    ///
    /// This option stores data in native memory using [NativeAllocator] and returns the memory address as handle.
    /// This storage method has no on-heap presence and is as efficient as it gets. Used when the memtable allocation
    /// type is `offheap_objects`.
    @VisibleForTesting
    public static class NativeBufferManager extends CellDataBufferManager
    {
        final NativeAllocator allocator;

        @VisibleForTesting
        public NativeBufferManager(NativeAllocator allocator)
        {
            this.allocator = allocator;
        }

        @Override
        public long store(ByteBuffer buffer, int length)
        {
            long address = allocator.allocate(length, opOrderGroup);
            MemoryUtil.setBytes(address, buffer);
            return address;
        }

        @Override
        public ByteBuffer load(long address, int length)
        {
            return MemoryUtil.getByteBuffer(address, length, ByteOrder.BIG_ENDIAN);
        }

        @Override
        long onHeapSize()
        {
            return 0;
        }

        @Override
        public boolean releaseNeeded()
        {
            return false;
        }

        @Override
        public void release(long handle, int length)
        {
            // Nothing to do as we can't release data in the allocator. Trie will remove its cells as needed.
        }

        @Override
        public void completeMutation()
        {
            // Nothing needed as we can't recycle allocator memory
        }

        @Override
        public void abortMutation()
        {
            // Nothing needed as we can't recycle allocator memory
        }

        @Override
        long unusedReservedOnHeapMemory()
        {
            return 0;
        }

        @Override
        void releaseReferencesUnsafe()
        {
            // no references held
        }
    }

    /// Trie dumper attaching paths and types to cells and a translation of the key for rows and partitions.
    static class Dumper extends TrieDumperWithPath.DeletionAware<Object, TrieTombstoneMarker>
    {
        final TableMetadata metadata;
        Columns columns;
        int rowKeyLength = 0;
        int partitionKeyLength = 0;

        Dumper(TableMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public String contentToString(Object content)
        {
            if (content instanceof TrieCellData)
            {
                byte[] cellPath = Arrays.copyOfRange(keyBytes, rowKeyLength, keyPos);
                Cell<?> asCell = TrieBackedRow.cellFromCellData((TrieCellData) content, cellPath, cellPath.length, columns);
                return asCell.toString();
            }
            else if (content instanceof LivenessInfo)
            {
                rowKeyLength = keyPos;
                Clustering<?> clustering = metadata.comparator.clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                                            ByteComparable.preencoded(BYTE_COMPARABLE_VERSION, keyBytes, partitionKeyLength, keyPos - partitionKeyLength));
                columns = metadata.regularAndStaticColumns().columns(clustering == Clustering.STATIC_CLUSTERING);
                return content + " at " + clustering.toString(metadata);
            }
            else if (content instanceof PartitionData)
            {
                partitionKeyLength = keyPos;
                BufferDecoratedKey key = BufferDecoratedKey.fromByteComparable(ByteComparable.preencoded(BYTE_COMPARABLE_VERSION, keyBytes, 0, keyPos),
                                                                               BYTE_COMPARABLE_VERSION,
                                                                               metadata.partitioner);
                return content + " at " + metadata.partitionKeyType.getString(key.getKey());
            }

            return content.toString();
        }

        @Override
        public String deletionToString(TrieTombstoneMarker deletionMarker)
        {
            return deletionMarker.toString();
        }
    }
}
