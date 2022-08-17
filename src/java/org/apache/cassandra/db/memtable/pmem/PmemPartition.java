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

package org.apache.cassandra.db.memtable.pmem;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableSet;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.pmem.llpl.util.AutoCloseableIterator;
import com.intel.pmem.llpl.util.LongART;
import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RangeTombstoneList;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowAndDeletionMergeIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;

import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

//PmemPartition is not thread-safe
public class PmemPartition implements Partition
{
    private static final Logger logger = LoggerFactory.getLogger(PmemPartition.class);
    /*
      binary format:
      - address of maps of rows - 8 bytes
      - address of maps of rangeTombstoneMarker - 8 bytes
      - address of static row - 8 bytes
      - tombstone information - 1 byte
      - partition key size - 4 bytes
      - partition key
   */
    private static final long ROW_MAP_OFFSET = 0; // long, handle
    private static final long STATIC_ROW_OFFSET = 8; // long, handle
    private static final long DELETION_INFO_OFFSET = 16; // long, tombstone
    private static final long DECORATED_KEY_SIZE_OFFSET = 24;
    private static final long RTM_INFO_OFFSET = 28; // long, RTM handle
    private static final long HEADER_SIZE = 36;

    /**
     * The block in memory where the high-level data for this partition is stored.
     */
    private TransactionalMemoryBlock block;
    private final TableMetadata tableMetadata;

    // memoized DK
    private DecoratedKey key;
    private AutoCloseableIterator cartIterator;

    protected static final class Holder
    {
        final RegularAndStaticColumns columns;
        final DeletionInfo deletionInfo;
        final Row staticRow;
        final EncodingStats stats;

        Holder(RegularAndStaticColumns columns, DeletionInfo deletionInfo, Row staticRow, EncodingStats stats)
        {
            this.columns = columns;
            this.staticRow = staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
            this.stats = stats;
            this.deletionInfo = deletionInfo;
        }
    }

    // memoized map  Clustering -> Row, where Row is serialized in to one blob
    // - allows compression (debatable if needed)
    protected static final Holder EMPTY = new Holder(RegularAndStaticColumns.NONE, DeletionInfo.LIVE, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
    private final TransactionalHeap heap;
    private Holder ref;
    Row staticRow = Rows.EMPTY_STATIC_ROW;
    PmemRowMap pmemRowMap;
    PmemRowMap pmemRtmMap;
    private final UpdateTransaction indexer;
    private final PmemTableInfo pmemTableInfo;

    public PmemPartition(TransactionalHeap heap, DecoratedKey dkey, AutoCloseableIterator<LongART.Entry> cartIterator, UpdateTransaction indexer, PmemTableInfo pmemTableInfo)
    {
        this.heap = heap;
        this.key = dkey;
        this.tableMetadata = pmemTableInfo.getMetadata();
        this.ref = EMPTY;
        this.cartIterator = cartIterator;
        this.indexer = indexer;
        this.pmemTableInfo = pmemTableInfo;
    }

    public PmemPartition(TransactionalHeap heap, UpdateTransaction indexer, PmemTableInfo pmemTableInfo)
    {
        this.heap = heap;
        this.tableMetadata = pmemTableInfo.getMetadata();
        this.indexer = indexer;
        this.pmemTableInfo = pmemTableInfo;
    }

    //This load function is called during puts
    public void load(TransactionalHeap heap, long partitionHandle, EncodingStats stats)
    {
        block = heap.memoryBlockFromHandle(partitionHandle);

        long staticRowAddress = block.getLong(STATIC_ROW_OFFSET);
        if (staticRowAddress != 0)
        {
            staticRow = getStaticRow(staticRowAddress);
        }
        long rowMapAddress = block.getLong(ROW_MAP_OFFSET);
        if (rowMapAddress != 0)
        {
            pmemRowMap = PmemRowMap.loadFromAddress(heap, rowMapAddress, indexer, pmemTableInfo);
        }
        final long rtmTreeHandle = block.getLong(RTM_INFO_OFFSET);
        if (rtmTreeHandle != 0)
        {
            pmemRtmMap = PmemRowMap.loadFromRtmHandle(heap, rtmTreeHandle, UpdateTransaction.NO_OP, pmemTableInfo);
        }
        if (block.getLong(DELETION_INFO_OFFSET) > 0)
        {
            TransactionalMemoryBlock deletedPartitionBlock = heap.memoryBlockFromHandle(block.getLong(DELETION_INFO_OFFSET));
            DeletionTime deletionTime = getPartitionDeletionTime(deletedPartitionBlock);
            assert deletionTime != null;
            ref = new Holder(tableMetadata.regularAndStaticColumns(), new MutableDeletionInfo(deletionTime.markedForDeleteAt(), deletionTime.localDeletionTime()), staticRow, EncodingStats.NO_STATS);
        }
        else
        {
            ref = new Holder(tableMetadata.regularAndStaticColumns(), DeletionInfo.LIVE, staticRow, stats);
        }
    }

    public void initialize(PartitionUpdate update, TransactionalHeap heap) throws IOException
    {
        Holder current = ref;
        RegularAndStaticColumns columns = update.columns().mergeTo(current.columns);
        staticRow = update.staticRow();
        EncodingStats newStats = current.stats.mergeWith(update.stats());
        ref = new Holder(columns, update.deletionInfo(), staticRow, newStats);

        ByteBuffer partitionKey = update.partitionKey().getKey();
        int keySize = partitionKey.limit();
        long mblockSize = HEADER_SIZE + keySize;
        block = heap.allocateMemoryBlock(mblockSize);
        // handle static row seperate from regular row(s)
        indexer.start();
        if (!staticRow.isEmpty())
        {
            staticRow = updateStaticRow(staticRow, indexer);
        }

        final long rowMapAddress;
        // Range Tombstone Marker
        final long rtmTreeHandle;

        pmemRowMap = PmemRowMap.create(heap, indexer, pmemTableInfo);
        rowMapAddress = pmemRowMap.getHandle(); //gets address of arTree

        pmemRtmMap = PmemRowMap.createForTombstone(heap, UpdateTransaction.NO_OP, pmemTableInfo);
        rtmTreeHandle = pmemRtmMap.getRtmTreeHandle();//gets handle of RTM arTree

        for (Row r : update)
        {
            pmemRowMap.put(r, update);
        }
        indexer.commit();
        // Range Tombstones
        if (update.deletionInfo().hasRanges())
        {
            Iterator<RangeTombstone> ranges = update.deletionInfo().rangeIterator(false);

            try (UnfilteredRowIterator iterator = new RowAndDeletionMergeIterator(tableMetadata, update.partitionKey(), DeletionInfo.LIVE.getPartitionDeletion(),
                                                                                  ColumnFilter.NONE, staticRow, false, ref.stats,
                                                                                  Collections.emptyIterator(), ranges,
                                                                                  false))
            {
                while (iterator.hasNext())
                {
                    Unfiltered unfiltered = iterator.next();
                    pmemRtmMap.putRangeTombstoneMarker(unfiltered, update);
                }
            }
            while (ranges.hasNext())
            {
                RangeTombstone rt = ranges.next();
                DeletionTime tombstoneDeletionTime = rt.deletionTime();
                Slice slice = rt.deletedSlice();
                filterOutPmemRowMap(tombstoneDeletionTime, slice);
            }
        }
        block.setLong(ROW_MAP_OFFSET, rowMapAddress);
        //TOMBSTONE
        block.setInt(DECORATED_KEY_SIZE_OFFSET, partitionKey.remaining());
        block.setLong(RTM_INFO_OFFSET, rtmTreeHandle);
        //In index transaction flow , the Partition key can be empty
        if (keySize > 0)
            block.copyFromArray(partitionKey.array(), 0, HEADER_SIZE, keySize);
        deletePartition(update);
    }

    public void update(PartitionUpdate update, TransactionalHeap heap) throws IOException
    {
        indexer.start();
        if (!update.staticRow().isEmpty())
        {
            staticRow = updateStaticRow(update.staticRow(), indexer);
        }

        for (Row r : update)
            pmemRowMap.put(r, update);

        indexer.commit();
        // Range Tombstones
        if (update.deletionInfo().hasRanges())
        {
            Iterator<RangeTombstone> ranges = getMergedRangeTombstones(update);

            try (UnfilteredRowIterator iterator = new RowAndDeletionMergeIterator(tableMetadata, key, DeletionInfo.LIVE.getPartitionDeletion(),
                                                                                  ColumnFilter.NONE, staticRow, false, update.stats(),
                                                                                  Collections.emptyIterator(), ranges,
                                                                                  false))
            {
                while (iterator.hasNext())
                {
                    Unfiltered unfiltered = iterator.next();
                    pmemRtmMap.putRangeTombstoneMarker(unfiltered, update);
                }
            }
            Iterator<RangeTombstone> currentRtItr = update.deletionInfo().rangeIterator(false);
            while (currentRtItr.hasNext())
            {
                RangeTombstone rt = currentRtItr.next();
                DeletionTime tombstoneDeletionTime = rt.deletionTime();
                Slice slice = rt.deletedSlice();
                filterOutPmemRowMap(tombstoneDeletionTime, slice);
            }
        }
        deletePartition(update);
    }

    private void deletePartition(PartitionUpdate update) throws IOException
    {
        DeletionTime partitionDeleteTime = update.deletionInfo().getPartitionDeletion();
        long partitionDeleteSize = partitionDeleteTime.isLive() ? 0 : DeletionTime.serializer.serializedSize(partitionDeleteTime);

        if (partitionDeleteSize > 0)
        {
            updatePartitionDeletionInfo(partitionDeleteTime, partitionDeleteSize);
        }
    }

    // AtomicBTreePartition.addAllWithSizeDelta & RowUpdater are the places to look to see how classic storage engine stashes things
    private Row updateStaticRow(Row staticRow, UpdateTransaction indexer) throws IOException
    {
        Row newStaticRow = staticRow;
        long oldStaticRowHandle = 0;
        TransactionalMemoryBlock oldStaticBlock = null;
        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {

            oldStaticRowHandle = block.getLong(STATIC_ROW_OFFSET);
            if (oldStaticRowHandle != 0)
            {
                oldStaticBlock = heap.memoryBlockFromHandle(oldStaticRowHandle);
                newStaticRow = getMergedStaticRow(staticRow, oldStaticRowHandle);
            }

            SerializationHeader serializationHeader = new SerializationHeader(false,
                                                                              tableMetadata,
                                                                              tableMetadata.regularAndStaticColumns(),
                                                                              EncodingStats.NO_STATS);
            SerializationHelper helper = new SerializationHelper(serializationHeader);

            int version = pmemTableInfo.getMetadataVersion();
            PmemRowSerializer.serializer.serializeStaticRow(newStaticRow, helper, dob, version);
            int size = dob.getLength();

            TransactionalMemoryBlock staticRowBlock;
            if (oldStaticRowHandle == 0)
            {
                staticRowBlock = heap.allocateMemoryBlock(size);
                indexer.onInserted(staticRow);
            }
            else if (oldStaticBlock.size() < size)
            {
                staticRowBlock = heap.allocateMemoryBlock(size);
                oldStaticBlock.free();
            }
            else
            {
                staticRowBlock = oldStaticBlock;
            }
            MemoryBlockDataOutputPlus cellsOutputPlus = new MemoryBlockDataOutputPlus(staticRowBlock, 0);
            cellsOutputPlus.write(dob.getData(), 0, dob.getLength());
            dob.clear();
            dob.close();
            block.setLong(STATIC_ROW_OFFSET, staticRowBlock.handle());
            return newStaticRow;
        }
    }

    public Row getMergedStaticRow(Row newRow, Long mb)
    {
        Row currentRow = getStaticRow(mb);
        Row reconciled = Rows.merge(currentRow, newRow);
        indexer.onUpdated(currentRow, reconciled);
        return reconciled;
    }

    private Row getStaticRow(long staticRowAddress)
    {
        TransactionalMemoryBlock staticRowMemoryBlock = heap.memoryBlockFromHandle(staticRowAddress);

        DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(staticRowMemoryBlock);

        try
        {
            int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
            SerializationHeader serializationHeader = pmemTableInfo.getSerializationHeader(savedVersion);
            DeserializationHelper helper = new DeserializationHelper(tableMetadata, -1, DeserializationHelper.Flag.LOCAL);
            return PmemRowSerializer.serializer.deserializeStaticRow(memoryBlockDataInputPlus, serializationHeader, helper);
        }
        catch (IOException | IndexOutOfBoundsException e)
        {
            closeCartIterator();
            throw new IOError(e);
        }
    }

    /**
     * This method updates keeps a check to markedForDeleteAt property of Partition Deletiontime and updates the latest partition deletion time Info
     *
     * @param partitionDeleteTime
     * @param partitionDeleteSize
     */
    @SuppressWarnings({ "resource" })
    private void updatePartitionDeletionInfo(DeletionTime partitionDeleteTime, long partitionDeleteSize) throws IOException
    {
        long deletedHandle = block.getLong(DELETION_INFO_OFFSET);
        TransactionalMemoryBlock deletedPartitionBlock = null;
        if (deletedHandle > 0)
        {
            deletedPartitionBlock = heap.memoryBlockFromHandle(deletedHandle);
            DeletionTime latestDeletionTime = this.getPartitionDeletionTime(deletedPartitionBlock);
            assert latestDeletionTime != null;
            if (latestDeletionTime.markedForDeleteAt() >= partitionDeleteTime.markedForDeleteAt())
            {
                return;
            }
        }
        MemoryBlockDataOutputPlus partitionBlockOutputPlus = getPartitionDeleteInfoBuffer(deletedPartitionBlock, partitionDeleteSize);
        DeletionTime.serializer.serialize(partitionDeleteTime, partitionBlockOutputPlus);
        filterOutPmemRowMap(partitionDeleteTime,Slice.ALL);
        block.setLong(STATIC_ROW_OFFSET, 0);
    }

    /**
     * This sets the partition delete info to the DELETION_INFO_OFFSET and returns the output buffer
     *
     * @param deletedPartitionBlock
     * @param partitionDeleteSize
     * @return MemoryBlockDataOutputPlus reference
     */
    private MemoryBlockDataOutputPlus getPartitionDeleteInfoBuffer(TransactionalMemoryBlock deletedPartitionBlock, long partitionDeleteSize)
    {
        if (deletedPartitionBlock != null && partitionDeleteSize <= deletedPartitionBlock.size())
        {
            return new MemoryBlockDataOutputPlus(deletedPartitionBlock, 0);
        }
        else
        {
            TransactionalMemoryBlock newBlock = heap.allocateMemoryBlock(partitionDeleteSize);
            MemoryBlockDataOutputPlus partitionBlockOutputPlus = new MemoryBlockDataOutputPlus(newBlock, 0);
            if (deletedPartitionBlock != null)
            {
                deletedPartitionBlock.free();
            }
            block.setLong(DELETION_INFO_OFFSET, newBlock.handle());
            return partitionBlockOutputPlus;
        }
    }

    public long getAddress()
    {
        return block.handle();
    }

    public long getRowMapAddress()
    {
        return pmemRowMap.getHandle();
    }

    @Override
    public TableMetadata metadata()
    {
        return tableMetadata;
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return this.key;
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        return ref.deletionInfo.getPartitionDeletion();
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        return ref.columns;
    }

    @Override
    public EncodingStats stats()
    {
        return ref.stats;
    }

    @Override
    public boolean isEmpty()
    {
        return ref.deletionInfo.isLive() && pmemRowMap.size() == 0 && ref.staticRow.isEmpty();
    }

    @Override
    public boolean hasRows()
    {
        return pmemRowMap.size() != 0;
    }

    @Nullable
    @Override
    public Row getRow(Clustering<?> clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
        {
            return staticRow;
        }
        return pmemRowMap.getRow(clustering, tableMetadata);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator()
    {
        return unfilteredIterator(ColumnFilter.selection(columns()), Slices.ALL, false);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed)
    {
        return unfilteredIterator(ref, columns, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
    {
        if (block.getLong(DELETION_INFO_OFFSET) == -1 || block.getLong(DELETION_INFO_OFFSET) > 0)
        {
            TransactionalMemoryBlock deletedPartitionBlock = heap.memoryBlockFromHandle(block.getLong(DELETION_INFO_OFFSET));
            DeletionTime deletionTime = getPartitionDeletionTime(deletedPartitionBlock);
            return new PmemRowMapClusteringIterator(columns, clusteringsInQueryOrder, pmemRowMap.getHandle(),
                                                    deletionTime, reversed);
        }
        return new PmemRowMapClusteringIterator(columns, clusteringsInQueryOrder, pmemRowMap.getHandle(),
                                                ref.deletionInfo.getPartitionDeletion(), reversed);
    }

    private DeletionTime getPartitionDeletionTime(TransactionalMemoryBlock referenceBlock)
    {
        try
        {
            return DeletionTime.serializer.deserialize(new MemoryBlockDataInputPlus(referenceBlock));
        }
        catch (Exception e)
        {
            logger.error("Failed to retrieve the partition deletion time !!", e);
        }
        return null;
    }

    public UnfilteredRowIterator unfilteredIterator(Holder current, ColumnFilter columns, Slices slices, boolean reversed)
    {
        if (block.getLong(DELETION_INFO_OFFSET) > 0)
        {
            TransactionalMemoryBlock deletedPartitionBlock = heap.memoryBlockFromHandle(block.getLong(DELETION_INFO_OFFSET));
            DeletionTime deletionTime = getPartitionDeletionTime(deletedPartitionBlock);
            assert deletionTime != null;
            Holder deletionHolder = new Holder(tableMetadata.regularAndStaticColumns(), new MutableDeletionInfo(deletionTime.markedForDeleteAt(), deletionTime.localDeletionTime()), staticRow, EncodingStats.NO_STATS);
            return new SlicesIterator(deletionHolder, columns, slices, reversed);
        }
        else if (slices.size() == 0)
        {
            DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
            closeCartIterator();
            return UnfilteredRowIterators.noRowsIterator(metadata(), partitionKey(), staticRow, partitionDeletion, reversed);
        }
        if (slices.size() == 1)
        {
            return PmemRowAndRtmIterator.create(key, current.deletionInfo, columns, staticRow, reversed, pmemRtmMap.getRangeTombstoneMarkerTree(), cartIterator, pmemRowMap.getRowMapTree(), heap, slices.get(0), ref.stats, pmemTableInfo);
        }
        return new SlicesIterator(current, columns, slices, reversed);
    }

    //Get All the RTM from RangeMapTree and Build DeletionInfo
    private DeletionInfo buildDeletionInfoForUpdate()
    {
        if (pmemRtmMap.getRangeTombstoneMarkerTree().size() == 0)
            return DeletionInfo.LIVE;

        Iterator<LongART.Entry> tombstoneMarker = pmemRtmMap.getRangeTombstoneMarkerTree().getEntryIterator();
        MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(partitionLevelDeletion(), tableMetadata.comparator, false);
        Row.Builder builder = BTreeRow.sortedBuilder();

        while (tombstoneMarker.hasNext())
        {
            LongART.Entry nextEntry =  tombstoneMarker.next();
            TransactionalMemoryBlock rangeMemoryBlock = heap.memoryBlockFromHandle(nextEntry.getValue());
            DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(rangeMemoryBlock);
            try
            {
                int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
                SerializationHeader serializationHeader = pmemTableInfo.getSerializationHeader(savedVersion);
                DeserializationHelper helper = new DeserializationHelper(metadata(), -1, DeserializationHelper.Flag.LOCAL);

                Unfiltered unfiltered = PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);

                if (unfiltered != null)
                {
                    deletionBuilder.add((RangeTombstoneMarker) unfiltered);
                }
            }
            catch (IOException | IndexOutOfBoundsException e)
            {
                closeCartIterator();
                throw new IOError(e);
            }
        }

        return deletionBuilder.build();
    }

    public Iterator<RangeTombstone> getMergedRangeTombstones(PartitionUpdate update)
    {
        RangeTombstoneList ranges = new RangeTombstoneList(update.metadata().comparator, DatabaseDescriptor.getInitialRangeTombstoneListAllocationSize());
        Iterator<RangeTombstone> rtItrFromUpdate = update.deletionInfo().rangeIterator(false);
        while (rtItrFromUpdate.hasNext())
        {
            ranges.add(rtItrFromUpdate.next());
        }
        Iterator<RangeTombstone> rtItrFromBuildDeletionInfo = buildDeletionInfoForUpdate().rangeIterator(false);

        while (rtItrFromBuildDeletionInfo.hasNext())
        {
            ranges.add(rtItrFromBuildDeletionInfo.next());
        }
        //Remove  Previously saved RTM from RangeTombstoneMarkerTree
        pmemRtmMap.getRangeTombstoneMarkerTree().clear(PmemRowMap::clearData);
        return ranges.iterator();
    }

    /**
     * Removes Tombstones which are older than gcGraceSeconds
     *
     * @return Expired Tombstone count
     */
    public long vaccumTombstones()
    {
        long removedTombstones = 0;

        if (block.getLong(DELETION_INFO_OFFSET) > 0)
        {
            TransactionalMemoryBlock deletedPartitionBlock = heap.memoryBlockFromHandle(block.getLong(DELETION_INFO_OFFSET));
            DeletionTime deletionTime = getPartitionDeletionTime(deletedPartitionBlock);
            int gcBefore = getGcBefore(metadata(), FBUtilities.nowInSeconds());
            if (deletionTime != null && deletionTime.localDeletionTime() < gcBefore)
            {
                deletedPartitionBlock.free();
                block.setLong(DELETION_INFO_OFFSET, 0);
            }
        }
        removedTombstones += removeExpiredRangeTombstone();
        removedTombstones += removeExpiredDeletedRows();
        return removedTombstones;
    }

    //Removes Row Tombstones which are older then gcgraceseconds
    private int removeExpiredDeletedRows()
    {
        Iterator<LongART.Entry> rowItr = pmemRowMap.getRowMapTree().getEntryIterator();
        final ArrayList<byte[]> artEntryKeyArray = new ArrayList<>();
        while (rowItr.hasNext())
        {
            LongART.Entry nextEntry = rowItr.next();
            Unfiltered unfiltered = getUnfiltered(nextEntry, true);
            if (unfiltered != null && isExpired(pmemTableInfo, unfiltered))
            {
                artEntryKeyArray.add(nextEntry.getKey());
            }
        }
        if (!artEntryKeyArray.isEmpty())
        {
            for (byte[] artEntryKey : artEntryKeyArray)
            {
                pmemRowMap.getRowMapTree().remove(artEntryKey, (Long rowhandle) -> {
                    TransactionalMemoryBlock rowMemoryBlock = heap.memoryBlockFromHandle(rowhandle);
                    rowMemoryBlock.free();
                });
            }
        }

        return artEntryKeyArray.size();
    }

    //Removes RangeTombstones which are older then gcgraceseconds
    private int removeExpiredRangeTombstone()
    {
        Iterator<LongART.Entry> tombstoneMarkerItr = pmemRtmMap.getRangeTombstoneMarkerTree().getEntryIterator();
        final ArrayList<byte[]> artEntryKeyArray = new ArrayList<>();
        while (tombstoneMarkerItr.hasNext())
        {
            LongART.Entry nextEntry = tombstoneMarkerItr.next();
            Unfiltered unfiltered = getUnfiltered(nextEntry, false);
            if (unfiltered != null && isExpired(pmemTableInfo, unfiltered))
            {
                artEntryKeyArray.add(nextEntry.getKey());
            }
        }
        if (!artEntryKeyArray.isEmpty())
        {
            for (byte[] artEntryKey : artEntryKeyArray)
            {
                pmemRtmMap.getRangeTombstoneMarkerTree().remove(artEntryKey, (Long rowhandle) -> {
                    TransactionalMemoryBlock rowMemoryBlock = heap.memoryBlockFromHandle(rowhandle);
                    rowMemoryBlock.free();
                });
            }
        }

        return artEntryKeyArray.size();
    }

    private static int getGcBefore(TableMetadata metadata, int nowInSec)
    {
        ColumnFamilyStore cfs = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.id);
        return cfs.gcBefore(nowInSec);
    }

    // Returns RTM deletion time
    private static DeletionTime rtmDeletionTime(RangeTombstoneMarker marker)
    {
        return marker.isOpen(false) ? marker.openDeletionTime(false) : marker.closeDeletionTime(false);
    }

    private Unfiltered getUnfiltered(LongART.Entry nextEntry , boolean isRow)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        ByteComparable clusteringByteComparable = ByteComparable.fixedLength(nextEntry.getKey());
        if(isRow)
        {
            Clustering<?> clustering = tableMetadata.comparator.clusteringFromByteComparable(ByteArrayAccessor.instance, clusteringByteComparable);
            builder.newRow(clustering);
        }
        TransactionalMemoryBlock rangeMemoryBlock = heap.memoryBlockFromHandle(nextEntry.getValue());
        DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(rangeMemoryBlock);
        try
        {
            int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
            SerializationHeader serializationHeader = pmemTableInfo.getSerializationHeader(savedVersion);
            DeserializationHelper helper = new DeserializationHelper(metadata(), -1, DeserializationHelper.Flag.LOCAL);
            return PmemRowSerializer.serializer.deserializeTombstone(memoryBlockDataInputPlus, serializationHeader, helper, builder);
        }
        catch (IOException e)
        {
            closeCartIterator();
            throw new IOError(e);
        }
    }

    /**
     * True if unfiltered is expired otherwise false
     * @param pmemTableInfo
     * @param unfiltered
     * @return
     */
    public static boolean isExpired(PmemTableInfo pmemTableInfo, Unfiltered unfiltered)
    {
        int localDeletionTime;
        int gcBefore = getGcBefore(pmemTableInfo.getMetadata(),FBUtilities.nowInSeconds());
        if (unfiltered.isRow())
        {
            Row row = (Row) unfiltered;
            localDeletionTime = row.primaryKeyLivenessInfo().isExpiring() ? row.primaryKeyLivenessInfo().localExpirationTime() : row.deletion().time().localDeletionTime();
        }
        else
        {
            localDeletionTime = rtmDeletionTime((RangeTombstoneMarker) unfiltered).localDeletionTime();
        }
        return localDeletionTime < gcBefore;
    }

    private void filterOutPmemRowMap(DeletionTime time,Slice slice)
    {
        try (UnfilteredRowIterator iter = new PmemRowMapDeletionIterator(tableMetadata, pmemRowMap.getRowMapTree(), heap, partitionKey(), time, slice))
        {
            //Consume iterator to remove rows
            if (iter.hasNext())
            {
                iter.next();
            }
        }
    }

    private void closeCartIterator()
    {
        try
        {
            if (cartIterator != null)
            {
                cartIterator.close();
                cartIterator = null;
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    //Addresses cases when read happens over multiple slices
    private class SlicesIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
    {
        private final Slices slices;
        private int idx;
        private Iterator<Unfiltered> currentSlice;
        private ColumnFilter columnFilter;
        boolean isReversed;
        Holder current;

        private SlicesIterator(Holder current, ColumnFilter selection, Slices slices, boolean isReversed)
        {
            this.isReversed = isReversed;
            this.columnFilter = selection;
            this.current = current;
            this.slices = slices;
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                if (currentSlice == null)
                {
                    if (idx >= slices.size())
                        return endOfData();

                    int sliceIdx = isReversed ? slices.size() - idx - 1 : idx;
                    currentSlice = PmemRowAndRtmIterator.create(key, current.deletionInfo, columnFilter, staticRow, isReversed, pmemRtmMap.getRangeTombstoneMarkerTree(), cartIterator, pmemRowMap.getRowMapTree(), heap, slices.get(sliceIdx), ref.stats, pmemTableInfo);
                    idx++;
                }
                if (currentSlice.hasNext())
                    return currentSlice.next();

                currentSlice = null;
            }
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return current.deletionInfo.getPartitionDeletion();
        }

        @Override
        public EncodingStats stats()
        {
            return current.stats;
        }

        @Override
        public TableMetadata metadata()
        {
            return tableMetadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return this.isReversed;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return tableMetadata.regularAndStaticColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return key;
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public void close()
        {
            closeCartIterator();
        }
    }

    //Addresses cases where there are multiple values for a clustering key
    private class PmemRowMapClusteringIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
    {
        private final Iterator<Clustering<?>> clusteringsIterator;
        private Iterator<Unfiltered> currentIterator;
        private final LongART pmemRowTree;
        private boolean reversed = false;
        SerializationHeader header;
        DeletionTime deletionTime = DeletionTime.LIVE;
        ColumnFilter selection;

        private PmemRowMapClusteringIterator(ColumnFilter selection,
                                             NavigableSet<Clustering<?>> clusteringsInQueryOrder,
                                             long pmemRowMapTreeAddr,
                                             DeletionTime deletionTime,
                                             boolean isReversed)
        {
            this.header = SerializationHeader.makeWithoutStats(tableMetadata);
            this.pmemRowTree = LongART.fromHandle(heap, pmemRowMapTreeAddr);
            this.deletionTime = deletionTime;
            this.clusteringsIterator = clusteringsInQueryOrder.iterator();
            this.reversed = isReversed;
            this.selection = selection;
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                if (currentIterator == null)
                {
                    if (!clusteringsIterator.hasNext())
                        return endOfData();
                    currentIterator = nextIterator(clusteringsIterator.next());
                }
                if (currentIterator != null && currentIterator.hasNext())
                    return currentIterator.next();
                currentIterator = null;
            }
        }

        private Iterator<Unfiltered> nextIterator(Clustering<?> clustering)
        {
            return PmemRowAndRtmIterator.create(key, ref.deletionInfo, selection, staticRow, reversed, pmemRtmMap.getRangeTombstoneMarkerTree(), cartIterator, pmemRowTree, heap, Slice.make(clustering), ref.stats, pmemTableInfo);
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return deletionTime;
        }

        @Override
        public EncodingStats stats()
        {
            return ref.stats;
        }

        @Override
        public TableMetadata metadata()
        {
            return tableMetadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return reversed;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return selection.fetchedColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return key;
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public void close()
        {
            closeCartIterator();
        }
    }

    //Remove the rows which are part of Delete statement slice criteria
    private class PmemRowMapDeletionIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
    {
        protected final TableMetadata metadata;
        private final TransactionalHeap heap;
        private Iterator<LongART.Entry> pmemRowTreeIterator;
        private LongART pmemRowTree;
        private DecoratedKey dkey;
        SerializationHeader header;
        DeletionTime deletionTime;
        private Row staticRow = Rows.EMPTY_STATIC_ROW;

        public PmemRowMapDeletionIterator(TableMetadata metadata,
                                          LongART pmemRowMapTree, TransactionalHeap heap, DecoratedKey key,
                                          DeletionTime deletionTime, Slice slice)
        {
            this.metadata = metadata;
            this.heap = heap;
            this.header = SerializationHeader.makeWithoutStats(metadata);
            this.dkey = key;
            this.deletionTime = deletionTime;
            this.pmemRowTree = pmemRowMapTree;
            boolean includeStart = slice.start().isInclusive();
            boolean includeEnd = slice.end().isInclusive();
            ClusteringBound start = slice.start() == ClusteringBound.BOTTOM ? null : slice.start();
            ClusteringBound end = slice.end() == ClusteringBound.TOP ? null : slice.end();

            if ((start != null && start.size() != 0) && (end != null && end.size() != 0))
            {

                ByteSource clusteringByteSource = metadata.comparator.asByteComparable(start).asComparableBytes(ByteComparable.Version.OSS42);
                byte[] clusteringStartBytes = ByteSourceInverse.readBytes(clusteringByteSource);
                clusteringByteSource = metadata.comparator.asByteComparable(end).asComparableBytes(ByteComparable.Version.OSS42);
                byte[] clusteringEndBytes = ByteSourceInverse.readBytes(clusteringByteSource);
                this.pmemRowTreeIterator = pmemRowMapTree.getEntryIterator(clusteringStartBytes, includeStart, clusteringEndBytes, includeEnd);
            }
            else if ((start != null) && (start.size() != 0))
            {

                ByteSource clusteringByteSource = metadata.comparator.asByteComparable(start).asComparableBytes(ByteComparable.Version.OSS42);
                byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
                this.pmemRowTreeIterator = pmemRowMapTree.getTailEntryIterator(clusteringBytes, includeStart);
            }
            else if ((end != null) && (end.size() != 0))
            {

                ByteSource clusteringByteSource = metadata.comparator.asByteComparable(end).asComparableBytes(ByteComparable.Version.OSS42);
                byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
                this.pmemRowTreeIterator = pmemRowMapTree.getHeadEntryIterator(clusteringBytes, includeEnd);
            }
            else
            {
                this.pmemRowTreeIterator = pmemRowMapTree.getEntryIterator();
            }
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return false;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return metadata.regularAndStaticColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return dkey;
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return this.deletionTime;
        }

        @Override
        public EncodingStats stats()
        {
            return EncodingStats.NO_STATS;
        }

        @Override
        protected Unfiltered computeNext()
        {
            final ArrayList<byte[]> artEntryKeyArray = new ArrayList<>();
            while (pmemRowTreeIterator.hasNext())
            {
                LongART.Entry nextEntry = pmemRowTreeIterator.next();
                Unfiltered unfiltered = computeNextInternal(nextEntry);
                if (unfiltered != null && (((Row) unfiltered).primaryKeyLivenessInfo().timestamp() <= deletionTime.markedForDeleteAt()))
                {
                    artEntryKeyArray.add(nextEntry.getKey());
                }
            }

            for (byte[] artEntryKey : artEntryKeyArray)
            {
                pmemRowTree.remove(artEntryKey, (Long rowhandle) -> {
                    TransactionalMemoryBlock rowMemoryBlock = heap.memoryBlockFromHandle(rowhandle);
                    rowMemoryBlock.free();
                });
            }

            return endOfData();
        }

        private Unfiltered computeNextInternal(LongART.Entry nextEntry)
        {
            Row.Builder builder = BTreeRow.sortedBuilder();

            TransactionalMemoryBlock mb = heap.memoryBlockFromHandle(nextEntry.getValue());

            ByteComparable clusteringByteComparable = ByteComparable.fixedLength(nextEntry.getKey());
            Clustering<?> clustering = metadata.comparator.clusteringFromByteComparable(ByteArrayAccessor.instance, clusteringByteComparable);
            builder.newRow(clustering);

            DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(mb);
            try
            {
                int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
                SerializationHeader serializationHeader = pmemTableInfo.getSerializationHeader(savedVersion);
                DeserializationHelper helper = new DeserializationHelper(metadata, -1, DeserializationHelper.Flag.LOCAL);
                Unfiltered unfiltered = PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);

                return unfiltered == null ? endOfData() : unfiltered;
            }
            catch (IOException | IndexOutOfBoundsException e)
            {
                closeCartIterator();
                throw new IOError(e);
            }
        }

        @Override
        public void close()
        {
            closeCartIterator();
        }
    }
}
