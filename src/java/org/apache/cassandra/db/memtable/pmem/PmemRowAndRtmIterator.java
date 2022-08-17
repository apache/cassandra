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
import java.util.Comparator;
import java.util.Iterator;

import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import com.intel.pmem.llpl.util.AutoCloseableIterator;
import com.intel.pmem.llpl.util.LongART;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class PmemRowAndRtmIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
{
    private final Comparator<Clusterable> comparator;
    private final ColumnFilter selection;
    private Row nextRow;
    private final Iterator<LongART.Entry> rtmIterator;
    private RangeTombstoneMarker nextMarker = null;
    private RangeTombstoneMarker openMarker = null;
    private RangeTombstoneMarker closeMarker = null;
    private RangeTombstoneMarker startMarker = null;
    protected final TableMetadata metadata;
    private final TransactionalHeap heap;
    private final Iterator<LongART.Entry> pmemRowTreeIterator;
    private DecoratedKey dkey;
    private boolean reversed;
    private DeletionTime deletionTime;
    private AutoCloseableIterator cartIterator;
    private Row staticRow = Rows.EMPTY_STATIC_ROW;
    private boolean isFirstMarker = true;
    private EncodingStats stats;
    private boolean isSliceDone = false;
    private Slice slice;
    private  PmemTableInfo pmemTableInfo;

    public PmemRowAndRtmIterator(DecoratedKey partitionKey, DeletionInfo deletionInfo, ColumnFilter selection, Row staticRow, boolean isReversed, LongART pmemRowMapTree, Slice slice, AutoCloseableIterator cartIterator, LongART pmemRtmMapTree, TransactionalHeap heap, EncodingStats stats, PmemTableInfo pmemTableInfo)
    {
        this.metadata = pmemTableInfo.getMetadata();
        this.comparator = isReversed ? metadata.comparator.reversed() : metadata.comparator;
        this.selection = selection;
        this.heap = heap;
        this.stats = stats;
        this.dkey = partitionKey;
        this.reversed = isReversed;
        this.staticRow = staticRow;
        this.deletionTime = deletionInfo.getPartitionDeletion();
        this.cartIterator = cartIterator;
        this.slice = slice;
        this.rtmIterator = buildRtmTreeIterator(pmemRtmMapTree, slice);
        this.pmemRowTreeIterator = buildRowTreeIterator(pmemRowMapTree, slice);
        this.pmemTableInfo = pmemTableInfo;
    }

    public static UnfilteredRowIterator create(DecoratedKey key, DeletionInfo deletionTime, ColumnFilter columns, Row staticRow, boolean reversed
    , LongART pmemRtmMapTree, AutoCloseableIterator cartIterator, LongART pmemRowMapTree, TransactionalHeap heap, Slice slice, EncodingStats stats, PmemTableInfo pmemTableInfo)
    {
        PmemRowAndRtmIterator pmemRowAndRtmIterator = new PmemRowAndRtmIterator(key, deletionTime, columns, staticRow, reversed, pmemRowMapTree, slice, cartIterator, pmemRtmMapTree, heap, stats, pmemTableInfo);
        pmemRowAndRtmIterator.staticRow = staticRow;
        return pmemRowAndRtmIterator;
    }

    private Unfiltered computeNextInternal()
    {
        while (true)
        {
            updateNextRow();
            if (nextRow == null)
            {
                if (openMarker != null)
                    return closeOpenMarker();

                updateNextMarker();

                return nextMarker == null ? endOfData() : openMarker();
            }

            // We have a next row
            if (openMarker == null)
            {
                // We have no currently open marker. So check if we have a next marker and if it sorts before this row.
                // If it does, the opening of that marker should go first. Otherwise, the row goes first.
                updateNextMarker();
                if (startMarker == null && nextMarker != null)
                    startMarker = processNextMarker();

                if (startMarker != null && comparator.compare(startMarker.clustering(), nextRow.clustering()) < 0)
                    return openMarker();

                Row row = consumeNextRow();
                // it's possible for the row to be fully shadowed by the current range tombstone
                if (row != null)
                    return row;
            }
            else
            {
                // We have both a next row and a currently opened tombstone. Check which goes first between the range closing and the row.
                if (closeMarker == null)
                    closeMarker = processMarkerToClose();

                if (closeMarker != null && comparator.compare(closeMarker, nextRow.clustering()) < 0)
                    return closeOpenMarker();

                Row row = consumeNextRow();
                if (row != null)
                    return row;
            }
        }
    }

    @Override
    protected Unfiltered computeNext()
    {
        Unfiltered next = computeNextInternal();
        return next;
    }

    private Unfiltered computeNextRowInternal(Iterator<LongART.Entry> rowIterator)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        LongART.Entry nextEntry = rowIterator.next();
        TransactionalMemoryBlock cellMemoryBlock = heap.memoryBlockFromHandle(nextEntry.getValue());
        ByteComparable clusteringByteComparable = ByteComparable.fixedLength(nextEntry.getKey());
        Clustering<?> clustering = metadata.comparator.clusteringFromByteComparable(ByteArrayAccessor.instance, clusteringByteComparable);
        builder.newRow(clustering);
        DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(cellMemoryBlock);
        try
        {
            int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
            SerializationHeader serializationHeader = pmemTableInfo.getSerializationHeader(savedVersion);
            DeserializationHelper helper = new DeserializationHelper(metadata, -1, DeserializationHelper.Flag.LOCAL);
            Unfiltered unfiltered = PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);
            return unfiltered;
        }
        catch (IndexOutOfBoundsException | IOException e)
        {
            closeCartIterator();
            throw new IOError(e);
        }
    }

    private RangeTombstoneMarker computeNextRtmInternal(Iterator<LongART.Entry> rtmTreeIterator)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        LongART.Entry nextEntry = rtmTreeIterator.next();
        TransactionalMemoryBlock rangeMemoryBlock = heap.memoryBlockFromHandle(nextEntry.getValue());
        DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(rangeMemoryBlock);
        try
        {
            int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
            SerializationHeader serializationHeader = pmemTableInfo.getSerializationHeader(savedVersion);
            DeserializationHelper helper = new DeserializationHelper(metadata, -1, DeserializationHelper.Flag.LOCAL);
            Unfiltered unfiltered = PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);
            return (RangeTombstoneMarker) unfiltered;
        }
        catch (IndexOutOfBoundsException | IOException e)
        {
            closeCartIterator();
            throw new IOError(e);
        }
    }

    private void updateNextRow()
    {
        while (nextRow == null && pmemRowTreeIterator.hasNext())
        {
            nextRow = (Row) computeNextRowInternal(pmemRowTreeIterator);
            //Cassandra recommends to Skip empty rows
            if (nextRow.isEmpty())
                nextRow = null;
        }
    }

    private void updateNextMarker()
    {
        while (nextMarker == null && rtmIterator.hasNext() && !isSliceDone)
        {
            nextMarker = computeNextRtmInternal(rtmIterator);
            if (nextMarker != null && (slice.end() != ClusteringBound.TOP && slice.start() != ClusteringBound.BOTTOM))
            {
                int cmp = compareRtmWithEndSlice(nextMarker);
                isSliceDone = cmp >= 0;
                if (isSliceDone)
                {
                    if (!nextMarker.isBoundary() && nextMarker.isOpen(reversed))
                    {
                        nextMarker = null;
                    }
                }
            }
        }
        //OpenMarker should always hold latest timestamp
        if (nextMarker != null && nextMarker.isClose(reversed) && openMarker != null)
            openMarker = nextMarker;
    }

    private Row consumeNextRow()
    {
        Row row = nextRow;
        nextRow = null;

        DeletionTime activeDeletion = openMarker == null ? partitionLevelDeletion() : openMarker.isClose(reversed) ? openMarker.closeDeletionTime(reversed) : openMarker.openDeletionTime(reversed);

        if (row != null && (row.deletion().time().markedForDeleteAt() <= activeDeletion.markedForDeleteAt()))
        {
            return row.filter(selection, activeDeletion, false, metadata);
        }
        return row;
    }

    private RangeTombstoneMarker processNextMarker()
    {
        RangeTombstoneMarker marker = nextMarker;
        if (isFirstMarker && nextMarker.isClose(reversed))
        {
            if (nextMarker.isBoundary())
            {
                boolean startEqualsNextMarker = comparator.compare(nextMarker.clustering(), slice.open(reversed).clustering()) == 0;
                if (startEqualsNextMarker)
                    marker = new RangeTombstoneBoundMarker(slice.open(reversed), nextMarker.openDeletionTime(reversed));
                else
                    marker = new RangeTombstoneBoundMarker(slice.open(reversed), nextMarker.closeDeletionTime(reversed));

                if (isSliceDone)
                {
                    if (startEqualsNextMarker)
                        nextMarker = new RangeTombstoneBoundMarker(slice.close(reversed), nextMarker.openDeletionTime(reversed));
                    else
                        nextMarker = new RangeTombstoneBoundMarker(slice.close(reversed), nextMarker.closeDeletionTime(reversed));
                }
                else if (rtmIterator.hasNext() && startEqualsNextMarker)
                    nextMarker = null;
            }
            else
                marker = new RangeTombstoneBoundMarker(slice.open(reversed), nextMarker.closeDeletionTime(reversed));
        }
        else if (openMarker == null)
        {
            //If the last bound is start bound then we should create respective end bound
            if ((nextMarker != null && nextMarker.isOpen(reversed)) && (isSliceDone || !rtmIterator.hasNext()))
                nextMarker = new RangeTombstoneBoundMarker(slice.close(reversed), nextMarker.openDeletionTime(reversed));
            else
                nextMarker = null;  //set to null to read next set of RTMS
        }
        //If nextmarker is boundary and its last marker then create new end bound from boundry
        else if (nextMarker.isBoundary() && (!rtmIterator.hasNext() || isSliceDone))
        {
            int cmp = compareRtmWithEndSlice(nextMarker);
            if (cmp >= 0)
            {
                marker = new RangeTombstoneBoundMarker(slice.close(reversed), nextMarker.closeDeletionTime(reversed));
                nextMarker = null;
            }
            else
                nextMarker = new RangeTombstoneBoundMarker(slice.close(reversed), nextMarker.openDeletionTime(reversed));
        }
        else
            nextMarker = null;

        return marker;
    }

    private RangeTombstoneMarker processMarkerToClose()
    {
        updateNextMarker();
        if (nextMarker != null && nextMarker.isBoundary())
            return processNextMarker();
        else
        {
            RangeTombstoneMarker toClose = nextMarker;
            if (toClose != null && (slice.end() != ClusteringBound.TOP && slice.start() != ClusteringBound.BOTTOM))
            {    // RangeTomstoneMarkers should stricly included within the queried slice, if marker is greater then slice end then create respective end bound
                if (toClose.isClose(reversed) && comparator.compare(toClose.clustering(), slice.close(reversed)) > 0)
                    return new RangeTombstoneBoundMarker(slice.close(reversed), toClose.closeDeletionTime(reversed));
            }
            return toClose;
        }
    }

    private RangeTombstoneMarker closeOpenMarker()
    {
        if (closeMarker == null)
            closeMarker = processMarkerToClose();

        RangeTombstoneMarker marker = closeMarker;
        if (marker != null && !marker.isBoundary())
        {
            nextMarker = null;
            openMarker = null;
        }
        closeMarker = null;
        return marker;
    }

    private RangeTombstoneMarker openMarker()
    {
        assert openMarker == null;
        if (startMarker == null && nextMarker != null)
            startMarker = processNextMarker();
        openMarker = startMarker;
        isFirstMarker = false;
        startMarker = null;
        return openMarker;
    }

    private Iterator<LongART.Entry> buildRowTreeIterator(LongART pmemRowMapTree, Slice slice)
    {
        Iterator<LongART.Entry> rowTreeIterator;
        boolean includeStart = slice.start().isInclusive();
        boolean includeEnd = slice.end().isInclusive();
        ClusteringBound<?> start = slice.start() == ClusteringBound.BOTTOM ? null : slice.start();
        ClusteringBound<?> end = slice.end() == ClusteringBound.TOP ? null : slice.end();

        if ((start != null && start.size() != 0) && (end != null && end.size() != 0))
        {
            ByteSource clusteringByteSource = metadata.comparator.asByteComparable(start).asComparableBytes(ByteComparable.Version.OSS42);
            byte[] clusteringStartBytes = ByteSourceInverse.readBytes(clusteringByteSource);
            clusteringByteSource = metadata.comparator.asByteComparable(end).asComparableBytes(ByteComparable.Version.OSS42);
            byte[] clusteringEndBytes = ByteSourceInverse.readBytes(clusteringByteSource);
            if (reversed)
                rowTreeIterator = pmemRowMapTree.getReverseEntryIterator(clusteringStartBytes, includeStart, clusteringEndBytes, includeEnd);
            else
                rowTreeIterator = pmemRowMapTree.getEntryIterator(clusteringStartBytes, includeStart, clusteringEndBytes, includeEnd);
        }
        else if ((start != null) && (start.size() != 0))
        {
            ByteSource clusteringByteSource = metadata.comparator.asByteComparable(start).asComparableBytes(ByteComparable.Version.OSS42);
            byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
            if (reversed)
                rowTreeIterator = pmemRowMapTree.getReverseTailEntryIterator(clusteringBytes, includeStart);
            else
                rowTreeIterator = pmemRowMapTree.getTailEntryIterator(clusteringBytes, includeStart);
        }
        else if ((end != null) && (end.size() != 0))
        {
            ByteSource clusteringByteSource = metadata.comparator.asByteComparable(end).asComparableBytes(ByteComparable.Version.OSS42);
            byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
            if (reversed)
                rowTreeIterator = pmemRowMapTree.getReverseHeadEntryIterator(clusteringBytes, includeEnd);
            else
                rowTreeIterator = pmemRowMapTree.getHeadEntryIterator(clusteringBytes, includeEnd);
        }
        else
        {
            if (reversed)
                rowTreeIterator = pmemRowMapTree.getReverseEntryIterator();
            else
                rowTreeIterator = pmemRowMapTree.getEntryIterator();
        }
        return rowTreeIterator;
    }

    private Iterator<LongART.Entry> buildRtmTreeIterator(LongART pmemRtmMapTree, Slice slice)
    {
        Iterator<LongART.Entry> rtmTreeIterator;
        boolean includeStart = slice.start().isInclusive();
        boolean includeEnd = slice.end().isInclusive();
        ClusteringBound<?> start = slice.start() == ClusteringBound.BOTTOM ? null : slice.start();
        ClusteringBound<?> end = slice.end() == ClusteringBound.TOP ? null : slice.end();

        if ((start != null) && (start.size() != 0))
        {
            ByteSource clusteringByteSource = metadata.comparator.asByteComparable(start).asComparableBytes(ByteComparable.Version.OSS42);
            byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
            if (reversed)
            {
                if ((end != null) && (end.size() != 0))
                {
                    ByteSource endClusteringByteSource = metadata.comparator.asByteComparable(end).asComparableBytes(ByteComparable.Version.OSS42);
                    byte[] endClusteringBytes = ByteSourceInverse.readBytes(endClusteringByteSource);
                    rtmTreeIterator = pmemRtmMapTree.getReverseHeadEntryIterator(endClusteringBytes, includeEnd);
                }
                else
                    rtmTreeIterator = pmemRtmMapTree.getReverseTailEntryIterator(clusteringBytes, includeStart);
            }
            else
                rtmTreeIterator = pmemRtmMapTree.getTailEntryIterator(clusteringBytes, includeStart);
        }
        else if ((end != null) && (end.size() != 0))
        {
            ByteSource clusteringByteSource = metadata.comparator.asByteComparable(end).asComparableBytes(ByteComparable.Version.OSS42);
            byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
            if (reversed)
                rtmTreeIterator = pmemRtmMapTree.getReverseHeadEntryIterator(clusteringBytes, includeEnd);
            else
                rtmTreeIterator = pmemRtmMapTree.getHeadEntryIterator(clusteringBytes, includeEnd);
        }
        else
        {
            if (reversed)
                rtmTreeIterator = pmemRtmMapTree.getReverseEntryIterator();
            else
                rtmTreeIterator = pmemRtmMapTree.getEntryIterator();
        }
        return rtmTreeIterator;
    }

    private int compareRtmWithEndSlice(RangeTombstoneMarker marker)
    {
        return comparator.compare(marker.clustering(), slice.close(reversed).clustering());
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    public boolean isReverseOrder()
    {
        return reversed;
    }

    public RegularAndStaticColumns columns()
    {
        return selection.fetchedColumns();
    }

    public DecoratedKey partitionKey()
    {
        return dkey;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return this.deletionTime;
    }

    public EncodingStats stats()
    {
        return this.stats;
    }

    @Override
    public void close()
    {
        closeCartIterator();
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
}
