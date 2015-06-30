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
package org.apache.cassandra.db.columniterator;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.AbstractThreadUnsafePartition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

/**
 *  A Cell Iterator in reversed clustering order over SSTable
 */
public class SSTableReversedIterator extends AbstractSSTableIterator
{
    public SSTableReversedIterator(SSTableReader sstable, DecoratedKey key, ColumnFilter columns, boolean isForThrift)
    {
        this(sstable, null, key, sstable.getPosition(key, SSTableReader.Operator.EQ), columns, isForThrift);
    }

    public SSTableReversedIterator(SSTableReader sstable,
                                   FileDataInput file,
                                   DecoratedKey key,
                                   RowIndexEntry indexEntry,
                                   ColumnFilter columns,
                                   boolean isForThrift)
    {
        super(sstable, file, key, indexEntry, columns, isForThrift);
    }

    protected Reader createReader(RowIndexEntry indexEntry, FileDataInput file, boolean isAtPartitionStart, boolean shouldCloseFile)
    {
        return indexEntry.isIndexed()
             ? new ReverseIndexedReader(indexEntry, file, isAtPartitionStart, shouldCloseFile)
             : new ReverseReader(file, isAtPartitionStart, shouldCloseFile);
    }

    public boolean isReverseOrder()
    {
        return true;
    }

    private class ReverseReader extends Reader
    {
        protected ReusablePartitionData buffer;
        protected Iterator<Unfiltered> iterator;

        private ReverseReader(FileDataInput file, boolean isAtPartitionStart, boolean shouldCloseFile)
        {
            super(file, isAtPartitionStart, shouldCloseFile);
        }

        protected ReusablePartitionData createBuffer(int blocksCount)
        {
            int estimatedRowCount = 16;
            int columnCount = metadata().partitionColumns().regulars.columnCount();
            if (columnCount == 0 || metadata().clusteringColumns().isEmpty())
            {
                estimatedRowCount = 1;
            }
            else
            {
                try
                {
                    // To avoid wasted resizing we guess-estimate the number of rows we're likely to read. For that
                    // we use the stats on the number of rows per partition for that sstable.
                    // FIXME: so far we only keep stats on cells, so to get a rough estimate on the number of rows,
                    // we divide by the number of regular columns the table has. We should fix once we collect the
                    // stats on rows
                    int estimatedRowsPerPartition = (int)(sstable.getEstimatedColumnCount().percentile(0.75) / columnCount);
                    estimatedRowCount = Math.max(estimatedRowsPerPartition / blocksCount, 1);
                }
                catch (IllegalStateException e)
                {
                    // The EstimatedHistogram mean() method can throw this (if it overflows). While such overflow
                    // shouldn't happen, it's not worth taking the risk of letting the exception bubble up.
                }
            }
            return new ReusablePartitionData(metadata(), partitionKey(), columns(), estimatedRowCount);
        }

        protected void init() throws IOException
        {
            // We should always have been initialized (at the beginning of the partition). Only indexed readers may
            // have to initialize.
            throw new IllegalStateException();
        }

        public void setForSlice(Slice slice) throws IOException
        {
            // If we have read the data, just create the iterator for the slice. Otherwise, read the data.
            if (buffer == null)
            {
                buffer = createBuffer(1);
                // Note that we can reuse that buffer between slices (we could alternatively re-read from disk
                // every time, but that feels more wasteful) so we want to include everything from the beginning.
                // We can stop at the slice end however since any following slice will be before that.
                loadFromDisk(null, slice.end());
            }
            setIterator(slice);
        }

        protected void setIterator(Slice slice)
        {
            assert buffer != null;
            iterator = buffer.unfilteredIterator(columns, Slices.with(metadata().comparator, slice), true);
        }

        protected boolean hasNextInternal() throws IOException
        {
            // If we've never called setForSlice, we're reading everything
            if (iterator == null)
                setForSlice(Slice.ALL);

            return iterator.hasNext();
        }

        protected Unfiltered nextInternal() throws IOException
        {
            if (!hasNext())
                throw new NoSuchElementException();
            return iterator.next();
        }

        protected boolean stopReadingDisk()
        {
            return false;
        }

        // Reads the unfiltered from disk and load them into the reader buffer. It stops reading when either the partition
        // is fully read, or when stopReadingDisk() returns true.
        protected void loadFromDisk(Slice.Bound start, Slice.Bound end) throws IOException
        {
            buffer.reset();

            // If the start might be in this block, skip everything that comes before it.
            if (start != null)
            {
                while (deserializer.hasNext() && deserializer.compareNextTo(start) <= 0 && !stopReadingDisk())
                {
                    if (deserializer.nextIsRow())
                        deserializer.skipNext();
                    else
                        updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());
                }
            }

            // If we have an open marker, it's either one from what we just skipped (if start != null), or it's from the previous index block.
            if (openMarker != null)
            {
                RangeTombstone.Bound markerStart = start == null ? RangeTombstone.Bound.BOTTOM : RangeTombstone.Bound.fromSliceBound(start);
                buffer.add(new RangeTombstoneBoundMarker(markerStart, openMarker));
            }

            // Now deserialize everything until we reach our requested end (if we have one)
            while (deserializer.hasNext()
                   && (end == null || deserializer.compareNextTo(end) <= 0)
                   && !stopReadingDisk())
            {
                Unfiltered unfiltered = deserializer.readNext();
                buffer.add(unfiltered);

                if (unfiltered.isRangeTombstoneMarker())
                    updateOpenMarker((RangeTombstoneMarker)unfiltered);
            }

            // If we have an open marker, we should close it before finishing
            if (openMarker != null)
            {
                // If we have no end and still an openMarker, this means we're indexed and the marker is closed in a following block.
                RangeTombstone.Bound markerEnd = end == null ? RangeTombstone.Bound.TOP : RangeTombstone.Bound.fromSliceBound(end);
                buffer.add(new RangeTombstoneBoundMarker(markerEnd, getAndClearOpenMarker()));
            }

            buffer.build();
        }
    }

    private class ReverseIndexedReader extends ReverseReader
    {
        private final IndexState indexState;

        // The slice we're currently iterating over
        private Slice slice;
        // The last index block to consider for the slice
        private int lastBlockIdx;

        private ReverseIndexedReader(RowIndexEntry indexEntry, FileDataInput file, boolean isAtPartitionStart, boolean shouldCloseFile)
        {
            super(file, isAtPartitionStart, shouldCloseFile);
            this.indexState = new IndexState(this, sstable.metadata.comparator, indexEntry, true);
        }

        protected void init() throws IOException
        {
            // If this is called, it means we're calling hasNext() before any call to setForSlice. Which means
            // we're reading everything from the end. So just set us up on the last block.
            indexState.setToBlock(indexState.blocksCount() - 1);
        }

        @Override
        public void setForSlice(Slice slice) throws IOException
        {
            this.slice = slice;
            isInit = true;

            // if our previous slicing already got us pas the beginning of the sstable, we're done
            if (indexState.isDone())
            {
                iterator = Collections.emptyIterator();
                return;
            }

            // Find the first index block we'll need to read for the slice.
            int startIdx = indexState.findBlockIndex(slice.end());
            if (startIdx < 0)
            {
                iterator = Collections.emptyIterator();
                return;
            }

            boolean isCurrentBlock = startIdx == indexState.currentBlockIdx();
            if (!isCurrentBlock)
                indexState.setToBlock(startIdx);

            lastBlockIdx = indexState.findBlockIndex(slice.start());

            if (!isCurrentBlock)
                readCurrentBlock(true);

            setIterator(slice);
        }

        @Override
        protected boolean hasNextInternal() throws IOException
        {
            if (super.hasNextInternal())
                return true;

            // We have nothing more for our current block, move the previous one.
            int previousBlockIdx = indexState.currentBlockIdx() - 1;
            if (previousBlockIdx < 0 || previousBlockIdx < lastBlockIdx)
                return false;

            // The slice start can be in 
            indexState.setToBlock(previousBlockIdx);
            readCurrentBlock(false);
            setIterator(slice);
            // since that new block is within the bounds we've computed in setToSlice(), we know there will
            // always be something matching the slice unless we're on the lastBlockIdx (in which case there
            // may or may not be results, but if there isn't, we're done for the slice).
            return iterator.hasNext();
        }

        /**
         * Reads the current block, the last one we've set.
         *
         * @param canIncludeSliceEnd whether the block can include the slice end.
         */
        private void readCurrentBlock(boolean canIncludeSliceEnd) throws IOException
        {
            if (buffer == null)
                buffer = createBuffer(indexState.blocksCount());

            boolean canIncludeSliceStart = indexState.currentBlockIdx() == lastBlockIdx;
            loadFromDisk(canIncludeSliceStart ? slice.start() : null, canIncludeSliceEnd ? slice.end() : null);
        }

        @Override
        protected boolean stopReadingDisk()
        {
            return indexState.isPastCurrentBlock();
        }
    }

    private class ReusablePartitionData extends AbstractThreadUnsafePartition
    {
        private MutableDeletionInfo.Builder deletionBuilder;
        private MutableDeletionInfo deletionInfo;

        private ReusablePartitionData(CFMetaData metadata,
                                      DecoratedKey partitionKey,
                                      PartitionColumns columns,
                                      int initialRowCapacity)
        {
            super(metadata, partitionKey, columns, new ArrayList<>(initialRowCapacity));
        }

        public DeletionInfo deletionInfo()
        {
            return deletionInfo;
        }

        protected boolean canHaveShadowedData()
        {
            return false;
        }

        public Row staticRow()
        {
            return Rows.EMPTY_STATIC_ROW; // we don't actually use that
        }

        public RowStats stats()
        {
            return RowStats.NO_STATS; // we don't actually use that
        }

        public void add(Unfiltered unfiltered)
        {
            if (unfiltered.isRow())
                rows.add((Row)unfiltered);
            else
                deletionBuilder.add((RangeTombstoneMarker)unfiltered);
        }

        public void reset()
        {
            rows.clear();
            deletionBuilder = MutableDeletionInfo.builder(partitionLevelDeletion, metadata().comparator, false);
        }

        public void build()
        {
            deletionInfo = deletionBuilder.build();
            deletionBuilder = null;
        }
    }
}
