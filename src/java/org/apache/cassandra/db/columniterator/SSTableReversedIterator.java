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

import com.google.common.base.Verify;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.btree.BTree;

/**
 *  A Cell Iterator in reversed clustering order over SSTable
 */
public class SSTableReversedIterator extends AbstractSSTableIterator
{
    /**
     * The index of the slice being processed.
     */
    private int slice;

    public SSTableReversedIterator(SSTableReader sstable,
                                   FileDataInput file,
                                   DecoratedKey key,
                                   RowIndexEntry indexEntry,
                                   Slices slices,
                                   ColumnFilter columns,
                                   boolean isForThrift,
                                   FileHandle ifile)
    {
        super(sstable, file, key, indexEntry, slices, columns, isForThrift, ifile);
    }

    protected Reader createReaderInternal(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile)
    {
        return indexEntry.isIndexed()
             ? new ReverseIndexedReader(indexEntry, file, shouldCloseFile)
             : new ReverseReader(file, shouldCloseFile);
    }

    public boolean isReverseOrder()
    {
        return true;
    }

    protected int nextSliceIndex()
    {
        int next = slice;
        slice++;
        return slices.size() - (next + 1);
    }

    protected boolean hasMoreSlices()
    {
        return slice < slices.size();
    }

    private class ReverseReader extends Reader
    {
        protected ReusablePartitionData buffer;
        protected Iterator<Unfiltered> iterator;

        // Set in loadFromDisk () and used in setIterator to handle range tombstone extending on multiple index block. See
        // loadFromDisk for details. Note that those are always false for non-indexed readers.
        protected boolean skipFirstIteratedItem;
        protected boolean skipLastIteratedItem;

        protected Unfiltered mostRecentlyEmitted = null;

        private ReverseReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
        }

        protected ReusablePartitionData createBuffer(int blocksCount)
        {
            int estimatedRowCount = 16;
            int columnCount = metadata().partitionColumns().regulars.size();
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

        public void setForSlice(Slice slice) throws IOException
        {
            // If we have read the data, just create the iterator for the slice. Otherwise, read the data.
            if (buffer == null)
            {
                buffer = createBuffer(1);
                // Note that we can reuse that buffer between slices (we could alternatively re-read from disk
                // every time, but that feels more wasteful) so we want to include everything from the beginning.
                // We can stop at the slice end however since any following slice will be before that.
                loadFromDisk(null, slice.end(), false, false, null, null);
            }
            setIterator(slice);
        }

        protected void setIterator(Slice slice)
        {
            assert buffer != null;
            iterator = buffer.built.unfilteredIterator(columns, Slices.with(metadata().comparator, slice), true);

            if (!iterator.hasNext())
                return;

            if (skipFirstIteratedItem)
                iterator.next();

            if (skipLastIteratedItem)
                iterator = new SkipLastIterator(iterator);
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
            Unfiltered next = iterator.next();
            mostRecentlyEmitted = next;
            return next;
        }

        protected boolean stopReadingDisk() throws IOException
        {
            return false;
        }

        // checks if left prefix precedes right prefix
        private boolean precedes(ClusteringPrefix left, ClusteringPrefix right)
        {
            return metadata().comparator.compare(left, right) < 0;
        }

        // Reads the unfiltered from disk and load them into the reader buffer. It stops reading when either the partition
        // is fully read, or when stopReadingDisk() returns true.
        protected void loadFromDisk(ClusteringBound start,
                                    ClusteringBound end,
                                    boolean hasPreviousBlock,
                                    boolean hasNextBlock,
                                    ClusteringPrefix currentFirstName,
                                    ClusteringPrefix nextLastName) throws IOException
        {
            // start != null means it's the block covering the beginning of the slice, so it has to be the last block for this slice.
            assert start == null || !hasNextBlock;

            buffer.reset();
            skipFirstIteratedItem = false;
            skipLastIteratedItem = false;

            boolean isFirst = true;

            // If the start might be in this block, skip everything that comes before it.
            if (start != null)
            {
                while (deserializer.hasNext() && deserializer.compareNextTo(start) <= 0 && !stopReadingDisk())
                {
                    isFirst = false;
                    if (deserializer.nextIsRow())
                        deserializer.skipNext();
                    else
                        updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());
                }
            }

            // If we have an open marker, it's either one from what we just skipped or it's one that open in the next (or
            // one of the next) index block (if openMarker == openMarkerAtStartOfBlock).
            if (openMarker != null)
            {
                // We have to feed a marker to the buffer, because that marker is likely to be close later and ImmtableBTreePartition
                // doesn't take kindly to marker that comes without their counterpart. If that's the last block we're gonna read (for
                // the current slice at least) it's easy because we'll want to return that open marker at the end of the data in this
                // block anyway, so we have nothing more to do than adding it to the buffer.
                // If it's not the last block however, in which case we know we'll have start == null, it means this marker is really
                // open in a next block and so while we do need to add it the buffer for the reason mentioned above, we don't
                // want to "return" it just yet, we'll wait until we reach it in the next blocks. That's why we trigger
                // skipLastIteratedItem in that case (this is first item of the block, but we're iterating in reverse order
                // so it will be last returned by the iterator).
                ClusteringBound markerStart = start == null ? ClusteringBound.BOTTOM : start;
                buffer.add(new RangeTombstoneBoundMarker(markerStart, openMarker));
                if (hasNextBlock)
                    skipLastIteratedItem = true;
            }

            // Now deserialize everything until we reach our requested end (if we have one)
            // See SSTableIterator.ForwardRead.computeNext() for why this is a strict inequality below: this is the same
            // reasoning here.
            while (deserializer.hasNext()
                   && (end == null || deserializer.compareNextTo(end) < 0)
                   && !stopReadingDisk())
            {
                Unfiltered unfiltered = deserializer.readNext();

                if (isFirst && openMarker == null
                    && currentFirstName != null && nextLastName != null
                    && (precedes(currentFirstName, nextLastName) || precedes(unfiltered.clustering(), currentFirstName)))
                {
                    // Range tombstones spanning multiple index blocks when reading legacy sstables need special handling.
                    // Pre-3.0, the column index didn't encode open markers. Instead, open range tombstones were rewritten
                    // at the start of index blocks they at least partially covered. These rewritten RTs found at the
                    // beginning of index blocks need to be handled as though they were an open marker, otherwise iterator
                    // validation will fail and/or some rows will be excluded from the result. These rewritten RTs can be
                    // detected based on their relation to the current index block and the next one depending on what wrote
                    // the sstable. For sstables coming from a memtable flush, a rewritten RT will have a clustering value
                    // less than the first name of its index block. For sstables coming from compaction, the index block
                    // first name will be the RT open bound, which will be less than the last name of the next block. So,
                    // here we compare the first name of this block to the last name of the next block to detect the
                    // compaction case, and clustering value of the unfiltered we just read to the index block's first name
                    // to detect the flush case.
                    Verify.verify(!sstable.descriptor.version.storeRows());
                    Verify.verify(openMarker == null);
                    Verify.verify(!skipLastIteratedItem);
                    Verify.verify(unfiltered.isRangeTombstoneMarker());
                    buffer.add(unfiltered);
                    if (hasNextBlock)
                        skipLastIteratedItem = true;
                }
                else if (isFirst && nextLastName != null && !precedes(nextLastName, unfiltered.clustering()))
                {
                    // When dealing with old format sstable, we have the problem that a row can span 2 index block, i.e. it can
                    // start at the end of a block and end at the beginning of the next one. That's not a problem per se for
                    // UnfilteredDeserializer.OldFormatSerializer, since it always read rows entirely, even if they span index
                    // blocks, but as we reading index block in reverse we must be careful to not read the end of the row at
                    // beginning of a block before we're reading the beginning of that row. So what we do is that if we detect
                    // that the row starting this block is also the row ending the next one we're read (previous on disk), then
                    // we'll skip that first result and  let it be read with the next block.
                    Verify.verify(!sstable.descriptor.version.storeRows());
                    isFirst = false;
                }
                else if (unfiltered.isEmpty())
                {
                    isFirst = false;
                }
                else
                {
                    buffer.add(unfiltered);
                    isFirst = false;
                }

                if (unfiltered.isRangeTombstoneMarker())
                    updateOpenMarker((RangeTombstoneMarker)unfiltered);
            }

            if (!sstable.descriptor.version.storeRows()
                && deserializer.hasNext()
                && (end == null || deserializer.compareNextTo(end) < 0))
            {
                // Range tombstone start and end bounds are stored together in legacy sstables. When we read one, we
                // stash the closing bound until we reach the appropriate place to emit it, which is immediately before
                // the next unfiltered with a greater clustering.
                // If SSTRI considers the block exhausted before encountering such a clustering though, this end marker
                // will never be emitted. So here we just check if there's a closing bound left in the deserializer.
                // If there is, we compare it against the most recently emitted unfiltered (i.e.: the last unfiltered
                // that this RT would enclose. And we have to do THAT comparison because the last name field on the
                // current index block will be whatever was written at the end of the index block (i.e. the last name
                // physically in the block), not the closing bound of the range tombstone (i.e. the last name logically
                // in the block). If all this indicates that there is indeed a range tombstone we're missing, we add it
                // to the buffer and update the open marker field.
                Unfiltered unfiltered = deserializer.readNext();
                RangeTombstoneMarker marker = unfiltered.isRangeTombstoneMarker() ? (RangeTombstoneMarker) unfiltered : null;
                if (marker != null && marker.isClose(false)
                    && (mostRecentlyEmitted == null || precedes(marker.clustering(), mostRecentlyEmitted.clustering())))
                {
                    buffer.add(marker);
                    updateOpenMarker(marker);
                }
            }

            // If we have an open marker, we should close it before finishing
            if (openMarker != null)
            {
                // This is the reverse problem than the one at the start of the block. Namely, if it's the first block
                // we deserialize for the slice (the one covering the slice end basically), then it's easy, we just want
                // to add the close marker to the buffer and return it normally.
                // If it's note our first block (for the slice) however, it means that marker closed in a previously read
                // block and we have already returned it. So while we should still add it to the buffer for the sake of
                // not breaking ImmutableBTreePartition, we should skip it when returning from the iterator, hence the
                // skipFirstIteratedItem (this is the last item of the block, but we're iterating in reverse order so it will
                // be the first returned by the iterator).
                ClusteringBound markerEnd = end == null ? ClusteringBound.TOP : end;
                buffer.add(new RangeTombstoneBoundMarker(markerEnd, openMarker));
                if (hasPreviousBlock)
                    skipFirstIteratedItem = true;
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

        private ReverseIndexedReader(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
            this.indexState = new IndexState(this, sstable.metadata.comparator, indexEntry, true, ifile);
        }

        @Override
        public void close() throws IOException
        {
            super.close();
            this.indexState.close();
        }

        @Override
        public void setForSlice(Slice slice) throws IOException
        {
            this.slice = slice;

            // if our previous slicing already got us past the beginning of the sstable, we're done
            if (indexState.isDone())
            {
                iterator = Collections.emptyIterator();
                return;
            }

            // Find the first index block we'll need to read for the slice.
            int startIdx = indexState.findBlockIndex(slice.end(), indexState.currentBlockIdx());
            if (startIdx < 0)
            {
                iterator = Collections.emptyIterator();
                indexState.setToBlock(startIdx);
                return;
            }

            lastBlockIdx = indexState.findBlockIndex(slice.start(), startIdx);

            // If the last block to look (in reverse order) is after the very last block, we have nothing for that slice
            if (lastBlockIdx >= indexState.blocksCount())
            {
                assert startIdx >= indexState.blocksCount();
                iterator = Collections.emptyIterator();
                return;
            }

            // If we start (in reverse order) after the very last block, just read from the last one.
            if (startIdx >= indexState.blocksCount())
                startIdx = indexState.blocksCount() - 1;

            // Note that even if we were already set on the proper block (which would happen if the previous slice
            // requested ended on the same block this one start), we can't reuse it because when reading the previous
            // slice we've only read that block from the previous slice start. Re-reading also handles
            // skipFirstIteratedItem/skipLastIteratedItem that we would need to handle otherwise.
            indexState.setToBlock(startIdx);

            readCurrentBlock(false, startIdx != lastBlockIdx);
        }

        @Override
        protected boolean hasNextInternal() throws IOException
        {
            if (super.hasNextInternal())
                return true;

            while (true)
            {
                // We have nothing more for our current block, move the next one (so the one before on disk).
                int nextBlockIdx = indexState.currentBlockIdx() - 1;
                if (nextBlockIdx < 0 || nextBlockIdx < lastBlockIdx)
                    return false;

                // The slice start can be in
                indexState.setToBlock(nextBlockIdx);
                readCurrentBlock(true, nextBlockIdx != lastBlockIdx);

                // If an indexed block only contains data for a dropped column, the iterator will be empty, even
                // though we may still have data to read in subsequent blocks

                // also, for pre-3.0 storage formats, index blocks that only contain a single row and that row crosses
                // index boundaries, the iterator will be empty even though we haven't read everything we're intending
                // to read. In that case, we want to read the next index block. This shouldn't be possible in 3.0+
                // formats (see next comment)
                if (!iterator.hasNext() && nextBlockIdx > lastBlockIdx)
                {
                    continue;
                }

                return iterator.hasNext();
            }
        }

        /**
         * Reads the current block, the last one we've set.
         *
         * @param hasPreviousBlock is whether we have already read a previous block for the current slice.
         * @param hasNextBlock is whether we have more blocks to read for the current slice.
         */
        private void readCurrentBlock(boolean hasPreviousBlock, boolean hasNextBlock) throws IOException
        {
            if (buffer == null)
                buffer = createBuffer(indexState.blocksCount());

            int currentBlock = indexState.currentBlockIdx();

            // The slice start (resp. slice end) is only meaningful on the last (resp. first) block read (since again,
            // we read blocks in reverse order).
            boolean canIncludeSliceStart = !hasNextBlock;
            boolean canIncludeSliceEnd = !hasPreviousBlock;

            ClusteringPrefix currentFirstName = null;
            ClusteringPrefix nextLastName = null;
            if (!sstable.descriptor.version.storeRows() && currentBlock > 0)
            {
                currentFirstName = indexState.index(currentBlock).firstName;
                nextLastName = indexState.index(currentBlock - 1).lastName;
            }

            loadFromDisk(canIncludeSliceStart ? slice.start() : null,
                         canIncludeSliceEnd ? slice.end() : null,
                         hasPreviousBlock,
                         hasNextBlock,
                         currentFirstName,
                         nextLastName
            );
            setIterator(slice);
        }

        @Override
        protected boolean stopReadingDisk() throws IOException
        {
            return indexState.isPastCurrentBlock();
        }
    }

    private class ReusablePartitionData
    {
        private final CFMetaData metadata;
        private final DecoratedKey partitionKey;
        private final PartitionColumns columns;

        private MutableDeletionInfo.Builder deletionBuilder;
        private MutableDeletionInfo deletionInfo;
        private BTree.Builder<Row> rowBuilder;
        private ImmutableBTreePartition built;

        private ReusablePartitionData(CFMetaData metadata,
                                      DecoratedKey partitionKey,
                                      PartitionColumns columns,
                                      int initialRowCapacity)
        {
            this.metadata = metadata;
            this.partitionKey = partitionKey;
            this.columns = columns;
            this.rowBuilder = BTree.builder(metadata.comparator, initialRowCapacity);
        }


        public void add(Unfiltered unfiltered)
        {
            if (unfiltered.isRow())
                rowBuilder.add((Row)unfiltered);
            else
                deletionBuilder.add((RangeTombstoneMarker)unfiltered);
        }

        public void reset()
        {
            built = null;
            rowBuilder.reuse();
            deletionBuilder = MutableDeletionInfo.builder(partitionLevelDeletion, metadata().comparator, false);
        }

        public void build()
        {
            deletionInfo = deletionBuilder.build();
            built = new ImmutableBTreePartition(metadata, partitionKey, columns, Rows.EMPTY_STATIC_ROW, rowBuilder.build(),
                                                deletionInfo, EncodingStats.NO_STATS);
            deletionBuilder = null;
        }
    }

    private static class SkipLastIterator extends AbstractIterator<Unfiltered>
    {
        private final Iterator<Unfiltered> iterator;

        private SkipLastIterator(Iterator<Unfiltered> iterator)
        {
            this.iterator = iterator;
        }

        protected Unfiltered computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Unfiltered next = iterator.next();
            return iterator.hasNext() ? next : endOfData();
        }
    }
}
