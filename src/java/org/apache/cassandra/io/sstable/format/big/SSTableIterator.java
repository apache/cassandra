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
package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;

/**
 *  A Cell Iterator over SSTable
 */
public class SSTableIterator extends AbstractBigTableIterator
{
    /**
     * The index of the slice being processed.
     */
    private int slice;

    public SSTableIterator(SSTableReader sstable,
                           FileDataInput file,
                           DecoratedKey key,
                           BigTableRowIndexEntry indexEntry,
                           Slices slices,
                           ColumnFilter columns,
                           FileHandle ifile)
    {
        super(sstable, file, key, indexEntry, slices, columns, ifile);
    }

    @SuppressWarnings("resource") // caller to close
    protected RowReader createReaderInternal(BigTableRowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile)
    {
        return indexEntry.isIndexed()
             ? new ForwardIndexedReader(indexEntry, file, shouldCloseFile)
             : new ForwardReader(file, shouldCloseFile);
    }

    protected int nextSliceIndex()
    {
        int next = slice;
        slice++;
        return next;
    }

    protected boolean hasMoreSlices()
    {
        return slice < slices.size();
    }

    public boolean isReverseOrder()
    {
        return false;
    }

    private class ForwardReader extends RowReader
    {
        // The start of the current slice. This will be null as soon as we know we've passed that bound.
        protected ClusteringBound<?> start;
        // The end of the current slice. Will never be null.
        protected ClusteringBound<?> end = BufferClusteringBound.TOP;

        protected Unfiltered next; // the next element to return: this is computed by hasNextInternal().

        protected boolean sliceDone; // set to true once we know we have no more result for the slice. This is in particular
                                     // used by the indexed reader when we know we can't have results based on the index.

        private ForwardReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
        }

        public void setForSlice(Slice slice) throws IOException
        {
            start = slice.start().isBottom() ? null : slice.start();
            end = slice.end();

            sliceDone = false;
            next = null;
        }

        // Skip all data that comes before the currently set slice.
        // Return what should be returned at the end of this, or null if nothing should.
        private Unfiltered handlePreSliceData() throws IOException
        {
            assert deserializer != null;

            // Note that the following comparison is not strict. The reason is that the only cases
            // where it can be == is if the "next" is a RT start marker (either a '[' of a ')[' boundary),
            // and if we had a strict inequality and an open RT marker before this, we would issue
            // the open marker first, and then return then next later, which would send in the
            // stream both '[' (or '(') and then ')[' for the same clustering value, which is wrong.
            // By using a non-strict inequality, we avoid that problem (if we do get ')[' for the same
            // clustering value than the slice, we'll simply record it in 'openMarker').
            while (deserializer.hasNext() && deserializer.compareNextTo(start) <= 0)
            {
                if (deserializer.nextIsRow())
                    deserializer.skipNext();
                else
                    updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());
            }

            ClusteringBound<?> sliceStart = start;
            start = null;

            // We've reached the beginning of our queried slice. If we have an open marker
            // we should return that first.
            if (openMarker != null)
                return new RangeTombstoneBoundMarker(sliceStart, openMarker);

            return null;
        }

        // Compute the next element to return, assuming we're in the middle to the slice
        // and the next element is either in the slice, or just after it. Returns null
        // if we're done with the slice.
        protected Unfiltered computeNext() throws IOException
        {
            assert deserializer != null;

            while (true)
            {
                // We use a same reasoning as in handlePreSliceData regarding the strictness of the inequality below.
                // We want to exclude deserialized unfiltered equal to end, because 1) we won't miss any rows since those
                // woudn't be equal to a slice bound and 2) a end bound can be equal to a start bound
                // (EXCL_END(x) == INCL_START(x) for instance) and in that case we don't want to return start bound because
                // it's fundamentally excluded. And if the bound is a  end (for a range tombstone), it means it's exactly
                // our slice end, but in that  case we will properly close the range tombstone anyway as part of our "close
                // an open marker" code in hasNextInterna
                if (!deserializer.hasNext() || deserializer.compareNextTo(end) >= 0)
                    return null;

                Unfiltered next = deserializer.readNext();
                UnfilteredValidation.maybeValidateUnfiltered(next, metadata(), key, sstable);
                // We may get empty row for the same reason expressed on UnfilteredSerializer.deserializeOne.
                if (next.isEmpty())
                    continue;

                if (next.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                    updateOpenMarker((RangeTombstoneMarker) next);
                return next;
            }
        }

        protected boolean hasNextInternal() throws IOException
        {
            if (next != null)
                return true;

            if (sliceDone)
                return false;

            if (start != null)
            {
                Unfiltered unfiltered = handlePreSliceData();
                if (unfiltered != null)
                {
                    next = unfiltered;
                    return true;
                }
            }

            next = computeNext();
            if (next != null)
                return true;

            // for current slice, no data read from deserialization
            sliceDone = true;
            // If we have an open marker, we should not close it, there could be more slices
            if (openMarker != null)
            {
                next = new RangeTombstoneBoundMarker(end, openMarker);
                return true;
            }
            return false;
        }

        protected Unfiltered nextInternal() throws IOException
        {
            if (!hasNextInternal())
                throw new NoSuchElementException();

            Unfiltered toReturn = next;
            next = null;
            return toReturn;
        }
    }

    private class ForwardIndexedReader extends ForwardReader
    {
        private final IndexState indexState;

        private int lastBlockIdx; // the last index block that has data for the current query

        private ForwardIndexedReader(BigTableRowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
            this.indexState = new IndexState(this, metadata.comparator, indexEntry, false, ifile);
            this.lastBlockIdx = indexState.blocksCount(); // if we never call setForSlice, that's where we want to stop
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
            super.setForSlice(slice);

            // if our previous slicing already got us the biggest row in the sstable, we're done
            if (indexState.isDone())
            {
                sliceDone = true;
                return;
            }

            // Find the first index block we'll need to read for the slice.
            int startIdx = indexState.findBlockIndex(slice.start(), indexState.currentBlockIdx());
            if (startIdx >= indexState.blocksCount())
            {
                sliceDone = true;
                return;
            }

            // Find the last index block we'll need to read for the slice.
            lastBlockIdx = indexState.findBlockIndex(slice.end(), startIdx);

            // If the slice end is before the very first block, we have nothing for that slice
            if (lastBlockIdx < 0)
            {
                assert startIdx < 0;
                sliceDone = true;
                return;
            }

            // If we start before the very first block, just read from the first one.
            if (startIdx < 0)
                startIdx = 0;

            // If that's the last block we were reading, we're already where we want to be. Otherwise,
            // seek to that first block
            if (startIdx != indexState.currentBlockIdx())
                indexState.setToBlock(startIdx);

            // The index search is based on the last name of the index blocks, so at that point we have that:
            //   1) indexes[currentIdx - 1].lastName < slice.start <= indexes[currentIdx].lastName
            //   2) indexes[lastBlockIdx - 1].lastName < slice.end <= indexes[lastBlockIdx].lastName
            // so if currentIdx == lastBlockIdx and slice.end < indexes[currentIdx].firstName, we're guaranteed that the
            // whole slice is between the previous block end and this block start, and thus has no corresponding
            // data. One exception is if the previous block ends with an openMarker as it will cover our slice
            // and we need to return it.
            if (indexState.currentBlockIdx() == lastBlockIdx
                && metadata().comparator.compare(slice.end(), indexState.currentIndex().firstName) < 0
                && openMarker == null)
            {
                sliceDone = true;
            }
        }

        @Override
        protected Unfiltered computeNext() throws IOException
        {
            while (true)
            {
                // Our previous read might have made us cross an index block boundary. If so, update our informations.
                // If we read from the beginning of the partition, this is also what will initialize the index state.
                indexState.updateBlock();

                // Return the next unfiltered unless we've reached the end, or we're beyond our slice
                // end (note that unless we're on the last block for the slice, there is no point
                // in checking the slice end).
                if (indexState.isDone()
                    || indexState.currentBlockIdx() > lastBlockIdx
                    || !deserializer.hasNext()
                    || (indexState.currentBlockIdx() == lastBlockIdx && deserializer.compareNextTo(end) >= 0))
                    return null;


                Unfiltered next = deserializer.readNext();
                UnfilteredValidation.maybeValidateUnfiltered(next, metadata(), key, sstable);
                // We may get empty row for the same reason expressed on UnfilteredSerializer.deserializeOne.
                if (next.isEmpty())
                    continue;

                if (next.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                    updateOpenMarker((RangeTombstoneMarker) next);
                return next;
            }
        }
    }
}
