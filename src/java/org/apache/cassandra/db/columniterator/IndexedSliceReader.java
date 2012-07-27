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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * This is a reader that finds the block for a starting column and returns blocks before/after it for each next call.
 * This function assumes that the CF is sorted by name and exploits the name index.
 */
class IndexedSliceReader extends AbstractIterator<OnDiskAtom> implements OnDiskAtomIterator
{
    private final ColumnFamily emptyColumnFamily;

    private final SSTableReader sstable;
    private final List<IndexHelper.IndexInfo> indexes;
    private final FileDataInput originalInput;
    private FileDataInput file;
    private final boolean reversed;
    private final ColumnSlice[] slices;
    private final BlockFetcher fetcher;
    private final Deque<OnDiskAtom> blockColumns = new ArrayDeque<OnDiskAtom>();
    private final AbstractType<?> comparator;

    /**
     * This slice reader assumes that slices are sorted correctly, e.g. that for forward lookup slices are in
     * lexicographic order of start elements and that for reverse lookup they are in reverse lexicographic order of
     * finish (reverse start) elements. i.e. forward: [a,b],[d,e],[g,h] reverse: [h,g],[e,d],[b,a]. This reader also
     * assumes that validation has been performed in terms of intervals (no overlapping intervals).
     */
    public IndexedSliceReader(SSTableReader sstable, RowIndexEntry indexEntry, FileDataInput input, ColumnSlice[] slices, boolean reversed)
    {
        this.sstable = sstable;
        this.originalInput = input;
        this.reversed = reversed;
        this.slices = slices;
        this.comparator = sstable.metadata.comparator;

        try
        {
            if (sstable.descriptor.version.hasPromotedIndexes)
            {
                this.indexes = indexEntry.columnsIndex();
                if (indexes.isEmpty())
                {
                    setToRowStart(sstable, indexEntry, input);
                    this.emptyColumnFamily = ColumnFamily.create(sstable.metadata);
                    emptyColumnFamily.delete(DeletionInfo.serializer().deserializeFromSSTable(file, sstable.descriptor.version));
                    fetcher = new SimpleBlockFetcher();
                }
                else
                {
                    this.emptyColumnFamily = ColumnFamily.create(sstable.metadata);
                    emptyColumnFamily.delete(indexEntry.deletionInfo());
                    fetcher = new IndexedBlockFetcher(indexEntry.position);
                }
            }
            else
            {
                setToRowStart(sstable, indexEntry, input);
                IndexHelper.skipBloomFilter(file);
                this.indexes = IndexHelper.deserializeIndex(file);
                this.emptyColumnFamily = ColumnFamily.create(sstable.metadata);
                emptyColumnFamily.delete(DeletionInfo.serializer().deserializeFromSSTable(file, sstable.descriptor.version));
                fetcher = indexes.isEmpty()
                        ? new SimpleBlockFetcher()
                        : new IndexedBlockFetcher(file.getFilePointer() + 4); // We still have the column count to
                                                                              // skip to get the basePosition
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.descriptor);
        }
    }

    /**
     * Sets the seek position to the start of the row for column scanning.
     */
    private void setToRowStart(SSTableReader reader, RowIndexEntry indexEntry, FileDataInput input) throws IOException
    {
        if (input == null)
        {
            this.file = sstable.getFileDataInput(indexEntry.position);
        }
        else
        {
            this.file = input;
            input.seek(indexEntry.position);
        }
        sstable.decodeKey(ByteBufferUtil.readWithShortLength(file));
        SSTableReader.readRowSize(file, sstable.descriptor);
    }

    public ColumnFamily getColumnFamily()
    {
        return emptyColumnFamily;
    }

    public DecoratedKey getKey()
    {
        throw new UnsupportedOperationException();
    }

    protected OnDiskAtom computeNext()
    {
        while (true)
        {
            OnDiskAtom column = blockColumns.poll();
            if (column == null)
            {
                if (!fetcher.fetchMoreData())
                    return endOfData();
            }
            else
            {
                return column;
            }
        }
    }

    public void close() throws IOException
    {
        if (originalInput == null && file != null)
            file.close();
    }

    protected void addColumn(OnDiskAtom col)
    {
        if (reversed)
            blockColumns.addFirst(col);
        else
            blockColumns.addLast(col);
    }

    private abstract class BlockFetcher
    {
        protected int currentSliceIdx;

        protected BlockFetcher(int sliceIdx)
        {
            this.currentSliceIdx = sliceIdx;
        }

        /*
         * Return the smallest key selected by the current ColumnSlice.
         */
        protected ByteBuffer currentStart()
        {
            return reversed ? slices[currentSliceIdx].finish : slices[currentSliceIdx].start;
        }

        /*
         * Return the biggest key selected by the current ColumnSlice.
         */
        protected ByteBuffer currentFinish()
        {
            return reversed ? slices[currentSliceIdx].start : slices[currentSliceIdx].finish;
        }

        protected abstract boolean setNextSlice();

        protected abstract boolean fetchMoreData();

        protected boolean isColumnBeforeSliceStart(OnDiskAtom column)
        {
            return isBeforeSliceStart(column.name());
        }

        protected boolean isBeforeSliceStart(ByteBuffer name)
        {
            ByteBuffer start = currentStart();
            return start.remaining() != 0 && comparator.compare(name, start) < 0;
        }

        protected boolean isColumnBeforeSliceFinish(OnDiskAtom column)
        {
            ByteBuffer finish = currentFinish();
            return finish.remaining() == 0 || comparator.compare(column.name(), finish) <= 0;
        }

        protected boolean isAfterSliceFinish(ByteBuffer name)
        {
            ByteBuffer finish = currentFinish();
            return finish.remaining() != 0 && comparator.compare(name, finish) > 0;
        }
    }

    private class IndexedBlockFetcher extends BlockFetcher
    {
        // where this row starts
        private final long basePosition;

        // the index entry for the next block to deserialize
        private int nextIndexIdx = -1;

        // index of the last block we've read from disk;
        private int lastDeserializedBlock = -1;

        // For reversed, keep columns at the beginning of the last deserialized block that
        // may still match a slice
        private final Deque<OnDiskAtom> prefetched;

        public IndexedBlockFetcher(long basePosition)
        {
            super(-1);
            this.basePosition = basePosition;
            this.prefetched = reversed ? new ArrayDeque<OnDiskAtom>() : null;
            setNextSlice();
        }

        protected boolean setNextSlice()
        {
            while (++currentSliceIdx < slices.length)
            {
                nextIndexIdx = IndexHelper.indexFor(slices[currentSliceIdx].start, indexes, comparator, reversed, nextIndexIdx);
                if (nextIndexIdx < 0 || nextIndexIdx >= indexes.size())
                    // no index block for that slice
                    continue;

                // Check if we can exclude this slice entirely from the index
                IndexInfo info = indexes.get(nextIndexIdx);
                if (reversed)
                {
                    if (!isBeforeSliceStart(info.lastName))
                        return true;
                }
                else
                {
                    if (!isAfterSliceFinish(info.firstName))
                        return true;
                }
            }
            nextIndexIdx = -1;
            return false;
        }

        protected boolean hasMoreSlice()
        {
            return currentSliceIdx < slices.length;
        }

        protected boolean fetchMoreData()
        {
            if (!hasMoreSlice())
                return false;

            // If we read blocks in reversed disk order, we may have columns from the previous block to handle.
            // Note that prefetched keeps columns in reversed disk order.
            if (reversed && !prefetched.isEmpty())
            {
                boolean gotSome = false;
                // Avoids some comparison when we know it's not useful
                boolean inSlice = false;

                OnDiskAtom prefetchedCol;
                while ((prefetchedCol = prefetched.peek() ) != null)
                {
                    // col is before slice, we update the slice
                    if (isColumnBeforeSliceStart(prefetchedCol))
                    {
                        inSlice = false;
                        if (!setNextSlice())
                            return false;
                    }
                    // col is within slice, all columns
                    // (we go in reverse, so as soon as we are in a slice, no need to check
                    // we're after the slice until we change slice)
                    else if (inSlice || isColumnBeforeSliceFinish(prefetchedCol))
                    {
                        blockColumns.addLast(prefetched.poll());
                        gotSome = true;
                        inSlice = true;
                    }
                    // if col is after slice, ignore
                    else
                    {
                        prefetched.poll();
                    }
                }
                if (gotSome)
                    return true;
            }
            try
            {
                return getNextBlock();
            }
            catch (IOException e)
            {
                throw new CorruptSSTableException(e, file.getPath());
            }
        }

        private boolean getNextBlock() throws IOException
        {
            if (lastDeserializedBlock == nextIndexIdx)
            {
                if (reversed)
                    nextIndexIdx--;
                else
                    nextIndexIdx++;
            }
            lastDeserializedBlock = nextIndexIdx;

            // Are we done?
            if (lastDeserializedBlock < 0 || lastDeserializedBlock >= indexes.size())
                return false;

            IndexInfo currentIndex = indexes.get(lastDeserializedBlock);

            /* seek to the correct offset to the data, and calculate the data size */
            long positionToSeek = basePosition + currentIndex.offset;

            // With new promoted indexes, our first seek in the data file will happen at that point.
            if (file == null)
                file = originalInput == null ? sstable.getFileDataInput(positionToSeek) : originalInput;

            OnDiskAtom.Serializer atomSerializer = emptyColumnFamily.getOnDiskSerializer();
            file.seek(positionToSeek);
            FileMark mark = file.mark();

            // We remenber when we are whithin a slice to avoid some comparison
            boolean inSlice = false;

            // scan from index start
            OnDiskAtom column = null;
            while (file.bytesPastMark(mark) < currentIndex.width)
            {
                // Only fetch a new column if we haven't dealt with the previous one.
                if (column == null)
                    column = atomSerializer.deserializeFromSSTable(file, sstable.descriptor.version);

                // col is before slice
                // (If in slice, don't bother checking that until we change slice)
                if (!inSlice && isColumnBeforeSliceStart(column))
                {
                    if (reversed)
                    {
                        // the next slice select columns that are before the current one, so it may
                        // match this column, so keep it around.
                        prefetched.addFirst(column);
                    }
                    column = null;
                }
                // col is within slice
                else if (isColumnBeforeSliceFinish(column))
                {
                    inSlice = true;
                    addColumn(column);
                    column = null;
                }
                // col is after slice.
                else
                {
                    // When reading forward, if we hit a column that sorts after the current slice, it means we're done with this slice.
                    // For reversed, this may either mean that we're done with the current slice, or that we need to read the previous
                    // index block. However, we can be sure that we are in the first case though (the current slice is done) if the first
                    // columns of the block were not part of the current slice, i.e. if we have columns in prefetched.
                    if (reversed && prefetched.isEmpty())
                        break;

                    if (!setNextSlice())
                        break;

                    inSlice = false;

                    // The next index block now corresponds to the first block that may have columns for the newly set slice.
                    // So if it's different from the current block, we're done with this block. And in that case, we know
                    // that our prefetched columns won't match.
                    if (nextIndexIdx != lastDeserializedBlock)
                    {
                        if (reversed)
                            prefetched.clear();
                        break;
                    }

                    // Even if the next slice may have column in this blocks, if we're reversed, those columns have been
                    // prefetched and we're done with that block
                    if (reversed)
                        break;

                    // otherwise, we will deal with that column at the next iteration
                }
            }
            return true;
        }
    }

    private class SimpleBlockFetcher extends BlockFetcher
    {
        public SimpleBlockFetcher() throws IOException
        {
            // Since we have to deserialize in order and will read all slices might as well reverse the slices and
            // behave as if it was not reversed
            super(reversed ? slices.length - 1 : 0);

            // We remenber when we are whithin a slice to avoid some comparison
            boolean inSlice = false;

            OnDiskAtom.Serializer atomSerializer = emptyColumnFamily.getOnDiskSerializer();
            int columns = file.readInt();

            for (int i = 0; i < columns; i++)
            {
                OnDiskAtom column = atomSerializer.deserializeFromSSTable(file, sstable.descriptor.version);

                // col is before slice
                // (If in slice, don't bother checking that until we change slice)
                if (!inSlice && isColumnBeforeSliceStart(column))
                    continue;

                // col is within slice
                if (isColumnBeforeSliceFinish(column))
                {
                    inSlice = true;
                    addColumn(column);
                }
                // col is after slice. more slices?
                else
                {
                    inSlice = false;
                    if (!setNextSlice())
                        break;
                }
            }
        }

        protected boolean setNextSlice()
        {
            if (reversed)
            {
                if (currentSliceIdx <= 0)
                    return false;

                currentSliceIdx--;
            }
            else
            {
                if (currentSliceIdx >= slices.length - 1)
                    return false;

                currentSliceIdx++;
            }
            return true;
        }

        protected boolean fetchMoreData()
        {
            return false;
        }
    }
}
