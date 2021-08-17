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
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class AbstractSSTableIterator implements UnfilteredRowIterator
{
    protected final SSTableReader sstable;
    // We could use sstable.metadata(), but that can change during execution so it's good hygiene to grab an immutable instance
    protected final TableMetadata metadata;

    protected final DecoratedKey key;
    protected final DeletionTime partitionLevelDeletion;
    protected final ColumnFilter columns;
    protected final DeserializationHelper helper;

    protected final Row staticRow;
    protected final Reader reader;

    protected final FileHandle ifile;

    private boolean isClosed;

    protected final Slices slices;

    @SuppressWarnings("resource") // We need this because the analysis is not able to determine that we do close
                                  // file on every path where we created it.
    protected AbstractSSTableIterator(SSTableReader sstable,
                                      FileDataInput file,
                                      DecoratedKey key,
                                      RowIndexEntry indexEntry,
                                      Slices slices,
                                      ColumnFilter columnFilter,
                                      FileHandle ifile)
    {
        this.sstable = sstable;
        this.metadata = sstable.metadata();
        this.ifile = ifile;
        this.key = key;
        this.columns = columnFilter;
        this.slices = slices;
        this.helper = new DeserializationHelper(metadata, sstable.descriptor.version.correspondingMessagingVersion(), DeserializationHelper.Flag.LOCAL, columnFilter);

        if (indexEntry == null)
        {
            this.partitionLevelDeletion = DeletionTime.LIVE;
            this.reader = null;
            this.staticRow = Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            boolean shouldCloseFile = file == null;
            try
            {
                // We seek to the beginning to the partition if either:
                //   - the partition is not indexed; we then have a single block to read anyway
                //     (and we need to read the partition deletion time).
                //   - we're querying static columns.
                boolean needSeekAtPartitionStart = !indexEntry.isIndexed() || !columns.fetchedColumns().statics.isEmpty();

                if (needSeekAtPartitionStart)
                {
                    // Not indexed (or is reading static), set to the beginning of the partition and read partition level deletion there
                    if (file == null)
                        file = sstable.getFileDataInput(indexEntry.position);
                    else
                        file.seek(indexEntry.position);

                    ByteBufferUtil.skipShortLength(file); // Skip partition key
                    this.partitionLevelDeletion = DeletionTime.serializer.deserialize(file);

                    // Note that this needs to be called after file != null and after the partitionDeletion has been set, but before readStaticRow
                    // (since it uses it) so we can't move that up (but we'll be able to simplify as soon as we drop support for the old file format).
                    this.reader = createReader(indexEntry, file, shouldCloseFile);
                    this.staticRow = readStaticRow(sstable, file, helper, columns.fetchedColumns().statics);
                }
                else
                {
                    this.partitionLevelDeletion = indexEntry.deletionTime();
                    this.staticRow = Rows.EMPTY_STATIC_ROW;
                    this.reader = createReader(indexEntry, file, shouldCloseFile);
                }
                if (!partitionLevelDeletion.validate())
                    UnfilteredValidation.handleInvalid(metadata(), key, sstable, "partitionLevelDeletion="+partitionLevelDeletion.toString());

                if (reader != null && !slices.isEmpty())
                    reader.setForSlice(nextSlice());

                if (reader == null && file != null && shouldCloseFile)
                    file.close();
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                String filePath = file.getPath();
                if (shouldCloseFile)
                {
                    try
                    {
                        file.close();
                    }
                    catch (IOException suppressed)
                    {
                        e.addSuppressed(suppressed);
                    }
                }
                throw new CorruptSSTableException(e, filePath);
            }
        }
    }

    private Slice nextSlice()
    {
        return slices.get(nextSliceIndex());
    }

    /**
     * Returns the index of the next slice to process.
     * @return the index of the next slice to process
     */
    protected abstract int nextSliceIndex();

    /**
     * Checks if there are more slice to process.
     * @return {@code true} if there are more slice to process, {@code false} otherwise.
     */
    protected abstract boolean hasMoreSlices();

    private static Row readStaticRow(SSTableReader sstable,
                                     FileDataInput file,
                                     DeserializationHelper helper,
                                     Columns statics) throws IOException
    {
        if (!sstable.header.hasStatic())
            return Rows.EMPTY_STATIC_ROW;

        if (statics.isEmpty())
        {
            UnfilteredSerializer.serializer.skipStaticRow(file, sstable.header, helper);
            return Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            return UnfilteredSerializer.serializer.deserializeStaticRow(file, sstable.header, helper);
        }
    }

    protected abstract Reader createReaderInternal(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile);

    private Reader createReader(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile)
    {
        return slices.isEmpty() ? new NoRowsReader(file, shouldCloseFile)
                                : createReaderInternal(indexEntry, file, shouldCloseFile);
    };

    public TableMetadata metadata()
    {
        return metadata;
    }

    public RegularAndStaticColumns columns()
    {
        return columns.fetchedColumns();
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public EncodingStats stats()
    {
        return sstable.stats();
    }

    public boolean hasNext()
    {
        while (true)
        {
            if (reader == null)
                return false;

            if (reader.hasNext())
                return true;

            if (!hasMoreSlices())
                return false;

            slice(nextSlice());
        }
    }

    public Unfiltered next()
    {
        assert reader != null;
        return reader.next();
    }

    private void slice(Slice slice)
    {
        try
        {
            if (reader != null)
                reader.setForSlice(slice);
        }
        catch (IOException e)
        {
            try
            {
                closeInternal();
            }
            catch (IOException suppressed)
            {
                e.addSuppressed(suppressed);
            }
            sstable.markSuspect();
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private void closeInternal() throws IOException
    {
        // It's important to make closing idempotent since it would bad to double-close 'file' as its a RandomAccessReader
        // and its close is not idemptotent in the case where we recycle it.
        if (isClosed)
            return;

        if (reader != null)
            reader.close();

        isClosed = true;
    }

    public void close()
    {
        try
        {
            closeInternal();
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    protected abstract class Reader implements Iterator<Unfiltered>
    {
        private final boolean shouldCloseFile;
        public FileDataInput file;

        protected UnfilteredDeserializer deserializer;

        // Records the currently open range tombstone (if any)
        protected DeletionTime openMarker = null;

        protected Reader(FileDataInput file, boolean shouldCloseFile)
        {
            this.file = file;
            this.shouldCloseFile = shouldCloseFile;

            if (file != null)
                createDeserializer();
        }

        private void createDeserializer()
        {
            assert file != null && deserializer == null;
            deserializer = UnfilteredDeserializer.create(metadata, file, sstable.header, helper);
        }

        protected void seekToPosition(long position) throws IOException
        {
            // This may be the first time we're actually looking into the file
            if (file == null)
            {
                file = sstable.getFileDataInput(position);
                createDeserializer();
            }
            else
            {
                file.seek(position);
            }
        }

        protected void updateOpenMarker(RangeTombstoneMarker marker)
        {
            // Note that we always read index blocks in forward order so this method is always called in forward order
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        public boolean hasNext()
        {
            try
            {
                return hasNextInternal();
            }
            catch (IOException | IndexOutOfBoundsException e)
            {
                try
                {
                    closeInternal();
                }
                catch (IOException suppressed)
                {
                    e.addSuppressed(suppressed);
                }
                sstable.markSuspect();
                throw new CorruptSSTableException(e, reader.file.getPath());
            }
        }

        public Unfiltered next()
        {
            try
            {
                return nextInternal();
            }
            catch (IOException e)
            {
                try
                {
                    closeInternal();
                }
                catch (IOException suppressed)
                {
                    e.addSuppressed(suppressed);
                }
                sstable.markSuspect();
                throw new CorruptSSTableException(e, reader.file.getPath());
            }
        }

        // Set the reader so its hasNext/next methods return values within the provided slice
        public abstract void setForSlice(Slice slice) throws IOException;

        protected abstract boolean hasNextInternal() throws IOException;
        protected abstract Unfiltered nextInternal() throws IOException;

        public void close() throws IOException
        {
            if (shouldCloseFile && file != null)
                file.close();
        }
    }

    // Reader for when we have Slices.NONE but need to read static row or partition level deletion
    private class NoRowsReader extends AbstractSSTableIterator.Reader
    {
        private NoRowsReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
        }

        public void setForSlice(Slice slice) throws IOException
        {
            return;
        }

        protected boolean hasNextInternal() throws IOException
        {
            return false;
        }

        protected Unfiltered nextInternal() throws IOException
        {
            throw new NoSuchElementException();
        }
    }

    // Used by indexed readers to store where they are of the index.
    public static class IndexState implements AutoCloseable
    {
        private final Reader reader;
        private final ClusteringComparator comparator;

        private final RowIndexEntry indexEntry;
        private final RowIndexEntry.IndexInfoRetriever indexInfoRetriever;
        private final boolean reversed;

        private int currentIndexIdx;

        // Marks the beginning of the block corresponding to currentIndexIdx.
        private DataPosition mark;

        public IndexState(Reader reader, ClusteringComparator comparator, RowIndexEntry indexEntry, boolean reversed, FileHandle indexFile)
        {
            this.reader = reader;
            this.comparator = comparator;
            this.indexEntry = indexEntry;
            this.indexInfoRetriever = indexEntry.openWithIndex(indexFile);
            this.reversed = reversed;
            this.currentIndexIdx = reversed ? indexEntry.columnsIndexCount() : -1;
        }

        public boolean isDone()
        {
            return reversed ? currentIndexIdx < 0 : currentIndexIdx >= indexEntry.columnsIndexCount();
        }

        // Sets the reader to the beginning of blockIdx.
        public void setToBlock(int blockIdx) throws IOException
        {
            if (blockIdx >= 0 && blockIdx < indexEntry.columnsIndexCount())
            {
                reader.seekToPosition(columnOffset(blockIdx));
                mark = reader.file.mark();
                reader.deserializer.clearState();
            }

            currentIndexIdx = blockIdx;
            reader.openMarker = blockIdx > 0 ? index(blockIdx - 1).endOpenMarker : null;
        }

        private long columnOffset(int i) throws IOException
        {
            return indexEntry.position + index(i).offset;
        }

        public int blocksCount()
        {
            return indexEntry.columnsIndexCount();
        }

        // Update the block idx based on the current reader position if we're past the current block.
        // This only makes sense for forward iteration (for reverse ones, when we reach the end of a block we
        // should seek to the previous one, not update the index state and continue).
        public void updateBlock() throws IOException
        {
            assert !reversed;

            // If we get here with currentBlockIdx < 0, it means setToBlock() has never been called, so it means
            // we're about to read from the beginning of the partition, but haven't "prepared" the IndexState yet.
            // Do so by setting us on the first block.
            if (currentIndexIdx < 0)
            {
                setToBlock(0);
                return;
            }

            while (currentIndexIdx + 1 < indexEntry.columnsIndexCount() && isPastCurrentBlock())
            {
                reader.openMarker = currentIndex().endOpenMarker;
                ++currentIndexIdx;

                // We have to set the mark, and we have to set it at the beginning of the block. So if we're not at the beginning of the block, this forces us to a weird seek dance.
                // This can only happen when reading old file however.
                long startOfBlock = columnOffset(currentIndexIdx);
                long currentFilePointer = reader.file.getFilePointer();
                if (startOfBlock == currentFilePointer)
                {
                    mark = reader.file.mark();
                }
                else
                {
                    reader.seekToPosition(startOfBlock);
                    mark = reader.file.mark();
                    reader.seekToPosition(currentFilePointer);
                }
            }
        }

        // Check if we've crossed an index boundary (based on the mark on the beginning of the index block).
        public boolean isPastCurrentBlock() throws IOException
        {
            assert reader.deserializer != null;
            return reader.file.bytesPastMark(mark) >= currentIndex().width;
        }

        public int currentBlockIdx()
        {
            return currentIndexIdx;
        }

        public IndexInfo currentIndex() throws IOException
        {
            return index(currentIndexIdx);
        }

        public IndexInfo index(int i) throws IOException
        {
            return indexInfoRetriever.columnsIndex(i);
        }

        // Finds the index of the first block containing the provided bound, starting at the provided index.
        // Will be -1 if the bound is before any block, and blocksCount() if it is after every block.
        public int findBlockIndex(ClusteringBound<?> bound, int fromIdx) throws IOException
        {
            if (bound.isBottom())
                return -1;
            if (bound.isTop())
                return blocksCount();

            return indexFor(bound, fromIdx);
        }

        public int indexFor(ClusteringPrefix<?> name, int lastIndex) throws IOException
        {
            IndexInfo target = new IndexInfo(name, name, 0, 0, null);
            /*
            Take the example from the unit test, and say your index looks like this:
            [0..5][10..15][20..25]
            and you look for the slice [13..17].

            When doing forward slice, we are doing a binary search comparing 13 (the start of the query)
            to the lastName part of the index slot. You'll end up with the "first" slot, going from left to right,
            that may contain the start.

            When doing a reverse slice, we do the same thing, only using as a start column the end of the query,
            i.e. 17 in this example, compared to the firstName part of the index slots.  bsearch will give us the
            first slot where firstName > start ([20..25] here), so we subtract an extra one to get the slot just before.
            */
            int startIdx = 0;
            int endIdx = indexEntry.columnsIndexCount() - 1;

            if (reversed)
            {
                if (lastIndex < endIdx)
                {
                    endIdx = lastIndex;
                }
            }
            else
            {
                if (lastIndex > 0)
                {
                    startIdx = lastIndex;
                }
            }

            int index = binarySearch(target, comparator.indexComparator(reversed), startIdx, endIdx);
            return (index < 0 ? -index - (reversed ? 2 : 1) : index);
        }

        private int binarySearch(IndexInfo key, Comparator<IndexInfo> c, int low, int high) throws IOException
        {
            while (low <= high)
            {
                int mid = (low + high) >>> 1;
                IndexInfo midVal = index(mid);
                int cmp = c.compare(midVal, key);

                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return mid;
            }
            return -(low + 1);
        }

        @Override
        public String toString()
        {
            return String.format("IndexState(indexSize=%d, currentBlock=%d, reversed=%b)", indexEntry.columnsIndexCount(), currentIndexIdx, reversed);
        }

        @Override
        public void close() throws IOException
        {
            indexInfoRetriever.close();
        }
    }
}
