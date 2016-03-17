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
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.utils.ByteBufferUtil;

abstract class AbstractSSTableIterator implements UnfilteredRowIterator
{
    protected final SSTableReader sstable;
    protected final DecoratedKey key;
    protected final DeletionTime partitionLevelDeletion;
    protected final ColumnFilter columns;
    protected final SerializationHelper helper;

    protected final Row staticRow;
    protected final Reader reader;

    private final boolean isForThrift;

    private boolean isClosed;

    protected final Slices slices;
    protected int slice;

    @SuppressWarnings("resource") // We need this because the analysis is not able to determine that we do close
                                  // file on every path where we created it.
    protected AbstractSSTableIterator(SSTableReader sstable,
                                      FileDataInput file,
                                      DecoratedKey key,
                                      RowIndexEntry indexEntry,
                                      Slices slices,
                                      ColumnFilter columnFilter,
                                      boolean isForThrift)
    {
        this.sstable = sstable;
        this.key = key;
        this.columns = columnFilter;
        this.slices = slices;
        this.helper = new SerializationHelper(sstable.metadata, sstable.descriptor.version.correspondingMessagingVersion(), SerializationHelper.Flag.LOCAL, columnFilter);
        this.isForThrift = isForThrift;

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

                // For CQL queries on static compact tables, we only want to consider static value (only those are exposed),
                // but readStaticRow have already read them and might in fact have consumed the whole partition (when reading
                // the legacy file format), so set the reader to null so we don't try to read anything more. We can remove this
                // once we drop support for the legacy file format
                boolean needsReader = sstable.descriptor.version.storeRows() || isForThrift || !sstable.metadata.isStaticCompactTable();

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
                    this.reader = needsReader ? createReader(indexEntry, file, shouldCloseFile) : null;
                    this.staticRow = readStaticRow(sstable, file, helper, columns.fetchedColumns().statics, isForThrift, reader == null ? null : reader.deserializer);
                }
                else
                {
                    this.partitionLevelDeletion = indexEntry.deletionTime();
                    this.staticRow = Rows.EMPTY_STATIC_ROW;
                    this.reader = needsReader ? createReader(indexEntry, file, shouldCloseFile) : null;
                }

                if (reader != null && slices.size() > 0)
                    reader.setForSlice(slices.get(0));

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

    private static Row readStaticRow(SSTableReader sstable,
                                     FileDataInput file,
                                     SerializationHelper helper,
                                     Columns statics,
                                     boolean isForThrift,
                                     UnfilteredDeserializer deserializer) throws IOException
    {
        if (!sstable.descriptor.version.storeRows())
        {
            if (!sstable.metadata.isCompactTable())
            {
                assert deserializer != null;
                return deserializer.hasNext() && deserializer.nextIsStatic()
                     ? (Row)deserializer.readNext()
                     : Rows.EMPTY_STATIC_ROW;
            }

            // For compact tables, we use statics for the "column_metadata" definition. However, in the old format, those
            // "column_metadata" are intermingled as any other "cell". In theory, this means that we'd have to do a first
            // pass to extract the static values. However, for thrift, we'll use the ThriftResultsMerger right away which
            // will re-merge static values with dynamic ones, so we can just ignore static and read every cell as a
            // "dynamic" one. For CQL, if the table is a "static compact", then is has only static columns exposed and no
            // dynamic ones. So we do a pass to extract static columns here, but will have no more work to do. Otherwise,
            // the table won't have static columns.
            if (statics.isEmpty() || isForThrift)
                return Rows.EMPTY_STATIC_ROW;

            assert sstable.metadata.isStaticCompactTable();

            // As said above, if it's a CQL query and the table is a "static compact", the only exposed columns are the
            // static ones. So we don't have to mark the position to seek back later.
            return LegacyLayout.extractStaticColumns(sstable.metadata, file, statics);
        }

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

    protected abstract Reader createReader(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile);

    public CFMetaData metadata()
    {
        return sstable.metadata;
    }

    public PartitionColumns columns()
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

            if (++slice >= slices.size())
                return false;

            slice(slices.get(slice));
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
            deserializer = UnfilteredDeserializer.create(sstable.metadata, file, sstable.header, helper, partitionLevelDeletion, isForThrift);
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

        protected DeletionTime getAndClearOpenMarker()
        {
            DeletionTime toReturn = openMarker;
            openMarker = null;
            return toReturn;
        }

        public boolean hasNext() 
        {
            try
            {
                return hasNextInternal();
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

    // Used by indexed readers to store where they are of the index.
    protected static class IndexState
    {
        private final Reader reader;
        private final ClusteringComparator comparator;

        private final RowIndexEntry indexEntry;
        private final List<IndexHelper.IndexInfo> indexes;
        private final boolean reversed;

        private int currentIndexIdx;

        // Marks the beginning of the block corresponding to currentIndexIdx.
        private DataPosition mark;

        public IndexState(Reader reader, ClusteringComparator comparator, RowIndexEntry indexEntry, boolean reversed)
        {
            this.reader = reader;
            this.comparator = comparator;
            this.indexEntry = indexEntry;
            this.indexes = indexEntry.columnsIndex();
            this.reversed = reversed;
            this.currentIndexIdx = reversed ? indexEntry.columnsIndex().size() : -1;
        }

        public boolean isDone()
        {
            return reversed ? currentIndexIdx < 0 : currentIndexIdx >= indexes.size();
        }

        // Sets the reader to the beginning of blockIdx.
        public void setToBlock(int blockIdx) throws IOException
        {
            if (blockIdx >= 0 && blockIdx < indexes.size())
            {
                reader.seekToPosition(columnOffset(blockIdx));
                reader.deserializer.clearState();
            }

            currentIndexIdx = blockIdx;
            reader.openMarker = blockIdx > 0 ? indexes.get(blockIdx - 1).endOpenMarker : null;
            mark = reader.file.mark();
        }

        private long columnOffset(int i)
        {
            return indexEntry.position + indexes.get(i).offset;
        }

        public int blocksCount()
        {
            return indexes.size();
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

            while (currentIndexIdx + 1 < indexes.size() && isPastCurrentBlock())
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
        public boolean isPastCurrentBlock()
        {
            assert reader.deserializer != null;
            long correction = reader.deserializer.bytesReadForUnconsumedData();
            return reader.file.bytesPastMark(mark) - correction >= currentIndex().width;
        }

        public int currentBlockIdx()
        {
            return currentIndexIdx;
        }

        public IndexHelper.IndexInfo currentIndex()
        {
            return index(currentIndexIdx);
        }

        public IndexHelper.IndexInfo index(int i)
        {
            return indexes.get(i);
        }

        // Finds the index of the first block containing the provided bound, starting at the provided index.
        // Will be -1 if the bound is before any block, and blocksCount() if it is after every block.
        public int findBlockIndex(Slice.Bound bound, int fromIdx)
        {
            if (bound == Slice.Bound.BOTTOM)
                return -1;
            if (bound == Slice.Bound.TOP)
                return blocksCount();

            return IndexHelper.indexFor(bound, indexes, comparator, reversed, fromIdx);
        }

        @Override
        public String toString()
        {
            return String.format("IndexState(indexSize=%d, currentBlock=%d, reversed=%b)", indexes.size(), currentIndexIdx, reversed);
        }
    }
}
