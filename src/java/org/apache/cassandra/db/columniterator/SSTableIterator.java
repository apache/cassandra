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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 *  A Cell Iterator over SSTable
 */
public class SSTableIterator extends AbstractSSTableIterator
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIterator.class);

    public SSTableIterator(SSTableReader sstable, DecoratedKey key, ColumnFilter columns, boolean isForThrift)
    {
        this(sstable, null, key, sstable.getPosition(key, SSTableReader.Operator.EQ), columns, isForThrift);
    }

    public SSTableIterator(SSTableReader sstable,
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
             ? new ForwardIndexedReader(indexEntry, file, isAtPartitionStart, shouldCloseFile)
             : new ForwardReader(file, isAtPartitionStart, shouldCloseFile);
    }

    public boolean isReverseOrder()
    {
        return false;
    }

    private class ForwardReader extends Reader
    {
        private ForwardReader(FileDataInput file, boolean isAtPartitionStart, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
            assert isAtPartitionStart;
        }

        public boolean hasNext() throws IOException
        {
            assert deserializer != null;
            return deserializer.hasNext();
        }

        public Unfiltered next() throws IOException
        {
            return deserializer.readNext();
        }

        public Iterator<Unfiltered> slice(final Slice slice) throws IOException
        {
            return new AbstractIterator<Unfiltered>()
            {
                private boolean beforeStart = true;

                protected Unfiltered computeNext()
                {
                    try
                    {
                        // While we're before the start of the slice, we can skip row but we should keep
                        // track of open range tombstones
                        if (beforeStart)
                        {
                            // Note that the following comparison is not strict. The reason is that the only cases
                            // where it can be == is if the "next" is a RT start marker (either a '[' of a ')[' boundary),
                            // and if we had a strict inequality and an open RT marker before this, we would issue
                            // the open marker first, and then return then next later, which would yet in the
                            // stream both '[' (or '(') and then ')[' for the same clustering value, which is wrong.
                            // By using a non-strict inequality, we avoid that problem (if we do get ')[' for the same
                            // clustering value than the slice, we'll simply record it in 'openMarker').
                            while (deserializer.hasNext() && deserializer.compareNextTo(slice.start()) <= 0)
                            {
                                if (deserializer.nextIsRow())
                                    deserializer.skipNext();
                                else
                                    updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());
                            }

                            beforeStart = false;

                            // We've reached the beginning of our queried slice. If we have an open marker
                            // we should return that first.
                            if (openMarker != null)
                                return new RangeTombstoneBoundMarker(slice.start(), openMarker);
                        }

                        if (deserializer.hasNext() && deserializer.compareNextTo(slice.end()) <= 0)
                        {
                            Unfiltered next = deserializer.readNext();
                            if (next.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                                updateOpenMarker((RangeTombstoneMarker)next);
                            return next;
                        }

                        // If we have an open marker, we should close it before finishing
                        if (openMarker != null)
                            return new RangeTombstoneBoundMarker(slice.end(), getAndClearOpenMarker());

                        return endOfData();
                    }
                    catch (IOException e)
                    {
                        try
                        {
                            close();
                        }
                        catch (IOException suppressed)
                        {
                            e.addSuppressed(suppressed);
                        }
                        sstable.markSuspect();
                        throw new CorruptSSTableException(e, file.getPath());
                    }
                }
            };
        }
    }

    private class ForwardIndexedReader extends IndexedReader
    {
        private ForwardIndexedReader(RowIndexEntry indexEntry, FileDataInput file, boolean isAtPartitionStart, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile, indexEntry, isAtPartitionStart);
        }

        public boolean hasNext() throws IOException
        {
            // If it's called before we've created the file, create it. This then mean
            // we're reading from the beginning of the partition.
            if (!isInit)
            {
                seekToPosition(indexEntry.position);
                ByteBufferUtil.skipShortLength(file); // partition key
                DeletionTime.serializer.skip(file);   // partition deletion
                if (sstable.header.hasStatic())
                    UnfilteredSerializer.serializer.skipStaticRow(file, sstable.header, helper);
                isInit = true;
            }
            return deserializer.hasNext();
        }

        public Unfiltered next() throws IOException
        {
            return deserializer.readNext();
        }

        public Iterator<Unfiltered> slice(final Slice slice) throws IOException
        {
            final List<IndexHelper.IndexInfo> indexes = indexEntry.columnsIndex();

            // if our previous slicing already got us the biggest row in the sstable, we're done
            if (currentIndexIdx >= indexes.size())
                return Collections.emptyIterator();

            // Find the first index block we'll need to read for the slice.
            final int startIdx = IndexHelper.indexFor(slice.start(), indexes, sstable.metadata.comparator, false, currentIndexIdx);
            if (startIdx >= indexes.size())
                return Collections.emptyIterator();

            // If that's the last block we were reading, we're already where we want to be. Otherwise,
            // seek to that first block
            if (startIdx != currentIndexIdx)
                updateBlock(startIdx);

            // Find the last index block we'll need to read for the slice.
            final int endIdx = IndexHelper.indexFor(slice.end(), indexes, sstable.metadata.comparator, false, startIdx);

            final IndexHelper.IndexInfo startIndex = currentIndex();

            // The index search is based on the last name of the index blocks, so at that point we have that:
            //   1) indexes[startIdx - 1].lastName < slice.start <= indexes[startIdx].lastName
            //   2) indexes[endIdx - 1].lastName < slice.end <= indexes[endIdx].lastName
            // so if startIdx == endIdx and slice.end < indexes[startIdx].firstName, we're guaranteed that the
            // whole slice is between the previous block end and this bloc start, and thus has no corresponding
            // data. One exception is if the previous block ends with an openMarker as it will cover our slice
            // and we need to return it.
            if (startIdx == endIdx && metadata().comparator.compare(slice.end(), startIndex.firstName) < 0 && openMarker == null && sstable.descriptor.version.storeRows())
                return Collections.emptyIterator();

            return new AbstractIterator<Unfiltered>()
            {
                private boolean beforeStart = true;
                private int currentIndexIdx = startIdx;

                protected Unfiltered computeNext()
                {
                    try
                    {
                        // While we're before the start of the slice, we can skip row but we should keep
                        // track of open range tombstones
                        if (beforeStart)
                        {
                            // See ForwardReader equivalent method to see why this inequality is not strict.
                            while (deserializer.hasNext() && deserializer.compareNextTo(slice.start()) <= 0)
                            {
                                if (deserializer.nextIsRow())
                                    deserializer.skipNext();
                                else
                                    updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());
                            }

                            beforeStart = false;

                            // We've reached the beginning of our queried slice. If we have an open marker
                            // we should return that first.
                            if (openMarker != null)
                                return new RangeTombstoneBoundMarker(slice.start(), openMarker);
                        }

                        // If we've crossed an index block boundary, update our informations
                        if (currentIndexIdx < indexes.size() && file.bytesPastMark(mark) >= currentIndex().width)
                            updateBlock(++currentIndexIdx);

                        // Return the next atom unless we've reached the end, or we're beyond our slice
                        // end (note that unless we're on the last block for the slice, there is no point
                        // in checking the slice end).
                        if (currentIndexIdx < indexes.size()
                            && currentIndexIdx <= endIdx
                            && deserializer.hasNext()
                            && (currentIndexIdx != endIdx || deserializer.compareNextTo(slice.end()) <= 0))
                        {
                            Unfiltered next = deserializer.readNext();
                            if (next.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                                updateOpenMarker((RangeTombstoneMarker)next);
                            return next;
                        }

                        // If we have an open marker, we should close it before finishing
                        if (openMarker != null)
                            return new RangeTombstoneBoundMarker(slice.end(), getAndClearOpenMarker());

                        return endOfData();
                    }
                    catch (IOException e)
                    {
                        try
                        {
                            close();
                        }
                        catch (IOException suppressed)
                        {
                            e.addSuppressed(suppressed);
                        }
                        sstable.markSuspect();
                        throw new CorruptSSTableException(e, file.getPath());
                    }
                }
            };
        }
    }
}
