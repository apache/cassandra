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

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.AbstractPartitionData;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 *  A Cell Iterator in reversed clustering order over SSTable
 */
public class SSTableReversedIterator extends AbstractSSTableIterator
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReversedIterator.class);

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

    private ReusablePartitionData createBuffer(int blocksCount)
    {
        int estimatedRowCount = 16;
        int columnCount = metadata().partitionColumns().regulars.columnCount();
        if (columnCount == 0 || metadata().clusteringColumns().size() == 0)
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
        return new ReusablePartitionData(metadata(), partitionKey(), DeletionTime.LIVE, columns(), estimatedRowCount);
    }

    private class ReverseReader extends Reader
    {
        private ReusablePartitionData partition;
        private UnfilteredRowIterator iterator;

        private ReverseReader(FileDataInput file, boolean isAtPartitionStart, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
            assert isAtPartitionStart;
        }

        public boolean hasNext() throws IOException
        {
            if (partition == null)
            {
                partition = createBuffer(1);
                partition.populateFrom(this, null, null, new Tester()
                {
                    public boolean isDone()
                    {
                        return false;
                    }
                });
                iterator = partition.unfilteredIterator(columns, Slices.ALL, true);
            }
            return iterator.hasNext();
        }

        public Unfiltered next() throws IOException
        {
            if (!hasNext())
                throw new NoSuchElementException();
            return iterator.next();
        }

        public Iterator<Unfiltered> slice(final Slice slice) throws IOException
        {
            if (partition == null)
            {
                partition = createBuffer(1);
                partition.populateFrom(this, slice.start(), slice.end(), new Tester()
                {
                    public boolean isDone()
                    {
                        return false;
                    }
                });
            }

            return partition.unfilteredIterator(columns, Slices.with(metadata().comparator, slice), true);
        }
    }

    private class ReverseIndexedReader extends IndexedReader
    {
        private ReusablePartitionData partition;
        private UnfilteredRowIterator iterator;

        private ReverseIndexedReader(RowIndexEntry indexEntry, FileDataInput file, boolean isAtPartitionStart, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile, indexEntry, isAtPartitionStart);
            this.currentIndexIdx = indexEntry.columnsIndex().size();
        }

        public boolean hasNext() throws IOException
        {
            // If it's called before we've created the file, create it. This then mean
            // we're reading from the end of the partition.
            if (!isInit)
            {
                seekToPosition(indexEntry.position);
                ByteBufferUtil.skipShortLength(file); // partition key
                DeletionTime.serializer.skip(file);   // partition deletion
                if (sstable.header.hasStatic())
                    UnfilteredSerializer.serializer.skipStaticRow(file, sstable.header, helper);
                isInit = true;
            }

            if (partition == null)
            {
                partition = createBuffer(indexes.size());
                partition.populateFrom(this, null, null, new Tester()
                {
                    public boolean isDone()
                    {
                        return false;
                    }
                });
                iterator = partition.unfilteredIterator(columns, Slices.ALL, true);
            }

            return iterator.hasNext();
        }

        public Unfiltered next() throws IOException
        {
            if (!hasNext())
                throw new NoSuchElementException();
            return iterator.next();
        }

        private void prepareBlock(int blockIdx, Slice.Bound start, Slice.Bound end) throws IOException
        {
            updateBlock(blockIdx);

            if (partition == null)
                partition = createBuffer(indexes.size());
            else
                partition.clear();

            final FileMark fileMark = mark;
            final long width = currentIndex().width;

            partition.populateFrom(this, start, end, new Tester()
            {
                public boolean isDone()
                {
                    return file.bytesPastMark(fileMark) >= width;
                }
            });
        }

        @Override
        public Iterator<Unfiltered> slice(final Slice slice) throws IOException
        {
            // if our previous slicing already got us the smallest row in the sstable, we're done
            if (currentIndexIdx < 0)
                return Collections.emptyIterator();

            final List<IndexHelper.IndexInfo> indexes = indexEntry.columnsIndex();

            // Find the first index block we'll need to read for the slice.
            final int startIdx = IndexHelper.indexFor(slice.end(), indexes, sstable.metadata.comparator, true, currentIndexIdx);
            if (startIdx < 0)
                return Collections.emptyIterator();

            // Find the last index block we'll need to read for the slice.
            int lastIdx = IndexHelper.indexFor(slice.start(), indexes, sstable.metadata.comparator, true, startIdx);

            // The index search is by firstname and so lastIdx is such that
            //   indexes[lastIdx].firstName < slice.start <= indexes[lastIdx + 1].firstName
            // However, if indexes[lastIdx].lastName < slice.start we can bump lastIdx.
            if (lastIdx >= 0 && metadata().comparator.compare(indexes.get(lastIdx).lastName, slice.start()) < 0)
                ++lastIdx;

            final int endIdx = lastIdx;

            // Because we're reversed, even if it is our current block, we should re-prepare the block since we would
            // have skipped anything not in the previous slice.
            prepareBlock(startIdx, slice.start(), slice.end());

            return new AbstractIterator<Unfiltered>()
            {
                private Iterator<Unfiltered> currentBlockIterator = partition.unfilteredIterator(columns, Slices.with(metadata().comparator, slice), true);

                protected Unfiltered computeNext()
                {
                    try
                    {
                        if (currentBlockIterator.hasNext())
                            return currentBlockIterator.next();

                        --currentIndexIdx;
                        if (currentIndexIdx < 0 || currentIndexIdx < endIdx)
                            return endOfData();

                        // Note that since we know we're read blocks backward, there is no point in checking the slice end, so we pass null
                        prepareBlock(currentIndexIdx, slice.start(), null);
                        currentBlockIterator = partition.unfilteredIterator(columns, Slices.with(metadata().comparator, slice), true);
                        return computeNext();
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

    private abstract class Tester
    {
        public abstract boolean isDone();
    }

    private class ReusablePartitionData extends AbstractPartitionData
    {
        private final Writer rowWriter;
        private final RangeTombstoneCollector markerWriter;

        private ReusablePartitionData(CFMetaData metadata,
                                      DecoratedKey partitionKey,
                                      DeletionTime deletionTime,
                                      PartitionColumns columns,
                                      int initialRowCapacity)
        {
            super(metadata, partitionKey, deletionTime, columns, initialRowCapacity, false);

            this.rowWriter = new Writer(true);
            // Note that even though the iterator handles the reverse case, this object holds the data for a single index bock, and we read index blocks in
            // forward clustering order.
            this.markerWriter = new RangeTombstoneCollector(false);
        }

        // Note that this method is here rather than in the readers because we want to use it for both readers and they
        // don't extend one another
        private void populateFrom(Reader reader, Slice.Bound start, Slice.Bound end, Tester tester) throws IOException
        {
            // If we have a start bound, skip everything that comes before it.
            while (reader.deserializer.hasNext() && start != null && reader.deserializer.compareNextTo(start) <= 0 && !tester.isDone())
            {
                if (reader.deserializer.nextIsRow())
                    reader.deserializer.skipNext();
                else
                    reader.updateOpenMarker((RangeTombstoneMarker)reader.deserializer.readNext());
            }

            // If we have an open marker, it's either one from what we just skipped (if start != null), or it's from the previous index block.
            if (reader.openMarker != null)
            {
                // If we have no start but still an openMarker, this means we're indexed and it's coming from the previous block
                Slice.Bound markerStart = start;
                if (start == null)
                {
                    ClusteringPrefix c = ((IndexedReader)reader).previousIndex().lastName;
                    markerStart = Slice.Bound.exclusiveStartOf(c);
                }
                writeMarker(markerStart, reader.openMarker);
            }

            // Now deserialize everything until we reach our requested end (if we have one)
            while (reader.deserializer.hasNext()
                   && (end == null || reader.deserializer.compareNextTo(end) <= 0)
                   && !tester.isDone())
            {
                Unfiltered unfiltered = reader.deserializer.readNext();
                if (unfiltered.kind() == Unfiltered.Kind.ROW)
                {
                    ((Row) unfiltered).copyTo(rowWriter);
                }
                else
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
                    reader.updateOpenMarker(marker);
                    marker.copyTo(markerWriter);
                }
            }

            // If we have an open marker, we should close it before finishing
            if (reader.openMarker != null)
            {
                // If we no end and still an openMarker, this means we're indexed and the marker can be close using the blocks end
                Slice.Bound markerEnd = end;
                if (end == null)
                {
                    ClusteringPrefix c = ((IndexedReader)reader).currentIndex().lastName;
                    markerEnd = Slice.Bound.inclusiveEndOf(c);
                }
                writeMarker(markerEnd, reader.getAndClearOpenMarker());
            }
        }

        private void writeMarker(Slice.Bound bound, DeletionTime dt)
        {
            bound.writeTo(markerWriter);
            markerWriter.writeBoundDeletion(dt);
            markerWriter.endOfMarker();
        }

        @Override
        public void clear()
        {
            super.clear();
            rowWriter.reset();
            markerWriter.reset();
        }
    }
}
