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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnIndex
{
    public final List<IndexHelper.IndexInfo> columnsIndex;

    private static final ColumnIndex EMPTY = new ColumnIndex(Collections.<IndexHelper.IndexInfo>emptyList());

    private ColumnIndex(List<IndexHelper.IndexInfo> columnsIndex)
    {
        assert columnsIndex != null;

        this.columnsIndex = columnsIndex;
    }

    @VisibleForTesting
    public static ColumnIndex nothing()
    {
        return EMPTY;
    }

    /**
     * Help to create an index for a column family based on size of columns,
     * and write said columns to disk.
     */
    public static class Builder
    {
        private final ColumnIndex result;
        private final long indexOffset;
        private long startPosition = -1;
        private long endPosition = 0;
        private long blockSize;
        private OnDiskAtom firstColumn;
        private OnDiskAtom lastColumn;
        private OnDiskAtom lastBlockClosing;
        private final DataOutputPlus output;
        private final RangeTombstone.Tracker tombstoneTracker;
        private int atomCount;
        private final ByteBuffer key;
        private final DeletionInfo deletionInfo; // only used for serializing and calculating row header size

        private final OnDiskAtom.Serializer atomSerializer;

        public Builder(ColumnFamily cf,
                       ByteBuffer key,
                       DataOutputPlus output)
        {
            assert cf != null;
            assert key != null;
            assert output != null;

            this.key = key;
            deletionInfo = cf.deletionInfo();
            this.indexOffset = rowHeaderSize(key, deletionInfo);
            this.result = new ColumnIndex(new ArrayList<IndexHelper.IndexInfo>());
            this.output = output;
            this.tombstoneTracker = new RangeTombstone.Tracker(cf.getComparator());
            this.atomSerializer = cf.getComparator().onDiskAtomSerializer();
        }

        /**
         * Returns the number of bytes between the beginning of the row and the
         * first serialized column.
         */
        private static long rowHeaderSize(ByteBuffer key, DeletionInfo delInfo)
        {
            TypeSizes typeSizes = TypeSizes.NATIVE;
            // TODO fix constantSize when changing the nativeconststs.
            int keysize = key.remaining();
            return typeSizes.sizeof((short) keysize) + keysize          // Row key
                 + DeletionTime.serializer.serializedSize(delInfo.getTopLevelDeletion(), typeSizes);
        }

        public RangeTombstone.Tracker tombstoneTracker()
        {
            return tombstoneTracker;
        }

        public int writtenAtomCount()
        {
            return atomCount + tombstoneTracker.writtenAtom();
        }

        /**
         * Serializes the index into in-memory structure with all required components
         * such as Bloom Filter, index block size, IndexInfo list
         *
         * @param cf Column family to create index for
         *
         * @return information about index - it's Bloom Filter, block size and IndexInfo list
         */
        public ColumnIndex build(ColumnFamily cf) throws IOException
        {
            // cf has disentangled the columns and range tombstones, we need to re-interleave them in comparator order
            Comparator<Composite> comparator = cf.getComparator();
            DeletionInfo.InOrderTester tester = cf.deletionInfo().inOrderTester();
            Iterator<RangeTombstone> rangeIter = cf.deletionInfo().rangeIterator();
            RangeTombstone tombstone = rangeIter.hasNext() ? rangeIter.next() : null;

            for (Cell c : cf)
            {
                while (tombstone != null && comparator.compare(c.name(), tombstone.min) >= 0)
                {
                    // skip range tombstones that are shadowed by partition tombstones
                    if (!cf.deletionInfo().getTopLevelDeletion().isDeleted(tombstone))
                        add(tombstone);
                    tombstone = rangeIter.hasNext() ? rangeIter.next() : null;
                }

                // We can skip any cell if it's shadowed by a tombstone already. This is a more
                // general case than was handled by CASSANDRA-2589.
                if (!tester.isDeleted(c))
                    add(c);
            }

            while (tombstone != null)
            {
                add(tombstone);
                tombstone = rangeIter.hasNext() ? rangeIter.next() : null;
            }
            ColumnIndex index = build();

            maybeWriteEmptyRowHeader();

            return index;
        }

        /**
         * The important distinction wrt build() is that we may be building for a row that ends up
         * being compacted away entirely, i.e., the input consists only of expired tombstones (or
         * columns shadowed by expired tombstone).  Thus, it is the caller's responsibility
         * to decide whether to write the header for an empty row.
         */
        public ColumnIndex buildForCompaction(Iterator<OnDiskAtom> columns) throws IOException
        {
            while (columns.hasNext())
            {
                OnDiskAtom c =  columns.next();
                add(c);
            }

            return build();
        }

        public void add(OnDiskAtom column) throws IOException
        {
            atomCount++;

            if (firstColumn == null)
            {
                firstColumn = column;
                startPosition = endPosition;
                // TODO: have that use the firstColumn as min + make sure we optimize that on read
                endPosition += tombstoneTracker.writeOpenedMarker(firstColumn, output, atomSerializer);
                blockSize = 0; // We don't count repeated tombstone marker in the block size, to avoid a situation
                               // where we wouldn't make any progress because a block is filled by said marker
            }

            long size = atomSerializer.serializedSizeForSSTable(column);
            endPosition += size;
            blockSize += size;

            // if we hit the column index size that we have to index after, go ahead and index it.
            if (blockSize >= DatabaseDescriptor.getColumnIndexSize())
            {
                IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstColumn.name(), column.name(), indexOffset + startPosition, endPosition - startPosition);
                result.columnsIndex.add(cIndexInfo);
                firstColumn = null;
                lastBlockClosing = column;
            }

            maybeWriteRowHeader();
            atomSerializer.serializeForSSTable(column, output);

            // TODO: Should deal with removing unneeded tombstones
            tombstoneTracker.update(column, false);

            lastColumn = column;
        }

        private void maybeWriteRowHeader() throws IOException
        {
            if (lastColumn == null)
            {
                ByteBufferUtil.writeWithShortLength(key, output);
                DeletionTime.serializer.serialize(deletionInfo.getTopLevelDeletion(), output);
            }
        }

        public ColumnIndex build()
        {
            // all columns were GC'd after all
            if (lastColumn == null)
                return ColumnIndex.EMPTY;

            // the last column may have fallen on an index boundary already.  if not, index it explicitly.
            if (result.columnsIndex.isEmpty() || lastBlockClosing != lastColumn)
            {
                IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstColumn.name(), lastColumn.name(), indexOffset + startPosition, endPosition - startPosition);
                result.columnsIndex.add(cIndexInfo);
            }

            // we should always have at least one computed index block, but we only write it out if there is more than that.
            assert result.columnsIndex.size() > 0;
            return result;
        }

        public void maybeWriteEmptyRowHeader() throws IOException
        {
            if (!deletionInfo.isLive())
                maybeWriteRowHeader();
        }
    }
}
