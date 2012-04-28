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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.IIterableColumns;
import org.apache.cassandra.utils.Filter;
import org.apache.cassandra.utils.FilterFactory;

public class ColumnIndex
{
    public final List<IndexHelper.IndexInfo> columnsIndex;
    public final Filter bloomFilter;

    private static final ColumnIndex EMPTY = new ColumnIndex(Collections.<IndexHelper.IndexInfo>emptyList(), FilterFactory.emptyFilter());

    private ColumnIndex(int estimatedColumnCount)
    {
        this(new ArrayList<IndexHelper.IndexInfo>(), FilterFactory.getFilter(estimatedColumnCount, 4));
    }

    private ColumnIndex(List<IndexHelper.IndexInfo> columnsIndex, Filter bloomFilter)
    {
        this.columnsIndex = columnsIndex;
        this.bloomFilter = bloomFilter;
    }

    /**
     * Help to create an index for a column family based on size of columns
     */
    public static class Builder
    {
        private final ColumnIndex result;
        private final Comparator<ByteBuffer> comparator;
        private final long indexOffset;
        private long startPosition = -1;
        private long endPosition = 0;
        private IColumn firstColumn = null;
        private IColumn lastColumn = null;

        public Builder(Comparator<ByteBuffer> comparator, ByteBuffer key, int estimatedColumnCount)
        {
            this.comparator = comparator;
            this.indexOffset = rowHeaderSize(key);
            this.result = new ColumnIndex(estimatedColumnCount);
        }

        /**
         * Returns the number of bytes between the beginning of the row and the
         * first serialized column.
         */
        private static long rowHeaderSize(ByteBuffer key)
        {
            DBTypeSizes typeSizes = DBTypeSizes.NATIVE;
            // TODO fix constantSize when changing the nativeconststs.
            int keysize = key.remaining();
            return typeSizes.sizeof((short) keysize) + keysize + // Row key
                 + typeSizes.sizeof(0L)                        // Row data size
                 + typeSizes.sizeof(0) + typeSizes.sizeof(0L) // Deletion info
                 + typeSizes.sizeof(0);                        // Column count
        }

        /**
         * Serializes the index into in-memory structure with all required components
         * such as Bloom Filter, index block size, IndexInfo list
         *
         * @param columns Column family to create index for
         *
         * @return information about index - it's Bloom Filter, block size and IndexInfo list
         */
        public ColumnIndex build(IIterableColumns columns)
        {
            int columnCount = columns.getEstimatedColumnCount();

            if (columnCount == 0)
                return ColumnIndex.EMPTY;

            for (IColumn c : columns)
                add(c);

            return build();
        }

        public void add(IColumn column)
        {
            result.bloomFilter.add(column.name());

            if (firstColumn == null)
            {
                firstColumn = column;
                startPosition = endPosition;
            }

            endPosition += column.serializedSize(DBTypeSizes.NATIVE);

            // if we hit the column index size that we have to index after, go ahead and index it.
            if (endPosition - startPosition >= DatabaseDescriptor.getColumnIndexSize())
            {
                IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstColumn.name(), column.name(), indexOffset + startPosition, endPosition - startPosition);
                result.columnsIndex.add(cIndexInfo);
                firstColumn = null;
            }

            lastColumn = column;
        }

        public ColumnIndex build()
        {
            // all columns were GC'd after all
            if (lastColumn == null)
                return ColumnIndex.EMPTY;

            // the last column may have fallen on an index boundary already.  if not, index it explicitly.
            if (result.columnsIndex.isEmpty() || comparator.compare(result.columnsIndex.get(result.columnsIndex.size() - 1).lastName, lastColumn.name()) != 0)
            {
                IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstColumn.name(), lastColumn.name(), indexOffset + startPosition, endPosition - startPosition);
                result.columnsIndex.add(cIndexInfo);
            }

            // we should always have at least one computed index block, but we only write it out if there is more than that.
            assert result.columnsIndex.size() > 0;
            return result;
        }
    }
}
