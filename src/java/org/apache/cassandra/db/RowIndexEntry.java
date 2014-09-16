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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.primitives.Ints;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ObjectSizes;

public class RowIndexEntry implements IMeasurableMemory
{
    public static final Serializer serializer = new Serializer();

    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    public int serializedSize()
    {
        int size = TypeSizes.NATIVE.sizeof(position) + TypeSizes.NATIVE.sizeof(promotedSize());

        if (isIndexed())
        {
            List<IndexHelper.IndexInfo> index = columnsIndex();

            size += DeletionTime.serializer.serializedSize(deletionTime(), TypeSizes.NATIVE);
            size += TypeSizes.NATIVE.sizeof(index.size());
            for (IndexHelper.IndexInfo info : index)
                size += info.serializedSize(TypeSizes.NATIVE);
        }

        return size;
    }

    protected int promotedSize()
    {
        return 0;
    }

    public static RowIndexEntry create(long position, DeletionTime deletionTime, ColumnIndex index)
    {
        assert index != null;
        assert deletionTime != null;

        // we only consider the columns summary when determining whether to create an IndexedEntry,
        // since if there are insufficient columns to be worth indexing we're going to seek to
        // the beginning of the row anyway, so we might as well read the tombstone there as well.
        if (index.columnsIndex.size() > 1)
            return new IndexedEntry(position, deletionTime, index.columnsIndex);
        else
            return new RowIndexEntry(position);
    }

    /**
     * @return true if this index entry contains the row-level tombstone and column summary.  Otherwise,
     * caller should fetch these from the row header.
     */
    public boolean isIndexed()
    {
        return !columnsIndex().isEmpty();
    }

    public DeletionTime deletionTime()
    {
        throw new UnsupportedOperationException();
    }

    public List<IndexHelper.IndexInfo> columnsIndex()
    {
        return Collections.emptyList();
    }

    public long memorySize()
    {
        return ObjectSizes.getFieldSize(TypeSizes.NATIVE.sizeof(position));
    }

    public static class Serializer
    {
        public void serialize(RowIndexEntry rie, DataOutput out) throws IOException
        {
            out.writeLong(rie.position);
            out.writeInt(rie.promotedSize());

            if (rie.isIndexed())
            {
                DeletionTime.serializer.serialize(rie.deletionTime(), out);
                out.writeInt(rie.columnsIndex().size());
                for (IndexHelper.IndexInfo info : rie.columnsIndex())
                    info.serialize(out);
            }
        }

        public RowIndexEntry deserialize(DataInput in, Descriptor.Version version) throws IOException
        {
            long position = in.readLong();

            int size = in.readInt();
            if (size > 0)
            {
                DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);

                int entries = in.readInt();
                List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<IndexHelper.IndexInfo>(entries);
                for (int i = 0; i < entries; i++)
                    columnsIndex.add(IndexHelper.IndexInfo.deserialize(in));

                return new IndexedEntry(position, deletionTime, columnsIndex);
            }
            else
            {
                return new RowIndexEntry(position);
            }
        }

        public void skip(DataInput in) throws IOException
        {
            in.readLong();
            skipPromotedIndex(in);
        }

        public void skipPromotedIndex(DataInput in) throws IOException
        {
            int size = in.readInt();
            if (size <= 0)
                return;

            FileUtils.skipBytesFully(in, size);
        }
    }

    /**
     * An entry in the row index for a row whose columns are indexed.
     */
    private static class IndexedEntry extends RowIndexEntry
    {
        private final DeletionTime deletionTime;
        private final List<IndexHelper.IndexInfo> columnsIndex;

        private IndexedEntry(long position, DeletionTime deletionTime, List<IndexHelper.IndexInfo> columnsIndex)
        {
            super(position);
            assert deletionTime != null;
            assert columnsIndex != null && columnsIndex.size() > 1;
            this.deletionTime = deletionTime;
            this.columnsIndex = columnsIndex;
        }

        @Override
        public DeletionTime deletionTime()
        {
            return deletionTime;
        }

        @Override
        public List<IndexHelper.IndexInfo> columnsIndex()
        {
            return columnsIndex;
        }

        @Override
        public int promotedSize()
        {
            TypeSizes typeSizes = TypeSizes.NATIVE;
            long size = DeletionTime.serializer.serializedSize(deletionTime, typeSizes);
            size += typeSizes.sizeof(columnsIndex.size()); // number of entries
            for (IndexHelper.IndexInfo info : columnsIndex)
                size += info.serializedSize(typeSizes);

            return Ints.checkedCast(size);
        }

        @Override
        public long memorySize()
        {
            long entrySize = 0;
            for (IndexHelper.IndexInfo idx : columnsIndex)
                entrySize += idx.memorySize();

            return ObjectSizes.getSuperClassFieldSize(TypeSizes.NATIVE.sizeof(position))
                   + ObjectSizes.getFieldSize(// deletionTime
                                              ObjectSizes.getReferenceSize() +
                                              // columnsIndex
                                              ObjectSizes.getReferenceSize())
                   + deletionTime.memorySize()
                   + ObjectSizes.getArraySize(columnsIndex.size(), ObjectSizes.getReferenceSize()) + entrySize + 4;
        }
    }
}
