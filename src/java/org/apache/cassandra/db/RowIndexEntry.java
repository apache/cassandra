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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.primitives.Ints;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ObjectSizes;

public class RowIndexEntry implements IMeasurableMemory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowIndexEntry(0));

    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    protected int promotedSize(CType type)
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

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    public static class Serializer
    {
        private final CType type;

        public Serializer(CType type)
        {
            this.type = type;
        }

        public void serialize(RowIndexEntry rie, DataOutputPlus out) throws IOException
        {
            out.writeLong(rie.position);
            out.writeInt(rie.promotedSize(type));

            if (rie.isIndexed())
            {
                DeletionTime.serializer.serialize(rie.deletionTime(), out);
                out.writeInt(rie.columnsIndex().size());
                ISerializer<IndexHelper.IndexInfo> idxSerializer = type.indexSerializer();
                for (IndexHelper.IndexInfo info : rie.columnsIndex())
                    idxSerializer.serialize(info, out);
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
                ISerializer<IndexHelper.IndexInfo> idxSerializer = type.indexSerializer();
                List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<IndexHelper.IndexInfo>(entries);
                for (int i = 0; i < entries; i++)
                    columnsIndex.add(idxSerializer.deserialize(in));

                return new IndexedEntry(position, deletionTime, columnsIndex);
            }
            else
            {
                return new RowIndexEntry(position);
            }
        }

        public static void skip(DataInput in) throws IOException
        {
            in.readLong();
            skipPromotedIndex(in);
        }

        public static void skipPromotedIndex(DataInput in) throws IOException
        {
            int size = in.readInt();
            if (size <= 0)
                return;

            FileUtils.skipBytesFully(in, size);
        }

        public int serializedSize(RowIndexEntry rie)
        {
            int size = TypeSizes.NATIVE.sizeof(rie.position) + TypeSizes.NATIVE.sizeof(rie.promotedSize(type));

            if (rie.isIndexed())
            {
                List<IndexHelper.IndexInfo> index = rie.columnsIndex();

                size += DeletionTime.serializer.serializedSize(rie.deletionTime(), TypeSizes.NATIVE);
                size += TypeSizes.NATIVE.sizeof(index.size());

                ISerializer<IndexHelper.IndexInfo> idxSerializer = type.indexSerializer();
                for (IndexHelper.IndexInfo info : index)
                    size += idxSerializer.serializedSize(info, TypeSizes.NATIVE);
            }

            return size;
        }
    }

    /**
     * An entry in the row index for a row whose columns are indexed.
     */
    private static class IndexedEntry extends RowIndexEntry
    {
        private final DeletionTime deletionTime;
        private final List<IndexHelper.IndexInfo> columnsIndex;
        private static final long BASE_SIZE =
                ObjectSizes.measure(new IndexedEntry(0, DeletionTime.LIVE, Arrays.<IndexHelper.IndexInfo>asList(null, null)))
              + ObjectSizes.measure(new ArrayList<>(1));

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
        public int promotedSize(CType type)
        {
            TypeSizes typeSizes = TypeSizes.NATIVE;
            long size = DeletionTime.serializer.serializedSize(deletionTime, typeSizes);
            size += typeSizes.sizeof(columnsIndex.size()); // number of entries
            ISerializer<IndexHelper.IndexInfo> idxSerializer = type.indexSerializer();
            for (IndexHelper.IndexInfo info : columnsIndex)
                size += idxSerializer.serializedSize(info, typeSizes);

            return Ints.checkedCast(size);
        }

        @Override
        public long unsharedHeapSize()
        {
            long entrySize = 0;
            for (IndexHelper.IndexInfo idx : columnsIndex)
                entrySize += idx.unsharedHeapSize();

            return BASE_SIZE
                   + entrySize
                   + deletionTime.unsharedHeapSize()
                   + ObjectSizes.sizeOfReferenceArray(columnsIndex.size());
        }
    }
}
