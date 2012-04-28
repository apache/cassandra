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

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Filter;
import org.apache.cassandra.utils.FilterFactory;

public class RowIndexEntry
{
    public static final Serializer serializer = new Serializer();

    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    public static RowIndexEntry create(long position, DeletionInfo deletionInfo, ColumnIndex index)
    {
        if (index != null && index.columnsIndex != null && index.columnsIndex.size() > 1)
            return new IndexedEntry(position, deletionInfo, index.columnsIndex, index.bloomFilter);
        else
            return new RowIndexEntry(position);
    }

    public boolean isIndexed()
    {
        return !columnsIndex().isEmpty();
    }

    public DeletionInfo deletionInfo()
    {
        throw new UnsupportedOperationException();
    }

    public List<IndexHelper.IndexInfo> columnsIndex()
    {
        return Collections.<IndexHelper.IndexInfo>emptyList();
    }

    public Filter bloomFilter()
    {
        throw new UnsupportedOperationException();
    }

    public static class Serializer
    {
        public void serialize(RowIndexEntry rie, DataOutput dos) throws IOException
        {
            dos.writeLong(rie.position);
            if (rie.isIndexed())
            {
                dos.writeInt(((IndexedEntry)rie).serializedSize());
                dos.writeInt(rie.deletionInfo().localDeletionTime);
                dos.writeLong(rie.deletionInfo().markedForDeleteAt);
                dos.writeInt(rie.columnsIndex().size());
                for (IndexHelper.IndexInfo info : rie.columnsIndex())
                    info.serialize(dos);
                FilterFactory.serialize(rie.bloomFilter(), dos);
            }
            else
            {
                dos.writeInt(0);
            }
        }

        public RowIndexEntry deserializePositionOnly(DataInput dis, Descriptor descriptor) throws IOException
        {
            long position = dis.readLong();
            if (descriptor.hasPromotedIndexes)
            {
                int size = dis.readInt();
                if (size > 0)
                    FileUtils.skipBytesFully(dis, size);
            }
            return new RowIndexEntry(position);
        }

        public RowIndexEntry deserialize(DataInput dis, Descriptor descriptor) throws IOException
        {
            long position = dis.readLong();
            if (descriptor.hasPromotedIndexes)
            {
                int size = dis.readInt();
                if (size > 0)
                {
                    int ldt = dis.readInt();
                    long mfda = dis.readLong();
                    int entries = dis.readInt();
                    List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<IndexHelper.IndexInfo>(entries);
                    for (int i = 0; i < entries; i++)
                        columnsIndex.add(IndexHelper.IndexInfo.deserialize(dis));
                    Filter bf = FilterFactory.deserialize(dis, descriptor.filterType);
                    return new IndexedEntry(position, new DeletionInfo(mfda, ldt), columnsIndex, bf);
                }
                else
                {
                    return new RowIndexEntry(position);
                }
            }
            else
            {
                return new RowIndexEntry(position);
            }
        }

        public void skip(DataInput dis, Descriptor descriptor) throws IOException
        {
            dis.readLong();
            if (!descriptor.hasPromotedIndexes)
                return;

            int size = dis.readInt();
            if (size <= 0)
                return;

            FileUtils.skipBytesFully(dis, size);
        }
    }

    /**
     * An entry in the row index for a row whose columns are indexed.
     */
    private static class IndexedEntry extends RowIndexEntry
    {
        private final DeletionInfo deletionInfo;
        private final List<IndexHelper.IndexInfo> columnsIndex;
        private final Filter bloomFilter;

        private IndexedEntry(long position, DeletionInfo deletionInfo, List<IndexHelper.IndexInfo> columnsIndex, Filter bloomFilter)
        {
            super(position);
            assert deletionInfo != null;
            assert columnsIndex != null && columnsIndex.size() > 1;
            this.deletionInfo = deletionInfo;
            this.columnsIndex = columnsIndex;
            this.bloomFilter = bloomFilter;
        }

        @Override
        public DeletionInfo deletionInfo()
        {
            return deletionInfo;
        }

        @Override
        public List<IndexHelper.IndexInfo> columnsIndex()
        {
            return columnsIndex;
        }

        @Override
        public Filter bloomFilter()
        {
            return bloomFilter;
        }

        public int serializedSize()
        {
            DBTypeSizes typeSizes = DBTypeSizes.NATIVE;
            int size = typeSizes.sizeof(deletionInfo.localDeletionTime) + typeSizes.sizeof(deletionInfo.markedForDeleteAt); // deletion info
            size += typeSizes.sizeof(columnsIndex.size()); // number of entries
            for (IndexHelper.IndexInfo info : columnsIndex)
                size += info.serializedSize(typeSizes);

            return size + (int) FilterFactory.serializedSize(bloomFilter);
        }
    }
}
