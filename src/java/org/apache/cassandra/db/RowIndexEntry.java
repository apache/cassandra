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

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.FilterFactory;
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
        return TypeSizes.NATIVE.sizeof(position) + promotedSize();
    }

    public int promotedSize()
    {
        return 0;
    }

    // TODO only store DeletionTime
    public static RowIndexEntry create(long position, DeletionInfo deletionInfo, ColumnIndex index)
    {
        assert deletionInfo != null;
        assert index != null;

        if (index.columnsIndex.size() > 1 || deletionInfo.getTopLevelDeletion() != DeletionTime.LIVE)
            return new IndexedEntry(position,
                                    deletionInfo,
                                    index.columnsIndex.isEmpty() ? Collections.<IndexHelper.IndexInfo>emptyList() : index.columnsIndex,
                                    index.columnsIndex.isEmpty() ? AlwaysPresentFilter.instance : index.bloomFilter);
        else
            return new RowIndexEntry(position);
    }

    public DeletionInfo deletionInfo()
    {
        return DeletionInfo.LIVE;
    }

    public List<IndexHelper.IndexInfo> columnsIndex()
    {
        return Collections.emptyList();
    }

    public IFilter bloomFilter()
    {
        return AlwaysPresentFilter.instance;
    }

    public long memorySize()
    {
        long fields = TypeSizes.NATIVE.sizeof(position) + ObjectSizes.getReferenceSize(); 
        return ObjectSizes.getFieldSize(fields);
    }

    public static class Serializer
    {
        public void serialize(RowIndexEntry rie, DataOutput dos) throws IOException
        {
            dos.writeLong(rie.position);
            if (!rie.columnsIndex().isEmpty() || rie.deletionInfo().getTopLevelDeletion() != DeletionTime.LIVE)
            {
                dos.writeInt(rie.promotedSize());
                DeletionInfo.serializer().serializeForSSTable(rie.deletionInfo(), dos);
                dos.writeInt(rie.columnsIndex().size());
                for (IndexHelper.IndexInfo info : rie.columnsIndex())
                    info.serialize(dos);
                if (!rie.columnsIndex().isEmpty())
                    FilterFactory.serialize(rie.bloomFilter(), dos);
            }
            else
            {
                dos.writeInt(0);
            }
        }

        public RowIndexEntry deserialize(DataInput dis, Descriptor.Version version) throws IOException
        {
            long position = dis.readLong();
            if (!version.hasPromotedIndexes)
                return new RowIndexEntry(position);

            int size = dis.readInt();
            if (size > 0)
            {
                DeletionInfo delInfo = DeletionInfo.serializer().deserializeFromSSTable(dis, version);
                int entries = dis.readInt();
                List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<IndexHelper.IndexInfo>(entries);
                for (int i = 0; i < entries; i++)
                    columnsIndex.add(IndexHelper.IndexInfo.deserialize(dis));
                IFilter bf = entries == 0
                             ? AlwaysPresentFilter.instance
                             : FilterFactory.deserialize(dis, version.filterType, false);
                return new IndexedEntry(position, delInfo, columnsIndex, bf);
            }
            else
            {
                return new RowIndexEntry(position);
            }
        }

        public void skip(DataInput dis, Descriptor.Version version) throws IOException
        {
            dis.readLong();
            if (version.hasPromotedIndexes)
                skipPromotedIndex(dis);
        }

        public void skipPromotedIndex(DataInput dis) throws IOException
        {
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
        private final IFilter bloomFilter;

        private IndexedEntry(long position, DeletionInfo deletionInfo, List<IndexHelper.IndexInfo> columnsIndex, IFilter bloomFilter)
        {
            super(position);
            assert deletionInfo != null;
            assert columnsIndex != null;
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
        public IFilter bloomFilter()
        {
            return bloomFilter;
        }

        @Override
        public int promotedSize()
        {
            TypeSizes typeSizes = TypeSizes.NATIVE;
            long size = DeletionTime.serializer.serializedSize(deletionInfo.getTopLevelDeletion(), typeSizes);
            size += typeSizes.sizeof(columnsIndex.size()); // number of entries
            for (IndexHelper.IndexInfo info : columnsIndex)
                size += info.serializedSize(typeSizes);

            size += bloomFilter instanceof AlwaysPresentFilter ? 0 : FilterFactory.serializedSize(bloomFilter);
            assert size <= Integer.MAX_VALUE;
            return (int)size;
        }

        public long memorySize()
        {
            long internal = 0;
            for (IndexHelper.IndexInfo idx : columnsIndex)
                internal += idx.memorySize();
            long listSize = ObjectSizes.getFieldSize(ObjectSizes.getArraySize(columnsIndex.size(), internal) + 4);
            return ObjectSizes.getFieldSize(deletionInfo.memorySize() + listSize);
        }
    }
}
