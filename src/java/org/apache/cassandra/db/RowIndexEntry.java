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

public class RowIndexEntry
{
    public static final Serializer serializer = new Serializer();

    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    public int serializedSize()
    {
        return TypeSizes.NATIVE.sizeof(position);
    }

    public static RowIndexEntry create(long position, DeletionInfo deletionInfo, ColumnIndex index)
    {
        if (index != null && index.columnsIndex != null && index.columnsIndex.size() > 1)
            return new IndexedEntry(position, deletionInfo, index.columnsIndex);
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
        return Collections.emptyList();
    }

    public static class Serializer
    {
        public void serialize(RowIndexEntry rie, DataOutput dos) throws IOException
        {
            dos.writeLong(rie.position);
            if (rie.isIndexed())
            {
                dos.writeInt(rie.serializedSize());
                DeletionInfo.serializer().serializeForSSTable(rie.deletionInfo(), dos);
                dos.writeInt(rie.columnsIndex().size());
                for (IndexHelper.IndexInfo info : rie.columnsIndex())
                    info.serialize(dos);
            }
            else
            {
                dos.writeInt(0);
            }
        }

        public RowIndexEntry deserialize(DataInput dis, Descriptor.Version version) throws IOException
        {
            long position = dis.readLong();
            if (version.hasPromotedIndexes)
            {
                int size = dis.readInt();
                if (size > 0)
                {
                    DeletionInfo delInfo = DeletionInfo.serializer().deserializeFromSSTable(dis, version);
                    int entries = dis.readInt();
                    List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<IndexHelper.IndexInfo>(entries);
                    for (int i = 0; i < entries; i++)
                        columnsIndex.add(IndexHelper.IndexInfo.deserialize(dis));

                    if (version.hasRowLevelBF)
                        IndexHelper.skipBloomFilter(dis);
                    return new IndexedEntry(position, delInfo, columnsIndex);
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

        private IndexedEntry(long position, DeletionInfo deletionInfo, List<IndexHelper.IndexInfo> columnsIndex)
        {
            super(position);
            assert deletionInfo != null;
            assert columnsIndex != null && columnsIndex.size() > 1;
            this.deletionInfo = deletionInfo;
            this.columnsIndex = columnsIndex;
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
        public int serializedSize()
        {
            TypeSizes typeSizes = TypeSizes.NATIVE;
            long size = DeletionTime.serializer.serializedSize(deletionInfo.getTopLevelDeletion(), typeSizes);
            size += typeSizes.sizeof(columnsIndex.size()); // number of entries
            for (IndexHelper.IndexInfo info : columnsIndex)
                size += info.serializedSize(typeSizes);

            assert size <= Integer.MAX_VALUE;
            return (int)size;
        }
    }
}
