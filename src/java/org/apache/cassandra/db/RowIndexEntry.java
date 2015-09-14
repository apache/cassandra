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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.primitives.Ints;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ObjectSizes;

public class RowIndexEntry<T> implements IMeasurableMemory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowIndexEntry(0));

    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    protected int promotedSize(IndexHelper.IndexInfo.Serializer idxSerializer)
    {
        return 0;
    }

    public static RowIndexEntry<IndexHelper.IndexInfo> create(long position, DeletionTime deletionTime, ColumnIndex index)
    {
        assert index != null;
        assert deletionTime != null;

        // we only consider the columns summary when determining whether to create an IndexedEntry,
        // since if there are insufficient columns to be worth indexing we're going to seek to
        // the beginning of the row anyway, so we might as well read the tombstone there as well.
        if (index.columnsIndex.size() > 1)
            return new IndexedEntry(position, deletionTime, index.partitionHeaderLength, index.columnsIndex);
        else
            return new RowIndexEntry<>(position);
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

    /**
     * @return the offset to the start of the header information for this row.
     * For some formats this may not be the start of the row.
     */
    public long headerOffset()
    {
        return 0;
    }

    /**
     * The length of the row header (partition key, partition deletion and static row).
     * This value is only provided for indexed entries and this method will throw
     * {@code UnsupportedOperationException} if {@code !isIndexed()}.
     */
    public long headerLength()
    {
        throw new UnsupportedOperationException();
    }

    public List<T> columnsIndex()
    {
        return Collections.emptyList();
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    public interface IndexSerializer<T>
    {
        void serialize(RowIndexEntry<T> rie, DataOutputPlus out) throws IOException;
        RowIndexEntry<T> deserialize(DataInputPlus in) throws IOException;
        int serializedSize(RowIndexEntry<T> rie);
    }

    public static class Serializer implements IndexSerializer<IndexHelper.IndexInfo>
    {
        private final IndexHelper.IndexInfo.Serializer idxSerializer;
        private final Version version;

        public Serializer(CFMetaData metadata, Version version, SerializationHeader header)
        {
            this.idxSerializer = new IndexHelper.IndexInfo.Serializer(metadata, version, header);
            this.version = version;
        }

        public void serialize(RowIndexEntry<IndexHelper.IndexInfo> rie, DataOutputPlus out) throws IOException
        {
            assert version.storeRows() : "We read old index files but we should never write them";

            out.writeUnsignedVInt(rie.position);
            out.writeUnsignedVInt(rie.promotedSize(idxSerializer));

            if (rie.isIndexed())
            {
                out.writeUnsignedVInt(rie.headerLength());
                DeletionTime.serializer.serialize(rie.deletionTime(), out);
                out.writeUnsignedVInt(rie.columnsIndex().size());

                // Calculate and write the offsets to the IndexInfo objects.

                int[] offsets = new int[rie.columnsIndex().size()];

                if (out.hasPosition())
                {
                    // Out is usually a SequentialWriter, so using the file-pointer is fine to generate the offsets.
                    // A DataOutputBuffer also works.
                    long start = out.position();
                    int i = 0;
                    for (IndexHelper.IndexInfo info : rie.columnsIndex())
                    {
                        offsets[i] = i == 0 ? 0 : (int)(out.position() - start);
                        i++;
                        idxSerializer.serialize(info, out);
                    }
                }
                else
                {
                    // Not sure this branch will ever be needed, but if it is called, it has to calculate the
                    // serialized sizes instead of simply using the file-pointer.
                    int i = 0;
                    int offset = 0;
                    for (IndexHelper.IndexInfo info : rie.columnsIndex())
                    {
                        offsets[i++] = offset;
                        idxSerializer.serialize(info, out);
                        offset += idxSerializer.serializedSize(info);
                    }
                }

                for (int off : offsets)
                    out.writeInt(off);
            }
        }

        public RowIndexEntry<IndexHelper.IndexInfo> deserialize(DataInputPlus in) throws IOException
        {
            if (!version.storeRows())
            {
                long position = in.readLong();

                int size = in.readInt();
                if (size > 0)
                {
                    DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);

                    int entries = in.readInt();
                    List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<>(entries);

                    long headerLength = 0L;
                    for (int i = 0; i < entries; i++)
                    {
                        IndexHelper.IndexInfo info = idxSerializer.deserialize(in);
                        columnsIndex.add(info);
                        if (i == 0)
                            headerLength = info.offset;
                    }

                    return new IndexedEntry(position, deletionTime, headerLength, columnsIndex);
                }
                else
                {
                    return new RowIndexEntry<>(position);
                }
            }

            long position = in.readUnsignedVInt();

            int size = (int)in.readUnsignedVInt();
            if (size > 0)
            {
                long headerLength = in.readUnsignedVInt();
                DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);
                int entries = (int)in.readUnsignedVInt();
                List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<>(entries);
                for (int i = 0; i < entries; i++)
                    columnsIndex.add(idxSerializer.deserialize(in));

                in.skipBytesFully(entries * TypeSizes.sizeof(0));

                return new IndexedEntry(position, deletionTime, headerLength, columnsIndex);
            }
            else
            {
                return new RowIndexEntry<>(position);
            }
        }

        // Reads only the data 'position' of the index entry and returns it. Note that this left 'in' in the middle
        // of reading an entry, so this is only useful if you know what you are doing and in most case 'deserialize'
        // should be used instead.
        public static long readPosition(DataInputPlus in, Version version) throws IOException
        {
            return version.storeRows() ? in.readUnsignedVInt() : in.readLong();
        }

        public static void skip(DataInputPlus in, Version version) throws IOException
        {
            readPosition(in, version);
            skipPromotedIndex(in, version);
        }

        private static void skipPromotedIndex(DataInputPlus in, Version version) throws IOException
        {
            int size = version.storeRows() ? (int)in.readUnsignedVInt() : in.readInt();
            if (size <= 0)
                return;

            in.skipBytesFully(size);
        }

        public int serializedSize(RowIndexEntry<IndexHelper.IndexInfo> rie)
        {
            assert version.storeRows() : "We read old index files but we should never write them";

            int indexedSize = 0;
            if (rie.isIndexed())
            {
                List<IndexHelper.IndexInfo> index = rie.columnsIndex();

                indexedSize += TypeSizes.sizeofUnsignedVInt(rie.headerLength());
                indexedSize += DeletionTime.serializer.serializedSize(rie.deletionTime());
                indexedSize += TypeSizes.sizeofUnsignedVInt(index.size());

                for (IndexHelper.IndexInfo info : index)
                    indexedSize += idxSerializer.serializedSize(info);

                indexedSize += index.size() * TypeSizes.sizeof(0);
            }

            return TypeSizes.sizeofUnsignedVInt(rie.position) + TypeSizes.sizeofUnsignedVInt(indexedSize) + indexedSize;
        }
    }

    /**
     * An entry in the row index for a row whose columns are indexed.
     */
    private static class IndexedEntry extends RowIndexEntry<IndexHelper.IndexInfo>
    {
        private final DeletionTime deletionTime;

        // The offset in the file when the index entry end
        private final long headerLength;
        private final List<IndexHelper.IndexInfo> columnsIndex;
        private static final long BASE_SIZE =
                ObjectSizes.measure(new IndexedEntry(0, DeletionTime.LIVE, 0, Arrays.<IndexHelper.IndexInfo>asList(null, null)))
              + ObjectSizes.measure(new ArrayList<>(1));

        private IndexedEntry(long position, DeletionTime deletionTime, long headerLength, List<IndexHelper.IndexInfo> columnsIndex)
        {
            super(position);
            assert deletionTime != null;
            assert columnsIndex != null && columnsIndex.size() > 1;
            this.deletionTime = deletionTime;
            this.headerLength = headerLength;
            this.columnsIndex = columnsIndex;
        }

        @Override
        public DeletionTime deletionTime()
        {
            return deletionTime;
        }

        @Override
        public long headerLength()
        {
            return headerLength;
        }

        @Override
        public List<IndexHelper.IndexInfo> columnsIndex()
        {
            return columnsIndex;
        }

        @Override
        protected int promotedSize(IndexHelper.IndexInfo.Serializer idxSerializer)
        {
            long size = TypeSizes.sizeofUnsignedVInt(headerLength)
                      + DeletionTime.serializer.serializedSize(deletionTime)
                      + TypeSizes.sizeofUnsignedVInt(columnsIndex.size()); // number of entries
            for (IndexHelper.IndexInfo info : columnsIndex)
                size += idxSerializer.serializedSize(info);

            size += columnsIndex.size() * TypeSizes.sizeof(0);

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
