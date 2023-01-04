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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.format.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * An entry in the row index for a partition whose rows are indexed in a trie.
 * <p>
 * Not to be used outside of package. Public only for IndexRewriter tool.
 */
public final class TrieIndexEntry extends AbstractRowIndexEntry
{
    private static final long BASE_SIZE;

    static
    {
        BASE_SIZE = ObjectSizes.measure(new TrieIndexEntry(0, 0, 10, DeletionTime.LIVE));
    }

    final long indexTrieRoot;
    private final int rowIndexCount;
    private final DeletionTime deletionTime;

    TrieIndexEntry(long dataFilePosition, long indexTrieRoot, int rowIndexCount, DeletionTime deletionTime)
    {
        super(dataFilePosition);
        this.indexTrieRoot = indexTrieRoot;
        this.rowIndexCount = rowIndexCount;
        this.deletionTime = deletionTime;
    }

    public TrieIndexEntry(long position)
    {
        super(position);
        this.indexTrieRoot = -1;
        this.rowIndexCount = 0;
        this.deletionTime = null;
    }

    @Override
    public int columnsIndexCount()
    {
        return rowIndexCount;
    }

    @Override
    public SSTableFormat<?, ?> getSSTableFormat()
    {
        return BtiFormat.instance;
    }

    @Override
    public void serializeForCache(DataOutputPlus out) throws IOException
    {
        if (indexTrieRoot == -1)
        {
            out.writeByte(0);
            out.writeUnsignedVInt(position);
        }
        else
        {
            out.writeByte(1);
            serialize(out, 0);
        }
    }

    public static TrieIndexEntry deserializeForCache(DataInputPlus in) throws IOException
    {
        byte type = in.readByte();
        if (type == 0)
            return new TrieIndexEntry(in.readUnsignedVInt(), -1, 0, null);
        else
            return deserialize(in, 0);
    }

    @Override
    public DeletionTime deletionTime()
    {
        return deletionTime;
    }

    @Override
    public long unsharedHeapSize()
    {
        return BASE_SIZE;
    }

    public void serialize(DataOutputPlus indexFile, long basePosition) throws IOException
    {
        assert indexTrieRoot != -1 && rowIndexCount > 0 && deletionTime != null;
        indexFile.writeUnsignedVInt(position);
        indexFile.writeVInt(indexTrieRoot - basePosition);
        indexFile.writeUnsignedVInt(rowIndexCount);
        DeletionTime.serializer.serialize(deletionTime, indexFile);
    }

    /**
     * Create an index entry. The row index trie must already have been written (by RowIndexWriter) to the row index
     * file and its root position must be specified in trieRoot.
     */
    public static TrieIndexEntry create(long dataStartPosition,
                                        long trieRoot,
                                        DeletionTime partitionLevelDeletion,
                                        int rowIndexCount)
    {
        return new TrieIndexEntry(dataStartPosition, trieRoot, trieRoot == -1 ? 0 : rowIndexCount, partitionLevelDeletion);
    }

    public static TrieIndexEntry deserialize(DataInputPlus in, long basePosition) throws IOException
    {
        long dataFilePosition = in.readUnsignedVInt();
        long indexTrieRoot = in.readVInt() + basePosition;
        int rowIndexCount = (int) in.readUnsignedVInt();
        DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);
        return new TrieIndexEntry(dataFilePosition, indexTrieRoot, rowIndexCount, deletionTime);
    }

    static class KeyCacheValueSerializer implements AbstractRowIndexEntry.KeyCacheValueSerializer<BtiTableReader, TrieIndexEntry>
    {
        public final static KeyCacheValueSerializer instance = new KeyCacheValueSerializer();

        @Override
        public void skip(DataInputPlus input) throws IOException
        {
            deserializeForCache(input);
        }

        @Override
        public TrieIndexEntry deserialize(BtiTableReader reader, DataInputPlus input) throws IOException
        {
            return deserializeForCache(input);
        }

        @Override
        public void serialize(TrieIndexEntry entry, DataOutputPlus output) throws IOException
        {
            entry.serializeForCache(output);
        }
    }
}
