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
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * An entry in the row index for a partition whose rows are indexed in a trie.
 *
 * Not to be used outside of package. Public only for IndexRewriter tool.
 */
public final class TrieIndexEntry extends RowIndexEntry
{
    private static final long BASE_SIZE;

    static
    {
        BASE_SIZE = ObjectSizes.measure(new TrieIndexEntry(0, 0, 10, DeletionTime.LIVE));
    }

    final long indexTrieRoot;
    private final int rowIndexCount;
    private final DeletionTime deletionTime;

    TrieIndexEntry(long dataFilePosition, long indexTrieRoot, int rowIndexCount,
                   DeletionTime deletionTime)
    {
        super(dataFilePosition);

        assert rowIndexCount > 1;
        this.indexTrieRoot = indexTrieRoot;
        this.rowIndexCount = rowIndexCount;
        this.deletionTime = deletionTime;
    }

    @Override
    public int columnsIndexCount()
    {
        return rowIndexCount;
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
        indexFile.writeUnsignedVInt(position);
        indexFile.writeVInt(indexTrieRoot - basePosition);
        indexFile.writeUnsignedVInt(rowIndexCount);
        DeletionTime.serializer.serialize(deletionTime, indexFile);
    }

    /**
     * Create an index entry. The row index trie must already have been written (by RowIndexWriter) to the row index
     * file and its root position must be specified in trieRoot.
     */
    public static RowIndexEntry create(long dataStartPosition,
                                       long trieRoot,
                                       DeletionTime partitionLevelDeletion,
                                       int rowIndexCount)
    {
        if (trieRoot == -1)
            return new RowIndexEntry(dataStartPosition) {};
        return new TrieIndexEntry(dataStartPosition, trieRoot, rowIndexCount, partitionLevelDeletion);
    }

    public static RowIndexEntry deserialize(DataInputPlus in, long basePosition) throws IOException
    {
        long dataFilePosition = in.readUnsignedVInt();
        long indexTrieRoot = in.readVInt() + basePosition;
        int rowIndexCount = (int) in.readUnsignedVInt();
        DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);
        return new TrieIndexEntry(dataFilePosition, indexTrieRoot, rowIndexCount, deletionTime);
    }
}