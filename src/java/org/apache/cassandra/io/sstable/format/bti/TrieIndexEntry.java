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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Index entry for the BTI partition index. This can be a simple position in the data file, or an entry in the row
 * index file where the rows are indexed in blocks (see {@link RowIndexReader}).
 */
final class TrieIndexEntry extends AbstractRowIndexEntry
{
    final long indexTrieRoot;
    private final int rowIndexBlockCount;
    private final DeletionTime deletionTime;

    TrieIndexEntry(long dataFilePosition, long indexTrieRoot, int rowIndexBlockCount, DeletionTime deletionTime)
    {
        super(dataFilePosition);
        this.indexTrieRoot = indexTrieRoot;
        this.rowIndexBlockCount = rowIndexBlockCount;
        this.deletionTime = deletionTime;
    }

    public TrieIndexEntry(long position)
    {
        super(position);
        this.indexTrieRoot = -1;
        this.rowIndexBlockCount = 0;
        this.deletionTime = null;
    }

    @Override
    public int blockCount()
    {
        return rowIndexBlockCount;
    }

    @Override
    public SSTableFormat<?, ?> getSSTableFormat()
    {
        throw noKeyCacheError();
    }

    @Override
    public void serializeForCache(DataOutputPlus out)
    {
        throw noKeyCacheError();
    }

    private static AssertionError noKeyCacheError()
    {
        return new AssertionError("BTI SSTables should not use key cache");
    }

    @Override
    public DeletionTime deletionTime()
    {
        return deletionTime;
    }

    @Override
    public long unsharedHeapSize()
    {
        throw new AssertionError("BTI SSTables index entries should not be persisted in any in-memory structure");
    }

    public void serialize(DataOutputPlus indexFile, long basePosition, Version version) throws IOException
    {
        assert indexTrieRoot != -1 && rowIndexBlockCount > 0 && deletionTime != null;
        indexFile.writeUnsignedVInt(position);
        indexFile.writeVInt(indexTrieRoot - basePosition);
        indexFile.writeUnsignedVInt32(rowIndexBlockCount);
        DeletionTime.getSerializer(version).serialize(deletionTime, indexFile);
    }

    /**
     * Create an index entry. The row index trie must already have been written (by RowIndexWriter) to the row index
     * file and its root position must be specified in trieRoot.
     */
    public static TrieIndexEntry create(long dataStartPosition,
                                        long trieRoot,
                                        DeletionTime partitionLevelDeletion,
                                        int rowIndexBlockCount)
    {
        return new TrieIndexEntry(dataStartPosition, trieRoot, trieRoot == -1 ? 0 : rowIndexBlockCount, partitionLevelDeletion);
    }

    public static TrieIndexEntry deserialize(DataInputPlus in, long basePosition, Version version) throws IOException
    {
        long dataFilePosition = in.readUnsignedVInt();
        long indexTrieRoot = in.readVInt() + basePosition;
        int rowIndexBlockCount = in.readUnsignedVInt32();
        DeletionTime deletionTime = DeletionTime.getSerializer(version).deserialize(in);
        return new TrieIndexEntry(dataFilePosition, indexTrieRoot, rowIndexBlockCount, deletionTime);
    }

}
