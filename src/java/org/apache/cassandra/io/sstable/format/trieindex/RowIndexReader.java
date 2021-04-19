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
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.tries.SerializationNode;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.TrieSerializer;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.SizedInts;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Reader class for row index files.
 *
 * Row index tries do not need to store whole keys, as what we need from them is to be able to tell where in the data file
 * to start looking for a given key. Instead, we store some prefix that is greater than the greatest key of the previous
 * index section and smaller than or equal to the smallest key of the next. So for a given key the first index section
 * that could potentially contain it is given by the trie's floor for that key.
 *
 * This builds upon the trie Walker class which provides basic trie walking functionality. The class is thread-unsafe
 * and must be re-instantiated for every thread that needs access to the trie (its overhead is below that of a
 * RandomAccessReader).
 */
class RowIndexReader extends Walker<RowIndexReader>
{
    private static final int FLAG_OPEN_MARKER = 8;

    static class IndexInfo
    {
        final long offset;
        final DeletionTime openDeletion;

        IndexInfo(long offset, DeletionTime openDeletion)
        {
            this.offset = offset;
            this.openDeletion = openDeletion;
        }
    }

    public RowIndexReader(FileHandle file, long root)
    {
        super(file.instantiateRebufferer(), root);
    }

    public RowIndexReader(FileHandle file, TrieIndexEntry entry)
    {
        this(file, entry.indexTrieRoot);
    }

    /**
     * Computes the floor for a given key.
     */
    public IndexInfo separatorFloor(ByteComparable key)
    {
        // Check for a prefix and find closest smaller branch.
        IndexInfo res = prefixAndNeighbours(key, RowIndexReader::readPayload);
        // If there's a prefix, in a separator trie it could be less than, equal, or greater than sought value.
        // Sought value is still greater than max of previous section.
        // On match the prefix must be used as a starting point.
        if (res != null)
            return res;

        // Otherwise return the IndexInfo for the closest entry of the smaller branch (which is the max of lesserBranch).
        // Note (see prefixAndNeighbours): since we accept prefix matches above, at this point there cannot be another
        // prefix match that is closer than max(lesserBranch).
        if (lesserBranch == -1)
            return null;
        goMax(lesserBranch);
        return getCurrentIndexInfo();
    }

    public IndexInfo min()
    {
        goMin(root);
        return getCurrentIndexInfo();
    }

    protected IndexInfo getCurrentIndexInfo()
    {
        return readPayload(payloadPosition(), payloadFlags());
    }

    protected IndexInfo readPayload(int ppos, int bits)
    {
        return readPayload(buf, ppos, bits);
    }

    static IndexInfo readPayload(ByteBuffer buf, int ppos, int bits)
    {
        long dataOffset;
        if (bits == 0)
            return null;
        int bytes = bits & ~FLAG_OPEN_MARKER;
        dataOffset = SizedInts.read(buf, ppos, bytes);
        ppos += bytes;
        DeletionTime deletion = (bits & FLAG_OPEN_MARKER) != 0
                ? DeletionTime.serializer.deserialize(buf, ppos)
                : null;
        return new IndexInfo(dataOffset, deletion);
    }

    // The trie serializer describes how the payloads are written. Placed here (instead of writer) so that reading and
    // writing the payload are close together should they need to be changed.
    static final TrieSerializer<IndexInfo, DataOutputPlus> trieSerializer = new TrieSerializer<IndexInfo, DataOutputPlus>()
    {
        @Override
        public int sizeofNode(SerializationNode<IndexInfo> node, long nodePosition)
        {
            return TrieNode.typeFor(node, nodePosition).sizeofNode(node) + sizeof(node.payload());
        }

        @Override
        public void write(DataOutputPlus dest, SerializationNode<IndexInfo> node, long nodePosition) throws IOException
        {
            write(dest, TrieNode.typeFor(node, nodePosition), node, nodePosition);
        }

        public int sizeof(IndexInfo payload)
        {
            int size = 0;
            if (payload != null)
            {
                size += SizedInts.nonZeroSize(payload.offset);
                if (!payload.openDeletion.isLive())
                    size += DeletionTime.serializer.serializedSize(payload.openDeletion);
            }
            return size;
        }

        public void write(DataOutputPlus dest, TrieNode type, SerializationNode<IndexInfo> node, long nodePosition) throws IOException
        {
            IndexInfo payload = node.payload();
            int bytes = 0;
            int hasOpenMarker = 0;
            if (payload != null)
            {
                bytes = SizedInts.nonZeroSize(payload.offset);
                if (!payload.openDeletion.isLive())
                    hasOpenMarker = FLAG_OPEN_MARKER;
            }
            type.serialize(dest, node, bytes | hasOpenMarker, nodePosition);
            if (payload != null)
            {
                SizedInts.write(dest, payload.offset, bytes);

                if (hasOpenMarker != 0)
                    DeletionTime.serializer.serialize(payload.openDeletion, dest);
            }
        }

    };

    @SuppressWarnings("unused")
    public void dumpTrie(PrintStream out)
    {
        dumpTrie(out, (buf, ppos, bits) -> {
            IndexInfo ii = readPayload(buf, ppos, bits);

            return ii != null
                   ? String.format("pos %x %s", ii.offset, ii.openDeletion == null ? "" : ii.openDeletion)
                   : "pos null";
        });
    }
}
