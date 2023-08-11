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
import java.io.PrintStream;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.tries.SerializationNode;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.TrieSerializer;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SizedInts;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Reader class for row index files created by {@link RowIndexWriter}.
 * <p>
 * Row index "tries" do not need to store whole keys, as what we need from them is to be able to tell where in the data file
 * to start looking for a given key. Instead, we store some prefix that is greater than the greatest key of the previous
 * index section and smaller than or equal to the smallest key of the next. So for a given key the first index section
 * that could potentially contain it is given by the trie's floor for that key.
 * <p>
 * This builds upon the trie Walker class which provides basic trie walking functionality. The class is thread-unsafe
 * and must be re-instantiated for every thread that needs access to the trie (its overhead is below that of a
 * {@link org.apache.cassandra.io.util.RandomAccessReader}).
 */
@NotThreadSafe
public class RowIndexReader extends Walker<RowIndexReader>
{
    public final Version version;
    private static final int FLAG_OPEN_MARKER = 8;

    public static class IndexInfo
    {
        public final long offset;
        public final DeletionTime openDeletion;

        IndexInfo(long offset, DeletionTime openDeletion)
        {
            this.offset = offset;
            this.openDeletion = openDeletion;
        }
    }

    public RowIndexReader(FileHandle file, long root, Version version)
    {
        super(file.instantiateRebufferer(null), root);
        this.version = version;
    }

    public RowIndexReader(FileHandle file, TrieIndexEntry entry, Version version)
    {
        this(file, entry.indexTrieRoot, version);
    }

    /**
     * Computes the floor for a given key.
     * @throws IOException 
     */
    public IndexInfo separatorFloor(ByteComparable key) throws IOException
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
        if (lesserBranch == NONE)
            return null;
        goMax(lesserBranch);
        return getCurrentIndexInfo();
    }

    public IndexInfo min() throws IOException
    {
        goMin(root);
        return getCurrentIndexInfo();
    }

    protected IndexInfo getCurrentIndexInfo() throws IOException
    {
        return readPayload(payloadPosition(), payloadFlags());
    }

    protected IndexInfo readPayload(int ppos, int bits) throws IOException
    {
        return readPayload(buf, ppos, bits, version);
    }

    static IndexInfo readPayload(ByteBuffer buf, int ppos, int bits, Version version) throws IOException
    {
        long dataOffset;
        if (bits == 0)
            return null;
        int bytes = bits & ~FLAG_OPEN_MARKER;
        dataOffset = SizedInts.read(buf, ppos, bytes);
        ppos += bytes;
        DeletionTime deletion = (bits & FLAG_OPEN_MARKER) != 0
                ? DeletionTime.getSerializer(version).deserialize(buf, ppos)
                : null;
        return new IndexInfo(dataOffset, deletion);
    }

    // The trie serializer describes how the payloads are written. Placed here (instead of writer) so that reading and
    // writing the payload are close together should they need to be changed.
    static final TrieSerializer<IndexInfo, DataOutputPlus> getSerializer(Version version)
    {
        return new TrieSerializer<IndexInfo, DataOutputPlus>()
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

            private int sizeof(IndexInfo payload)
            {
                int size = 0;
                if (payload != null)
                {
                    size += SizedInts.nonZeroSize(payload.offset);
                    if (!payload.openDeletion.isLive())
                        size += DeletionTime.getSerializer(version).serializedSize(payload.openDeletion);
                }
                return size;
            }

            private void write(DataOutputPlus dest, TrieNode type, SerializationNode<IndexInfo> node, long nodePosition) throws IOException
            {
                IndexInfo payload = node.payload();
                int bytes = 0;
                int hasOpenMarker = 0;
                if (payload != null)
                {
                    bytes = SizedInts.nonZeroSize(payload.offset);
                    assert bytes < 8 : "Row index does not support rows larger than 32 PiB";
                    if (!payload.openDeletion.isLive())
                        hasOpenMarker = FLAG_OPEN_MARKER;
                }
                type.serialize(dest, node, bytes | hasOpenMarker, nodePosition);
                if (payload != null)
                {
                    SizedInts.write(dest, payload.offset, bytes);

                    if (hasOpenMarker == FLAG_OPEN_MARKER)
                        DeletionTime.getSerializer(version).serialize(payload.openDeletion, dest);
                }
            }

        };
    }

    // debug/test code
    @SuppressWarnings("unused")
    public void dumpTrie(PrintStream out) throws IOException
    {
        dumpTrie(out, RowIndexReader::dumpRowIndexEntry, version);
    }

    static String dumpRowIndexEntry(ByteBuffer buf, int ppos, int bits, Version version) throws IOException
    {
        IndexInfo ii = readPayload(buf, ppos, bits, version);

        return ii != null
               ? String.format("pos %x %s", ii.offset, ii.openDeletion == null ? "" : ii.openDeletion)
               : "pos null";
    }
}
