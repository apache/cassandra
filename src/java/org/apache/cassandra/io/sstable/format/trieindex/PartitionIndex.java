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

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.tries.SerializationNode;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.TrieSerializer;
import org.apache.cassandra.io.tries.ValueIterator;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.PageAware;
import org.apache.cassandra.utils.SizedInts;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * This class holds the partition index as an on-disk trie mapping unique prefixes of decorated keys to:
 *     - data file position if the partition is small enough to not need an index
 *     - row index file position if the partition has a row index
 * plus
 *     - the last 8 bits of the key's filter hash which is used to filter out mismatched keys without reading the key
 *
 * To avoid having to create an object to carry the result, the two are distinguished by sign. Direct-to-dfile entries
 * are recorded as ~position (~ instead of - to differentiate 0 in ifile from 0 in dfile).
 *
 * In either case the contents of the file at this position start with a serialization of the key which can be used
 * to verify the correct key is found.
 *
 * To read the index one must obtain a thread-unsafe Reader or IndexPosIterator.
 *
 * Not to be used outside of package. Public only so that it can be used by tools (IndexAnalyzer and IndexRewriter).
 */
public class PartitionIndex implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionIndex.class);

    private final FileHandle fh;
    private final long keyCount;
    private final DecoratedKey first;
    private final DecoratedKey last;
    private final long root;
    /** Key to apply when a caller asks for a full index. Normally null, but set to first for zero-copied indexes. */
    private final DecoratedKey filterFirst;
    /** Key to apply when a caller asks for a full index. Normally null, but set to last for zero-copied indexes. */
    private final DecoratedKey filterLast;

    public static final long NOT_FOUND = Long.MIN_VALUE;
    public static final int FOOTER_LENGTH = 3 * 8;

    public PartitionIndex(FileHandle fh, long trieRoot, long keyCount, DecoratedKey first, DecoratedKey last, DecoratedKey filterFirst, DecoratedKey filterLast)
    {
        this.keyCount = keyCount;
        this.fh = fh.sharedCopy();
        this.first = first;
        this.last = last;
        this.root = trieRoot;
        this.filterFirst = filterFirst;
        this.filterLast = filterLast;
    }

    private PartitionIndex(PartitionIndex src)
    {
        this(src.fh, src.root, src.keyCount, src.first, src.last, src.filterFirst, src.filterLast);
    }

    static class Payload
    {
        final long position;
        final short hashBits;

        public Payload(long position, short hashBits)
        {
            this.position = position;
            assert this.position != NOT_FOUND;
            this.hashBits = hashBits;
        }
    }

    static final PartitionIndexSerializer TRIE_SERIALIZER = new PartitionIndexSerializer();

    private static class PartitionIndexSerializer implements TrieSerializer<Payload, DataOutput>
    {
        public int sizeofNode(SerializationNode<Payload> node, long nodePosition)
        {
            return TrieNode.typeFor(node, nodePosition).sizeofNode(node) +
                   (node.payload() != null ? 1 + SizedInts.nonZeroSize(node.payload().position) : 0);
        }

        @Override
        public void write(DataOutput dest, SerializationNode<Payload> node, long nodePosition) throws IOException
        {
            write(dest, TrieNode.typeFor(node, nodePosition), node, nodePosition);
        }

        public void write(DataOutput dest, TrieNode type, SerializationNode<Payload> node, long nodePosition) throws IOException
        {
            Payload payload = node.payload();
            if (payload != null)
            {
                int payloadBits;
                int size = SizedInts.nonZeroSize(payload.position);
                payloadBits = 7 + size;
                type.serialize(dest, node, payloadBits, nodePosition);
                dest.writeByte(payload.hashBits);
                SizedInts.write(dest, payload.position, size);
            }
            else
                type.serialize(dest, node, 0, nodePosition);
        }
    }

    public long size()
    {
        return keyCount;
    }

    public DecoratedKey firstKey()
    {
        return first;
    }

    public DecoratedKey lastKey()
    {
        return last;
    }

    public PartitionIndex sharedCopy()
    {
        return new PartitionIndex(this);
    }

    public void addTo(Ref.IdentityCollection identities)
    {
        fh.addTo(identities);
    }

    public static PartitionIndex load(FileHandle.Builder fhBuilder,
                                      IPartitioner partitioner,
                                      boolean preload) throws IOException
    {
        try (FileHandle fh = fhBuilder.complete())
        {
            return load(fh, partitioner, preload);
        }
    }

    public static PartitionIndex load(FileHandle fh, IPartitioner partitioner, boolean preload) throws IOException
    {
        try (FileDataInput rdr = fh.createReader(fh.dataLength() - FOOTER_LENGTH))
        {
            long firstPos = rdr.readLong();
            long keyCount = rdr.readLong();
            long root = rdr.readLong();
            rdr.seek(firstPos);
            DecoratedKey first = partitioner != null ? partitioner.decorateKey(ByteBufferUtil.readWithShortLength(rdr)) : null;
            DecoratedKey last = partitioner != null ? partitioner.decorateKey(ByteBufferUtil.readWithShortLength(rdr)) : null;
            if (preload)
            {
                int csum = 0;
                // force a read of all the pages of the index
                for (long pos = 0; pos < fh.dataLength(); pos += PageAware.PAGE_SIZE)
                {
                    rdr.seek(pos);
                    csum += rdr.readByte();
                }
                logger.trace("Checksum {}", csum);      // Note: trace is required so that reads aren't optimized away.
            }

            return new PartitionIndex(fh, root, keyCount, first, last, null, null);
        }
    }

    @Override
    public void close()
    {
        fh.close();
    }

    public Reader openReader()
    {
        return new Reader(this);
    }

    protected IndexPosIterator allKeysIterator()
    {
        return new IndexPosIterator(this);
    }

    protected Rebufferer instantiateRebufferer()
    {
        return fh.instantiateRebufferer();
    }


    /**
     * @return the file handle to the file on disk. This is needed for locking the index in RAM,
     * see DB-342 and follow up ticket on how this should be reworked.
     */
    FileHandle getFileHandle()
    {
        return fh;
    }

    private static long getIndexPos(ByteBuffer contents, int payloadPos, int bytes)
    {
        if (bytes > 7)
        {
            ++payloadPos;
            bytes -= 7;
        }
        if (bytes == 0)
            return NOT_FOUND;
        return SizedInts.read(contents, payloadPos, bytes);
    }

    public interface Acceptor<ArgType, ResultType>
    {
        ResultType accept(long position, boolean assumeNoMatch, ArgType v) throws IOException;
    }

    /**
     * Provides methods to read the partition index trie.
     * Thread-unsafe, uses class members to store lookup state.
     */
    public static class Reader extends Walker<Reader>
    {
        protected Reader(PartitionIndex index)
        {
            super(index.instantiateRebufferer(), index.root);
        }

        /**
         * Finds a candidate for an exact key search. Returns an ifile (if positive) or dfile (if negative, using ~)
         * position. The position returned has a low chance of being a different entry, but only if the sought key
         * is not present in the file.
         */
        public long exactCandidate(DecoratedKey key)
        {
            // A hit must be a prefix of the byte-comparable representation of the key.
            int b = follow(key);
            // If the prefix ended in a node with children it is only acceptable if it is a full match.
            if (b != ByteSource.END_OF_STREAM && hasChildren())
                return NOT_FOUND;
            if (!checkHashBits(key.filterHashLowerBits()))
                return NOT_FOUND;
            return getCurrentIndexPos();
        }

        final boolean checkHashBits(short hashBits)
        {
            int bytes = payloadFlags();
            if (bytes <= 7)
                return bytes > 0;
            return (buf.get(payloadPosition()) == (byte) hashBits);
        }

        public <ResultType> ResultType ceiling(PartitionPosition key, Acceptor<PartitionPosition, ResultType> acceptor) throws IOException
        {
            // Look for a prefix of the key. If there is one, the key it stands for could be less, equal, or greater
            // than the required value so try that first.
            int b = followWithGreater(key);
            // If the prefix ended in a node with children it is only acceptable if it is a full match.
            if (!hasChildren() || b == ByteSource.END_OF_STREAM)
            {
                long indexPos = getCurrentIndexPos();
                if (indexPos != NOT_FOUND)
                {
                    ResultType res = acceptor.accept(indexPos, false, key);
                    if (res != null)
                        return res;
                }
            }
            // If that was not found, the closest greater value can be used instead, and we know that
            // it stands for a key greater than the argument.
            if (greaterBranch == -1)
                return null;
            goMin(greaterBranch);
            long indexPos = getCurrentIndexPos();
            return acceptor.accept(indexPos, true, key);
        }


        public <ResultType> ResultType floor(PartitionPosition key, Acceptor<PartitionPosition, ResultType> acceptor) throws IOException
        {
            // Check for a prefix and find closest smaller branch.
            Long indexPos = prefixAndNeighbours(key, Reader::getSpecificIndexPos);

            if (indexPos != null && indexPos != NOT_FOUND)
            {
                ResultType res = acceptor.accept(indexPos, false, key);
                if (res != null)
                    return res;
            }

            // Otherwise return the IndexInfo for the closest entry of the smaller branch (which is the max of lesserBranch).
            // Note (see prefixAndNeighbours): since we accept prefix matches above, at this point there cannot be another
            // prefix match that is closer than max(lesserBranch).
            if (lesserBranch == -1)
                return null;
            goMax(lesserBranch);
            indexPos = getCurrentIndexPos();

            return acceptor.accept(indexPos, true, key);
        }


        public Long getSpecificIndexPos(int pos, int bits)
        {
            return getIndexPos(buf, pos, bits);
        }

        public long getCurrentIndexPos()
        {
            return getIndexPos(buf, payloadPosition(), payloadFlags());
        }

        public long getLastIndexPosition()
        {
            goMax(root);
            return getCurrentIndexPos();
        }

        /**
         * To be used only in analysis.
         */
        @SuppressWarnings("unused")
        protected int payloadSize()
        {
            int bytes = payloadFlags();
            return bytes > 7 ? bytes - 6 : bytes;
        }
    }

    /**
     * Iterator of index positions covered between two keys. Since we store prefixes only, the first and last returned
     * values can be outside the span (and inclusiveness is not given as we cannot verify it).
     */
    public static class IndexPosIterator extends ValueIterator<IndexPosIterator>
    {
        static final long INVALID = -1;
        long pos = INVALID;

        /**
         * @param index PartitionIndex to use for the iteration.
         *
         * Note: For performance reasons this class does not keep a reference of the index. Caller must ensure a
         * reference is held for the lifetime of this object.
         */
        public IndexPosIterator(PartitionIndex index)
        {
            super(index.instantiateRebufferer(), index.root, index.filterFirst, index.filterLast, true);
        }

        IndexPosIterator(PartitionIndex index, PartitionPosition start, PartitionPosition end)
        {
            super(index.instantiateRebufferer(), index.root, start, end, true);
        }

        /**
         * Returns the position in the row index or data file.
         */
        protected long nextIndexPos()
        {
            // without missing positions, we save and reuse the unreturned position.
            if (pos == INVALID)
            {
                pos = nextPayloadedNode();
                if (pos == INVALID)
                    return NOT_FOUND;
            }

            go(pos);

            pos = INVALID; // make sure next time we call nextPayloadedNode() again
            return getIndexPos(buf, payloadPosition(), payloadFlags()); // this should not throw
        }
    }

    /**
     * debug/test code
     */
    @VisibleForTesting
    public void dumpTrie(String fileName)
    {
        try(PrintStream ps = new PrintStream(fileName))
        {
            dumpTrie(ps);
        }
        catch (Throwable t)
        {
            logger.warn("Failed to dump trie to {} due to exception {}", fileName, t);
        }
    }

    private void dumpTrie(PrintStream out)
    {
        try (Reader rdr = openReader())
        {
            rdr.dumpTrie(out, (buf, ppos, pbits) -> Long.toString(getIndexPos(buf, ppos, pbits)));
        }
    }

}
