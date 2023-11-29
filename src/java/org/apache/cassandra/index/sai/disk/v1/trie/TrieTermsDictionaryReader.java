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
package org.apache.cassandra.index.sai.disk.v1.trie;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.tries.SerializationNode;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.TrieSerializer;
import org.apache.cassandra.io.tries.ValueIterator;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SizedInts;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Page-aware random access reader for a trie terms dictionary written by {@link TrieTermsDictionaryWriter}.
 */
@NotThreadSafe
public class TrieTermsDictionaryReader extends ValueIterator<TrieTermsDictionaryReader> implements Iterator<Pair<ByteComparable, Long>>
{
    public static final long NOT_FOUND = -1;

    public TrieTermsDictionaryReader(Rebufferer rebufferer, long root)
    {
        super(rebufferer, root, true);
    }

    public static final TrieSerializer<Long, DataOutputPlus> trieSerializer = new TrieSerializer<>()
    {
        @Override
        public int sizeofNode(SerializationNode<Long> node, long nodePosition)
        {
            return TrieNode.typeFor(node, nodePosition).sizeofNode(node) + sizeof(node.payload());
        }

        @Override
        public void write(DataOutputPlus dest, SerializationNode<Long> node, long nodePosition) throws IOException
        {
            final TrieNode type = TrieNode.typeFor(node, nodePosition);
            final Long payload = node.payload();
            if (payload != null)
            {
                final int payloadBits = SizedInts.nonZeroSize(payload);
                type.serialize(dest, node, payloadBits, nodePosition);
                SizedInts.write(dest, payload, payloadBits);
            }
            else
            {
                type.serialize(dest, node, 0, nodePosition);
            }
        }

        private int sizeof(Long payload)
        {
            if (payload != null)
            {
                return SizedInts.nonZeroSize(payload);
            }
            return 0;
        }
    };

    public long exactMatch(ByteComparable key)
    {
        int b = follow(key);
        if (b != ByteSource.END_OF_STREAM)
        {
            return NOT_FOUND;
        }
        return getCurrentPayload();
    }

    /**
     * Returns the position associated with the least term greater than or equal to the given key, or
     * a negative value if there is no such term. In order to optimize the search, the trie is traversed
     * statefully. Therefore, this method only returns correct results when called for increasing keys.
     * Warning: ceiling is not idempotent. Calling ceiling() twice for the same key will return successive
     * values instead of the same value. This is acceptable for the current usage of the method.
     * @param key the prefix to traverse in the trie
     * @return a position, if found, or a negative value if there is no such position
     */
    public long ceiling(ByteComparable key)
    {
        skipTo(key, LeftBoundTreatment.ADMIT_EXACT);
        return nextAsLong();
    }

    public long nextAsLong()
    {
        return nextValueAsLong(this::getCurrentPayload, NOT_FOUND);
    }

    @Override
    public boolean hasNext()
    {
        return super.hasNext();
    }

    @Override
    public Pair<ByteComparable, Long> next()
    {
        return nextValue(this::getKeyAndPayload);
    }

    private Pair<ByteComparable, Long> getKeyAndPayload()
    {
        return Pair.create(collectedKey(), getCurrentPayload());
    }

    /**
     * Returns the position associated with the greatest term less than or equal to the given key, or
     * a negative value if there is no such term.
     * @param key the prefix to traverse in the trie
     * @return a position, if found, or a negative value if there is no such position
     */
    public long floor(ByteComparable key)
    {
        Long result = prefixAndNeighbours(key, TrieTermsDictionaryReader::getPayload);
        if (result != null && result != NOT_FOUND)
            return result;
        if (lesserBranch == -1)
            return NOT_FOUND;
        goMax(lesserBranch);
        return getCurrentPayload();
    }

    public ByteComparable getMaxTerm()
    {
        final TransitionBytesCollector collector = new TransitionBytesCollector();
        go(root);
        while (true)
        {
            int lastIdx = transitionRange() - 1;
            long lastChild = transition(lastIdx);
            if (lastIdx < 0)
            {
                return collector.toByteComparable();
            }
            collector.add(transitionByte(lastIdx));
            go(lastChild);
        }
    }

    public ByteComparable getMinTerm()
    {
        final TransitionBytesCollector collector = new TransitionBytesCollector();
        go(root);
        while (true)
        {
            int payloadBits = payloadFlags();
            if (payloadBits > 0)
            {
                return collector.toByteComparable();
            }
            collector.add(transitionByte(0));
            go(transition(0));
        }
    }

    private long getCurrentPayload()
    {
        return getPayload(payloadPosition(), payloadFlags());
    }

    private long getPayload(int payloadPos, int bits)
    {
        return getPayload(buf, payloadPos, bits);
    }

    private static long getPayload(ByteBuffer contents, int payloadPos, int bytes)
    {
        if (bytes == 0)
            return NOT_FOUND;

        return SizedInts.read(contents, payloadPos, bytes);
    }
}
