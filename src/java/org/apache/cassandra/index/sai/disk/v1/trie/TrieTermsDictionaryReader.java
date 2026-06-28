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
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.tries.SerializationNode;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.TrieSerializer;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SizedInts;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Page-aware random access reader for a trie terms dictionary written by {@link TrieTermsDictionaryWriter}.
 */
@NotThreadSafe
public class TrieTermsDictionaryReader extends Walker<TrieTermsDictionaryReader>
{
    public static final long NOT_FOUND = -1;

    public TrieTermsDictionaryReader(Rebufferer rebufferer, long root)
    {
        super(rebufferer, root);
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
            TrieNode type = TrieNode.typeFor(node, nodePosition);
            Long payload = node.payload();
            int payloadBits = sizeof(payload);
            type.serialize(dest, node, payloadBits, nodePosition);

            if (payload != null)
                SizedInts.write(dest, payload, payloadBits);
        }

        private int sizeof(Long payload)
        {
            return payload == null ? 0 : SizedInts.nonZeroSize(payload);
        }
    };

    public long exactMatch(ByteComparable key)
    {
        // Since we are looking for an exact match we are always expecting the follow
        // to return END_OF_STREAM if the key was found.
        return follow(key) == ByteSource.END_OF_STREAM ? getCurrentPayload() : NOT_FOUND;
    }

    /**
     * Navigates to the trie node for {@code prefix}, positioning the walker at the subtree root for
     * all terms that start with {@code prefix}.
     * <p>
     * The {@link ByteSource} encoding for string types ends with an {@code ESCAPE} (0x00) terminator
     * before {@code END_OF_STREAM}. Using a one-byte look-ahead, we detect the terminator before
     * following it: when the current byte is {@code ESCAPE} and the next is {@code END_OF_STREAM},
     * we stop at the current node (which is exactly the prefix subtree root) rather than following
     * the terminator into the exact-match child. This correctly handles both:
     * <ul>
     *   <li>Prefixes that are NOT terms themselves (e.g. "grp42x" when only "grp42x…" variants exist)</li>
     *   <li>Prefixes that ARE terms (e.g. "exact" when both "exact" and "exact_*" are indexed)</li>
     * </ul>
     *
     * @return the trie node position if the full prefix path exists in the trie, or {@link #NOT_FOUND}
     *         when no indexed term starts with {@code prefix}.
     */
    public long followToPrefix(ByteComparable prefix)
    {
        ByteSource stream = prefix.asComparableBytes(BYTE_COMPARABLE_VERSION);
        go(root);

        int cur = stream.next();
        while (cur != ByteSource.END_OF_STREAM)
        {
            int next = stream.next(); // one-byte look-ahead

            // ESCAPE (0x00) followed by END_OF_STREAM is the null-escape terminator — do NOT follow it.
            // The current node is the prefix subtree root: all children represent "prefix*" terms.
            if (cur == ByteSource.ESCAPE && next == ByteSource.END_OF_STREAM)
                return position;

            int childIndex = search(cur);
            if (childIndex < 0)
                return NOT_FOUND; // a content byte is not in the trie → no terms start with prefix
            go(transition(childIndex));
            cur = next;
        }
        return position; // all bytes consumed without failure
    }

    /**
     * Positions the walker at {@code nodePos} and returns its payload (postings file offset),
     * or {@link #NOT_FOUND} if the node carries no payload.
     */
    public long payloadAt(long nodePos)
    {
        go(nodePos);
        return getCurrentPayload();
    }

    /**
     * Positions the walker at {@code nodePos} and returns the file positions of all non-null children,
     * in transition-byte order. Children are collected into an array before returning so that the caller
     * can safely recurse without the walker position being clobbered mid-iteration.
     */
    public long[] childrenOf(long nodePos)
    {
        go(nodePos);
        int count = transitionRange();
        long[] children = new long[count];
        int filled = 0;
        for (int i = 0; i < count; i++)
        {
            long child = transition(i);
            if (child != NONE)
                children[filled++] = child;
        }
        return filled == count ? children : Arrays.copyOf(children, filled);
    }

    private long getCurrentPayload()
    {
        return getPayloadAt(buf, payloadPosition(), payloadFlags());
    }

    private long getPayloadAt(ByteBuffer contents, int payloadPos, int bytes)
    {
        if (bytes == 0)
            return NOT_FOUND;
        return SizedInts.read(contents, payloadPos, bytes);
    }
}

