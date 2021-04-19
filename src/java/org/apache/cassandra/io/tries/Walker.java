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
package org.apache.cassandra.io.tries;

import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.Rebufferer.BufferHolder;
import org.apache.cassandra.utils.PageAware;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Thread-unsafe trie walking helper. This is analogous to RandomAccessReader for tries -- takes an on-disk trie
 * accessible via a supplied Rebufferer and lets user seek to nodes and work with them.
 * <p>
 * Assumes data was written using page-aware builder and thus no node crosses a page and thus a buffer boundary.
 */
// TODO STAR-247: unit test are insufficient - they did not catch a problem fixed in STAR-247
public class Walker<VALUE extends Walker<VALUE>> implements AutoCloseable
{
    private final Rebufferer source;
    protected final long root;

    // State relating to current node.
    private BufferHolder bh;    // from Rebufferer
    private int offset;         // offset of current node within buf
    protected TrieNode nodeType;  // type of current node
    protected ByteBuffer buf;   // buffer containing the data
    protected long position;    // file position of current node

    // State relating to searches.
    protected long greaterBranch;
    protected long lesserBranch;

    // Version of the byte comparable conversion to use -- trie-based indices use the 6.0 conversion
    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS41;

    /**
     * Creates a walker. Rebufferer must be aligned and with a buffer size that is at least 4k.
     */
    public Walker(Rebufferer source, long root)
    {
        this.source = source;
        this.root = root;
        try
        {
            bh = source.rebuffer(root);
            buf = bh.buffer();
        }
        catch (RuntimeException ex)
        {
            if (bh != null) bh.release();
            source.closeReader();
            throw ex;
        }
    }

    public void close()
    {
        bh.release();
        source.closeReader();
    }

    protected final void go(long position)
    {
        long curOffset = position - bh.offset();
        if (curOffset < 0 || curOffset >= buf.limit())
        {
            bh.release();
            bh = Rebufferer.EMPTY; // prevents double release if the call below fails
            bh = source.rebuffer(position);
            buf = bh.buffer();
            curOffset = position - bh.offset();
            assert curOffset >= 0 && curOffset < buf.limit() : String.format("Invalid offset: %d, buf: %s, bh: %s", curOffset, buf, bh);
        }
        this.offset = (int) curOffset;
        this.position = position;
        nodeType = TrieNode.at(buf, (int) curOffset);
    }

    protected final int payloadFlags()
    {
        return nodeType.payloadFlags(buf, offset);
    }

    protected final int payloadPosition()
    {
        return nodeType.payloadPosition(buf, offset);
    }

    protected final int search(int transitionByte)
    {
        return nodeType.search(buf, offset, transitionByte);
    }

    protected final long transition(int childIndex)
    {
        return nodeType.transition(buf, offset, position, childIndex);
    }

    protected final long lastTransition()
    {
        return nodeType.lastTransition(buf, offset, position);
    }

    protected final long greaterTransition(int searchIndex, long defaultValue)
    {
        return nodeType.greaterTransition(buf, offset, position, searchIndex, defaultValue);
    }

    protected final long lesserTransition(int searchIndex, long defaultValue)
    {
        return nodeType.lesserTransition(buf, offset, position, searchIndex, defaultValue);
    }

    protected final int transitionByte(int childIndex)
    {
        return nodeType.transitionByte(buf, offset, childIndex);
    }

    protected final int transitionRange()
    {
        return nodeType.transitionRange(buf, offset);
    }

    protected final boolean hasChildren()
    {
        return transitionRange() > 0;
    }

    protected final void goMax(long pos)
    {
        go(pos);
        while (true)
        {
            long lastChild = lastTransition();
            if (lastChild == -1)
                return;
            go(lastChild);
        }
    }

    protected final void goMin(long pos)
    {
        go(pos);
        while (true)
        {
            int payloadBits = payloadFlags();
            if (payloadBits > 0)
                return;

            go(transition(0));
        }
    }

    public interface Extractor<RESULT, VALUE>
    {
        RESULT extract(VALUE walker, int payloadPosition, int payloadFlags);
    }

    /**
     * Follows the given key while there are transitions in the trie for it.
     *
     * @return the first unmatched byte of the key, may be {@link ByteSource#END_OF_STREAM}
     */
    public int follow(ByteComparable key)
    {
        ByteSource stream = key.asComparableBytes(BYTE_COMPARABLE_VERSION);
        go(root);
        while (true)
        {
            int b = stream.next();
            int childIndex = search(b);

            if (childIndex < 0)
                return b;

            go(transition(childIndex));
        }
    }

    /**
     * Follows the trie for a given key, remembering the closest greater branch.
     * On return the walker is positioned at the longest prefix that matches the input (with or without payload), and
     * min(greaterBranch) is the immediate greater neighbour.
     *
     * @return the first unmatched byte of the key, may be {@link ByteSource#END_OF_STREAM}
     */
    public int followWithGreater(ByteComparable key)
    {
        greaterBranch = -1;

        ByteSource stream = key.asComparableBytes(BYTE_COMPARABLE_VERSION);
        go(root);
        while (true)
        {
            int b = stream.next();
            int searchIndex = search(b);

            greaterBranch = greaterTransition(searchIndex, greaterBranch);
            if (searchIndex < 0)
                return b;

            go(transition(searchIndex));
        }
    }

    /**
     * Follows the trie for a given key, remembering the closest lesser branch.
     * On return the walker is positioned at the longest prefix that matches the input (with or without payload), and
     * max(lesserBranch) is the immediate lesser neighbour.
     *
     * @return the first unmatched byte of the key, may be {@link ByteSource#END_OF_STREAM}
     */
    public int followWithLesser(ByteComparable key)
    {
        lesserBranch = -1;

        ByteSource stream = key.asComparableBytes(BYTE_COMPARABLE_VERSION);
        go(root);
        while (true)
        {
            int b = stream.next();
            int searchIndex = search(b);

            lesserBranch = lesserTransition(searchIndex, lesserBranch);

            if (searchIndex < 0)
                return b;

            go(transition(searchIndex));
        }
    }


    /**
     * Takes a prefix of the given key. The prefix is in the sense of a separator key match, i.e. it is only
     * understood as valid if there are no greater entries in the trie (e.g. data at 'a' is ignored if 'ab' or 'abba'
     * is in the trie when looking for 'abc' or 'ac', but accepted when looking for 'aa').
     * In order to not have to go back to data that may have exited cache, payloads are extracted when the node is
     * visited (instead of saving the node's position), which requires an extractor to be passed as parameter.
     */
    @SuppressWarnings("unchecked")
    public <RESULT> RESULT prefix(ByteComparable key, Extractor<RESULT, VALUE> extractor)
    {
        RESULT payload = null;

        ByteSource stream = key.asComparableBytes(BYTE_COMPARABLE_VERSION);
        go(root);
        while (true)
        {
            int b = stream.next();
            int childIndex = search(b);

            if (childIndex > 0)
                payload = null;
            else
            {
                int payloadBits = payloadFlags();
                if (payloadBits > 0)
                    payload = extractor.extract((VALUE) this, payloadPosition(), payloadBits);
                if (childIndex < 0)
                    return payload;
            }

            go(transition(childIndex));
        }
    }

    /**
     * Follows the trie for a given key, taking a prefix (in the sense above) and searching for neighboring values.
     * On return min(greaterBranch) and max(lesserBranch) are the immediate non-prefix neighbours for the sought value.
     * <p>
     * Note: in a separator trie the closest smaller neighbour can be another prefix of the given key. This method
     * does not take that into account. E.g. if trie contains "abba", "as" and "ask", looking for "asking" will find
     * "ask" as the match, but max(lesserBranch) will point to "abba" instead of the correct "as". This problem can
     * only occur if there is a valid prefix match.
     */
    @SuppressWarnings("unchecked")
    public <RESULT> RESULT prefixAndNeighbours(ByteComparable key, Extractor<RESULT, VALUE> extractor)
    {
        RESULT payload = null;
        greaterBranch = -1;
        lesserBranch = -1;

        ByteSource stream = key.asComparableBytes(BYTE_COMPARABLE_VERSION);
        go(root);
        while (true)
        {
            int b = stream.next();
            int searchIndex = search(b);

            greaterBranch = greaterTransition(searchIndex, greaterBranch);

            if (searchIndex == -1 || searchIndex == 0)
            {
                int payloadBits = payloadFlags();
                if (payloadBits > 0)
                    payload = extractor.extract((VALUE) this, payloadPosition(), payloadBits);
            }
            else
            {
                lesserBranch = lesserTransition(searchIndex, lesserBranch);
                payload = null;
            }

            if (searchIndex < 0)
                return payload;

            go(transition(searchIndex));
        }
    }

    /**
     * To be used only in analysis.
     */
    protected int nodeTypeOrdinal()
    {
        return nodeType.ordinal;
    }

    /**
     * To be used only in analysis.
     */
    protected int nodeSize()
    {
        return payloadPosition() - offset;
    }

    public interface PayloadToString
    {
        String payloadAsString(ByteBuffer buf, int payloadPos, int payloadFlags);
    }

    public void dumpTrie(PrintStream out, PayloadToString payloadReader)
    {
        out.print("ROOT");
        dumpTrie(out, payloadReader, root, "");
    }

    private void dumpTrie(PrintStream out, PayloadToString payloadReader, long node, String indent)
    {
        go(node);
        int bits = payloadFlags();
        out.format(" %s@%x %s%n", nodeType.toString(), node, bits == 0 ? "" : payloadReader.payloadAsString(buf, payloadPosition(), bits));
        int range = transitionRange();
        for (int i = 0; i < range; ++i)
        {
            long child = transition(i);
            if (child == -1)
                continue;
            out.format("%s%02x %s>", indent, transitionByte(i), PageAware.pageStart(position) == PageAware.pageStart(child) ? "--" : "==");
            dumpTrie(out, payloadReader, child, indent + "  ");
            go(node);
        }
    }

    @Override
    public String toString()
    {
        return String.format("[Trie Walker - NodeType: %s, source: %s, buffer: %s, buffer file offset: %d, Node buffer offset: %d, Node file position: %d]",
                             nodeType, source, buf, bh.offset(), offset, position);
    }
}
