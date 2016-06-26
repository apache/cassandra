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

package org.apache.cassandra.index.sasi.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.*;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractTokenTreeBuilder implements TokenTreeBuilder
{
    protected int numBlocks;
    protected Node root;
    protected InteriorNode rightmostParent;
    protected Leaf leftmostLeaf;
    protected Leaf rightmostLeaf;
    protected long tokenCount = 0;
    protected long treeMinToken;
    protected long treeMaxToken;

    public void add(TokenTreeBuilder other)
    {
        add(other.iterator());
    }

    public TokenTreeBuilder finish()
    {
        if (root == null)
            constructTree();

        return this;
    }

    public long getTokenCount()
    {
        return tokenCount;
    }

    public int serializedSize()
    {
        if (numBlocks == 1)
            return BLOCK_HEADER_BYTES + ((int) tokenCount * LEAF_ENTRY_BYTES);
        else
            return numBlocks * BLOCK_BYTES;
    }

    public void write(DataOutputPlus out) throws IOException
    {
        ByteBuffer blockBuffer = ByteBuffer.allocate(BLOCK_BYTES);
        Iterator<Node> levelIterator = root.levelIterator();
        long childBlockIndex = 1;

        while (levelIterator != null)
        {
            Node firstChild = null;
            while (levelIterator.hasNext())
            {
                Node block = levelIterator.next();

                if (firstChild == null && !block.isLeaf())
                    firstChild = ((InteriorNode) block).children.get(0);

                if (block.isSerializable())
                {
                    block.serialize(childBlockIndex, blockBuffer);
                    flushBuffer(blockBuffer, out, numBlocks != 1);
                }

                childBlockIndex += block.childCount();
            }

            levelIterator = (firstChild == null) ? null : firstChild.levelIterator();
        }
    }

    protected abstract void constructTree();

    protected void flushBuffer(ByteBuffer buffer, DataOutputPlus o, boolean align) throws IOException
    {
        // seek to end of last block before flushing
        if (align)
            alignBuffer(buffer, BLOCK_BYTES);

        buffer.flip();
        o.write(buffer);
        buffer.clear();
    }

    /**
     * Tree node,
     *
     * B+-tree consists of root, interior nodes and leaves. Root can be either a node or a leaf.
     *
     * Depending on the concrete implementation of {@code TokenTreeBuilder}
     * leaf can be partial or static (in case of {@code StaticTokenTreeBuilder} or dynamic in case
     * of {@code DynamicTokenTreeBuilder}
     */
    protected abstract class Node
    {
        protected InteriorNode parent;
        protected Node next;
        protected Long nodeMinToken, nodeMaxToken;

        public Node(Long minToken, Long maxToken)
        {
            nodeMinToken = minToken;
            nodeMaxToken = maxToken;
        }

        public abstract boolean isSerializable();
        public abstract void serialize(long childBlockIndex, ByteBuffer buf);
        public abstract int childCount();
        public abstract int tokenCount();

        public Long smallestToken()
        {
            return nodeMinToken;
        }

        public Long largestToken()
        {
            return nodeMaxToken;
        }

        public Iterator<Node> levelIterator()
        {
            return new LevelIterator(this);
        }

        public boolean isLeaf()
        {
            return (this instanceof Leaf);
        }

        protected boolean isLastLeaf()
        {
            return this == rightmostLeaf;
        }

        protected boolean isRoot()
        {
            return this == root;
        }

        protected void updateTokenRange(long token)
        {
            nodeMinToken = nodeMinToken == null ? token : Math.min(nodeMinToken, token);
            nodeMaxToken = nodeMaxToken == null ? token : Math.max(nodeMaxToken, token);
        }

        protected void serializeHeader(ByteBuffer buf)
        {
            Header header;
            if (isRoot())
                header = new RootHeader();
            else if (!isLeaf())
                header = new InteriorNodeHeader();
            else
                header = new LeafHeader();

            header.serialize(buf);
            alignBuffer(buf, BLOCK_HEADER_BYTES);
        }

        /**
         * Shared header part, written for all node types:
         *     [  info byte  ] [  token count   ] [ min node token ] [ max node token ]
         *     [      1b     ] [    2b (short)  ] [   8b (long)    ] [    8b (long)   ]
         **/
        private abstract class Header
        {
            /**
             * Serializes the shared part of the header
             */
            public void serialize(ByteBuffer buf)
            {
                buf.put(infoByte())
                   .putShort((short) (tokenCount()))
                   .putLong(nodeMinToken)
                   .putLong(nodeMaxToken);
            }

            protected abstract byte infoByte();
        }

        /**
         * In addition to shared header part, root header stores version information,
         * overall token count and min/max tokens for the whole tree:
         *     [      magic    ] [  overall token count  ] [ min tree token ] [ max tree token ]
         *     [   2b (short)  ] [         8b (long)     ] [   8b (long)    ] [    8b (long)   ]
         */
        private class RootHeader extends Header
        {
            public void serialize(ByteBuffer buf)
            {
                super.serialize(buf);
                writeMagic(buf);
                buf.putLong(tokenCount)
                   .putLong(treeMinToken)
                   .putLong(treeMaxToken);
            }

            protected byte infoByte()
            {
                // if leaf, set leaf indicator and last leaf indicator (bits 0 & 1)
                // if not leaf, clear both bits
                return isLeaf() ? ENTRY_TYPE_MASK : 0;
            }

            protected void writeMagic(ByteBuffer buf)
            {
                switch (Descriptor.CURRENT_VERSION)
                {
                    case ab:
                        buf.putShort(AB_MAGIC);
                        break;
                    case ac:
                        buf.putShort(AC_MAGIC);
                        break;
                    default:
                        throw new RuntimeException("Unsupported version");
                }

            }
        }

        private class InteriorNodeHeader extends Header
        {
            // bit 0 (leaf indicator) & bit 1 (last leaf indicator) cleared
            protected byte infoByte()
            {
                return 0;
            }
        }

        private class LeafHeader extends Header
        {
            // bit 0 set as leaf indicator
            // bit 1 set if this is last leaf of data
            protected byte infoByte()
            {
                byte infoByte = 1;
                infoByte |= (isLastLeaf()) ? (1 << LAST_LEAF_SHIFT) : 0;

                return infoByte;
            }
        }

    }

    /**
     * Leaf consists of
     *   - header (format described in {@code Header} )
     *   - data (format described in {@code LeafEntry})
     *   - overflow collision entries, that hold {@value OVERFLOW_TRAILER_CAPACITY} of {@code RowOffset}.
     */
    protected abstract class Leaf extends Node
    {
        protected LongArrayList overflowCollisions;

        public Leaf(Long minToken, Long maxToken)
        {
            super(minToken, maxToken);
        }

        public int childCount()
        {
            return 0;
        }

        protected void serializeOverflowCollisions(ByteBuffer buf)
        {
            if (overflowCollisions != null)
                for (LongCursor offset : overflowCollisions)
                    buf.putLong(offset.value);
        }

        public void serialize(long childBlockIndex, ByteBuffer buf)
        {
            serializeHeader(buf);
            serializeData(buf);
            serializeOverflowCollisions(buf);
        }

        protected abstract void serializeData(ByteBuffer buf);

        protected LeafEntry createEntry(final long tok, final KeyOffsets offsets)
        {
            LongArrayList rawOffsets = new LongArrayList(offsets.size());

            offsets.forEach(new Consumer<LongObjectCursor<long[]>>()
            {
                public void accept(LongObjectCursor<long[]> cursor)
                {
                    for (long l : cursor.value)
                    {
                        rawOffsets.add(cursor.key);
                        rawOffsets.add(l);
                    }
                }
            });

            int offsetCount = rawOffsets.size();
            switch (offsetCount)
            {
                case 0:
                    throw new AssertionError("no offsets for token " + tok);
                case 2:
                    return new SimpleLeafEntry(tok, rawOffsets.get(0), rawOffsets.get(1));
                default:
                    assert offsetCount % 2 == 0;
                    if (offsetCount == 4)
                    {
                        if (rawOffsets.get(0) < Integer.MAX_VALUE && rawOffsets.get(1) < Integer.MAX_VALUE &&
                            rawOffsets.get(2) < Integer.MAX_VALUE && rawOffsets.get(3) < Integer.MAX_VALUE)
                        {
                            return new PackedCollisionLeafEntry(tok, (int)rawOffsets.get(0), (int) rawOffsets.get(1),
                                                                (int) rawOffsets.get(2), (int) rawOffsets.get(3));
                        }
                    }
                    return createOverflowEntry(tok, offsetCount, rawOffsets);
            }
        }

        private LeafEntry createOverflowEntry(final long tok, final int offsetCount, final LongArrayList offsets)
        {
            if (overflowCollisions == null)
                overflowCollisions = new LongArrayList(offsetCount);

            int overflowCount = (overflowCollisions.size() + offsetCount) / 2;
            if (overflowCount >= OVERFLOW_TRAILER_CAPACITY)
                throw new AssertionError("cannot have more than " + OVERFLOW_TRAILER_CAPACITY + " overflow collisions per leaf, but had: " + overflowCount);

            LeafEntry entry = new OverflowCollisionLeafEntry(tok, (short) (overflowCollisions.size() / 2), (short) (offsetCount / 2));
            overflowCollisions.addAll(offsets);
            return entry;
        }

        /**
         * A leaf of the B+-Tree, that holds information about the row offset(s) for
         * the current token.
         *
         * Main 3 types of leaf entries are:
         *   1) simple leaf entry: holding just a single row offset
         *   2) packed collision leaf entry: holding two entries that would fit together into 168 bytes
         *   3) overflow entry: only holds offset in overflow trailer and amount of entries belonging to this leaf
         */
        protected abstract class LeafEntry
        {
            protected final long token;

            abstract public EntryType type();

            public LeafEntry(final long tok)
            {
                token = tok;
            }

            public abstract void serialize(ByteBuffer buf);

        }

        /**
         * Simple leaf, that can store a single row offset, having the following format:
         *
         *     [    type    ] [   token   ] [ partition offset ] [ row offset ]
         *     [ 2b (short) ] [ 8b (long) ] [     8b (long)    ] [  8b (long) ]
         */
        protected class SimpleLeafEntry extends LeafEntry
        {
            private final long partitionOffset;
            private final long rowOffset;

            public SimpleLeafEntry(final long tok, final long partitionOffset, final long rowOffset)
            {
                super(tok);
                this.partitionOffset = partitionOffset;
                this.rowOffset = rowOffset;
            }

            public EntryType type()
            {
                return EntryType.SIMPLE;
            }

            @Override
            public void serialize(ByteBuffer buf)
            {
                buf.putShort((short) type().ordinal())
                   .putLong(token)
                   .putLong(partitionOffset)
                   .putLong(rowOffset);
            }
        }

        /**
         * Packed collision entry, can store two offsets, if each one of their positions
         * fit into 4 bytes.
         *     [    type    ] [   token   ] [ partition offset 1 ] [ row offset  1] [ partition offset 1 ] [ row offset  1]
         *     [ 2b (short) ] [ 8b (long) ] [      4b (int)      ] [    4b (int)  ] [      4b (int)      ] [    4b (int)  ]
         */
        protected class PackedCollisionLeafEntry extends LeafEntry
        {
            private final int partitionOffset1;
            private final int rowOffset1;
            private final int partitionOffset2;
            private final int rowOffset2;

            public PackedCollisionLeafEntry(final long tok, final int partitionOffset1, final int rowOffset1,
                                            final int partitionOffset2, final int rowOffset2)
            {
                super(tok);
                this.partitionOffset1 = partitionOffset1;
                this.rowOffset1 = rowOffset1;
                this.partitionOffset2 = partitionOffset2;
                this.rowOffset2 = rowOffset2;

            }

            public EntryType type()
            {
                return EntryType.PACKED;
            }

            @Override
            public void serialize(ByteBuffer buf)
            {
                buf.putShort((short) type().ordinal())
                   .putLong(token)
                   .putInt(partitionOffset1)
                   .putInt(rowOffset1)
                   .putInt(partitionOffset2)
                   .putInt(rowOffset2);
            }
        }

        /**
         * Overflow collision entry, holds an entry with three or more offsets, or two offsets
         * that cannot be packed into 16 bytes.
         *     [    type    ] [   token   ] [ start index ] [    count    ]
         *     [ 2b (short) ] [ 8b (long) ] [   8b (long) ] [  8b (long)  ]
         *
         *   - [ start index ] is a position of first item belonging to this leaf entry in the overflow trailer
         *   - [ count ] is the amount of items belonging to this leaf entry that are stored in the overflow trailer
         */
        private class OverflowCollisionLeafEntry extends LeafEntry
        {
            private final short startIndex;
            private final short count;

            public OverflowCollisionLeafEntry(final long tok, final short collisionStartIndex, final short collisionCount)
            {
                super(tok);
                startIndex = collisionStartIndex;
                count = collisionCount;
            }

            public EntryType type()
            {
                return EntryType.OVERFLOW;
            }

            @Override
            public void serialize(ByteBuffer buf)
            {
                buf.putShort((short) type().ordinal())
                   .putLong(token)
                   .putLong(startIndex)
                   .putLong(count);
            }
        }
    }

    /**
     * Interior node consists of:
     *    - (interior node) header
     *    - tokens (serialized as longs, with count stored in header)
     *    - child offsets
     */
    protected class InteriorNode extends Node
    {
        protected List<Long> tokens = new ArrayList<>(TOKENS_PER_BLOCK);
        protected List<Node> children = new ArrayList<>(TOKENS_PER_BLOCK + 1);
        protected int position = 0;

        public InteriorNode()
        {
            super(null, null);
        }

        public boolean isSerializable()
        {
            return true;
        }

        public void serialize(long childBlockIndex, ByteBuffer buf)
        {
            serializeHeader(buf);
            serializeTokens(buf);
            serializeChildOffsets(childBlockIndex, buf);
        }

        public int childCount()
        {
            return children.size();
        }

        public int tokenCount()
        {
            return tokens.size();
        }

        public Long smallestToken()
        {
            return tokens.get(0);
        }

        protected void add(Long token, InteriorNode leftChild, InteriorNode rightChild)
        {
            int pos = tokens.size();
            if (pos == TOKENS_PER_BLOCK)
            {
                InteriorNode sibling = split();
                sibling.add(token, leftChild, rightChild);

            }
            else
            {
                if (leftChild != null)
                    children.add(pos, leftChild);

                if (rightChild != null)
                {
                    children.add(pos + 1, rightChild);
                    rightChild.parent = this;
                }

                updateTokenRange(token);
                tokens.add(pos, token);
            }
        }

        protected void add(Leaf node)
        {

            if (position == (TOKENS_PER_BLOCK + 1))
            {
                rightmostParent = split();
                rightmostParent.add(node);
            }
            else
            {

                node.parent = this;
                children.add(position, node);
                position++;

                // the first child is referenced only during bulk load. we don't take a value
                // to store into the tree, one is subtracted since position has already been incremented
                // for the next node to be added
                if (position - 1 == 0)
                    return;


                // tokens are inserted one behind the current position, but 2 is subtracted because
                // position has already been incremented for the next add
                Long smallestToken = node.smallestToken();
                updateTokenRange(smallestToken);
                tokens.add(position - 2, smallestToken);
            }

        }

        protected InteriorNode split()
        {
            Pair<Long, InteriorNode> splitResult = splitBlock();
            Long middleValue = splitResult.left;
            InteriorNode sibling = splitResult.right;
            InteriorNode leftChild = null;

            // create a new root if necessary
            if (parent == null)
            {
                parent = new InteriorNode();
                root = parent;
                sibling.parent = parent;
                leftChild = this;
                numBlocks++;
            }

            parent.add(middleValue, leftChild, sibling);

            return sibling;
        }

        protected Pair<Long, InteriorNode> splitBlock()
        {
            final int splitPosition = TOKENS_PER_BLOCK - 2;
            InteriorNode sibling = new InteriorNode();
            sibling.parent = parent;
            next = sibling;

            Long middleValue = tokens.get(splitPosition);

            for (int i = splitPosition; i < TOKENS_PER_BLOCK; i++)
            {
                if (i != TOKENS_PER_BLOCK && i != splitPosition)
                {
                    long token = tokens.get(i);
                    sibling.updateTokenRange(token);
                    sibling.tokens.add(token);
                }

                Node child = children.get(i + 1);
                child.parent = sibling;
                sibling.children.add(child);
                sibling.position++;
            }

            for (int i = TOKENS_PER_BLOCK; i >= splitPosition; i--)
            {
                if (i != TOKENS_PER_BLOCK)
                    tokens.remove(i);

                if (i != splitPosition)
                    children.remove(i);
            }

            nodeMinToken = smallestToken();
            nodeMaxToken = tokens.get(tokens.size() - 1);
            numBlocks++;

            return Pair.create(middleValue, sibling);
        }

        protected boolean isFull()
        {
            return (position >= TOKENS_PER_BLOCK + 1);
        }

        private void serializeTokens(ByteBuffer buf)
        {
            tokens.forEach(buf::putLong);
        }

        private void serializeChildOffsets(long childBlockIndex, ByteBuffer buf)
        {
            for (int i = 0; i < children.size(); i++)
                buf.putLong((childBlockIndex + i) * BLOCK_BYTES);
        }
    }

    public static class LevelIterator extends AbstractIterator<Node>
    {
        private Node currentNode;

        LevelIterator(Node first)
        {
            currentNode = first;
        }

        public Node computeNext()
        {
            if (currentNode == null)
                return endOfData();

            Node returnNode = currentNode;
            currentNode = returnNode.next;

            return returnNode;
        }
    }


    protected static void alignBuffer(ByteBuffer buffer, int blockSize)
    {
        long curPos = buffer.position();
        if ((curPos & (blockSize - 1)) != 0) // align on the block boundary if needed
            buffer.position((int) FBUtilities.align(curPos, blockSize));
    }

}
