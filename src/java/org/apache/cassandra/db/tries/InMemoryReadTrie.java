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
package org.apache.cassandra.db.tries;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * In-memory trie built for fast modification and reads executing concurrently with writes from a single mutator thread.
 *
 * This class provides the read-only functionality, expanded in {@link InMemoryTrie} to writes.
 */
public class InMemoryReadTrie<T> extends Trie<T>
{
    /*
    TRIE FORMAT AND NODE TYPES

    The memtable trie uses five different types of nodes:
     - "leaf" nodes, which have content and no children;
     - single-transition "chain" nodes, which have exactly one child; while each node is a single transition, they are
       called "chain" because multiple such transition are packed in a block.
     - "sparse" nodes which have between two and six children;
     - "split" nodes for anything above six children;
     - "prefix" nodes that augment one of the other types (except leaf) with content.

    The data for all nodes except leaf ones is stored in a contiguous 'node buffer' and laid out in blocks of 32 bytes.
    A block only contains data for a single type of node, but there is no direct correspondence between block and node
    in that:
     - a single block can contain multiple "chain" nodes.
     - a sparse node occupies exactly one block.
     - a split node occupies a variable number of blocks.
     - a prefix node can be placed in the same block as the node it augments, or in a separate block.

    Nodes are referenced in that buffer by an integer position/pointer, the 'node pointer'. Note that node pointers are
    not pointing at the beginning of blocks, and we call 'pointer offset' the offset of the node pointer to the block it
    points into. The value of a 'node pointer' is used to decide what kind of node is pointed:

     - If the pointer is negative, we have a leaf node. Since a leaf has no children, we need no data outside of its
       content to represent it, and that content is stored in a 'content list', not in the nodes buffer. The content
       of a particular leaf node is located at the ~pointer position in the content list (~ instead of - so that -1 can
       correspond to position 0).

     - If the 'pointer offset' is smaller than 28, we have a chain node with one transition. The transition character is
       the byte at the position pointed in the 'node buffer', and the child is pointed by:
       - the integer value at offset 28 of the block pointed if the 'pointer offset' is 27
       - pointer + 1 (which is guaranteed to have offset smaller than 28, i.e. to be a chain node), otherwise
       In other words, a chain block contains a sequence of characters that leads to the child whose address is at
       offset 28. It may have between 1 and 28 characters depending on the pointer with which the block is entered.

     - If the 'pointer offset' is 30, we have a sparse node. The data of a sparse node occupies a full block and is laid
       out as:
       - six pointers to children at offsets 0 to 24
       - six transition characters at offsets 24 to 30
       - an order word stored in the two bytes at offset 30
       To enable in-place addition of children, the pointers and transition characters are not stored ordered.
       Instead, we use an order encoding in the last 2 bytes of the node. The encoding is a base-6 number which
       describes the order of the transitions (least significant digit being the smallest).
       The node must have at least two transitions and the transition at position 0 is never the biggest (we can
       enforce this by choosing for position 0 the smaller of the two transitions a sparse node starts with). This
       allows iteration over the order word (which divides said word by 6 each step) to finish when the result becomes 0.

     - If the 'pointer offset' is 28, the node is a split one. Split nodes are dense, meaning that there is a direct
       mapping between a transition character and the address of the associated pointer, and new children can easily be
       added in place.
       Split nodes occupy multiple blocks, and a child is located by traversing 3 layers of pointers:
       - the first pointer is within the top-level block (the one pointed by the pointer) and points to a "mid" block.
         The top-level block has 4 such pointers to "mid" block, located between offset 16 and 32.
       - the 2nd pointer is within the "mid" block and points to a "tail" block. A "mid" block has 8 such pointers
         occupying the whole block.
       - the 3rd pointer is with the "tail" block and is the actual child pointer. Like "mid" block, there are 8 such
         pointers (so we finally address 4 * 8 * 8 = 256 children).
       To find a child, we thus need to know the index of the pointer to follow within the top-level block, the index
       of the one in the "mid" block and the index in the "tail" block. For that, we split the transition byte in a
       sequence of 2-3-3 bits:
       - the first 2 bits are the index in the top-level block;
       - the next 3 bits, the index in the "mid" block;
       - and the last 3 bits the index in the "tail" block.
       This layout allows the node to use the smaller fixed-size blocks (instead of 256*4 bytes for the whole character
       space) and also leaves some room in the head block (the 16 first bytes) for additional information (which we can
       use to store prefix nodes containing things like deletion times).
       One split node may need up to 1 + 4 + 4*8 blocks (1184 bytes) to store all its children.

     - If the pointer offset is 31, we have a prefix node. These are two types:
       -- Embedded prefix nodes occupy the free bytes in a chain or split node. The byte at offset 4 has the offset
          within the 32-byte block for the augmented node.
       -- Full prefix nodes have 0xFF at offset 4 and a pointer at 28, pointing to the augmented node.
       Both types contain an index for content at offset 0. The augmented node cannot be a leaf or NONE -- in the former
       case the leaf itself contains the content index, in the latter we use a leaf instead.
       The term "node" when applied to these is a bit of a misnomer as they are not presented as separate nodes during
       traversals. Instead, they augment a node, changing only its content. Internally we create a Node object for the
       augmented node and wrap a PrefixNode around it, which changes the `content()` method and routes all other
       calls to the augmented node's methods.

     When building a trie we first allocate the content, then create a chain node leading to it. While we only have
     single transitions leading to a chain node, we can expand that node (attaching a character and using pointer - 1)
     instead of creating a new one. When a chain node already has a child and needs a new one added we change the type
     (i.e. create a new node and remap the parent) to sparse with two children. When a six-child sparse node needs a new
     child, we switch to split.

     Blocks currently are not reused, because we do not yet have a mechanism to tell when readers are done with blocks
     they are referencing. This currently causes a very low overhead (because we change data in place with the only
     exception of nodes needing to change type) and is planned to be addressed later.

     For further descriptions and examples of the mechanics of the trie, see InMemoryTrie.md.
     */

    static final int BLOCK_SIZE = 32;

    // Biggest block offset that can contain a pointer.
    static final int LAST_POINTER_OFFSET = BLOCK_SIZE - 4;

    /*
     Block offsets used to identify node types (by comparing them to the node 'pointer offset').
     */

    // split node (dense, 2-3-3 transitions), laid out as 4 pointers to "mid" block, with has 8 pointers to "tail" block,
    // which has 8 pointers to children
    static final int SPLIT_OFFSET = BLOCK_SIZE - 4;
    // sparse node, unordered list of up to 6 transition, laid out as 6 transition pointers followed by 6 transition
    // bytes. The last two bytes contain an ordering of the transitions (in base-6) which is used for iteration. On
    // update the pointer is set last, i.e. during reads the node may show that a transition exists and list a character
    // for it, but pointer may still be null.
    static final int SPARSE_OFFSET = BLOCK_SIZE - 2;
    // min and max offset for a chain node. A block of chain node is laid out as a pointer at LAST_POINTER_OFFSET,
    // preceded by characters that lead to it. Thus a full chain block contains BLOCK_SIZE-4 transitions/chain nodes.
    static final int CHAIN_MIN_OFFSET = 0;
    static final int CHAIN_MAX_OFFSET = BLOCK_SIZE - 5;
    // Prefix node, an intermediate node augmenting its child node with content.
    static final int PREFIX_OFFSET = BLOCK_SIZE - 1;

    /*
     Offsets and values for navigating in a block for particular node type. Those offsets are 'from the node pointer'
     (not the block start) and can be thus negative since node pointers points towards the end of blocks.
     */

    // Limit for the starting cell / sublevel (2 bits -> 4 pointers).
    static final int SPLIT_START_LEVEL_LIMIT = 4;
    // Limit for the other sublevels (3 bits -> 8 pointers).
    static final int SPLIT_OTHER_LEVEL_LIMIT = 8;
    // Bitshift between levels.
    static final int SPLIT_LEVEL_SHIFT = 3;

    static final int SPARSE_CHILD_COUNT = 6;
    // Offset to the first child pointer of a spare node (laid out from the start of the block)
    static final int SPARSE_CHILDREN_OFFSET = 0 - SPARSE_OFFSET;
    // Offset to the first transition byte of a sparse node (laid out after the child pointers)
    static final int SPARSE_BYTES_OFFSET = SPARSE_CHILD_COUNT * 4 - SPARSE_OFFSET;
    // Offset to the order word of a sparse node (laid out after the children (pointer + transition byte))
    static final int SPARSE_ORDER_OFFSET = SPARSE_CHILD_COUNT * 5 - SPARSE_OFFSET;  // 0

    // Offset of the flag byte in a prefix node. In shared blocks, this contains the offset of the next node.
    static final int PREFIX_FLAGS_OFFSET = 4 - PREFIX_OFFSET;
    // Offset of the content id
    static final int PREFIX_CONTENT_OFFSET = 0 - PREFIX_OFFSET;
    // Offset of the next pointer in a non-shared prefix node
    static final int PREFIX_POINTER_OFFSET = LAST_POINTER_OFFSET - PREFIX_OFFSET;

    /**
     * Value used as null for node pointers.
     * No node can use this address (we enforce this by not allowing chain nodes to grow to position 0).
     * Do not change this as the code relies there being a NONE placed in all bytes of the block that are not set.
     */
    static final int NONE = 0;

    volatile int root;

    /*
     EXPANDABLE DATA STORAGE

     The tries will need more and more space in buffers and content lists as they grow. Instead of using ArrayList-like
     reallocation with copying, which may be prohibitively expensive for large buffers, we use a sequence of
     buffers/content arrays that double in size on every expansion.

     For a given address x the index of the buffer can be found with the following calculation:
        index_of_most_significant_set_bit(x / min_size + 1)
     (relying on sum (2^i) for i in [0, n-1] == 2^n - 1) which can be performed quickly on modern hardware.

     Finding the offset within the buffer is then
        x + min - (min << buffer_index)

     The allocated space starts 256 bytes for the buffer and 16 entries for the content list.

     Note that a buffer is not allowed to split 32-byte blocks (code assumes same buffer can be used for all bytes
     inside the block).
     */

    static final int BUF_START_SHIFT = 8;
    static final int BUF_START_SIZE = 1 << BUF_START_SHIFT;

    static final int CONTENTS_START_SHIFT = 4;
    static final int CONTENTS_START_SIZE = 1 << CONTENTS_START_SHIFT;

    static
    {
        assert BUF_START_SIZE % BLOCK_SIZE == 0 : "Initial buffer size must fit a full block.";
    }

    final UnsafeBuffer[] buffers;
    final AtomicReferenceArray<T>[] contentArrays;

    InMemoryReadTrie(UnsafeBuffer[] buffers, AtomicReferenceArray<T>[] contentArrays, int root)
    {
        this.buffers = buffers;
        this.contentArrays = contentArrays;
        this.root = root;
    }

    /*
     Buffer, content list and block management
     */
    int getChunkIdx(int pos, int minChunkShift, int minChunkSize)
    {
        return 31 - minChunkShift - Integer.numberOfLeadingZeros(pos + minChunkSize);
    }

    int inChunkPointer(int pos, int chunkIndex, int minChunkSize)
    {
        return pos + minChunkSize - (minChunkSize << chunkIndex);
    }

    UnsafeBuffer getChunk(int pos)
    {
        int leadBit = getChunkIdx(pos, BUF_START_SHIFT, BUF_START_SIZE);
        return buffers[leadBit];
    }

    int inChunkPointer(int pos)
    {
        int leadBit = getChunkIdx(pos, BUF_START_SHIFT, BUF_START_SIZE);
        return inChunkPointer(pos, leadBit, BUF_START_SIZE);
    }


    /**
     * Pointer offset for a node pointer.
     */
    int offset(int pos)
    {
        return pos & (BLOCK_SIZE - 1);
    }

    final int getUnsignedByte(int pos)
    {
        return getChunk(pos).getByte(inChunkPointer(pos)) & 0xFF;
    }

    final int getUnsignedShort(int pos)
    {
        return getChunk(pos).getShort(inChunkPointer(pos)) & 0xFFFF;
    }

    final int getInt(int pos)
    {
        return getChunk(pos).getInt(inChunkPointer(pos));
    }

    T getContent(int index)
    {
        int leadBit = getChunkIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inChunkPointer(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        return array.get(ofs);
    }

    /*
     Reading node content
     */

    boolean isNull(int node)
    {
        return node == NONE;
    }

    boolean isLeaf(int node)
    {
        return node < NONE;
    }

    boolean isNullOrLeaf(int node)
    {
        return node <= NONE;
    }

    /**
     * Returns the number of transitions in a chain block entered with the given pointer.
     */
    private int chainBlockLength(int node)
    {
        return LAST_POINTER_OFFSET - offset(node);
    }

    /**
     * Get a node's child for the given transition character
     */
    int getChild(int node, int trans)
    {
        if (isNullOrLeaf(node))
            return NONE;

        node = followContentTransition(node);

        switch (offset(node))
        {
            case SPARSE_OFFSET:
                return getSparseChild(node, trans);
            case SPLIT_OFFSET:
                return getSplitChild(node, trans);
            case CHAIN_MAX_OFFSET:
                if (trans != getUnsignedByte(node))
                    return NONE;
                return getInt(node + 1);
            default:
                if (trans != getUnsignedByte(node))
                    return NONE;
                return node + 1;
        }
    }

    protected int followContentTransition(int node)
    {
        if (isNullOrLeaf(node))
            return NONE;

        if (offset(node) == PREFIX_OFFSET)
        {
            int b = getUnsignedByte(node + PREFIX_FLAGS_OFFSET);
            if (b < BLOCK_SIZE)
                node = node - PREFIX_OFFSET + b;
            else
                node = getInt(node + PREFIX_POINTER_OFFSET);

            assert node >= 0 && offset(node) != PREFIX_OFFSET;
        }
        return node;
    }

    /**
     * Advance as long as the cell pointed to by the given pointer will let you.
     * <p>
     * This is the same as getChild(node, first), except for chain nodes where it would walk the fill chain as long as
     * the input source matches.
     */
    int advance(int node, int first, ByteSource rest)
    {
        if (isNullOrLeaf(node))
            return NONE;

        node = followContentTransition(node);

        switch (offset(node))
        {
            case SPARSE_OFFSET:
                return getSparseChild(node, first);
            case SPLIT_OFFSET:
                return getSplitChild(node, first);
            default:
                // Check the first byte matches the expected
                if (getUnsignedByte(node++) != first)
                    return NONE;
                // Check the rest of the bytes provided by the chain node
                for (int length = chainBlockLength(node); length > 0; --length)
                {
                    first = rest.next();
                    if (getUnsignedByte(node++) != first)
                        return NONE;
                }
                // All bytes matched, node is now positioned on the child pointer. Follow it.
                return getInt(node);
        }
    }

    /**
     * Get the child for the given transition character, knowing that the node is sparse
     */
    int getSparseChild(int node, int trans)
    {
        for (int i = 0; i < SPARSE_CHILD_COUNT; ++i)
        {
            if (getUnsignedByte(node + SPARSE_BYTES_OFFSET + i) == trans)
            {
                int child = getInt(node + SPARSE_CHILDREN_OFFSET + i * 4);

                // we can't trust the transition character read above, because it may have been fetched before a
                // concurrent update happened, and the update may have managed to modify the pointer by now.
                // However, if we read it now that we have accessed the volatile pointer, it must have the correct
                // value as it is set before the pointer.
                if (child != NONE && getUnsignedByte(node + SPARSE_BYTES_OFFSET + i) == trans)
                    return child;
            }
        }
        return NONE;
    }

    /**
     * Given a transition, returns the corresponding index (within the node block) of the pointer to the mid block of
     * a split node.
     */
    int splitNodeMidIndex(int trans)
    {
        // first 2 bits of the 2-3-3 split
        return (trans >> 6) & 0x3;
    }

    /**
     * Given a transition, returns the corresponding index (within the mid block) of the pointer to the tail block of
     * a split node.
     */
    int splitNodeTailIndex(int trans)
    {
        // second 3 bits of the 2-3-3 split
        return (trans >> 3) & 0x7;
    }

    /**
     * Given a transition, returns the corresponding index (within the tail block) of the pointer to the child of
     * a split node.
     */
    int splitNodeChildIndex(int trans)
    {
        // third 3 bits of the 2-3-3 split
        return trans & 0x7;
    }

    /**
     * Get the child for the given transition character, knowing that the node is split
     */
    int getSplitChild(int node, int trans)
    {
        int mid = getSplitBlockPointer(node, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        if (isNull(mid))
            return NONE;

        int tail = getSplitBlockPointer(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        if (isNull(tail))
            return NONE;
        return getSplitBlockPointer(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
    }

    /**
     * Get the content for a given node
     */
    T getNodeContent(int node)
    {
        if (isLeaf(node))
            return getContent(~node);

        if (offset(node) != PREFIX_OFFSET)
            return null;

        int index = getInt(node + PREFIX_CONTENT_OFFSET);
        return (index >= 0)
               ? getContent(index)
               : null;
    }

    int splitBlockPointerAddress(int node, int childIndex, int subLevelLimit)
    {
        return node - SPLIT_OFFSET + (8 - subLevelLimit + childIndex) * 4;
    }

    int getSplitBlockPointer(int node, int childIndex, int subLevelLimit)
    {
        return getInt(splitBlockPointerAddress(node, childIndex, subLevelLimit));
    }

    /**
     * Backtracking state for a cursor.
     *
     * To avoid allocations and pointer-chasing, the backtracking data is stored in a simple int array with
     * BACKTRACK_INTS_PER_ENTRY ints for each level.
     */
    private static class CursorBacktrackingState
    {
        static final int BACKTRACK_INTS_PER_ENTRY = 3;
        static final int BACKTRACK_INITIAL_SIZE = 16;
        private int[] backtrack = new int[BACKTRACK_INITIAL_SIZE * BACKTRACK_INTS_PER_ENTRY];
        int backtrackDepth = 0;

        void addBacktrack(int node, int data, int depth)
        {
            if (backtrackDepth * BACKTRACK_INTS_PER_ENTRY >= backtrack.length)
                backtrack = Arrays.copyOf(backtrack, backtrack.length * 2);
            backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 0] = node;
            backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 1] = data;
            backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 2] = depth;
            ++backtrackDepth;
        }

        int node(int backtrackDepth)
        {
            return backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 0];
        }

        int data(int backtrackDepth)
        {
            return backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 1];
        }

        int depth(int backtrackDepth)
        {
            return backtrack[backtrackDepth * BACKTRACK_INTS_PER_ENTRY + 2];
        }
    }

    /*
     * Cursor implementation.
     *
     * InMemoryTrie cursors maintain their backtracking state in CursorBacktrackingState where they store
     * information about the node to backtrack to and the transitions still left to take or attempt.
     *
     * This information is different for the different types of node:
     * - for leaf and chain no backtracking is saved (because we know there are no further transitions)
     * - for sparse we store the remainder of the order word
     * - for split we store one entry per sub-level of the 2-3-3 split
     *
     * When the cursor is asked to advance it first checks the current node for children, and if there aren't any
     * (i.e. it is positioned on a leaf node), it goes one level up the backtracking chain, where we are guaranteed to
     * have a remaining child to advance to. When there's nothing to backtrack to, the trie is exhausted.
     */
    class MemtableCursor extends CursorBacktrackingState implements Cursor<T>
    {
        private int currentNode;
        private int incomingTransition;
        private T content;
        private int depth = -1;

        MemtableCursor()
        {
            descendInto(root, -1);
        }

        @Override
        public int advance()
        {
            if (isNullOrLeaf(currentNode))
                return backtrack();
            else
                return advanceToFirstChild(currentNode);
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            int node = currentNode;
            if (!isChainNode(node))
                return advance();

            // Jump directly to the chain's child.
            UnsafeBuffer chunk = getChunk(node);
            int inChunkNode = inChunkPointer(node);
            int bytesJumped = chainBlockLength(node) - 1;   // leave the last byte for incomingTransition
            if (receiver != null && bytesJumped > 0)
                receiver.addPathBytes(chunk, inChunkNode, bytesJumped);
            depth += bytesJumped;    // descendInto will add one
            inChunkNode += bytesJumped;

            // inChunkNode is now positioned on the last byte of the chain.
            // Consume it to be the new state's incomingTransition.
            int transition = chunk.getByte(inChunkNode++) & 0xFF;
            // inChunkNode is now positioned on the child pointer.
            int child = chunk.getInt(inChunkNode);
            return descendInto(child, transition);
        }

        @Override
        public int skipChildren()
        {
            return backtrack();
        }

        @Override
        public int depth()
        {
            return depth;
        }

        @Override
        public T content()
        {
            return content;
        }

        @Override
        public int incomingTransition()
        {
            return incomingTransition;
        }

        private int backtrack()
        {
            if (--backtrackDepth < 0)
                return depth = -1;

            depth = depth(backtrackDepth);
            return advanceToNextChild(node(backtrackDepth), data(backtrackDepth));
        }

        private int advanceToFirstChild(int node)
        {
            assert (!isNullOrLeaf(node));

            switch (offset(node))
            {
                case SPLIT_OFFSET:
                    return descendInSplitSublevel(node, SPLIT_START_LEVEL_LIMIT, 0, SPLIT_LEVEL_SHIFT * 2);
                case SPARSE_OFFSET:
                    return nextValidSparseTransition(node, getUnsignedShort(node + SPARSE_ORDER_OFFSET));
                default:
                    return getChainTransition(node);
            }
        }

        private int advanceToNextChild(int node, int data)
        {
            assert (!isNullOrLeaf(node));

            switch (offset(node))
            {
                case SPLIT_OFFSET:
                    return nextValidSplitTransition(node, data);
                case SPARSE_OFFSET:
                    return nextValidSparseTransition(node, data);
                default:
                    throw new AssertionError("Unexpected node type in backtrack state.");
            }
        }

        /**
         * Descend into the sub-levels of a split node. Advances to the first child and creates backtracking entries
         * for the following ones. We use the bits of trans (lowest non-zero ones) to identify which sub-level an
         * entry refers to.
         *
         * @param node The node or block id, must have offset SPLIT_OFFSET.
         * @param limit The transition limit for the current sub-level (4 for the start, 8 for the others).
         * @param collected The transition bits collected from the parent chain (e.g. 0x40 after following 1 on the top
         *                  sub-level).
         * @param shift This level's bit shift (6 for start, 3 for mid and 0 for tail).
         * @return the depth reached after descending.
         */
        private int descendInSplitSublevel(int node, int limit, int collected, int shift)
        {
            while (true)
            {
                assert offset(node) == SPLIT_OFFSET;
                int childIndex;
                int child = NONE;
                // find the first non-null child
                for (childIndex = 0; childIndex < limit; ++childIndex)
                {
                    child = getSplitBlockPointer(node, childIndex, limit);
                    if (!isNull(child))
                        break;
                }
                // there must be at least one child
                assert childIndex < limit && child != NONE;

                // look for any more valid transitions and add backtracking if found
                maybeAddSplitBacktrack(node, childIndex, limit, collected, shift);

                // add the bits just found
                collected |= childIndex << shift;
                // descend to next sub-level or child
                if (shift == 0)
                    return descendInto(child, collected);
                // continue with next sublevel; same as
                // return descendInSplitSublevel(child + SPLIT_OFFSET, 8, collected, shift - 3)
                node = child;
                limit = SPLIT_OTHER_LEVEL_LIMIT;
                shift -= SPLIT_LEVEL_SHIFT;
            }
        }

        /**
         * Backtrack to a split sub-level. The level is identified by the lowest non-0 bits in trans.
         */
        private int nextValidSplitTransition(int node, int trans)
        {
            assert trans >= 0 && trans <= 0xFF;
            int childIndex = splitNodeChildIndex(trans);
            if (childIndex > 0)
            {
                maybeAddSplitBacktrack(node,
                                       childIndex,
                                       SPLIT_OTHER_LEVEL_LIMIT,
                                       trans & -(1 << (SPLIT_LEVEL_SHIFT * 1)),
                                       SPLIT_LEVEL_SHIFT * 0);
                int child = getSplitBlockPointer(node, childIndex, SPLIT_OTHER_LEVEL_LIMIT);
                return descendInto(child, trans);
            }
            int tailIndex = splitNodeTailIndex(trans);
            if (tailIndex > 0)
            {
                maybeAddSplitBacktrack(node,
                                       tailIndex,
                                       SPLIT_OTHER_LEVEL_LIMIT,
                                       trans & -(1 << (SPLIT_LEVEL_SHIFT * 2)),
                                       SPLIT_LEVEL_SHIFT * 1);
                int tail = getSplitBlockPointer(node, tailIndex, SPLIT_OTHER_LEVEL_LIMIT);
                return descendInSplitSublevel(tail,
                                              SPLIT_OTHER_LEVEL_LIMIT,
                                              trans,
                                              SPLIT_LEVEL_SHIFT * 0);
            }
            int midIndex = splitNodeMidIndex(trans);
            assert midIndex > 0;
            maybeAddSplitBacktrack(node,
                                   midIndex,
                                   SPLIT_START_LEVEL_LIMIT,
                                   0,
                                   SPLIT_LEVEL_SHIFT * 2);
            int mid = getSplitBlockPointer(node, midIndex, SPLIT_START_LEVEL_LIMIT);
            return descendInSplitSublevel(mid,
                                          SPLIT_OTHER_LEVEL_LIMIT,
                                          trans,
                                          SPLIT_LEVEL_SHIFT * 1);
        }

        /**
         * Look for any further non-null transitions on this sub-level and, if found, add a backtracking entry.
         */
        private void maybeAddSplitBacktrack(int node, int startAfter, int limit, int collected, int shift)
        {
            int nextChildIndex;
            for (nextChildIndex = startAfter + 1; nextChildIndex < limit; ++nextChildIndex)
            {
                if (!isNull(getSplitBlockPointer(node, nextChildIndex, limit)))
                    break;
            }
            if (nextChildIndex < limit)
                addBacktrack(node, collected | (nextChildIndex << shift), depth);
        }

        private int nextValidSparseTransition(int node, int data)
        {
            UnsafeBuffer chunk = getChunk(node);
            int inChunkNode = inChunkPointer(node);

            // Peel off the next index.
            int index = data % SPARSE_CHILD_COUNT;
            data = data / SPARSE_CHILD_COUNT;

            // If there are remaining transitions, add backtracking entry.
            if (data > 0)
                addBacktrack(node, data, depth);

            // Follow the transition.
            int child = chunk.getInt(inChunkNode + SPARSE_CHILDREN_OFFSET + index * 4);
            int transition = chunk.getByte(inChunkNode + SPARSE_BYTES_OFFSET + index) & 0xFF;
            return descendInto(child, transition);
        }

        private int getChainTransition(int node)
        {
            // No backtracking needed.
            UnsafeBuffer chunk = getChunk(node);
            int inChunkNode = inChunkPointer(node);
            int transition = chunk.getByte(inChunkNode) & 0xFF;
            int next = node + 1;
            if (offset(next) <= CHAIN_MAX_OFFSET)
                return descendIntoChain(next, transition);
            else
                return descendInto(chunk.getInt(inChunkNode + 1), transition);
        }

        private int descendInto(int child, int transition)
        {
            ++depth;
            incomingTransition = transition;
            content = getNodeContent(child);
            currentNode = followContentTransition(child);
            return depth;
        }

        private int descendIntoChain(int child, int transition)
        {
            ++depth;
            incomingTransition = transition;
            content = null;
            currentNode = child;
            return depth;
        }
    }

    private boolean isChainNode(int node)
    {
        return !isNullOrLeaf(node) && offset(node) <= CHAIN_MAX_OFFSET;
    }

    public MemtableCursor cursor()
    {
        return new MemtableCursor();
    }

    /*
     Direct read methods
     */

    /**
     * Get the content mapped by the specified key.
     * Fast implementation using integer node addresses.
     */
    public T get(ByteComparable path)
    {
        int n = root;
        ByteSource source = path.asComparableBytes(BYTE_COMPARABLE_VERSION);
        while (!isNull(n))
        {
            int c = source.next();
            if (c == ByteSource.END_OF_STREAM)
                return getNodeContent(n);

            n = advance(n, c, source);
        }

        return null;
    }

    public boolean isEmpty()
    {
        return isNull(root);
    }

    /**
     * Override of dump to provide more detailed printout that includes the type of each node in the trie.
     * We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
     */
    @Override
    public String dump(Function<T, String> contentToString)
    {
        MemtableCursor source = cursor();
        class TypedNodesCursor implements Cursor<String>
        {
            @Override
            public int advance()
            {
                return source.advance();
            }


            @Override
            public int advanceMultiple(TransitionsReceiver receiver)
            {
                return source.advanceMultiple(receiver);
            }

            @Override
            public int skipChildren()
            {
                return source.skipChildren();
            }

            @Override
            public int depth()
            {
                return source.depth();
            }

            @Override
            public int incomingTransition()
            {
                return source.incomingTransition();
            }

            @Override
            public String content()
            {
                String type = null;
                int node = source.currentNode;
                if (!isNullOrLeaf(node))
                {
                    switch (offset(node))
                    {
                        case SPARSE_OFFSET:
                            type = "[SPARSE]";
                            break;
                        case SPLIT_OFFSET:
                            type = "[SPLIT]";
                            break;
                        case PREFIX_OFFSET:
                            throw new AssertionError("Unexpected prefix as cursor currentNode.");
                        default:
                            type = "[CHAIN]";
                            break;
                    }
                }
                T content = source.content();
                if (content != null)
                {
                    if (type != null)
                        return contentToString.apply(content) + " -> " + type;
                    else
                        return contentToString.apply(content);
                }
                else
                    return type;
            }
        }
        return process(new TrieDumper<>(Function.identity()), new TypedNodesCursor());
    }
}
