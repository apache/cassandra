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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.ObjectSizes;
import org.github.jamm.MemoryLayoutSpecification;

/**
 * Memtable trie, i.e. an in-memory trie built for fast modification and reads executing concurrently with writes from
 * a single mutator thread.
 *
 * Writes to this should be atomic (i.e. reads should see either the content before the write, or the content after the
 * write; if any read sees the write, then any subsequent (i.e. started after it completed) read should also see it).
 * This implementation does not currently guarantee this, but we still get the desired result as `apply` is only used
 * with singleton tries.
 */
public class MemtableTrie<T> extends MemtableReadTrie<T>
{
    // See the trie format description in MemtableReadTrie.

    /**
     * Trie size limit. This is not enforced, but users must check from time to time that it is not exceeded (using
     * reachedAllocatedSizeThreshold()) and start switching to a new trie if it is.
     * This must be done to avoid tries growing beyond their hard 2GB size limit (due to the 32-bit pointers).
     */
    private static final int ALLOCATED_SIZE_THRESHOLD;
    static
    {
        String propertyName = "cassandra.trie_size_limit_mb";
        // Default threshold + 10% == 1 GB. Adjusted slightly up to avoid a tiny final allocation for the 2G max.
        int limitInMB = Integer.parseInt(System.getProperty(propertyName,
                                                            Integer.toString(1024 * 10 / 11 + 1)));
        if (limitInMB < 1 || limitInMB > 2047)
            throw new AssertionError(propertyName + " must be within 1 and 2047");
        ALLOCATED_SIZE_THRESHOLD = 1024 * 1024 * limitInMB;
    }

    private int allocatedPos = 0;
    private int contentCount = 0;

    private final BufferType bufferType;    // on or off heap

    private static final long EMPTY_SIZE_ON_HEAP; // for space calculations
    private static final long EMPTY_SIZE_OFF_HEAP; // for space calculations

    static
    {
        MemtableTrie<Object> empty = new MemtableTrie<>(BufferType.ON_HEAP);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new MemtableTrie<>(BufferType.OFF_HEAP);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    public MemtableTrie(BufferType bufferType)
    {
        super(new UnsafeBuffer[31 - BUF_START_SHIFT],  // last one is 1G for a total of ~2G bytes
              new AtomicReferenceArray[29 - CONTENTS_START_SHIFT],  // takes at least 4 bytes to write pointer to one content -> 4 times smaller than buffers
              NONE);
        this.bufferType = bufferType;
        assert INITIAL_BUFFER_CAPACITY % BLOCK_SIZE == 0;
    }

    // Buffer, content list and block management

    public static class SpaceExhaustedException extends Exception
    {
        public SpaceExhaustedException()
        {
            super("The hard 2GB limit on trie size has been exceeded");
        }
    }

    final void putInt(int pos, int value)
    {
        getBuffer(pos).putInt(getOffset(pos), value);
    }

    final void putIntOrdered(int pos, int value)
    {
        getBuffer(pos).putIntOrdered(getOffset(pos), value);
    }

    final void putIntVolatile(int pos, int value)
    {
        getBuffer(pos).putIntVolatile(getOffset(pos), value);
    }

    final void putShort(int pos, short value)
    {
        getBuffer(pos).putShort(getOffset(pos), value);
    }

    final void putShortVolatile(int pos, short value)
    {
        getBuffer(pos).putShort(getOffset(pos), value);
    }

    final void putByte(int pos, byte value)
    {
        getBuffer(pos).putByte(getOffset(pos), value);
    }


    private int allocateBlock() throws SpaceExhaustedException
    {
        // Note: If this method is modified, please run MemtableTrieTest.testOver1GSize to verify it acts correctly
        // close to the 2G limit.
        int v = allocatedPos;
        if (getOffset(v) == 0)
        {
            int leadBit = getChunkIdx(v, BUF_START_SHIFT, BUF_START_SIZE);
            if (leadBit == 31)
                throw new SpaceExhaustedException();

            assert buffers[leadBit] == null;
            ByteBuffer newBuffer = bufferType.allocate(BUF_START_SIZE << leadBit);
            buffers[leadBit] = new UnsafeBuffer(newBuffer);
            // The above does not contain any happens-before enforcing writes, thus at this point the new buffer may be
            // invisible to any concurrent readers. Touching the volatile root pointer (which any new read must go
            // through) enforces a happens-before that makes it visible to all new reads (note: when the write completes
            // it must do some volatile write, but that will be in the new buffer and without the line below could
            // remain unreachable by other cores).
            root = root;
        }

        allocatedPos += BLOCK_SIZE;
        return v;
    }

    private int addContent(T value)
    {
        int index = contentCount++;
        int leadBit = getChunkIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = getChunkOffset(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        if (array == null)
        {
            assert ofs == 0;
            contentArrays[leadBit] = array = new AtomicReferenceArray<>(CONTENTS_START_SIZE << leadBit);
        }
        array.lazySet(ofs, value); // no need for a volatile set here; at this point the item is not referenced
                                   // by any node in the trie, and a volatile set will be made to reference it.
        return index;
    }

    private void setContent(int index, T value)
    {
        int leadBit = getChunkIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = getChunkOffset(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        array.set(ofs, value);
    }

    public void discardBuffers()
    {
        if (bufferType == BufferType.ON_HEAP)
            return; // no cleaning needed

        for (UnsafeBuffer b : buffers)
        {
            if (b != null)
                FileUtils.clean(b.byteBuffer());
        }
    }

    // Write methods

    // Write visibility model: writes are not volatile, with the exception of the final write before a call returns
    // the same value that was present before (e.g. content was updated in-place / existing node got a new child or had
    // a child pointer updated); if the whole path including the root node changed, the root itself gets a volatile
    // write.
    // This final write is the point where any new cells created during the write become visible for readers for the
    // first time, and such readers must pass through reading that pointer, which forces a happens-before relationship
    // that extends to all values written by this thread before it.

    /**
     * Attach a child to the given non-content node. This may be an update for an existing branch, or a new child for
     * the node. An update _is_ required (i.e. this is only called when the newChild pointer is not the same as the
     * existing value).
     */
    private int attachChild(int node, int trans, int newChild) throws SpaceExhaustedException
    {
        if (isLeaf(node))
            throw new AssertionError("attachChild cannot be used on content nodes.");

        switch (offset(node))
        {
            case PREFIX_OFFSET:
                throw new AssertionError("attachChild cannot be used on content nodes.");
            case SPARSE_OFFSET:
                return attachChildToSparse(node, trans, newChild);
            case SPLIT_OFFSET:
                attachChildToSplit(node, trans, newChild);
                return node;
            case LAST_POINTER_OFFSET - 1:
                // If this is the last character in a Chain block, we can modify the child in-place
                if (trans == getByte(node))
                {
                    putIntVolatile(node + 1, newChild);
                    return node;
                }
                // else pass through
            default:
                return attachChildToChain(node, trans, newChild);
        }
    }

    /**
     * Attach a child to the given split node. This may be an update for an existing branch, or a new child for the node.
     */
    private void attachChildToSplit(int node, int trans, int newChild) throws SpaceExhaustedException
    {
        int midPos = node + SPLIT_POINTER_OFFSET + splitNodeMidIndex(trans) * 4;
        int mid = getInt(midPos);
        if (isNull(mid))
        {
            mid = allocateBlock();
            putIntOrdered(midPos, mid);  // ordered write to ensure no uncleaned state is visible to readers
            // i.e. if block is reused it may need to be set to all zero. if this is not ordered the writes clearing
            // it may execute after this link is created, and readers could see old content.
            // Not currently necessary (we don't reuse), but let's avoid the surprise when we start doing so.
        }

        int tailPos = mid + splitNodeTailIndex(trans) * 4;
        int tail = getInt(tailPos);
        if (isNull(tail))
        {
            tail = allocateBlock();
            putIntOrdered(tailPos, tail); // as above
        }

        int childPos = tail + splitNodeChildIndex(trans) * 4;
        putIntVolatile(childPos, newChild);
    }

    /**
     * Attach a child to the given sparse node. This may be an update for an existing branch, or a new child for the node.
     */
    private int attachChildToSparse(int node, int trans, int newChild) throws SpaceExhaustedException
    {
        int i;
        // first check if this is an update and modify in-place if so
        for (i = 0; i < SPARSE_CHILD_COUNT; ++i)
        {
            if (isNull(getInt(node + SPARSE_CHILDREN_OFFSET + i * 4)))
                break;
            if ((getByte(node + SPARSE_BYTES_OFFSET + i)) == trans)
            {
                putIntVolatile(node + SPARSE_CHILDREN_OFFSET + i * 4, newChild);
                return node;
            }
        }

        if (i == SPARSE_CHILD_COUNT)
        {
            // Node is full. Switch to split
            int split = createEmptySplitNode();
            for (i = 0; i < SPARSE_CHILD_COUNT; ++i)
            {
                int t = getByte(node + SPARSE_BYTES_OFFSET + i);
                int p = getInt(node + SPARSE_CHILDREN_OFFSET + i * 4);
                attachChildToSplitNonVolatile(split, t, p);
            }
            attachChildToSplitNonVolatile(split, trans, newChild);
            return split;
        }

        // Add a new transition. They are not kept in order, so append it at the first free position.
        putByte(node + SPARSE_BYTES_OFFSET + i,  (byte) trans);

        // Update order word.
        int order = getShort(node + SPARSE_ORDER_OFFSET) & 0xFFFF;
        int newOrder = insertInOrderWord(order, i, trans, node + SPARSE_BYTES_OFFSET);

        // Sparse nodes have two access modes: via the order word, when listing transitions, or directly to characters
        // and addresses.
        // To support the former, we volatile write to the order word last, and everything is correctly set up.
        // The latter does not touch the order word. To support that too, we volatile write the address, as the reader
        // can't determine if the position is in use based on the character byte alone (00 is also a valid transition).
        // Note that this means that reader must check the transition byte AFTER the address, to ensure they get the
        // correct value (see getSparseChild).

        // setting child enables reads to start seeing the new branch
        putIntVolatile(node + SPARSE_CHILDREN_OFFSET + i * 4, newChild);

        // some readers will decide whether to check the pointer based on the order word
        // write that volatile to make sure they see the new change too
        putShortVolatile(node + SPARSE_ORDER_OFFSET,  (short) newOrder);
        return node;
    }

    /**
     * Insert the given newIndex in the base-6 encoded order word in the correct position with respect to the ordering.
     *
     * E.g. if the existing bytes were 20, 50, 30 with order word 120 (decimal 48), then
     *   - insertOrderWord(120, 3, 5, ptr)  must return 1203 (decimal 48*6 + 3)
     *   - insertOrderWord(120, 3, 25, ptr) must return 1230 (decimal 8*36 + 3*6 + 0)
     *   - insertOrderWord(120, 3, 35, ptr) must return 1320 (decimal 1*216 + 3*36 + 12)
     *   - insertOrderWord(120, 3, 55, ptr) must return 3120 (decimal 3*216 + 48)
     */
    private int insertInOrderWord(int order, int newIndex, int transitionByte, int bytesPosition)
    {
        int s = order;
        int r = 1;
        while (s != 0)
        {
            int b = getByte(bytesPosition + s % SPARSE_CHILD_COUNT);
            if (b > transitionByte)
                break;

            assert b < transitionByte;
            r *= 6;
            s /= 6;
        }
        // insert i after the ones we have passed (order % r) and before the remaining (s)
        return order % r + (s * 6 + newIndex) * r;
    }

    /**
     * Non-volatile version of attachChildToSplit. Used when the split node is not reachable yet (during the conversion
     * from sparse).
     */
    private void attachChildToSplitNonVolatile(int node, int trans, int newChild) throws SpaceExhaustedException
    {
        int midPos = node + SPLIT_POINTER_OFFSET + splitNodeMidIndex(trans) * 4;
        int mid = getInt(midPos);
        if (isNull(mid))
        {
            mid = allocateBlock();
            putInt(midPos, mid);
        }

        int tailPos = mid + splitNodeTailIndex(trans) * 4;
        int tail = getInt(tailPos);
        if (isNull(tail))
        {
            tail = allocateBlock();
            putInt(tailPos, tail);
        }

        int childPos = tail + splitNodeChildIndex(trans) * 4;
        putInt(childPos, newChild);
    }

    /**
     * Attach a child to the given chain node. This may be an update for an existing branch with different target
     * address, or a second child for the node.
     * This method always copies the node -- with the exception of updates that change the child of the last node in a
     * chain block with matching transition byte (which this method is not used for, see attachChild), modifications to
     * chain nodes cannot be done in place, either because we introduce a new transition byte and have to convert from
     * the single-transition chain type to sparse, or because we have to remap the child from the implicit node + 1 to
     * something else.
     */
    private int attachChildToChain(int node, int transitionByte, int newChild) throws SpaceExhaustedException
    {
        int existingByte = getByte(node);
        if (transitionByte == existingByte)
        {
            // This will only be called if new child is different from old, and the update is not on the final child
            // where we can change it in place (see attachChild). We must always create something new.
            // If the child is a chain, we can expand it (since it's a different value, its branch must be new and
            // nothing can already reside in the rest of the block).
            return expandOrCreateChainNode(transitionByte, newChild);
        }

        // The new transition is different, so we no longer have only one transition. Change type.
        int existingChild = node + 1;
        if (offset(existingChild) == LAST_POINTER_OFFSET)
        {
            existingChild = getInt(existingChild);
        }
        return createSparseNode(existingByte, existingChild, transitionByte, newChild);
    }

    private boolean isExpandableChain(int newChild)
    {
        int newOffset = offset(newChild);
        return newChild > 0 && newChild - 1 > NONE && newOffset > CHAIN_MIN_OFFSET && newOffset <= CHAIN_MAX_OFFSET;
    }

    /**
     * Create a sparse node with two children.
     */
    private int createSparseNode(int byte1, int child1, int byte2, int child2) throws SpaceExhaustedException
    {
        assert byte1 != byte2;
        if (byte1 > byte2)
        {
            // swap them so the smaller is byte1, i.e. there's always something bigger than child 0 so 0 never is
            // at the end of the order
            int t = byte1; byte1 = byte2; byte2 = t;
            t = child1; child1 = child2; child2 = t;
        }

        int node = allocateBlock() + SPARSE_OFFSET;
        putByte(node + SPARSE_BYTES_OFFSET + 0,  (byte) byte1);
        putByte(node + SPARSE_BYTES_OFFSET + 1,  (byte) byte2);
        putInt(node + SPARSE_CHILDREN_OFFSET + 0 * 4, child1);
        putInt(node + SPARSE_CHILDREN_OFFSET + 1 * 4, child2);
        putShort(node + SPARSE_ORDER_OFFSET,  (short) (1 * 6 + 0));
        // Note: this does not need a volatile write as it is a new node, returning a new pointer, which needs to be
        // put in an existing node or the root. That action ends in a happens-before enforcing write.
        return node;
    }

    /**
     * Creates a chain node with the single provided transition (pointing to the provided child).
     * Note that to avoid creating inefficient tries with under-utilized chain nodes, this should only be called from
     * {@link #expandOrCreateChainNode} and other call-sites should call {@link #expandOrCreateChainNode}.
     */
    private int createNewChainNode(int transitionByte, int newChild) throws SpaceExhaustedException
    {
        int newNode = allocateBlock() + LAST_POINTER_OFFSET - 1;
        putByte(newNode, (byte) transitionByte);
        putInt(newNode + 1, newChild);
        // Note: this does not need a volatile write as it is a new node, returning a new pointer, which needs to be
        // put in an existing node or the root. That action ends in a happens-before enforcing write.
        return newNode;
    }

    /** Like {@link #createNewChainNode}, but if the new child is already a chain node and has room, expand
     * it instead of creating a brand new node. */
    private int expandOrCreateChainNode(int transitionByte, int newChild) throws SpaceExhaustedException
    {
        if (isExpandableChain(newChild))
        {
            // attach as a new character in child node
            int newNode = newChild - 1;
            putByte(newNode, (byte) transitionByte);
            return newNode;
        }

        return createNewChainNode(transitionByte, newChild);
    }

    private int createEmptySplitNode() throws SpaceExhaustedException
    {
        int pos = allocateBlock();
        return pos + SPLIT_OFFSET;
    }

    private int createContentNode(int contentIndex, int child, boolean isSafeChain) throws SpaceExhaustedException
    {
        assert !isLeaf(child);
        if (isNull(child))
            return ~contentIndex;

        int offset = offset(child);
        int node;
        if (offset == SPLIT_OFFSET || isSafeChain && offset > (PREFIX_FLAGS_OFFSET + PREFIX_OFFSET) && offset <= CHAIN_MAX_OFFSET)
        {
            // We can do an embedded prefix node
            // Note: for chain nodes we have a risk that the node continues beyond the current point, in which case
            // creating the embedded node may overwrite information that is still needed by concurrent readers or the
            // mutation process itself.
            node = (child & -BLOCK_SIZE) | PREFIX_OFFSET;
            putByte(node + PREFIX_FLAGS_OFFSET, (byte) offset);
        }
        else
        {
            // Full prefix node
            node = allocateBlock() + PREFIX_OFFSET;
            putByte(node + PREFIX_FLAGS_OFFSET, (byte) 0xFF);
            putInt(node + PREFIX_POINTER_OFFSET, child);
        }

        putInt(node + PREFIX_CONTENT_OFFSET, contentIndex);
        return node;
    }

    private int updatePrefixNodeChild(int node, int child) throws SpaceExhaustedException
    {
        assert offset(node) == PREFIX_OFFSET;

        if (isNull(child))
            return ~getInt(node + PREFIX_CONTENT_OFFSET);

        // We can only update in-place if we have a full prefix node
        if (!isEmbeddedPrefixNode(node))
        {
            // This attaches the child branch and makes it reachable -- the write must be volatile.
            putIntVolatile(node + PREFIX_POINTER_OFFSET, child);
            return node;
        }
        else
        {
            int contentIndex = getInt(node + PREFIX_CONTENT_OFFSET);
            return createContentNode(contentIndex, child, true);
        }
    }

    private boolean isEmbeddedPrefixNode(int node)
    {
        return getByte(node + PREFIX_FLAGS_OFFSET) < BLOCK_SIZE;
    }

    /**
     * Copy the content from an existing node, if it has any, to a newly-prepared update for its child.
     *
     * @param existingPreContentNode pointer to the existing node before skipping over content nodes, i.e. this is
     *                               either the same as existingPostContentNode or a pointer to a prefix or leaf node
     *                               whose child is existingPostContentNode
     * @param existingPostContentNode pointer to the existing node being updated, after any content nodes have been
     *                                skipped and before any modification have been applied; always a non-content node
     * @param updatedPostContentNode is the updated node, i.e. the node to which all relevant modifications have been
     *                               applied; if the modifications were applied in-place, this will be the same as
     *                               existingPostContentNode, otherwise a completely different pointer; always a non-
     *                               content node
     * @return a node which has the children of updatedPostContentNode combined with the content of
     *         existingPreContentNode
     */
    private int preserveContent(int existingPreContentNode,
                               int existingPostContentNode,
                               int updatedPostContentNode) throws SpaceExhaustedException
    {
        if (existingPreContentNode == existingPostContentNode)
            return updatedPostContentNode;     // no content to preserve

        if (existingPostContentNode == updatedPostContentNode)
            return existingPreContentNode;     // child didn't change, no update necessary

        // else we have existing prefix node, and we need to reference a new child
        if (isLeaf(existingPreContentNode))
        {
            assert isNull(existingPostContentNode);
            return createContentNode(~existingPreContentNode, updatedPostContentNode, true);
        }

        assert offset(existingPreContentNode) == PREFIX_OFFSET;
        return updatePrefixNodeChild(existingPreContentNode, updatedPostContentNode);
    }

    /**
     * State of the walk of the given mutation trie. Passed to mutation nodes in their parentState link.
     */
    class ApplyState<U>
    {
        /**
         * The node from the mutation trie.
         */
        final Node<U, ApplyState<U>> mutationNode;

        /**
         * Pointer to the existing node before skipping over content nodes, i.e. this is either the same as
         * existingPostContentNode or a pointer to a prefix or leaf node whose child is existingPostContentNode.
         */
        final int existingPreContentNode;

        /**
         * Pointer to the existing node being updated, after any content nodes have been skipped and before any
         * modification have been applied. Always a non-content node.
         */
        final int existingPostContentNode;

        /**
         * The updated node, i.e. the node to which the relevant modifications are being applied. This will change as
         * children are processed and attached to the node. After all children have been processed, this will contain
         * the fully updated node (i.e. the union of existingPostContentNode and mutationNode) without any content,
         * which will be processed separately and, if necessary, attached ahead of this. If the modifications were
         * applied in-place, this will be the same as existingPostContentNode, otherwise a completely different
         * pointer. Always a non-content node.
         */
        int updatedPostContentNode;

        ApplyState(Node<U, ApplyState<U>> mutationNode, int transition)
        {
            ApplyState<U> parentState = mutationNode.parentLink;
            if (parentState == null)
                existingPreContentNode = root;
            else
            {
                existingPreContentNode = isNull(parentState.existingPostContentNode)
                                         ? NONE
                                         : getChild(parentState.existingPostContentNode, transition);
            }

            existingPostContentNode = followContentTransition(existingPreContentNode);
            updatedPostContentNode = existingPostContentNode;

            this.mutationNode = mutationNode;
        }

        private void attachChild(int ourChild) throws SpaceExhaustedException
        {
            int transition = mutationNode.currentTransition;
            if (isNull(updatedPostContentNode))
                updatedPostContentNode = expandOrCreateChainNode(transition, ourChild);
            else
                updatedPostContentNode = MemtableTrie.this.attachChild(updatedPostContentNode,
                                                                       transition,
                                                                       ourChild);
        }

        private int applyContent(U mutationContent, UpsertTransformer<T, U> transformer) throws SpaceExhaustedException
        {
            // common case, no new content
            if (mutationContent == null)
                return preserveContent(existingPreContentNode, existingPostContentNode, updatedPostContentNode);

            int contentIndex = -1;
            int existingContentIndex = -1;

            if (existingPreContentNode != existingPostContentNode)
            {
                // There is pre-existing content which must be merged with the new.
                if (isLeaf(existingPreContentNode))
                    existingContentIndex = ~existingPreContentNode;
                else
                {
                    assert offset(existingPreContentNode) == PREFIX_OFFSET;
                    existingContentIndex = getInt(existingPreContentNode + PREFIX_CONTENT_OFFSET);
                }

                final T existingContent = getContent(existingContentIndex);
                T combinedContent = transformer.apply(existingContent, mutationContent);
                setContent(existingContentIndex, combinedContent);
                if (combinedContent != null)
                    contentIndex = existingContentIndex;
            }
            else
            {
                // No pre-existing content.
                T combinedContent = transformer.apply(null, mutationContent);
                if (combinedContent != null)
                    contentIndex = addContent(combinedContent);
            }

            // The supplied transformer may return null, e.g. to delete data. In this case we don't have a content index.
            if (contentIndex == -1)
                return updatedPostContentNode;

            if (isNull(updatedPostContentNode))
                return ~contentIndex;

            // We can't update in-place if there was no preexisting prefix, or if the prefix was embedded and the target
            // node must change.
            if (existingPreContentNode == existingPostContentNode ||
                isEmbeddedPrefixNode(existingPreContentNode) && updatedPostContentNode != existingPostContentNode)
                return createContentNode(contentIndex, updatedPostContentNode, isNull(existingPostContentNode));

            // Otherwise modify in place
            if (updatedPostContentNode != existingPostContentNode) // to use volatile write but also ensure we don't corrupt embedded nodes
                putIntVolatile(existingPreContentNode + PREFIX_POINTER_OFFSET, updatedPostContentNode);
            assert contentIndex == existingContentIndex;
            return existingPreContentNode;
        }

        private ApplyState<U> attachAndMoveToParentState(UpsertTransformer<T, U> transformer) throws SpaceExhaustedException
        {
            ApplyState<U> parentState = mutationNode.parentLink;

            int updatedPreContentNode = applyContent(mutationNode.content(),
                                                     transformer);

            if (parentState == null)
            {
                assert root == existingPreContentNode;
                if (updatedPreContentNode != existingPreContentNode)
                {
                    // Only write to root if they are different (value doesn't change, but
                    // we don't want to invalidate the value in other cores' caches unnecessarily).
                    root = updatedPreContentNode;
                }

                return null;
            }

            if (updatedPreContentNode != existingPreContentNode)
                parentState.attachChild(updatedPreContentNode);

            return parentState;
        }
    }

    /**
     * Somewhat similar to {@link MergeResolver}, this encapsulates logic to be applied whenever new content is being
     * upserted into a {@link MemtableTrie}. Unlike {@link MergeResolver}, {@link UpsertTransformer} will be applied no
     * matter if there's pre-existing content for that trie key/path or not.
     *
     * @param <T> The content type for this {@link MemtableTrie}.
     * @param <U> The type of the new content being applied to this {@link MemtableTrie}.
     */
    public interface UpsertTransformer<T, U>
    {
        /**
         * Called when there's content in the updating trie.
         *
         * @param existing Existing content for this key, or null if there isn't any.
         * @param update   The update, always non-null.
         * @return The combined value to use.
         */
        T apply(T existing, U update);
    }

    /**
     * Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
     * with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
     * @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
     * different than the element type for this memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value. Applied even if there's no pre-existing value in the memtable trie.
     */
    public <U> void apply(Trie<U> mutation, final UpsertTransformer<T, U> transformer) throws SpaceExhaustedException
    {
        Node<U, ApplyState<U>> current = mutation.root();
        if (current == null)
            return;

        ApplyState<U> state = new ApplyState<U>(current, current.parentLink != null ? current.parentLink.mutationNode.currentTransition : -1);

        Trie.Remaining has = current.startIteration();
        while (true)
        {
            if (has != null)
            {
                // We have a transition, get child to descend into
                Node<U, ApplyState<U>> child = current.getCurrentChild(state);

                if (child == null)
                {
                    // no child, get next
                    has = current.advanceIteration();
                }
                else
                {
                    state = new ApplyState<U>(child, current.currentTransition);
                    current = child;
                    has = current.startIteration();
                }
            }
            else
            {
                // There are no more children. Ascend to the parent state to continue walk.
                state = state.attachAndMoveToParentState(transformer);
                if (state == null)
                    break;
                current = state.mutationNode;
                has = current.advanceIteration();
            }
        }
    }

    /**
     * Map-like put method, using the apply machinery above which cannot run into stack overflow. When the correct
     * position in the trie has been reached, the value will be resolved with the given function before being placed in
     * the trie (even if there's no pre-existing content in this trie).
     * @param key the trie path/key for the given value.
     * @param value the value being put in the memtable trie. Note that it can be of type different than the element
     * type for this memtable trie. It's up to the {@code transformer} to return the final value that will stay in
     * the memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value (of a potentially different type), returning the final value that will stay in the memtable trie. Applied
     * even if there's no pre-existing value in the memtable trie.
     */
    public <R> void putSingleton(ByteComparable key,
                                 R value,
                                 UpsertTransformer<T, ? super R> transformer) throws SpaceExhaustedException
    {
        apply(Trie.singleton(key, value), transformer);
    }

    /**
     * A version of putSingleton which uses recursive put if the last argument is true.
     */
    public <R> void putSingleton(ByteComparable key,
                                 R value,
                                 UpsertTransformer<T, ? super R> transformer,
                                 boolean useRecursive) throws SpaceExhaustedException
    {
        if (useRecursive)
            putRecursive(key, value, transformer);
        else
            putSingleton(key, value, transformer);
    }

    /**
     * Map-like put method, using a fast recursive implementation through the key bytes. May run into stack overflow if
     * the trie becomes too deep. When the correct position in the trie has been reached, the value will be resolved
     * with the given function before being placed in the trie (even if there's no pre-existing content in this trie).
     * @param key the trie path/key for the given value.
     * @param value the value being put in the memtable trie. Note that it can be of type different than the element
     * type for this memtable trie. It's up to the {@code transformer} to return the final value that will stay in
     * the memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value (of a potentially different type), returning the final value that will stay in the memtable trie. Applied
     * even if there's no pre-existing value in the memtable trie.
     */
    public <R> void putRecursive(ByteComparable key, R value, final UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        int newRoot = putRecursive(root, key.asComparableBytes(BYTE_COMPARABLE_VERSION), value, transformer);
        if (newRoot != root)
            root = newRoot;
    }

    private <R> int putRecursive(int node, ByteSource key, R value, final UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        int transition = key.next();
        if (transition == ByteSource.END_OF_STREAM)
            return applyContent(node, value, transformer);

        int child = NONE;
        if (!isNull(node))
            child = getChild(node, transition);

        int newChild = putRecursive(child, key, value, transformer);
        if (newChild == child)
            return node;

        int skippedContent = followContentTransition(node);
        int attachedChild = !isNull(skippedContent)
                            ? attachChild(skippedContent, transition, newChild)  // Single path, no copying required
                            : expandOrCreateChainNode(transition, newChild);

        return preserveContent(node, skippedContent, attachedChild);
    }

    private <R> int applyContent(int node, R value, UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        if (isNull(node))
            return ~addContent(transformer.apply(null, value));

        if (isLeaf(node))
        {
            int contentIndex = ~node;
            setContent(contentIndex, transformer.apply(getContent(contentIndex), value));
            return node;
        }

        if (offset(node) == PREFIX_OFFSET)
        {
            int contentIndex = getInt(node + PREFIX_CONTENT_OFFSET);
            setContent(contentIndex, transformer.apply(getContent(contentIndex), value));
            return node;
        }
        else
            return createContentNode(addContent(transformer.apply(null, value)), node, false);
    }

    /**
     * Returns true if the allocation threshold has been reached. To be called by the the writing thread (ideally, just
     * after the write completes). When this returns true, the user should switch to a new trie as soon as feasible.
     *
     * The trie expects up to 10% growth above this threshold. Any growth beyond that may be done inefficiently, and
     * the trie will fail altogether when the size grows beyond 2G - 256 bytes.
     */
    public boolean reachedAllocatedSizeThreshold()
    {
        return allocatedPos >= ALLOCATED_SIZE_THRESHOLD;
    }

    /**
     * For tests only! Advance the allocation pointer (and allocate space) by this much to test behaviour close to
     * full.
     */
    @VisibleForTesting
    int advanceAllocatedPos(int wantedPos) throws SpaceExhaustedException
    {
        while (allocatedPos < wantedPos)
            allocateBlock();
        return allocatedPos;
    }

    /** Returns the off heap size of the memtable trie itself, not counting any space taken by referenced content. */
    public long sizeOffHeap()
    {
        return bufferType == BufferType.ON_HEAP ? 0 : allocatedPos;
    }

    /** Returns the on heap size of the memtable trie itself, not counting any space taken by referenced content. */
    public long sizeOnHeap()
    {
        return contentCount * MemoryLayoutSpecification.SPEC.getReferenceSize() +
               (bufferType == BufferType.ON_HEAP ? allocatedPos + EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP);
    }

    @Override
    public Iterable<T> valuesUnordered()
    {
        return () -> new Iterator<T>()
        {
            int idx = 0;

            public boolean hasNext()
            {
                return idx < contentCount;
            }

            public T next()
            {
                if (!hasNext())
                    throw new NoSuchElementException();

                return getContent(idx++);
            }
        };
    }

    public int valuesCount()
    {
        return contentCount;
    }
}
