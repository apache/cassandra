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
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;

/// Base class for mutable in-memory tries, providing the common infrastructure for plain, range and deletion-aware
/// in-memory tries.
public abstract class InMemoryBaseTrie<T> extends InMemoryReadTrie<T>
{
    // See the trie format description in InMemoryReadTrie.

    // constants for space calculations
    static final long REFERENCE_ARRAY_ON_HEAP_SIZE = ObjectSizes.measureDeep(new AtomicReferenceArray<>(0));

    public enum ExpectedLifetime
    {
        SHORT, LONG
    }

    InMemoryBaseTrie(ByteComparable.Version byteComparableVersion, boolean presentContentOnDescentPath, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        this(byteComparableVersion, presentContentOnDescentPath, null, bufferType, lifetime, opOrder);
    }

    InMemoryBaseTrie(ByteComparable.Version byteComparableVersion, boolean presentContentOnDescentPath, Predicate<T> shouldPreserveWithoutChildren, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        this(byteComparableVersion,
             presentContentOnDescentPath,
             new BufferManagerMultibuf(bufferType, lifetime, opOrder),
             new ContentManagerPojo<>(shouldPreserveWithoutChildren, lifetime, opOrder));
    }

    InMemoryBaseTrie(ByteComparable.Version byteComparableVersion, boolean presentContentOnDescentPath, BufferManager bufferManager, ContentManager<T> contentManager)
    {
        super(byteComparableVersion, presentContentOnDescentPath, bufferManager, contentManager, NONE);
    }

    // Buffer, content list and cell management

    private void putInt(int pos, int value)
    {
        getBuffer(pos).putInt(inBufferOffset(pos), value);
    }

    protected void putIntVolatile(int pos, int value)
    {
        getBuffer(pos).putIntVolatile(inBufferOffset(pos), value);
    }

    private void putShort(int pos, short value)
    {
        getBuffer(pos).putShort(inBufferOffset(pos), value);
    }

    private void putShortVolatile(int pos, short value)
    {
        getBuffer(pos).putShortVolatile(inBufferOffset(pos), value);
    }

    private void putByte(int pos, byte value)
    {
        getBuffer(pos).putByte(inBufferOffset(pos), value);
    }

    /// See [BufferManager#allocateCell].
    @VisibleForTesting
    int allocateCell() throws TrieSpaceExhaustedException
    {
        return bufferManager.allocateCell();
    }

    /// See [BufferManager#recycleCell].
    protected void recycleCell(int cell)
    {
        bufferManager.recycleCell(cell);
    }

    /// See [BufferManager#copyCell].
    protected int copyCell(int cell) throws TrieSpaceExhaustedException
    {
        return bufferManager.copyCell(cell);
    }

    /// Add a new content value.
    ///
    /// @return A content id that can be used to reference the content, a negative number where
    ///         `id & CONTENT_INDEX_MASK` encodes the position of the value in the content array.
    protected int addContent(T value, boolean contentAfterBranch) throws TrieSpaceExhaustedException
    {
        if (value == null)
            return NONE;
        int id = contentManager.addContent(value, contentAfterBranch);
        assert isLeaf(id);
        return id;
    }

    /// Change the content associated with a given content id.
    ///
    /// @param id encoded content id, where `id & CONTENT_INDEX_MASK` is the position in the content array
    /// @param value new content value to store
    /// @return the id to use for the modified content; an attempt will be made to make this the same as id, but not
    ///         all content managers will be able to freely modify the data for a given id.
    protected int setContent(int id, T value) throws TrieSpaceExhaustedException
    {
        return contentManager.setContent(id, value);
    }

    protected void releaseContent(int id)
    {
        contentManager.releaseContent(id);
    }

    protected boolean shouldPreserveWithoutChildren(int id)
    {
        return contentManager.shouldPreserveWithoutChildren(id);
    }

    /// Called to clean up all buffers when the trie is known to no longer be needed.
    public void discardBuffers()
    {
        bufferManager.discardBuffers();
    }

    @VisibleForTesting
    int copyIfOriginal(int node, int originalNode) throws TrieSpaceExhaustedException
    {
        return (node == originalNode)
               ? copyCell(originalNode)
               : node;
    }

    private int getOrAllocate(int pointerAddress, int offsetWhenAllocating) throws TrieSpaceExhaustedException
    {
        int child = getIntVolatile(pointerAddress);
        if (child != NONE)
            return child;

        child = allocateCell() | offsetWhenAllocating;
        // volatile writes not needed because this branch is not attached yet
        putInt(pointerAddress, child);
        return child;
    }

    private int getCopyOrAllocate(int pointerAddress, int originalChild, int offsetWhenAllocating) throws TrieSpaceExhaustedException
    {
        int child = getIntVolatile(pointerAddress);
        if (child == NONE)
        {
            child = allocateCell() | offsetWhenAllocating;
            // volatile writes not needed because this branch is not attached yet
            putInt(pointerAddress, child);
        }
        else if (child == originalChild)
        {
            child = copyCell(originalChild);
            // volatile writes not needed because this branch is not attached yet
            putInt(pointerAddress, child);
        }

        return child;
    }

    // Write methods

    // Write visibility model: writes are not volatile, with the exception of the final write before a call returns
    // the same value that was present before (e.g. content was updated in-place / existing node got a new child or had
    // a child pointer updated); if the whole path including the root node changed, the root itself gets a volatile
    // write.
    // This final write is the point where any new cells created during the write become visible for readers for the
    // first time, and such readers must pass through reading that pointer, which forces a happens-before relationship
    // that extends to all values written by this thread before it.

    /// Attach a child to the given non-content node. This may be an update for an existing branch, or a new child for
    /// the node. An update _is_ required (i.e. this is only called when the `newChild` pointer is not the same as the
    /// existing value).
    /// This method is called when the original node content must be preserved for concurrent readers (i.e. any cell to
    /// be modified needs to be copied first.)
    ///
    /// @param node pointer to the node to update or copy
    /// @param originalNode pointer to the node as it was before any updates in the current modification (i.e. apply
    ///                     call) were started. In other words, the node that is currently reachable by readers if they
    ///                     follow the same key, and which will become unreachable for new readers after this update
    ///                     completes. Used to avoid copying again if already done -- if `node` is already != `originalNode`
    ///                     (which is the case when a second or further child of a node is changed by an update),
    ///                     then node is currently not reachable and can be safely modified or completely overwritten.
    /// @param trans transition to modify/add
    /// @param newChild new child pointer
    /// @return pointer to the updated node
    protected int attachChildCopying(int node, int originalNode, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        assert !isLeaf(node) : "attachChild cannot be used on content nodes.";

        switch (offset(node))
        {
            case PREFIX_OFFSET:
                throw new AssertionError("attachChild cannot be used on content nodes.");
            case SPARSE_OFFSET:
                // If the node is already copied (e.g. this is not the first child being modified), there's no need to copy
                // it again.
                return attachChildToSparseCopying(node, originalNode, trans, newChild);
            case SPLIT_OFFSET:
                // This call will copy the split node itself and any intermediate cells as necessary to make sure cells
                // reachable from the original node are not modified.
                return attachChildToSplitCopying(node, originalNode, trans, newChild);
            default:
                // chain nodes
                return attachChildToChainCopying(node, trans, newChild); // always copies
        }
    }

    /// Attach a child to the given node. This may be an update for an existing branch, or a new child for the node.
    /// An update _is_ required (i.e. this is only called when the newChild pointer is not the same as the existing value).
    ///
    /// @param node pointer to the node to update or copy
    /// @param trans transition to modify/add
    /// @param newChild new child pointer
    /// @return pointer to the updated node; same as node if update was in-place
    protected int attachChild(int node, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        assert !isLeaf(node) : "attachChild cannot be used on content nodes.";

        switch (offset(node))
        {
            case PREFIX_OFFSET:
                throw new AssertionError("attachChild cannot be used on content nodes.");
            case SPARSE_OFFSET:
                return attachChildToSparse(node, trans, newChild);
            case SPLIT_OFFSET:
                return attachChildToSplit(node, trans, newChild);
            default:
                return attachChildToChain(node, trans, newChild);
        }
    }

    /// Attach a child to the given split node. This may be an update for an existing branch, or a new child for the node.
    private int attachChildToSplit(int node, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        int midPos = splitCellPointerAddress(node, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        int mid = getIntVolatile(midPos);
        if (isNull(mid))
        {
            if (isNull(newChild))
                return node;
            mid = createEmptySplitNode();
            int tailPos = splitCellPointerAddress(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
            int tail = createEmptySplitNode();
            int childPos = splitCellPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
            putInt(childPos, newChild);
            putInt(tailPos, tail);
            putIntVolatile(midPos, mid);
            return node;
        }

        int tailPos = splitCellPointerAddress(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        int tail = getIntVolatile(tailPos);
        if (isNull(tail))
        {
            if (isNull(newChild))
                return node;
            tail = createEmptySplitNode();
            int childPos = splitCellPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
            putInt(childPos, newChild);
            putIntVolatile(tailPos, tail);
            return node;
        }

        int childPos = splitCellPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        if (isNull(newChild))
            return removeSplitChildVolatile(node, midPos, mid, tailPos, tail, childPos);
        else
        {
            putIntVolatile(childPos, newChild);
            return node;    // normal path, adding data
        }
    }

    /// Remove a split child, propagating the removal upward if this results in an empty split node cell.
    /// This version of the method is called when the node being modified is reachable and may be concurrently accessed
    /// by reads.
    private int removeSplitChildVolatile(int node, int midPos, int mid, int tailPos, int tail, int childPos)
    throws TrieSpaceExhaustedException
    {
        if (isNull(getIntVolatile(childPos)))
            return node;

        // Because there may be concurrent accesses to this node that have saved the path we are removing as the next
        // transition and expect it to be valid, we need to copy any cell where we set the value to NONE.
        if (!isSplitBlockEmptyExcept(tail, SPLIT_OTHER_LEVEL_LIMIT, childPos))
        {
            int newTail = copyCell(tail);
            putInt(newTail + childPos - tail, NONE);
            putIntVolatile(tailPos, newTail);
            return node;
        }
        recycleCell(tail);
        if (!isSplitBlockEmptyExcept(mid, SPLIT_OTHER_LEVEL_LIMIT, tailPos))
        {
            int newMid = copyCell(mid);
            putInt(newMid + tailPos - mid, NONE);
            putIntVolatile(midPos, newMid);
            return node;
        }
        recycleCell(mid);
        if (!isSplitBlockEmptyExcept(node, SPLIT_START_LEVEL_LIMIT, midPos))
        {
            int newNode = copyCell(node);
            putInt(newNode + midPos - node, NONE);
            return newNode;
        }
        recycleCell(node);
        return NONE;
    }

    boolean isSplitBlockEmptyExcept(int node, int limit, int deletedPos)
    {
        for (int i = 0; i < limit; ++i)
        {
            int pos = splitCellPointerAddress(node, i, limit);
            if (pos != deletedPos && !isNull(getIntVolatile(pos)))
                return false;
        }
        return true;
    }

    /// Non-volatile version of `attachChildToSplit`. Used when the split node is not reachable yet (during the conversion
    /// from sparse).
    private int attachChildToSplitNonVolatile(int node, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        assert offset(node) == SPLIT_OFFSET : "Invalid split node in trie";
        int midPos = splitCellPointerAddress(node, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        int mid = getOrAllocate(midPos, SPLIT_OFFSET);
        assert offset(mid) == SPLIT_OFFSET : "Invalid split node in trie";
        int tailPos = splitCellPointerAddress(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        int tail = getOrAllocate(tailPos, SPLIT_OFFSET);
        assert offset(tail) == SPLIT_OFFSET : "Invalid split node in trie";
        int childPos = splitCellPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        putInt(childPos, newChild);
        if (isNull(newChild))
            return removeSplitPathNonVolatile(node, midPos, mid, tailPos, tail);
        else
            return node;    // normal path, adding data
    }

    /// Attach a child to the given split node, copying all modified content to enable atomic visibility
    /// of modification.
    /// This may be an update for an existing branch, or a new child for the node.
    private int attachChildToSplitCopying(int node, int originalNode, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        if (offset(originalNode) != SPLIT_OFFSET)  // includes originalNode == NONE
            return attachChildToSplitNonVolatile(node, trans, newChild);

        node = copyIfOriginal(node, originalNode);
        assert offset(node) == SPLIT_OFFSET : "Invalid split node in trie";

        int midPos = splitCellPointerAddress(node, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        int midOriginal = originalNode != NONE ? getIntVolatile(midPos + originalNode - node) : NONE;
        int mid = getCopyOrAllocate(midPos, midOriginal, SPLIT_OFFSET);
        assert offset(mid) == SPLIT_OFFSET : "Invalid split node in trie";

        int tailPos = splitCellPointerAddress(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        int tailOriginal = midOriginal != NONE ? getIntVolatile(tailPos + midOriginal - mid) : NONE;
        int tail = getCopyOrAllocate(tailPos, tailOriginal, SPLIT_OFFSET);
        assert offset(tail) == SPLIT_OFFSET : "Invalid split node in trie";

        int childPos = splitCellPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        putInt(childPos, newChild);
        if (isNull(newChild))
            return removeSplitPathNonVolatile(node, midPos, mid, tailPos, tail);
        else
            return node;
    }

    /// Propagate the removal of a split child upward if it resulted in an empty split node cell,
    /// assuming that the node being modified is not reachable and cannot be accessed concurrently.
    private int removeSplitPathNonVolatile(int node, int midPos, int mid, int tailPos, int tail)
    {
        if (!isSplitBlockEmpty(tail, SPLIT_OTHER_LEVEL_LIMIT))
            return node;
        recycleCell(tail);
        putInt(tailPos, NONE);
        if (!isSplitBlockEmpty(mid, SPLIT_OTHER_LEVEL_LIMIT))
            return node;
        recycleCell(mid);
        putInt(midPos, NONE);
        if (!isSplitBlockEmpty(node, SPLIT_START_LEVEL_LIMIT))
            return node;
        recycleCell(node);
        return NONE;
    }

    boolean isSplitBlockEmpty(int node, int limit)
    {
        for (int i = 0; i < limit; ++i)
            if (!isNull(getSplitCellPointer(node, i, limit)))
                return false;
        return true;
    }

    /// Attach a child to the given sparse node. This may be an update for an existing branch, or a new child for the node.
    private int attachChildToSparse(int node, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        int index;
        int smallerCount = 0;
        // first check if this is an update and modify in-place if so
        for (index = 0; index < SPARSE_CHILD_COUNT; ++index)
        {
            if (isNull(getIntVolatile(node + SPARSE_CHILDREN_OFFSET + index * Integer.BYTES)))
                break;
            final int existing = getUnsignedByte(node + SPARSE_BYTES_OFFSET + index);
            if (existing == trans)
            {
                if (isNull(newChild))
                    return removeSparseChild(node, index);
                putIntVolatile(node + SPARSE_CHILDREN_OFFSET + index * Integer.BYTES, newChild);
                return node;
            }
            else if (existing < trans)
                ++smallerCount;
        }
        int childCount = index;
        if (isNull(newChild))
            return node;

        if (childCount == SPARSE_CHILD_COUNT)
        {
            // Node is full. Switch to split
            return upgradeSparseToSplit(node, trans, newChild);
        }

        // Add a new transition. They are not kept in order, so append it at the first free position.
        putByte(node + SPARSE_BYTES_OFFSET + childCount, (byte) trans);

        // Update order word.
        int order = getUnsignedShortVolatile(node + SPARSE_ORDER_OFFSET);
        int newOrder = insertInOrderWord(order, childCount, smallerCount);

        // Sparse nodes have two access modes: via the order word, when listing transitions, or directly to characters
        // and addresses.
        // To support the former, we volatile write to the order word last, after everything is correctly set up.
        // The latter does not touch the order word. To support that too, we volatile write the address, as the reader
        // can't determine if the position is in use based on the character byte alone (00 is also a valid transition).
        // Note that this means that reader must check the transition byte AFTER the address, to ensure they get the
        // correct value (see getSparseChild).

        // setting child enables reads to start seeing the new branch
        putIntVolatile(node + SPARSE_CHILDREN_OFFSET + childCount * Integer.BYTES, newChild);

        // some readers will decide whether to check the pointer based on the order word
        // write that volatile to make sure they see the new change too
        putShortVolatile(node + SPARSE_ORDER_OFFSET,  (short) newOrder);
        return node;
    }

    /// Remove a child of the given sparse node. To ensure the safety of concurrent operations, this is always done
    /// as a copying operation as we can't safely shift entries in a sparse node.
    private int removeSparseChild(int node, int index) throws TrieSpaceExhaustedException
    {
        // Note: one may be tempted to allow removal of the last added child, but this is not a good idea as it opens
        // us up to ABA problems. See InMemoryTrie.md.

        // Mark the cell for recycling. Note that we can still use the cell's data as input as it will not be actually
        // released and cannot be modified before the mutation completes.
        recycleCell(node);

        int order = getUnsignedShortVolatile(node + SPARSE_ORDER_OFFSET);
        if (index <= 1 && order == 6)
        {
            int survivingIndex = index ^ 1;
            return expandOrCreateChainNode(getUnsignedByte(node + SPARSE_BYTES_OFFSET + survivingIndex),
                                           getIntVolatile(node + SPARSE_CHILDREN_OFFSET + survivingIndex * Integer.BYTES));
        }

        // Because we need the smallest child to not be the last (which can happen if we just remove entries), we will
        // put the remaining data in order.
        int newNode = allocateCell() | SPARSE_OFFSET;
        int i = 0;
        int newOrder = 0;
        int mul = 1;
        while (order > 0)
        {
            int next = order % SPARSE_CHILD_COUNT;
            order /= SPARSE_CHILD_COUNT;
            if (next == index)
                continue;
            putInt(newNode + SPARSE_CHILDREN_OFFSET + i * Integer.BYTES, getIntVolatile(node + SPARSE_CHILDREN_OFFSET + next * Integer.BYTES));
            putInt(newNode + SPARSE_BYTES_OFFSET + i, getUnsignedByte(node + SPARSE_BYTES_OFFSET + next));
            newOrder += i * mul;
            mul *= SPARSE_CHILD_COUNT;
            ++i;
        }
        putShort(newNode + SPARSE_ORDER_OFFSET, (short) newOrder);
        return newNode;
    }

    /**
     * Attach a child to the given sparse node. This may be an update for an existing branch, or a new child for the node.
     * Resulting node is not reachable, no volatile set needed.
     */
    private int attachChildToSparseCopying(int node, int originalNode, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        int index;
        int smallerCount = 0;
        // first check if this is an update and modify in-place if so
        for (index = 0; index < SPARSE_CHILD_COUNT; ++index)
        {
            if (isNull(getIntVolatile(node + SPARSE_CHILDREN_OFFSET + index * Integer.BYTES)))
                break;
            final int existing = getUnsignedByte(node + SPARSE_BYTES_OFFSET + index);
            if (existing == trans)
            {
                if (isNull(newChild))
                    return removeSparseChild(node, index);
                node = copyIfOriginal(node, originalNode);
                putInt(node + SPARSE_CHILDREN_OFFSET + index * Integer.BYTES, newChild);
                return node;
            }
            else if (existing < trans)
                ++smallerCount;
        }
        int childCount = index;

        if (isNull(newChild))
            return node;

        if (childCount == SPARSE_CHILD_COUNT)
        {
            // Node is full. Switch to split.
            // Note that even if node != originalNode, we still have to recycle it as it was a temporary one that will
            // no longer be attached.
            return upgradeSparseToSplit(node, trans, newChild);
        }

        node = copyIfOriginal(node, originalNode);

        // Add a new transition. They are not kept in order, so append it at the first free position.
        putByte(node + SPARSE_BYTES_OFFSET + childCount,  (byte) trans);

        putInt(node + SPARSE_CHILDREN_OFFSET + childCount * Integer.BYTES, newChild);

        // Update order word.
        int order = getUnsignedShortVolatile(node + SPARSE_ORDER_OFFSET);
        int newOrder = insertInOrderWord(order, childCount, smallerCount);
        putShort(node + SPARSE_ORDER_OFFSET,  (short) newOrder);

        return node;
    }

    private int upgradeSparseToSplit(int node, int trans, int newChild) throws TrieSpaceExhaustedException
    {
        int split = createEmptySplitNode();
        for (int i = 0; i < SPARSE_CHILD_COUNT; ++i)
        {
            int t = getUnsignedByte(node + SPARSE_BYTES_OFFSET + i);
            int p = getIntVolatile(node + SPARSE_CHILDREN_OFFSET + i * Integer.BYTES);
            attachChildToSplitNonVolatile(split, t, p);
        }
        attachChildToSplitNonVolatile(split, trans, newChild);
        recycleCell(node);
        return split;
    }

    /// Insert the given newIndex in the base-6 encoded order word in the correct position with respect to the ordering.
    ///
    /// E.g.
    ///   - `insertOrderWord(120, 3, 0)` must return 1203 (decimal 48*6 + 3)
    ///   - `insertOrderWord(120, 3, 1, ptr)` must return 1230 (decimal 8*36 + 3*6 + 0)
    ///   - `insertOrderWord(120, 3, 2, ptr)` must return 1320 (decimal 1*216 + 3*36 + 12)
    ///   - `insertOrderWord(120, 3, 3, ptr)` must return 3120 (decimal 3*216 + 48)
    @VisibleForTesting
    static int insertInOrderWord(int order, int newIndex, int smallerCount)
    {
        int r = 1;
        for (int i = 0; i < smallerCount; ++i)
            r *= 6;
        int head = order / r;
        int tail = order % r;
        // insert newIndex after the ones we have passed (order % r) and before the remaining (order / r)
        return tail + (head * 6 + newIndex) * r;
    }

    /// Attach a child to the given chain node. This may be an update for an existing branch with different target
    /// address, or a second child for the node.
    ///
    /// This method always copies the node -- with the exception of updates that change the child of the last node in a
    /// chain cell with matching transition byte (which this method is not used for, see [#attachChild]), modifications to
    /// chain nodes cannot be done in place, either because we introduce a new transition byte and have to convert from
    /// the single-transition chain type to sparse, or because we have to remap the child from the implicit node + 1 to
    /// something else.
    private int attachChildToChain(int node, int transitionByte, int newChild) throws TrieSpaceExhaustedException
    {
        int existingByte = getUnsignedByte(node);
        if (transitionByte == existingByte)
        {
            // This is still a single path. Update child if possible (only if this is the last character in the chain).
            if (offset(node) == LAST_POINTER_OFFSET - 1)
            {
                if (isNull(newChild))
                {
                    recycleCell(node);
                    return NONE;
                }

                putIntVolatile(node + 1, newChild);
                return node;
            }
            else
            {
                if (isNull(newChild))
                    return NONE;

                // This will only be called if new child is different from old, and the update is not on the final child
                // where we can change it in place (see attachChild). We must always create something new.
                // Note that since this is not the last character, we either still need this cell or we have already
                // released it (a createSparseNode must have been called earlier).
                // If the child is a chain, we can expand it (since it's a different value, its branch must be new and
                // nothing can already reside in the rest of the cell).
                return expandOrCreateChainNode(transitionByte, newChild);
            }
        }
        if (isNull(newChild))
            return node;

        // The new transition is different, so we no longer have only one transition. Change type.
        return convertChainToSparse(node, existingByte, newChild, transitionByte);
    }

    /// Attach a child to the given chain node, when we are force-copying.
    private int attachChildToChainCopying(int node, int transitionByte, int newChild)
    throws TrieSpaceExhaustedException
    {
        int existingByte = getUnsignedByte(node);
        if (transitionByte == existingByte)
        {
            // This is still a single path.
            // Make sure we release the cell if it will no longer be referenced (if we update last reference, the whole
            // path has to move as the other nodes in this chain can't be remapped).
            if (offset(node) == LAST_POINTER_OFFSET - 1)
                recycleCell(node);

            if (isNull(newChild))
                return NONE;

            return expandOrCreateChainNode(transitionByte, newChild);
        }
        else
        {
            if (isNull(newChild))
                return node;

            // The new transition is different, so we no longer have only one transition. Change type.
            return convertChainToSparse(node, existingByte, newChild, transitionByte);
        }
    }

    private int convertChainToSparse(int node, int existingByte, int newChild, int transitionByte)
    throws TrieSpaceExhaustedException
    {
        int existingChild = node + 1;
        if (offset(existingChild) == LAST_POINTER_OFFSET)
        {
            existingChild = getIntVolatile(existingChild);
            // This was a chain with just one transition which will no longer be referenced.
            // The cell may contain other characters/nodes leading to this, which are also guaranteed to be
            // unreferenced.
            // However, these leading nodes may still be in the parent path and will be needed until the
            // mutation completes.
            recycleCell(node);
        }
        // Otherwise the sparse node we will now create references this cell, so it can't be recycled.
        return createSparseNode(existingByte, existingChild, transitionByte, newChild);
    }

    @VisibleForTesting
    boolean isExpandableChain(int newChild)
    {
        // child must be a prefix node with at least one position character free, and must not become NONE when expanded.
        // newChild > NONE + 1 checks both that it is not a leaf/NONE and doesn't become NONE when expanded.
        if (newChild <= NONE + 1)
            return false;

        int newOffset = offset(newChild);
        return newOffset > CHAIN_MIN_OFFSET && newOffset <= CHAIN_MAX_OFFSET;
    }

    /// Create a sparse node with two children.
    private int createSparseNode(int byte1, int child1, int byte2, int child2) throws TrieSpaceExhaustedException
    {
        assert byte1 != byte2 : "Attempted to create a sparse node with two of the same transition";
        if (byte1 > byte2)
        {
            // swap them so the smaller is byte1, i.e. there's always something bigger than child 0 so 0 never is
            // at the end of the order
            int t = byte1; byte1 = byte2; byte2 = t;
            t = child1; child1 = child2; child2 = t;
        }

        int node = allocateCell() + SPARSE_OFFSET;
        putByte(node + SPARSE_BYTES_OFFSET + 0,  (byte) byte1);
        putByte(node + SPARSE_BYTES_OFFSET + 1,  (byte) byte2);
        putInt(node + SPARSE_CHILDREN_OFFSET + 0 * Integer.BYTES, child1);
        putInt(node + SPARSE_CHILDREN_OFFSET + 1 * Integer.BYTES, child2);
        putShort(node + SPARSE_ORDER_OFFSET,  (short) (1 * 6 + 0));
        // Note: this does not need a volatile write as it is a new node, returning a new pointer, which needs to be
        // put in an existing node or the root. That action ends in a happens-before enforcing write.
        return node;
    }

    /// Creates a chain node with the single provided transition (pointing to the provided child).
    /// Note that to avoid creating inefficient tries with under-utilized chain nodes, this should only be called from
    /// [#expandOrCreateChainNode] and other call-sites should call [#expandOrCreateChainNode].
    private int createNewChainNode(int transitionByte, int newChild) throws TrieSpaceExhaustedException
    {
        int newNode = allocateCell() + LAST_POINTER_OFFSET - 1;
        putByte(newNode, (byte) transitionByte);
        putInt(newNode + 1, newChild);
        // Note: this does not need a volatile write as it is a new node, returning a new pointer, which needs to be
        // put in an existing node or the root. That action ends in a happens-before enforcing write.
        return newNode;
    }

    /// Like [#createNewChainNode], but if the new child is already a chain node and has room, expand
    /// it instead of creating a brand new node.
    protected int expandOrCreateChainNode(int transitionByte, int newChild) throws TrieSpaceExhaustedException
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

    private int createEmptySplitNode() throws TrieSpaceExhaustedException
    {
        return allocateCell() + SPLIT_OFFSET;
    }

    protected int createContentNode(int contentId, int child, boolean isSafeChain) throws TrieSpaceExhaustedException
    {
        return createPrefixNode(contentId, NONE, child, isSafeChain);
    }

    protected int createPrefixNode(int contentId, int alternateBranch, int child, boolean isSafeChain) throws TrieSpaceExhaustedException
    {
        assert !isLeaf(child) : "Prefix node cannot reference a leaf node.";
        assert !isNull(child) || !isNull(alternateBranch) : "Prefix node can only have a null child if it includes an alternate branch.";

        int offset = offset(child);
        int node;
        if (offset == SPLIT_OFFSET || isSafeChain && offset > (PREFIX_FLAGS_OFFSET + PREFIX_OFFSET) && offset <= CHAIN_MAX_OFFSET)
        {
            // We can do an embedded prefix node
            // Note: for chain nodes we have a risk that the node continues beyond the current point, in which case
            // creating the embedded node may overwrite information that is still needed by concurrent readers or the
            // mutation process itself.
            node = (child & -CELL_SIZE) | PREFIX_OFFSET;
            putByte(node + PREFIX_FLAGS_OFFSET, (byte) offset);
        }
        else
        {
            // Full prefix node
            node = allocateCell() + PREFIX_OFFSET;
            putByte(node + PREFIX_FLAGS_OFFSET, (byte) 0xFF);
            putInt(node + PREFIX_POINTER_OFFSET, child);
        }

        putInt(node + PREFIX_CONTENT_OFFSET, contentId);
        putInt(node + PREFIX_ALTERNATE_OFFSET, alternateBranch);
        return node;
    }

    private int updatePrefixNodeChild(int node, int child, boolean forcedCopy) throws TrieSpaceExhaustedException
    {
        assert offset(node) == PREFIX_OFFSET : "updatePrefix called on non-prefix node";
        assert !isNullOrLeaf(child) : "Prefix node cannot reference a childless node.";

        // We can only update in-place if we have a full prefix node
        if (!isEmbeddedPrefixNode(node))
        {
            if (!forcedCopy)
            {
                // This attaches the child branch and makes it reachable -- the write must be volatile.
                putIntVolatile(node + PREFIX_POINTER_OFFSET, child);
            }
            else
            {
                node = copyCell(node);
                putInt(node + PREFIX_POINTER_OFFSET, child);
            }
            return node;
        }
        else
        {
            // No need to recycle this cell because that is already done by the modification of the child
            int contentId = getIntVolatile(node + PREFIX_CONTENT_OFFSET);
            int alternateBranch = getIntVolatile(node + PREFIX_ALTERNATE_OFFSET);
            return createPrefixNode(contentId, alternateBranch, child, true);
        }
    }

    protected boolean isEmbeddedPrefixNode(int node)
    {
        return getUnsignedByte(node + PREFIX_FLAGS_OFFSET) < CELL_SIZE;
    }

    /// Copy the content from an existing node, if it has any, to a newly-prepared update for its child.
    ///
    /// @param existingPreContentNode pointer to the existing node before skipping over content nodes, i.e. this is
    ///                               either the same as existingPostContentNode or a pointer to a prefix or leaf node
    ///                               whose child is `existingPostContentNode`
    /// @param existingPostContentNode pointer to the existing node being updated, after any content nodes have been
    ///                                skipped and before any modification have been applied; always a non-content node
    /// @param updatedPostContentNode is the updated node, i.e. the node to which all relevant modifications have been
    ///                               applied; if the modifications were applied in-place, this will be the same as
    ///                               `existingPostContentNode`, otherwise a completely different pointer; always a non-
    ///                               content node
    /// @param forcedCopy whether or not we need to preserve all pre-existing data for concurrent readers
    /// @return a node which has the children of updatedPostContentNode combined with the content of
    ///         `existingPreContentNode`
    private int preservePrefix(int existingPreContentNode,
                               int existingPostContentNode,
                               int updatedPostContentNode,
                               boolean forcedCopy)
    throws TrieSpaceExhaustedException
    {
        if (existingPreContentNode == existingPostContentNode)
            return updatedPostContentNode;     // no content to preserve

        if (existingPostContentNode == updatedPostContentNode)
        {
            assert !forcedCopy;
            return existingPreContentNode;     // child didn't change, no update necessary
        }

        // else we have existing prefix node, and we need to reference a new child
        if (isLeaf(existingPreContentNode))
        {
            return createContentNode(existingPreContentNode, updatedPostContentNode, true);
        }

        assert offset(existingPreContentNode) == PREFIX_OFFSET : "Unexpected content in non-prefix and non-leaf node.";
        if (updatedPostContentNode != NONE || getIntVolatile(existingPreContentNode + PREFIX_ALTERNATE_OFFSET) != NONE)
            return updatePrefixNodeChild(existingPreContentNode, updatedPostContentNode, forcedCopy);
        else
        {
            if (!isEmbeddedPrefixNode(existingPreContentNode))
                recycleCell(existingPreContentNode);
            // otherwise cell is recycled with the post-prefix node
            return getIntVolatile(existingPreContentNode + PREFIX_CONTENT_OFFSET);
        }
    }

    /// Represents the state for an [InMemoryTrie#apply] operation. Contains a stack of all nodes we descended through
    /// and used to update the nodes with any new data during ascent.
    ///
    /// To make this as efficient and GC-friendly as possible, we use an integer array (instead of is an object stack)
    /// and we reuse the same object. The latter is safe because memtable tries cannot be mutated in parallel by
    /// multiple writers.
    static class ApplyState<T>
    {
        static final int STATE_SIZE = 5;

        int[] data = new int[16 * STATE_SIZE];
        int currentDepth = -1;

        Consumer<? super T> droppedContentCallback = null;

        /// Pointer to the existing node before skipping over content nodes, i.e. this is either the same as
        /// existingPostContentNode or a pointer to a prefix or leaf node whose child is `existingPostContentNode`.
        int existingFullNode()
        {
            return data[currentDepth * STATE_SIZE + 0];
        }
        void setExistingFullNode(int value)
        {
            data[currentDepth * STATE_SIZE + 0] = value;
        }
        int existingFullNodeAtDepth(int stackDepth)
        {
            return data[stackDepth * STATE_SIZE + 0];
        }

        /// Pointer to the existing node being updated, after any content nodes have been skipped and before any
        /// modification have been applied. Always a non-content node.
        int existingPostContentNode()
        {
            return data[currentDepth * STATE_SIZE + 1];
        }
        void setExistingPostContentNode(int value)
        {
            data[currentDepth * STATE_SIZE + 1] = value;
        }

        /// The updated node, i.e. the node to which the relevant modifications are being applied. This will change as
        /// children are processed and attached to the node. After all children have been processed, this will contain
        /// the fully updated node (i.e. the union of `existingPostContentNode` and `mutationNode`) without any content,
        /// which will be processed separately and, if necessary, attached ahead of this. If the modifications were
        /// applied in-place, this will be the same as `existingPostContentNode`, otherwise a completely different
        /// pointer. Always a non-content node.
        int updatedPostContentNode()
        {
            return data[currentDepth * STATE_SIZE + 2];
        }
        void setUpdatedPostContentNode(int value)
        {
            data[currentDepth * STATE_SIZE + 2] = value;
        }

        /// The transition we took on the way down.
        int transition()
        {
            return data[currentDepth * STATE_SIZE + 3];
        }
        void setTransition(int transition)
        {
            data[currentDepth * STATE_SIZE + 3] = transition;
        }
        int transitionAtDepth(int stackDepth)
        {
            return data[stackDepth * STATE_SIZE + 3];
        }
        int incomingTransition()
        {
            return transitionAtDepth(currentDepth - 1);
        }

        /// The compiled content id. Needed because we can only access a cursor's content on the way down but we can't
        /// attach it until we ascend from the node.
        int descentPathContentId()
        {
            return data[currentDepth * STATE_SIZE + 4];
        }
        void setDescentPathContentId(int value)
        {
            data[currentDepth * STATE_SIZE + 4] = value;
        }
        int descentPathContentIdAtDepth(int stackDepth)
        {
            return data[stackDepth * STATE_SIZE + 4];
        }

        protected final InMemoryBaseTrie<T> trie;

        ApplyState(InMemoryBaseTrie<T> trie)
        {
            this.trie = trie;
        }

        ApplyState<T> start(int root, Consumer<? super T> droppedContentCallback)
        {
            this.currentDepth = -1;
            this.droppedContentCallback = droppedContentCallback;
            descendInto(root);
            return this;
        }

        /// Advance to the given depth and transition. Returns false if the depth signals mutation cursor is exhausted.
        boolean advanceTo(int depth, int transition, int forcedCopyDepth) throws TrieSpaceExhaustedException
        {
            return advanceTo(depth, transition, forcedCopyDepth, 0);
        }
        /// Advance to the given depth and transition. Returns false if the depth signals mutation cursor is exhausted.
        boolean advanceTo(int depth, int transition, int forcedCopyDepth, int ascendLimit) throws TrieSpaceExhaustedException
        {
            while (currentDepth >= Math.max(ascendLimit + 1, depth))
            {
                // There are no more children. Ascend to the parent state to continue walk.
                attachAndMoveToParentState(forcedCopyDepth);
            }
            if (depth <= ascendLimit)
                return false;

            // We have a transition, get child to descend into
            descend(transition);
            return true;
        }

        /// Advance to an existing position in the trie or the given limit, whichever comes first.
        /// If there is an existing position before this limit, the state will be positioned on it, and true will be
        /// returned. If not, we will advance and descend into the given limit position, and return false.
        ///
        /// The `limitDepth`, `limitTransition` and `limitIsOnReturnPath` parameters specify the limit position. This
        /// must be a valid non-exhausted position.
        boolean advanceToNextExistingOr(int limitDepth,
                                        int limitTransition,
                                        boolean limitIsOnReturnPath,
                                        int forcedCopyDepth,
                                        int ascendLimit)
        throws TrieSpaceExhaustedException
        {
            assert limitDepth >= ascendLimit;
            while (true)
            {
                int currentTransition = transition();
                int nextTransition = trie.getNextTransition(updatedPostContentNode(), currentTransition + 1);
                if (currentDepth + 1 == limitDepth && nextTransition >= limitTransition)
                {
                    descend(limitTransition);
                    return false;
                }
                if (nextTransition <= 0xFF)
                {
                    descend(nextTransition);
                    return true;
                }

                if (limitIsOnReturnPath && currentDepth == limitDepth &&
                    (limitDepth == ascendLimit || transitionAtDepth(currentDepth - 1) == limitTransition))
                    return false;

                attachAndMoveToParentState(forcedCopyDepth);
            }
        }

        /// Advance to the next existing position in the trie.
        boolean advanceToNextExisting(int forcedCopyDepth, int ascendLimit)
        throws TrieSpaceExhaustedException
        {
            while (true)
            {
                int currentTransition = transition();
                int nextTransition = trie.getNextTransition(updatedPostContentNode(), currentTransition + 1);
                if (nextTransition <= 0xFF)
                {
                    descend(nextTransition);
                    return true;
                }

                if (currentDepth <= ascendLimit)
                    return false;

                attachAndMoveToParentState(forcedCopyDepth);
            }
        }

        /// Descend to a child node. Prepares a new entry in the stack for the node.
        void descend(int transition)
        {
            setTransition(transition);
            int existingFullNode = trie.getChild(updatedPostContentNode(), transition);

            descendInto(existingFullNode);
        }

        private void descendInto(int existingFullNode)
        {
            ++currentDepth;
            if (currentDepth * STATE_SIZE >= data.length)
                data = Arrays.copyOf(data, currentDepth * STATE_SIZE * 2);
            setExistingFullNode(existingFullNode);

            int existingContentId = NONE;
            int existingPostContentNode;
            if (isLeaf(existingFullNode))
            {
                existingContentId = trie.shouldPresentAfterBranch(existingFullNode) ? NONE : existingFullNode;
                existingPostContentNode = NONE;
            }
            else if (offset(existingFullNode) == PREFIX_OFFSET)
            {
                existingContentId = trie.getIntVolatile(existingFullNode + PREFIX_CONTENT_OFFSET);
                existingPostContentNode = trie.followPrefixTransition(existingFullNode);
            }
            else
                existingPostContentNode = existingFullNode;

            setExistingPostContentNode(existingPostContentNode);
            setUpdatedPostContentNode(existingPostContentNode);
            setDescentPathContentId(existingContentId);
            setTransition(-1);
        }

        T getDescentPathContent()
        {
            int contentId = descentPathContentId();
            if (contentId == NONE)
                return null;
            return trie.getContent(descentPathContentId());
        }

        void setDescentPathContent(T content, boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            setDescentPathContentId(combineContent(descentPathContentId(), content, false, forcedCopy));
        }

        int combineContent(int existingContentId, T newContent, boolean contentAfterBranch, boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            if (existingContentId == NONE)
            {
                assert (newContent != null); // combineContent cannot be called if new is the same as existing
                return trie.addContent(newContent, contentAfterBranch);
            }
            else if (newContent == null)
            {
                trie.releaseContent(existingContentId);
                return NONE;
            }
            else if (forcedCopy)
            {
                trie.releaseContent(existingContentId);
                return trie.addContent(newContent, contentAfterBranch);
            }
            else
            {
                return trie.setContent(existingContentId, newContent);
            }
        }

        /// Attach a child to the current node.
        private void attachChild(int transition, int child, boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            int updatedPostContentNode = updatedPostContentNode();
            if (isNull(updatedPostContentNode))
                setUpdatedPostContentNode(trie.expandOrCreateChainNode(transition, child));
            else if (forcedCopy)
                setUpdatedPostContentNode(trie.attachChildCopying(updatedPostContentNode,
                                                             existingPostContentNode(),
                                                             transition,
                                                             child));
            else
                setUpdatedPostContentNode(trie.attachChild(updatedPostContentNode,
                                                                            transition,
                                                                            child));
        }

        /// Apply the collected content to a node. If there is content to add, converts `NONE` to a leaf node, and adds
        /// or updates a prefix for all others.
        protected int applyContent(boolean forcedCopy)
        throws TrieSpaceExhaustedException
        {
            // Note: the old content id itself is already released by setContent. Here we must release any standalone
            // prefix nodes that may reference it.
            int contentId = descentPathContentId();
            final int updatedPostContentNode = updatedPostContentNode();
            final int existingPreContentNode = existingFullNode();
            final int existingPostContentNode = existingPostContentNode();

            // applyPrefixChange does not understand leaf nodes, handle upgrade from and to one explicitly.
            if (isNull(updatedPostContentNode))
            {
                // This node has no children. If the content is metadata that has no meaning if no children exist,
                // remove it.
                if (!isNull(contentId) && !trie.shouldPreserveWithoutChildren(contentId))
                {
                    if (droppedContentCallback != null)
                        droppedContentCallback.accept(trie.getContent(contentId));
                    trie.releaseContent(contentId);
                    contentId = NONE;
                }

                if (existingPreContentNode != existingPostContentNode
                    && !isNullOrLeaf(existingPreContentNode)
                    && !trie.isEmbeddedPrefixNode(existingPreContentNode))
                    trie.recycleCell(existingPreContentNode);
                return contentId;   // also fine for contentId == NONE
            }

            if (isLeaf(existingPreContentNode))
                return contentId != NONE
                       ? trie.createContentNode(contentId, updatedPostContentNode, true)
                       : updatedPostContentNode;

            return applyPrefixChange(updatedPostContentNode,
                                     existingPreContentNode,
                                     existingPostContentNode,
                                     contentId,
                                     NONE,
                                     forcedCopy);
        }

        protected int applyPrefixChange(int updatedPostPrefixNode,
                                        int existingPrePrefixNode,
                                        int existingPostPrefixNode,
                                        int contentId,
                                        int alternateBranch,
                                        boolean forcedCopy)
        throws TrieSpaceExhaustedException
        {
            boolean prefixWasPresent = existingPrePrefixNode != existingPostPrefixNode;
            boolean prefixWasEmbedded = prefixWasPresent && trie.isEmbeddedPrefixNode(existingPrePrefixNode);
            if (contentId == NONE && alternateBranch == NONE)
            {
                if (prefixWasPresent && !prefixWasEmbedded)
                    trie.recycleCell(existingPrePrefixNode);
                return updatedPostPrefixNode;
            }

            boolean childChanged = updatedPostPrefixNode != existingPostPrefixNode;
            boolean dataChanged = !prefixWasPresent || contentId != trie.getIntVolatile(existingPrePrefixNode + PREFIX_CONTENT_OFFSET)
                    || alternateBranch != trie.getIntVolatile(existingPrePrefixNode + PREFIX_ALTERNATE_OFFSET);
            if (!childChanged && !dataChanged)
                return existingPrePrefixNode;

            if (forcedCopy)
            {
                if (!childChanged && prefixWasEmbedded)
                {
                    // If we directly create in this case, we will find embedding is possible and will overwrite the
                    // previous value.
                    // We could create a separate metadata node referencing the child, but in that case we'll
                    // use two nodes while one suffices. Instead, copy the child and embed the new metadata.
                    updatedPostPrefixNode = trie.copyCell(existingPostPrefixNode);
                }
                else if (prefixWasPresent && !prefixWasEmbedded)
                {
                    trie.recycleCell(existingPrePrefixNode);
                    // otherwise cell is already recycled by the recycling of the child
                }
                return trie.createPrefixNode(contentId, alternateBranch, updatedPostPrefixNode, isNull(existingPostPrefixNode));
            }

            // We can't update in-place if there was no preexisting prefix, or if the
            // prefix was embedded and the target node must change.
            if (!prefixWasPresent || prefixWasEmbedded && childChanged)
                return trie.createPrefixNode(contentId, alternateBranch, updatedPostPrefixNode, isNull(existingPostPrefixNode));

            // Otherwise modify in place
            if (childChanged) // to use volatile write but also ensure we don't corrupt embedded nodes
                trie.putIntVolatile(existingPrePrefixNode + PREFIX_POINTER_OFFSET, updatedPostPrefixNode);
            if (dataChanged)
            {
                trie.putIntVolatile(existingPrePrefixNode + PREFIX_CONTENT_OFFSET, contentId);
                trie.putIntVolatile(existingPrePrefixNode + PREFIX_ALTERNATE_OFFSET, alternateBranch);
            }
            return existingPrePrefixNode;
        }

        /// After a node's children are processed, this is called to ascend from it. This means applying the collected
        /// content to the compiled `updatedPostContentNode` and creating a mapping in the parent to it (or updating if
        /// one already exists).
        void attachAndMoveToParentState(int forcedCopyDepth) throws TrieSpaceExhaustedException
        {
            attachBranchAndMoveToParentState(applyContent(currentDepth >= forcedCopyDepth), forcedCopyDepth);
        }

        void attachBranchAndMoveToParentState(int updatedFullNode, int forcedCopyDepth) throws TrieSpaceExhaustedException {
            int existingFullNode = existingFullNode();
            --currentDepth;
            assert currentDepth >= 0;

            if (updatedFullNode != existingFullNode)
                attachChild(transition(), updatedFullNode, currentDepth >= forcedCopyDepth);
        }

        /// Ascend and update the root at the end of processing.
        void attachAndUpdateRoot(int forcedCopyDepth) throws TrieSpaceExhaustedException
        {
            attachRoot(applyContent(0 >= forcedCopyDepth), forcedCopyDepth);
        }

        void attachRoot(int updatedFullNode, int ignoredForcedCopyDepth)
        {
            int existingFullNode = existingFullNode();
            --currentDepth;
            assert trie.root == existingFullNode : "Unexpected change to root. Concurrent trie modification?";
            if (updatedFullNode != existingFullNode)
            {
                // Only write to root if they are different (value doesn't change, but
                // we don't want to invalidate the value in other cores' caches unnecessarily).
                trie.root = updatedFullNode;
            }
        }

        void prepareToWalkBranchAgain()
        {
            setTransition(-1);
        }

        public byte[] getBytes(int startDepth)
        {
            Preconditions.checkArgument(startDepth >= 0 && startDepth <= currentDepth);
            int arrSize = currentDepth - startDepth;
            byte[] data = new byte[arrSize];
            int pos = 0;
            for (int i = startDepth; i < currentDepth; ++i)
            {
                int trans = transitionAtDepth(i);
                data[pos++] = (byte) trans;
            }
            return data;
        }

        public int getNearestAncestorDepthSatisfying(Predicate<T> shouldStop)
        {
            return getNearestAncestorDepthSatisfying(shouldStop, currentDepth - 1);
        }

        public int getNearestAncestorDepthSatisfying(Predicate<T> shouldStop, int startDepth)
        {
            int i;
            for (i = startDepth; i >= 0; --i)
            {
                int content = descentPathContentIdAtDepth(i);
                if (!isNull(content) && shouldStop.test(trie.getContent(content)))
                    return i;
            }
            return -1;
        }

        public ByteComparable.Version byteComparableVersion()
        {
            return trie.byteComparableVersion;
        }

        public String toString()
        {
            if (data == null)
                return "uninitialized";
            StringBuilder sb = new StringBuilder();
            sb.append('@');
            for (int i = 0; i < currentDepth; ++i)
                sb.append(String.format("%02x", transitionAtDepth(i)));

            sb.append(" existingPostContentNode=").append(existingPostContentNode());
            sb.append(" updatedPostContentNode=").append(updatedPostContentNode());
            sb.append(" descentPathContentId=").append(descentPathContentId());
            return sb.toString();
        }

        public InMemoryBaseTrie<T> trie()
        {
            return trie;
        }
    }


    /// Somewhat similar to [Trie.MergeResolver], this encapsulates logic to be applied whenever new content is
    /// being upserted into a [InMemoryBaseTrie]. Unlike [Trie.MergeResolver], [UpsertTransformer] will be
    /// applied no matter if there's pre-existing content for that trie key/path or not.
    ///
    /// @param <T> The content type for this [InMemoryBaseTrie].
    /// @param <U> The type of the new content being applied to this [InMemoryBaseTrie].
    public interface UpsertTransformer<T, U>
    {
        /// Called when there's content in the updating trie.
        ///
        /// @param existing Existing content for this key, or null if there isn't any.
        /// @param update   The update, always non-null.
        /// @return The combined value to use. A value of null will delete the existing entry.
        T apply(T existing, @Nonnull U update);
    }

    /// Interface providing features of the mutating node during mutation done using [InMemoryTrie#apply].
    /// Effectively a subset of the [Cursor] interface which only permits operations that are safe to
    /// perform before iterating the children of the mutation node to apply the branch mutation.
    ///
    /// This is mainly used as an argument to predicates that decide when to copy substructure when modifying tries,
    /// which enables different kinds of atomicity and consistency guarantees.
    ///
    /// See the [InMemoryTrie] javadoc or [InMemoryTrieThreadedTest] for demonstration of the typical usages and what
    /// they achieve.
    public interface NodeFeatures<T>
    {
        /// Whether or not the node has more than one descendant. If a checker needs mutations to be atomic, they can
        /// return true when this becomes true.
        boolean isBranching();

        /// The metadata associated with the node. If readers need to see a consistent view (i.e. where older updates
        /// cannot be missed if a new one is presented) below some specified point (e.g. within a partition), the checker
        /// should return true when it identifies that point.
        T content();
    }

    /// This class includes the common functionality of the various trie mutators.
    ///
    /// Stores the configured transformers and flags of the operations, and can be reused to apply
    /// modifications with the same configuration multiple times.
    ///
    /// The mutator provides some methods that can be called by the given transformer to obtain information about the
    /// state when merging in data.
    protected static class Mutator<T, U, C extends Cursor<U>, A extends ApplyState<T>> implements NodeFeatures<U>
    {
        final UpsertTransformer<T, U> transformer;
        final Predicate<NodeFeatures<U>> needsForcedCopy;
        final Consumer<? super T> droppedContentCallback;
        final A state;

        C mutationCursor;
        int forcedCopyDepth;

        Mutator(UpsertTransformer<T, U> transformer,
                Predicate<NodeFeatures<U>> needsForcedCopy,
                Consumer<? super T> droppedContentCallback,
                A state)
        {
            this.transformer = transformer;
            this.needsForcedCopy = needsForcedCopy;
            this.droppedContentCallback = droppedContentCallback;
            this.state = state;
        }

        Mutator<T, U, C, A> start(int root, C mutationCursor, int initialForcedCopyDepth)
        {
            mutationCursor.assertFresh();

            this.mutationCursor = mutationCursor;
            this.forcedCopyDepth = initialForcedCopyDepth;
            this.state.start(root, droppedContentCallback);
            return this;
        }

        Mutator<T, U, C, A> start(C mutationCursor)
        {
            return start(state.trie.root, mutationCursor, Integer.MAX_VALUE);
        }

        Mutator<T, U, C, A> apply() throws TrieSpaceExhaustedException
        {
            int depth = state.currentDepth;
            long position = mutationCursor.encodedPosition();
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                applyContent((position & Cursor.MAY_HAVE_CONTENT_BIT) != 0 ? mutationCursor.content() : null);

                position = mutationCursor.advance();
                assert !Cursor.isOnReturnPath(position) : "Return path in forward direction can only be used in range tries.";
                depth = Cursor.depth(position);
                if (!state.advanceTo(depth, Cursor.incomingTransition(position), forcedCopyDepth))
                    break;
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
            }
            return this;
        }

        void applyContent(U content) throws TrieSpaceExhaustedException
        {
            if (content != null)
            {
                T existingContent = state.getDescentPathContent();
                T combinedContent = transformer.apply(existingContent, content);
                if (combinedContent != existingContent)
                    state.setDescentPathContent(combinedContent, // can be null
                                                state.currentDepth >= forcedCopyDepth); // this is called at the start of processing
            }
        }


        void complete() throws TrieSpaceExhaustedException
        {
            assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
            state.attachAndUpdateRoot(forcedCopyDepth);
        }

        @Override
        public boolean isBranching()
        {
            if (Cursor.isOnReturnPath(mutationCursor.encodedPosition()))
                return false;

            // This is not very efficient, but we only currently use this option in tests.
            // If it's needed for production use, isBranching should be implemented in the cursor interface.
            Cursor<U> dupe = mutationCursor.tailCursor(Direction.FORWARD);
            long childPosition = dupe.advance();
            return !Cursor.isExhausted(childPosition) &&
                   !Cursor.isExhausted(dupe.skipTo(Cursor.positionForSkippingBranch(childPosition)));
        }

        @Override
        public U content()
        {
            return (mutationCursor.encodedPosition() & Cursor.MAY_HAVE_CONTENT_BIT) != 0 ? mutationCursor.content() : null;
        }

        /// Return the depth of the currently processed node.
        ///
        /// This method may be called by the upsert transformer to get information about the current state.
        public int currentDepth()
        {
            return state.currentDepth;
        }

        /// Get the bytes of the path leading to this node. The returned array can be safely modified and/or stored.
        ///
        /// This method may be called by the upsert transformer to get information about the current state.
        public byte[] getCurrentKeyBytes()
        {
            return getCurrentKeyBytes(0);
        }

        /// Get the bytes of the path leading to this node from the given depth.
        /// The returned array can be safely modified and/or stored.
        ///
        /// This method may be called by the upsert transformer to get information about the current state.
        public byte[] getCurrentKeyBytes(int startDepth)
        {
            return state.getBytes(startDepth);
        }

        /// Get the depth of the nearest ancestor that has content satisfying the given predicate.
        ///
        /// This method may be called by the upsert transformer to get information about the current state.
        public int getNearestAncestorDepthSatisfying(Predicate<T> shouldStop)
        {
            return state.getNearestAncestorDepthSatisfying(shouldStop);
        }

        /// Get the key bytes to the nearest ancestor that has content satisfying the given predicate.
        ///
        /// This method may be called by the upsert transformer to get information about the current state.
        public byte[] getCurrentKeyBytesToNearestAncestorSatisfying(Predicate<T> shouldStop)
        {
            return state.getBytes(Math.max(0, getNearestAncestorDepthSatisfying(shouldStop)));
        }

        public ByteComparable.Version byteComparableVersion()
        {
            return state.byteComparableVersion();
        }
    }

    /// Map-like put method, using a fast recursive implementation through the key bytes. May run into stack overflow if
    /// the trie becomes too deep. When the correct position in the trie has been reached, the value will be resolved
    /// with the given function before being placed in the trie (even if there's no pre-existing content in this trie).
    /// @param key The trie path/key for the given value.
    /// @param value The value being put in the memtable trie. Note that it can be of type different than the element
    /// type for this memtable trie. It's up to the `transformer` to return the final value that will stay in
    /// the memtable trie.
    /// @param transformer A function applied to the potentially pre-existing value for the given key, and the new
    /// value (of a potentially different type), returning the final value that will stay in the memtable trie. Applied
    /// even if there's no pre-existing value in the memtable trie.
    public <R> void putRecursive(ByteComparable key, R value, final UpsertTransformer<T, R> transformer) throws TrieSpaceExhaustedException
    {
        putRecursive(key, value, false, transformer);
    }


    /// Map-like put method, using a fast recursive implementation through the key bytes. May run into stack overflow if
    /// the trie becomes too deep. When the correct position in the trie has been reached, the value will be resolved
    /// with the given function before being placed in the trie (even if there's no pre-existing content in this trie).
    /// @param key The trie path/key for the given value.
    /// @param value The value being put in the memtable trie. Note that it can be of type different than the element
    /// type for this memtable trie. It's up to the `transformer` to return the final value that will stay in
    /// the memtable trie.
    /// @param contentAfterBranch Whether the content should be placed after descendants in forward iteration order.
    /// Setting this to true only makes sense with ordered or range tries (i.e. where [#presentContentOnDescentPath] is
    /// false).
    /// @param transformer A function applied to the potentially pre-existing value for the given key, and the new
    /// value (of a potentially different type), returning the final value that will stay in the memtable trie. Applied
    /// even if there's no pre-existing value in the memtable trie.
    public <R> void putRecursive(ByteComparable key, R value, boolean contentAfterBranch, final UpsertTransformer<T, R> transformer) throws TrieSpaceExhaustedException
    {
        assert !(contentAfterBranch && presentContentOnDescentPath) : "After branch placement only makes sense with range or ordered tries";
        try
        {
            int newRoot = putRecursive(root, key.asComparableBytes(byteComparableVersion), value, contentAfterBranch, transformer);
            if (newRoot != root)
                root = newRoot;
            completeMutation();
        }
        catch (Throwable t)
        {
            abortMutation();
            throw t;
        }
    }

    private <R> int putRecursive(int node, ByteSource key, R value, boolean contentAfterBranch, final UpsertTransformer<T, R> transformer) throws TrieSpaceExhaustedException
    {
        int transition = key.next();
        if (transition == ByteSource.END_OF_STREAM)
            return applyContent(node, value, contentAfterBranch, transformer);

        int child = getChild(node, transition);

        int newChild = putRecursive(child, key, value, contentAfterBranch, transformer);
        if (newChild == child)
            return node;

        int skippedContent = followPrefixTransition(node);
        int attachedChild = !isNull(skippedContent)
                            ? attachChild(skippedContent, transition, newChild)  // Single path, no copying required
                            : expandOrCreateChainNode(transition, newChild);

        return preservePrefix(node, skippedContent, attachedChild, false);
    }

    private <R> int applyContent(int node, R value, boolean contentAfterBranch, UpsertTransformer<T, R> transformer) throws TrieSpaceExhaustedException
    {
        if (isNull(node))
            return addContent(transformer.apply(null, value), contentAfterBranch);
        else if (isLeaf(node))
            return applyContentToLeaf(node, value, contentAfterBranch, transformer);
        else if (offset(node) == PREFIX_OFFSET)
            return applyContentToPrefix(node, value, contentAfterBranch, transformer);
        else
        {
            T newContent = transformer.apply(null, value);
            if (newContent == null)
                return node;
            else
                return createContentNode(addContent(newContent, contentAfterBranch), node, false);
        }
    }

    private <R> int applyContentToLeaf(int node, R value, boolean contentAfterBranch, UpsertTransformer<T, R> transformer) throws TrieSpaceExhaustedException
    {
        int contentId = node;

        if (contentAfterBranch == shouldPresentAfterBranch(node))
        {
            T existingContent = getContent(contentId);
            T newContent = transformer.apply(existingContent, value);

            if (newContent == existingContent)
                return contentId;
            if (newContent != null)
            {
                return setContent(contentId, newContent);
            }
            else
            {
                releaseContent(contentId);
                return NONE;
            }
        }
        else
        {
            T newContent = transformer.apply(null, value);

            // We already have content, but we also need to add content on the other side of the branch.
            if (newContent == null)
                return contentId;

            // Convert this to prefix node to be able to store both.
            return createPrefixNode(contentId, addContent(newContent, contentAfterBranch), NONE, false);
        }
    }

    private <R> int applyContentToPrefix(int node, R value, boolean contentAfterBranch, UpsertTransformer<T, R> transformer) throws TrieSpaceExhaustedException
    {
        int contentOffset = contentAfterBranch ? PREFIX_ALTERNATE_OFFSET : PREFIX_CONTENT_OFFSET;
        int contentId = getIntVolatile(node + contentOffset);

        assert isNullOrLeaf(contentId) : "Content after branch cannot be used toghether with alternate branch";
        T existingContent = isNull(contentId) ? null : getContent(contentId);
        T newContent = transformer.apply(existingContent, value);
        if (newContent == existingContent)
            return node;

        if (newContent != null)
        {
            if (!isNull(contentId))
            {
                int newId = setContent(contentId, newContent);
                if (newId != contentId)
                    putIntVolatile(node + contentOffset, newId);
            }
            else
                putIntVolatile(node + contentOffset, addContent(newContent, contentAfterBranch));
            return node;
        }
        else
        {
            releaseContent(contentId);
            int otherContentOffset = contentAfterBranch ? PREFIX_CONTENT_OFFSET : PREFIX_ALTERNATE_OFFSET;

            if (!isNull(getIntVolatile(node + otherContentOffset)))
            {
                // keep the prefix node for the other content / alternate path
                putIntVolatile(node + contentOffset, NONE);
                return node;
            }

            int b = getUnsignedByte(node + PREFIX_FLAGS_OFFSET);
            if (b < CELL_SIZE)
            {
                // embedded prefix node
                return node - PREFIX_OFFSET + b;
            }
            else
            {
                // separate prefix node. recycle it as it's no longer needed
                recycleCell(node);
                return getIntVolatile(node + PREFIX_POINTER_OFFSET);
            }
        }
    }

    void completeMutation()
    {
        bufferManager.completeMutation();
        contentManager.completeMutation();
    }

    void abortMutation()
    {
        bufferManager.abortMutation();
        contentManager.abortMutation();
    }

    /// Returns true if the allocation threshold has been reached. To be called by the the writing thread (ideally, just
    /// after the write completes). When this returns true, the user should switch to a new trie as soon as feasible.
    ///
    /// The trie expects up to 10% growth above this threshold. Any growth beyond that may be done inefficiently, and
    /// the trie will fail altogether when the size grows beyond 2G - 256 bytes.
    public boolean reachedAllocatedSizeThreshold()
    {
        return bufferManager.reachedAllocatedSizeThreshold();
    }

    protected abstract long emptySizeOnHeap();

    /// Returns the off heap size of the memtable trie itself, not counting any space taken by referenced content, or
    /// any space that has been allocated but is not currently in use (e.g. recycled cells or preallocated buffer).
    /// The latter means we are undercounting the actual usage, but the purpose of this reporting is to decide when
    /// to flush out e.g. a memtable and if we include the unused space we would almost always end up flushing out
    /// immediately after allocating a large buffer and not having a chance to use it. Counting only used space makes it
    /// possible to flush out before making these large allocations.
    public long usedSizeOffHeap()
    {
        return contentManager.usedSizeOffHeap() + bufferManager.usedSizeOffHeap();
    }

    /// Returns the on heap size of the memtable trie itself, not counting any space taken by referenced content, or
    /// any space that has been allocated but is not currently in use (e.g. recycled cells or preallocated buffer).
    /// The latter means we are undercounting the actual usage, but the purpose of this reporting is to decide when
    /// to flush out e.g. a memtable and if we include the unused space we would almost always end up flushing out
    /// immediately after allocating a large buffer and not having a chance to use it. Counting only used space makes it
    /// possible to flush out before making these large allocations.
    public long usedSizeOnHeap()
    {
        return emptySizeOnHeap() +
               contentManager.usedSizeOnHeap() +
               bufferManager.usedSizeOnHeap();
    }

    @VisibleForTesting
    public long usedBufferSpace()
    {
        return bufferManager.usedBufferSpace();
    }

    /// Returns the amount of memory that has been allocated for various buffers but isn't currently in use.
    /// The total on-heap space used by the trie is `usedSizeOnHeap() + unusedReservedOnHeapMemory()`.
    @VisibleForTesting
    public long unusedReservedOnHeapMemory()
    {
        return bufferManager.unusedReservedOnHeapMemory() + contentManager.unusedReservedOnHeapMemory();
    }

    /// Release all recycled content references, including the ones waiting in still incomplete recycling lists.
    /// This is a test method and can cause null pointer exceptions if used on a live trie.
    ///
    /// If similar functionality is required for non-test purposes, a version of this should be developed that only
    /// releases references on barrier-complete lists.
    @VisibleForTesting
    public void releaseReferencesUnsafe()
    {
        contentManager.releaseReferencesUnsafe();
    }

    /**
     * For testing only. Overwrite every buffer that this trie releases on discard.
     */
    @VisibleForTesting
    public void overwriteAllBuffers()
    {
        bufferManager.overwriteAllBuffers();
    }


    /// Returns the number of values in the trie
    public int valuesCount()
    {
        return contentManager.valuesCount();
    }
}
