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

import com.google.common.annotations.VisibleForTesting;

import static org.apache.cassandra.db.tries.ContentSerializer.OFFSET_SPECIAL;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.offset;

/// Content manager used for storing data directly in trie cells.
///
/// Relies on a [ContentSerializer] to perform encoding and decoding of the content and refers to the trie's
/// [BufferManager] to manage the cells used for storing the data.
///
/// It also supports "special" values, encoded as negative integers, which are to be directly mapped to objects by the
/// serialized without taking up trie cells.
///
/// Because the trie cells are limited in size (32 bytes), the user must use its own method of handling payloads that
/// don't fit (e.g. deferring to [ContentManagerPojo] to generate negative special ids for larger objects).
class ContentManagerBytes<T> implements ContentManager<T>
{
    private final ContentSerializer<T> serializer;
    private final BufferManager bufferManager;
    private int valuesCount = 0;

    // Leaves have negative pointers. If we mask the sign bit and

    static final int SIGN = 0x80000000;
    static final int MASK_ID_TO_CELL = 0x7FFFFFE0;

    static
    {
        assert offset(specialToContent(0)) == OFFSET_SPECIAL
            : "OFFSET_SPECIAL must be 0x1F";
    }

    public ContentManagerBytes(ContentSerializer<T> serializer, BufferManager bufferManager)
    {
        this.serializer = serializer;
        this.bufferManager = bufferManager;
    }

    static final int contentToSpecial(int contentId)
    {
        return (~contentId) >> 5;
    }

    static final int specialToContent(int specialId)
    {
        assert specialId >= 0;
        // ~ rather than - to permit an id of 0, and it also sets the offset to OFFSET_SPECIAL
        return ~(specialId << 5);
    }

    @Override
    public T getContent(int id)
    {
        int offset = offset(id);
        if (offset == OFFSET_SPECIAL)
            return serializer.special(contentToSpecial(id));
        int cell = id & MASK_ID_TO_CELL;
        return serializer.deserialize(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell), offset);
    }

    @Override
    public boolean shouldPresentAfterBranch(int id)
    {
        int offset = offset(id);
        if (offset == OFFSET_SPECIAL)
            return serializer.shouldPresentSpecialAfterBranch(contentToSpecial(id));
        return serializer.shouldPresentAfterBranch(offset);
    }

    @Override
    public boolean shouldPreserveWithoutChildren(int id)
    {
        int offset = offset(id);
        if (offset == OFFSET_SPECIAL)
            return serializer.shouldPreserveSpecialWithoutChildren(contentToSpecial(id));
        if (serializer.shouldPreserveWithoutChildren(offset))
            return true;
        int cell = id & MASK_ID_TO_CELL;
        return serializer.shouldPreserveWithoutChildren(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell), offset);
    }

    @Override
    public int addContent(T value, boolean contentAfterBranch) throws TrieSpaceExhaustedException
    {
        ++valuesCount;
        int idIfSpecial = serializer.idIfSpecial(value, contentAfterBranch);
        if (idIfSpecial >= 0)
        {
            int contentId = specialToContent(idIfSpecial);
            // Because we encode specials using a cell offset, it is possible to exhaust the possible number of special
            // ids encoded. Verify this is not the case.
            if (contentToSpecial(contentId) != idIfSpecial)
                throw new TrieSpaceExhaustedException();
            return contentId; // special value
        }

        int cell = bufferManager.allocateCell();
        int offset = serializer.serialize(value, contentAfterBranch, bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell));
        return cell | offset | SIGN;
    }

    @Override
    public int setContent(int id, T value) throws TrieSpaceExhaustedException
    {
        int offset = offset(id);
        // check if we are switching from a special
        if (offset == OFFSET_SPECIAL)
        {
            int specialId = contentToSpecial(id);
            serializer.releaseSpecial(specialId);
            --valuesCount; // compensate for +1 in addContent
            return addContent(value, serializer.shouldPresentSpecialAfterBranch(specialId));
        }

        // Check if we need to switch to a special
        boolean afterBranch = serializer.shouldPresentAfterBranch(offset);
        int special = serializer.idIfSpecial(value, afterBranch);
        int cell = id & MASK_ID_TO_CELL;
        if (special >= 0)
        {
            if (serializer.releaseNeeded(offset))
                serializer.release(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell), offset);
            bufferManager.recycleCell(cell);
            return specialToContent(special);
        }

        int newOffset = serializer.updateInPlace(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell), offset, value);
        return cell | SIGN | newOffset;
    }

    @Override
    public void releaseContent(int id)
    {
        --valuesCount;
        int offset = offset(id);
        if (offset == OFFSET_SPECIAL)
        {
            serializer.releaseSpecial(contentToSpecial(id));
            return;
        }

        int cell = id & MASK_ID_TO_CELL;
        if (serializer.releaseNeeded(offset))
            serializer.release(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell), offset);
        bufferManager.recycleCell(cell);
    }

    @Override
    public void completeMutation()
    {
        serializer.completeMutation();
    }

    @Override
    public void abortMutation()
    {
        serializer.abortMutation();
    }

    @Override
    public String dumpContentId(int id)
    {
        int offset = offset(id);
        if (offset == OFFSET_SPECIAL)
            return serializer.dumpSpecial(contentToSpecial(id));

        int cell = id & MASK_ID_TO_CELL;
        return serializer.dumpContent(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell), offset);
    }

    @Override
    public int cellOrObjectSlotUsed(int id)
    {
        return offset(id) == OFFSET_SPECIAL ? -1 : (id & MASK_ID_TO_CELL);
    }

    @Override
    public long usedSizeOnHeap()
    {
        // serializer may store large blobs outside our buffers
        return serializer.usedSizeOnHeap();
    }

    @Override
    public long usedSizeOffHeap()
    {
        // serializer may store large blobs outside our buffers
        return serializer.usedSizeOffHeap();
    }

    @Override
    @VisibleForTesting
    public long unusedReservedOnHeapMemory()
    {
        return serializer.unusedReservedOnHeapMemory();
    }

    @Override
    @VisibleForTesting
    public void releaseReferencesUnsafe()
    {
        serializer.releaseReferencesUnsafe();
    }

    @Override
    public int valuesCount()
    {
        return valuesCount;
    }
}
