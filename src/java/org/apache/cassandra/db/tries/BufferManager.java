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

import org.apache.cassandra.io.compress.BufferType;

/// Buffer-managing component of in-memory tries. Deals with the allocation, access and
/// recycling of trie cells.
public interface BufferManager extends MemoryManager, BufferAccessor
{
    /// Allocate a cell to use for storing data. This uses the memory allocation strategy to reuse cells if any are
    /// available, or to allocate new cells. Because some node types rely on cells being filled with 0 as initial state,
    /// any cell we get through the allocator must also be cleaned.
    int allocateCell() throws TrieSpaceExhaustedException;

    /// Creates a copy of a given cell and marks the original for recycling. Used when a mutation needs to force-copy
    /// paths to ensure earlier states are still available for concurrent readers.
    int copyCell(int cell) throws TrieSpaceExhaustedException;

    /// Prepare the given cell for recycling. The cell cannot be immediately recycled,
    /// because read operations as well as the ongoing mutation may still need it.
    void recycleCell(int cell);

    /// Returns true if the allocation threshold has been reached. To be called by the mutating thread (ideally, just
    /// after the write completes). When this returns true, the user should switch to a new trie as soon as feasible.
    ///
    /// The trie expects up to 10% growth above this threshold. Any growth beyond that may be done inefficiently, and
    /// the trie will fail altogether when the size grows beyond 2G - 256 bytes.
    boolean reachedAllocatedSizeThreshold();

    /// Returns the amount of memory in use in buffers, excluding any cells that are marked for recycling.
    @VisibleForTesting
    long usedBufferSpace();

    /// Called to clean up all buffers when the trie is known to no longer be needed.
    void discardBuffers();

    BufferType bufferType();

    /**
     * For testing only. Overwrite every buffer that this trie releases on discard.
     */
    @VisibleForTesting
    void overwriteAllBuffers();
}
