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

/// Base class for the buffer and content managers of in-memory tries.
public interface MemoryManager
{
    /// To be called when a mutation completes. No new readers must be able to see recycled content at the time of this
    /// call (the paths for reaching them must have been overwritten via a volatile write; additionally, if the buffer
    /// has grown, the root variable (which is stored outside the buffer) must have accepted a volatile write).
    /// No recycled indexes can be made available for reuse before this is called, and before any readers started before
    /// this call have completed.
    void completeMutation();

    /// Called when a mutation is aborted because of an exception. This means that the indexes that were marked for
    /// recycling are still going to be in use (unless this is called, a later separate `completeMutation` call may
    /// release and reuse them, causing corruption).
    ///
    /// Aborted mutations are not normal, and at this time we are not trying to ensure that a trie will behave at its
    /// best if an abort has taken place (i.e. it may take more space, be slower etc.), but it should still operate
    /// correctly.
    void abortMutation();

    /// Returns the off heap size of the memtable trie itself, not counting any space taken by referenced content, or
    /// any space that has been allocated but is not currently in use (e.g. recycled cells or preallocated buffer).
    /// The latter means we are undercounting the actual usage, but the purpose of this reporting is to decide when
    /// to flush out e.g. a memtable and if we include the unused space we would almost always end up flushing out
    /// immediately after allocating a large buffer and not having a chance to use it. Counting only used space makes it
    /// possible to flush out before making these large allocations.
    long usedSizeOffHeap();

    /// Returns the on heap size of the memtable trie itself, not counting any space taken by referenced content, or
    /// any space that has been allocated but is not currently in use (e.g. recycled cells or preallocated buffer).
    /// The latter means we are undercounting the actual usage, but the purpose of this reporting is to decide when
    /// to flush out e.g. a memtable and if we include the unused space we would almost always end up flushing out
    /// immediately after allocating a large buffer and not having a chance to use it. Counting only used space makes it
    /// possible to flush out before making these large allocations.
    long usedSizeOnHeap();

    /// Returns the amount of memory that has been allocated for various buffers but isn't currently in use.
    /// The total on-heap space used by the trie is `usedSizeOnHeap() + unusedReservedOnHeapMemory()`.
    @VisibleForTesting
    long unusedReservedOnHeapMemory();
}
