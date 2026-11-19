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

import org.agrona.concurrent.UnsafeBuffer;

/// Object serialization used by [ContentManagerBytes]. Defines how the objects are store in trie cells, when they
/// are mapped to special ids, and the special id mapping itself.
public interface ContentSerializer<T> extends MemoryManager
{
    int OFFSET_SPECIAL = InMemoryReadTrie.offset(~0);

    /// Returns a negative special id if the given content should be stored as a special value, 0 or larger if the value
    /// should be serialized.
    int idIfSpecial(T content, boolean shouldPresentAfterBranch);

    /// Store the given content in the 32-byte cell at the given `inBufferPos` in `buffer`.
    /// Returns the offsetBits to add to the id of the leaf.
    int serialize(T content, boolean shouldPresentAfterBranch, UnsafeBuffer buffer, int inBufferPos) throws TrieSpaceExhaustedException;

    /// Returns the value associated with the given special id.
    T special(int id);

    /// Load the content from the 32-byte cell at the given `inBufferPos` in `buffer`.
    T deserialize(UnsafeBuffer buffer, int inBufferPos, int offsetBits);

    /// Update the value at the given `inBufferPos` in `buffer` if possible.
    /// If the call successfully update the value, it must return true. Otherwise, the value is stored in a different
    /// cell/id and this one is released.
    int updateInPlace(UnsafeBuffer buffer, int inBufferPos, int offsetBits, T newContent) throws TrieSpaceExhaustedException;

    /// Prepare the given special id for recycling.
    void releaseSpecial(int id);

    /// Should return true if this serializer needs to recycle external data for serialized content,
    /// i.e. if [#release] must be called in addition to recycling the cell when the associated content is no longer in
    /// use.
    boolean releaseNeeded(int offsetBits);

    /// Prepare the external content in the given cell for recycling. Called only if [#releaseNeeded] returns true.
    void release(UnsafeBuffer buffer, int inBufferPos, int offsetBits);

    /// See [ContentManager#shouldPreserveWithoutChildren].
    boolean shouldPreserveSpecialWithoutChildren(int id);

    /// See [ContentManager#shouldPreserveWithoutChildren].
    boolean shouldPreserveWithoutChildren(int offsetBits);

    /// See [ContentManager#shouldPreserveWithoutChildren].
    boolean shouldPreserveWithoutChildren(UnsafeBuffer buffer, int inBufferPos, int offsetBits);

    /// Whether the content with this special id should be presented before or after its branch.
    boolean shouldPresentSpecialAfterBranch(int id);

    /// Whether the content in this cell should be presented before or after its branch.
    ///
    /// Because this is used separately from getting and working with the content, reading the cell content is not
    /// acceptable from performance point of view. This information needs to be part of the offset.
    boolean shouldPresentAfterBranch(int offsetBits);

    /// Release all external references held. See [ContentManager#releaseReferencesUnsafe].
    @VisibleForTesting
    void releaseReferencesUnsafe();

    /// Make a string representation of the given id for debugging.
    String dumpSpecial(int id);

    /// Make a string representation of the given cell for debugging.
    String dumpContent(UnsafeBuffer buffer, int inBufferPos, int offsetBits);
}
