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

/// Content manager for in-memory tries. Deals with allocation, access and recycling of trie content, mapping objects
/// to and from leaf pointers in the trie.
public interface ContentManager<T> extends MemoryManager
{
    /// Add a new content value.
    ///
    /// @param value The value to add.
    /// @param contentAfterBranch Whether the content should be understood to reside after the branch, i.e. it is to be
    ///                           returned on the ascent path of the cursor walk.
    /// @return A content id that can be used to reference the content. This id must be interpreted as a leaf by the
    ///         trie code (i.e. it must either be negative or a valid pointer to a content cell).
    int addContent(T value, boolean contentAfterBranch) throws TrieSpaceExhaustedException;

    /// Change the content associated with a given content id.
    ///
    /// @param id Encoded content id, returned from a previous call to [#addContent] or [#setContent].
    /// @param value New content value to store.
    /// @return The id to use for the modified content; an attempt will be made to make this the same as `id`, but not
    ///         all content managers will be able to freely modify the data for a given id.
    ///         Implementations must ensure that if the id changes, the previous id is released.
    int setContent(int id, T value) throws TrieSpaceExhaustedException;

    /// Prepare the given content id for recycling. The id cannot be immediately recycled,
    /// because read operations as well as the ongoing mutation may still need it.
    void releaseContent(int id);

    /// Get the content for the given content pointer.
    ///
    /// @param id content pointer, returned by a previous call to [#addContent].
    /// @return the current content value.
    T getContent(int id);

    /// Returns false if the given contentId should be presented before the children of the branch in forward direction,
    /// and true if it should be presented after them.
    boolean shouldPresentAfterBranch(int contentId);

    /// This is called when content is left without children and is used to remove dangling metadata
    /// or markers for branches (e.g. rows) that have become empty.
    ///
    /// If it returns false, the content will be dropped when its branch becomes empty.
    boolean shouldPreserveWithoutChildren(int contentId);

    /// Make a textual representation of the id for debugging.
    String dumpContentId(int id);

    /// If the content manager uses trie cells, this must return the cell corresponding to the given id. If not, it
    /// should return ~idx where idx is the index of the object slot used to store the object.
    int cellOrObjectSlotUsed(int id);

    /// Release all recycled content references, including the ones waiting in still incomplete recycling lists.
    /// This is a test method and can cause null pointer exceptions if used on a live trie.
    ///
    /// If similar functionality is required for non-test purposes, a version of this should be developed that only
    /// releases references on barrier-complete lists.
    @VisibleForTesting
    void releaseReferencesUnsafe();

    /// Returns the number of values in the trie
    int valuesCount();
}
