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

package org.apache.cassandra.db.compression;

import javax.annotation.Nullable;

import org.apache.cassandra.utils.concurrent.Ref;

/**
 * Interface for managing compression dictionary caching and current dictionary state.
 * <p>
 * Implementations handle:
 * - Local caching of compression dictionaries with automatic cleanup
 * - Managing the current active dictionary for write operations
 * - Thread-safe access to cached dictionaries
 */
public interface ICompressionDictionaryCache extends AutoCloseable
{
    /**
     * Gets the current active compression dictionary.
     *
     * @return the current compression dictionary, or null if no dictionary is available
     */
    @Nullable
    CompressionDictionary getCurrent();

    /**
     * References the current dictionary for the caller, which owns the returned reference and has to release it.
     * The instance {@link #getCurrent()} returns is not referenced, so once it is not current anymore, it can be
     * released before the caller references it. This method references it while it is current.
     *
     * @return reference to the current compression dictionary, or null if there is none
     */
    @Nullable
    Ref<? extends CompressionDictionary> refCurrent();

    /**
     * Retrieves a specific compression dictionary by its identifier.
     *
     * @param dictId the dictionary identifier to look up
     * @return the compression dictionary with the given identifier, or null if not found in cache
     */
    @Nullable
    CompressionDictionary get(CompressionDictionary.DictId dictId);

    /**
     * Stores a compression dictionary in the local cache and updates the current dictionary if the new one is newer.
     * <p>
     * Returns the CANONICAL cached instance for this dictionary's id: either {@code compressionDictionary}
     * itself (when it populates the cache), a fresh copy of it (when it populates the cache but already has
     * a self-reference, e.g. it was cached and evicted before) or the instance already cached for the same id
     * (when a concurrent add won the race). Callers that go on to reference the dictionary MUST use the returned
     * instance rather than their argument — referencing a redundant "loser" instance would lazily create a selfRef
     * the cache never owns and therefore never releases, leaking it (CASSANDRA-21047).
     *
     * @param compressionDictionary the compression dictionary to cache, may be null
     * @return the canonical cached dictionary for this id, or null if the argument was null
     */
    CompressionDictionary add(@Nullable CompressionDictionary compressionDictionary);

    /**
     * Stores a compression dictionary in the local cache without making it the current dictionary, so it serves
     * reading only. This is the case of a dictionary of a sstable: the sstable does not tell whether this node is
     * meant to compress with it, e.g. it is local to another node and the sstable was streamed or imported from there.
     * <p>
     * Returns the CANONICAL cached instance for this dictionary's id, see {@link #add(CompressionDictionary)}.
     * It is not referenced for the caller, see {@link #refForReading(CompressionDictionary)}.
     *
     * @param compressionDictionary the compression dictionary to cache, may be null
     * @return the canonical cached dictionary for this id, or null if the argument was null
     */
    CompressionDictionary addForReading(@Nullable CompressionDictionary compressionDictionary);

    /**
     * Caches a compression dictionary like {@link #addForReading(CompressionDictionary)} and references it for the
     * caller, which owns the returned reference and has to release it. The instance {@code addForReading} returns is
     * not referenced, so an eviction can release it before the caller references it. This method does not fail that
     * way: when the cached instance was released meanwhile, a fresh instance, which is not cached, is referenced.
     *
     * @param compressionDictionary the compression dictionary to cache
     * @return reference to the canonical cached dictionary for this id, or to a fresh instance of it
     */
    Ref<? extends CompressionDictionary> refForReading(CompressionDictionary compressionDictionary);

    /**
     * Stores a compression dictionary in the local cache and makes it the current dictionary regardless of its id,
     * so it can also go back to an older dictionary, e.g. when a newer dictionary local to this node was discarded.
     *
     * @param compressionDictionary the compression dictionary to make current, null leaves no current dictionary
     */
    void setCurrent(@Nullable CompressionDictionary compressionDictionary);

    /**
     * Gives number of bytes cached compression dictionaries occupy in this cache.
     *
     * @return number of bytes cached dictionaries occupy
     */
    long cachedDictionariesMemoryUsed();
}
