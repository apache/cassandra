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
     * itself (when it populates the cache) or the instance already cached for the same id (when a concurrent
     * add won the race). Callers that go on to reference the dictionary MUST use the returned instance rather
     * than their argument — referencing a redundant "loser" instance would lazily create a selfRef the cache
     * never owns and therefore never releases, leaking it (CASSANDRA-21047).
     *
     * @param compressionDictionary the compression dictionary to cache, may be null
     * @return the canonical cached dictionary for this id, or null if the argument was null
     */
    CompressionDictionary add(@Nullable CompressionDictionary compressionDictionary);

    /**
     * Gives number of bytes cached compression dictionaries occupy in this cache.
     *
     * @return number of bytes cached dictionaries occupy
     */
    long cachedDictionariesMemoryUsed();
}
