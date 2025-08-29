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
     * Stores a compression dictionary in the local cache.
     *
     * @param compressionDictionary the compression dictionary to cache
     */
    void add(CompressionDictionary compressionDictionary);

    /**
     * Set the provided dictionary as the current if it is newer.
     * Also adds the dictionary to the cache.
     *
     * @param dictionary the dictionary to potentially set as current, may be null
     */
    void setCurrentIfNewer(@Nullable CompressionDictionary dictionary);
}
