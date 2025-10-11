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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;

/**
 * Manages caching and current dictionary state for compression dictionaries.
 * <p>
 * This class handles:
 * - Local caching of compression dictionaries with automatic cleanup
 * - Managing the current active dictionary for write operations
 * - Thread-safe access to cached dictionaries
 */
public class CompressionDictionaryCache implements ICompressionDictionaryCache
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionDictionaryCache.class);

    private final Cache<DictId, CompressionDictionary> cache;
    private final AtomicReference<DictId> currentDictId = new AtomicReference<>();

    public CompressionDictionaryCache()
    {
        this(DatabaseDescriptor.getCompressionDictionaryCacheSize(), DatabaseDescriptor.getCompressionDictionaryCacheExpireSeconds());
    }

    @VisibleForTesting
    CompressionDictionaryCache(int maximumSize, int expireAfterSeconds)
    {
        this.cache = Caffeine.newBuilder()
                             .maximumSize(maximumSize)
                             .expireAfterAccess(Duration.ofSeconds(expireAfterSeconds))
                             .removalListener((DictId dictId,
                                               CompressionDictionary dictionary,
                                               RemovalCause cause) -> {
                                 // Close dictionary when evicted from cache to free native resources
                                 // SelfRefCounted ensures dictionary won't be actually closed if still referenced by compressors
                                 if (dictionary != null)
                                 {
                                     try
                                     {
                                         dictionary.close();
                                     }
                                     catch (Exception e)
                                     {
                                         logger.warn("Failed to close compression dictionary {}", dictId, e);
                                     }
                                 }
                             })
                             .build();
    }

    @Nullable
    @Override
    public CompressionDictionary getCurrent()
    {
        DictId dictId = currentDictId.get();
        return dictId == null ? null : get(dictId);
    }

    @Nullable
    @Override
    public CompressionDictionary get(DictId dictId)
    {
        return cache.getIfPresent(dictId);
    }

    @Override
    public void add(@Nullable CompressionDictionary compressionDictionary)
    {
        if (compressionDictionary == null)
            return;

        // Only update cache if not already in the cache
        DictId newDictId = compressionDictionary.dictId();
        cache.get(newDictId, id -> compressionDictionary);

        // Update current dictionary if we don't have one or the new one has a higher ID (newer)
        DictId currentId = currentDictId.get();
        while ((currentId == null || newDictId.id > currentId.id)
               && !currentDictId.compareAndSet(currentId, newDictId))
        {
            currentId = currentDictId.get();
        }
    }

    @Override
    public synchronized void close()
    {
        currentDictId.set(null);
        // Invalidate cache will trigger removalListener to close all cached dictionaries, including the currentDictionary
        cache.invalidateAll();
    }
}
