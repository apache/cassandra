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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.utils.concurrent.Ref;

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
                                 // Release the cache's reference to the dictionary when evicted
                                 // The dictionary will only be truly cleaned up when all references are released
                                 if (dictionary != null)
                                 {
                                     try
                                     {
                                         // The dictionary's selfRef should never be null when evicting from cache
                                         // but using close() to have better resiliency
                                         dictionary.close();
                                     }
                                     catch (Exception e)
                                     {
                                         logger.warn("Failed to release compression dictionary {}", dictId, e);
                                     }
                                 }
                             })
                             .executor(ImmediateExecutor.INSTANCE)
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
        cache.get(newDictId, id -> {
            Ref<?> ref = compressionDictionary.initRefLazily();
            if (ref == null)
            {
                throw new IllegalStateException("Failed to acquire reference to compression dictionary");
            }
            return compressionDictionary;
        });

        // Update current dictionary if we don't have one or the new one has a higher ID (newer)
        DictId currentId = currentDictId.get();
        while ((currentId == null || newDictId.id > currentId.id)
               && !currentDictId.compareAndSet(currentId, newDictId))
        {
            currentId = currentDictId.get();
        }
    }

    @Override
    public long cachedDictionariesMemoryUsed()
    {
        AtomicInteger value = new AtomicInteger();
        cache.asMap().forEach((key, dict) -> value.addAndGet(dict.estimatedOccupiedMemoryBytes()));
        return value.get();
    }

    @Override
    public synchronized void close()
    {
        currentDictId.set(null);
        // Invalidate cache will trigger removalListener to close all cached dictionaries, including the currentDictionary
        cache.invalidateAll();
        // Force synchronous cleanup to ensure removal listener executes immediately
        cache.cleanUp();
    }
}
