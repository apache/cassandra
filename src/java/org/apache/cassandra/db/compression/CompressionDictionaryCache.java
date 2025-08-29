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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.cassandra.config.DatabaseDescriptor;

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

    private final Cache<CompressionDictionary.DictId, CompressionDictionary> cache;
    private final AtomicReference<CompressionDictionary> currentDictionary = new AtomicReference<>();

    public CompressionDictionaryCache()
    {
        Duration expiryTime = Duration.ofSeconds(DatabaseDescriptor.getCompressionDictionaryCacheExpireSeconds());
        this.cache = Caffeine.newBuilder()
                             .maximumSize(DatabaseDescriptor.getCompressionDictionaryCacheSize())
                             .expireAfterAccess(expiryTime)
                             .removalListener((CompressionDictionary.DictId dictId,
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
        return currentDictionary.get();
    }

    @Nullable
    @Override
    public CompressionDictionary get(CompressionDictionary.DictId dictId)
    {
        return cache.getIfPresent(dictId);
    }

    @Override
    public void add(CompressionDictionary compressionDictionary)
    {
        cache.put(compressionDictionary.identifier(), compressionDictionary);
    }

    @Override
    public void setCurrentIfNewer(@Nullable CompressionDictionary dictionary)
    {
        if (dictionary == null)
            return;

        add(dictionary);
        // Only update the current dictionary if we don't have one or the new one has a higher ID (newer)
        CompressionDictionary current = currentDictionary.get();
        while ((current == null || dictionary.identifier().id > current.identifier().id)
               && !currentDictionary.compareAndSet(current, dictionary))
        {
            current = currentDictionary.get();
        }
    }

    @Override
    public synchronized void close()
    {
        CompressionDictionary dictionary = currentDictionary.get();
        // Close current dictionary
        if (dictionary != null)
        {
            try
            {
                dictionary.close();
            }
            catch (Exception e)
            {
                logger.warn("Failed to close current compression dictionary", e);
            }
        }
        currentDictionary.set(null);

        // Invalidate cache - this will trigger removalListener to close all cached dictionaries
        cache.invalidateAll();
    }
}
