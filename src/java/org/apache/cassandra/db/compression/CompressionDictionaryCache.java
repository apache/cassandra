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
    // the cache holds a reference of its own to the current dictionary, so its eviction or expiry from the cache
    // does not release it
    private volatile CompressionDictionary current;
    private Ref<? extends CompressionDictionary> currentRef; // guarded by this

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
        CompressionDictionary dictionary = current;
        // keeps the cache entry of the current dictionary from expiring while the dictionary is in use
        if (dictionary != null)
            get(dictionary.dictId());
        return dictionary;
    }

    @Nullable
    @Override
    public synchronized Ref<? extends CompressionDictionary> refCurrent()
    {
        // the current dictionary can not change meanwhile, and the cache references it as long as it is current
        CompressionDictionary dictionary = getCurrent();
        return dictionary == null ? null : dictionary.tryRef();
    }

    @Nullable
    @Override
    public CompressionDictionary get(DictId dictId)
    {
        return cache.getIfPresent(dictId);
    }

    @Override
    public CompressionDictionary add(@Nullable CompressionDictionary compressionDictionary)
    {
        if (compressionDictionary == null)
            return null;

        Ref<? extends CompressionDictionary> ref = refForReading(compressionDictionary);
        CompressionDictionary dictionary = ref.get();
        useIfNewer(ref);
        return dictionary;
    }

    @Override
    public CompressionDictionary addForReading(@Nullable CompressionDictionary compressionDictionary)
    {
        if (compressionDictionary == null)
            return null;

        return cache.get(compressionDictionary.dictId(), id -> {
            CompressionDictionary dictionary = unreferenced(compressionDictionary);
            Ref<?> ref = dictionary.initRefLazily();
            if (ref == null)
            {
                throw new IllegalStateException("Failed to acquire reference to compression dictionary");
            }
            return dictionary;
        });
    }

    @Override
    public Ref<? extends CompressionDictionary> refForReading(CompressionDictionary compressionDictionary)
    {
        Ref<? extends CompressionDictionary> ref = addForReading(compressionDictionary).tryRef();
        if (ref != null)
            return ref;

        // the cached instance was released before it could be referenced, e.g. evicted right after it was cached or
        // looked up; a fresh instance is not cached, its self-reference is the caller's
        return unreferenced(compressionDictionary).initRefLazily();
    }

    @Override
    public void setCurrent(@Nullable CompressionDictionary compressionDictionary)
    {
        Ref<? extends CompressionDictionary> ref = compressionDictionary == null ? null : refForReading(compressionDictionary);
        replaceCurrent(ref == null ? null : ref.get(), ref);
    }

    /**
     * @return the dictionary when it has no self-reference yet, otherwise a fresh instance of it. An instance with
     *         a self-reference was referenced before, e.g. cached and then evicted, which released the self-reference;
     *         taking that self-reference over, e.g. by caching the instance again, would release it twice.
     */
    private static CompressionDictionary unreferenced(CompressionDictionary dictionary)
    {
        if (dictionary.selfRef() == null)
            return dictionary;

        return dictionary.kind().createDictionary(dictionary.dictId(),
                                                  dictionary.nodeId(),
                                                  dictionary.rawDictionary(),
                                                  dictionary.checksum(),
                                                  dictionary.createdAt());
    }

    /**
     * Makes the referenced dictionary the current one if there is no current dictionary or the dictionary has a higher
     * id (newer). The reference is kept for the current dictionary, otherwise it is released.
     */
    private synchronized void useIfNewer(Ref<? extends CompressionDictionary> ref)
    {
        CompressionDictionary previous = current;
        if (previous != null && ref.get().dictId().id <= previous.dictId().id)
            ref.release();
        else
            replaceCurrent(ref.get(), ref);
    }

    private synchronized void replaceCurrent(@Nullable CompressionDictionary dictionary, @Nullable Ref<? extends CompressionDictionary> ref)
    {
        if (currentRef != null)
            currentRef.release();
        currentRef = ref;
        current = dictionary;
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
        current = null;
        if (currentRef != null)
        {
            currentRef.release();
            currentRef = null;
        }
        // Invalidate cache will trigger removalListener to close all cached dictionaries
        cache.invalidateAll();
        // Force synchronous cleanup to ensure removal listener executes immediately
        cache.cleanUp();
    }
}
