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

package org.apache.cassandra.io.sstable.keycache;

import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;

/**
 * A simple wrapper of possibly global cache with local metrics.
 */
public class KeyCache
{
    public static final KeyCache NO_CACHE = new KeyCache(null);

    private final static Logger logger = LoggerFactory.getLogger(KeyCache.class);

    private final InstrumentingCache<KeyCacheKey, AbstractRowIndexEntry> cache;
    private final LongAdder hits = new LongAdder();
    private final LongAdder requests = new LongAdder();

    public KeyCache(@Nullable InstrumentingCache<KeyCacheKey, AbstractRowIndexEntry> cache)
    {
        this.cache = cache;
    }

    public long getHits()
    {
        return cache != null ? hits.sum() : 0;
    }

    public long getRequests()
    {
        return cache != null ? requests.sum() : 0;
    }

    public void put(@Nonnull KeyCacheKey cacheKey, @Nonnull AbstractRowIndexEntry info)
    {
        if (cache == null)
            return;

        logger.trace("Adding cache entry for {} -> {}", cacheKey, info);
        cache.put(cacheKey, info);
    }

    public @Nullable AbstractRowIndexEntry get(KeyCacheKey key, boolean updateStats)
    {
        if (cache == null)
            return null;

        if (updateStats)
        {
            requests.increment();
            AbstractRowIndexEntry r = cache.get(key);
            if (r != null)
                hits.increment();
            return r;
        }
        else
        {
            return cache.getInternal(key);
        }
    }

    public boolean isEnabled()
    {
        return cache != null;
    }
}
