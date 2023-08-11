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

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;

public interface KeyCacheSupport<T extends SSTableReader & KeyCacheSupport<T>>
{
    @Nonnull
    KeyCache getKeyCache();

    /**
     * Should quickly get a lower bound prefix from cache only if everything is already availabe in memory and does not
     * need to be loaded from disk.
     */
    @Nullable
    ClusteringBound<?> getLowerBoundPrefixFromCache(DecoratedKey partitionKey, boolean isReversed);

    default @Nonnull KeyCacheKey getCacheKey(ByteBuffer key)
    {
        T reader = (T) this;
        return new KeyCacheKey(reader.metadata(), reader.descriptor, key);
    }

    default @Nonnull KeyCacheKey getCacheKey(DecoratedKey key)
    {
        return getCacheKey(key.getKey());
    }

    /**
     * Will return null if key is not found or cache is not available.
     */
    default @Nullable AbstractRowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats)
    {
        return getCachedPosition(getCacheKey(key), updateStats);
    }

    /**
     * Will return null if key is not found or cache is not available.
     */
    default @Nullable AbstractRowIndexEntry getCachedPosition(KeyCacheKey key, boolean updateStats)
    {
        KeyCache keyCache = getKeyCache();
        AbstractRowIndexEntry cachedEntry = keyCache.get(key, updateStats);
        assert cachedEntry == null || cachedEntry.getSSTableFormat() == ((T) this).descriptor.version.format;

        return cachedEntry;
    }

    /**
     * Caches a key only if cache is available.
     */
    default void cacheKey(@Nonnull DecoratedKey key, @Nonnull AbstractRowIndexEntry info)
    {
        T reader = (T) this;
        assert info.getSSTableFormat() == reader.descriptor.version.format;

        KeyCacheKey cacheKey = getCacheKey(key);
        getKeyCache().put(cacheKey, info);
    }

    @Nonnull
    AbstractRowIndexEntry deserializeKeyCacheValue(@Nonnull DataInputPlus input) throws IOException;

    static boolean isSupportedBy(SSTableFormat<?, ?> format)
    {
        return KeyCacheSupport.class.isAssignableFrom(format.getReaderFactory().getReaderClass());
    }

}
