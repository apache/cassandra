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
package org.apache.cassandra.cache;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Objects;

/**
 * A sentinel object for row caches.  See comments to getThroughCache and CASSANDRA-3862.
 */
public class RowCacheSentinel implements IRowCacheEntry
{
    private static final AtomicLong generator = new AtomicLong();

    final long sentinelId;

    public RowCacheSentinel()
    {
        sentinelId = generator.getAndIncrement();
    }

    RowCacheSentinel(long sentinelId)
    {
        this.sentinelId = sentinelId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof RowCacheSentinel)) return false;

        RowCacheSentinel other = (RowCacheSentinel) o;
        return this.sentinelId == other.sentinelId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sentinelId);
    }
}
