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

package org.apache.cassandra.io.util;

/**
 * How a reader accesses a file. A caller states the access pattern when it instantiates a rebufferer. Each
 * rebufferer layer then reads only the decision it owns: {@link org.apache.cassandra.cache.ChunkCache} reads
 * {@link #usesCache()}, and {@link CompressedChunkReader} reads {@link #readsAhead()}.
 *
 * <p>The pattern carries two orthogonal decisions, but the three values below are the only meaningful pairs.
 * A pattern that neither caches nor reads ahead has no caller, so the enum does not offer it. See
 * CASSANDRA-21671.
 */
public enum ReadPattern
{
    /**
     * A read of specific rows: a single-partition read served by named or sliced clustering keys. It uses the
     * chunk cache and does not read ahead. Repeated reads of hot data hit the cache.
     */
    ROW_READ(true, false),

    /**
     * A read that walks a range of partitions in order, such as a token-range query. Re-use is likely, so it
     * keeps the chunk cache. It reads ahead only when the chunk cache is off.
     */
    PARTITION_READ(true, true),

    /**
     * An unbounded, one-shot read: compaction, cursor compaction, and similar callers. It reads each chunk once,
     * so it must bypass the chunk cache to avoid evicting hot data with one-shot chunks. It reads ahead through
     * its own buffer instead.
     */
    SCAN(false, true);

    private final boolean usesCache;
    private final boolean readsAhead;

    ReadPattern(boolean usesCache, boolean readsAhead)
    {
        this.usesCache = usesCache;
        this.readsAhead = readsAhead;
    }

    /** True if the reader should go through the chunk cache. */
    public boolean usesCache()
    {
        return usesCache;
    }

    /** True if the reader should use its own read-ahead buffer instead of the chunk cache. */
    public boolean readsAhead()
    {
        return readsAhead;
    }
}
