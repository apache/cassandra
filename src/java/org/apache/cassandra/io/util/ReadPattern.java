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
 * How a reader accesses a file, set when a rebufferer is created. {@link org.apache.cassandra.cache.ChunkCache}
 * checks {@link #usesCache()}; {@link CompressedChunkReader} checks {@link #readsAhead()}.
 */
public enum ReadPattern
{
    /** Single-partition reads use the chunk cache but do not read ahead. */
    ROW_READ(true, false),

    /** Range queries walking partitions in order  use the chunk cache and can read ahead only if the cache is disabled. */
    PARTITION_READ(true, true),

    /** One-shot scans (ex. compactions) bypass the cache to avoid evicting hot data but read ahead instead. */
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
