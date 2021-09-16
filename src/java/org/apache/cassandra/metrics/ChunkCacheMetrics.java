/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.metrics;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import org.apache.cassandra.cache.ChunkCache;

import static org.apache.cassandra.config.CassandraRelevantProperties.USE_MICROMETER;

public interface ChunkCacheMetrics extends StatsCounter
{
    static ChunkCacheMetrics create(ChunkCache cache)
    {
        return USE_MICROMETER.getBoolean() ? new MicrometerChunkCacheMetrics(cache) : new CodahaleChunkCacheMetrics(cache);
    }

    @Override
    void recordHits(int count);

    @Override
    void recordMisses(int count);

    @Override
    void recordLoadSuccess(long loadTime);

    @Override
    void recordLoadFailure(long loadTime);

    @Override
    void recordEviction();

    double hitRate();

    long requests();

    long misses();

    long hits();

    double missLatency();

    long capacity();

    long size();
    
    long entries();

    long requestsFifteenMinuteRate();

    long hitsFifteenMinuteRate();

    @Nonnull
    @Override
    CacheStats snapshot();

    @VisibleForTesting
    void reset();
}
