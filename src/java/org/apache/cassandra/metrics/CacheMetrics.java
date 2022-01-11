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
package org.apache.cassandra.metrics;

import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.service.CacheService;

import static org.apache.cassandra.config.CassandraRelevantProperties.USE_MICROMETER;

/**
 * Metrics for {@code ICache}.
 */
public interface CacheMetrics
{
    static CacheMetrics create(CacheService.CacheType cacheType, CacheSize cache)
    {
        return USE_MICROMETER.getBoolean()
               ? new MicrometerCacheMetrics(cacheType.micrometerMetricsPrefix(), cache)
               : new CodahaleCacheMetrics(cacheType.toString(), cache);
    }

    long requests();

    long capacity();

    long size();

    long entries();

    long hits();

    long misses();

    double hitRate();

    double hitOneMinuteRate();
    double hitFiveMinuteRate();
    double hitFifteenMinuteRate();

    double requestsFifteenMinuteRate();

    void recordHits(int count);

    void recordMisses(int count);
}
