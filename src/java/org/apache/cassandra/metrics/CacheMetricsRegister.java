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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.Sets;

public class CacheMetricsRegister
{
    private static final CacheMetricsRegister instance = new CacheMetricsRegister();

    private CacheMetricsRegister()
    {
    }

    public static CacheMetricsRegister getInstance()
    {
        return instance;
    }

    private final Collection<CacheMetrics> cacheMetrics = Sets.newConcurrentHashSet();
    private final Collection<UnweightedCacheMetrics> unweightedCacheMetrics = Sets.newConcurrentHashSet();

    public void reset()
    {
        cacheMetrics.clear();
        unweightedCacheMetrics.clear();
    }

    public void register(CacheMetrics metrics)
    {
        cacheMetrics.add(metrics);
    }

    public void unregister(CacheMetrics metrics)
    {
        cacheMetrics.remove(metrics);
    }

    public void register(UnweightedCacheMetrics unweightedMetrics)
    {
        unweightedCacheMetrics.add(unweightedMetrics);
    }

    public void unregister(UnweightedCacheMetrics unweightedMetrics)
    {
        unweightedCacheMetrics.remove(unweightedMetrics);
    }

    public void unregisterCacheMetrics(String metricsCacheName)
    {
        Optional<CacheMetrics> maybeCacheMetrics = cacheMetrics.stream()
                                                               .filter(m -> metricsCacheName.equals(m.type))
                                                               .findFirst();
        maybeCacheMetrics.map(cacheMetrics::remove);
    }

    public void unregisterUnweightedCacheMetrics(String metricsCacheName)
    {
        Optional<UnweightedCacheMetrics> maybeUnweightedCacheMetrics = unweightedCacheMetrics.stream()
                                                                                             .filter(m -> metricsCacheName.equals(m.type))
                                                                                             .findFirst();
        maybeUnweightedCacheMetrics.map(unweightedCacheMetrics::remove);
    }

    public Collection<CacheMetrics> getCacheMetrics()
    {
        return Collections.unmodifiableCollection(cacheMetrics);
    }

    public Collection<UnweightedCacheMetrics> getUnweightedCacheMetrics()
    {
        return Collections.unmodifiableCollection(unweightedCacheMetrics);
    }
}
