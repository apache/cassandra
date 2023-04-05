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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.cache.ICache;
import org.apache.cassandra.cache.InstrumentingCache;

import static org.junit.Assert.assertEquals;

public class CacheMetricsTest
{
    private static final long capacity = 65536;

    @Test
    public void testCacheMetrics()
    {
        ICache<String,Object> mockedCache = new MapMockedCache();
        InstrumentingCache<String,Object> cache = new InstrumentingCache<>("cache", mockedCache);
        CacheMetrics metrics = cache.getMetrics();

        assertCacheMetrics(metrics, expect(mockedCache));

        cache.put("k1", "v1");
        assertCacheMetrics(metrics, expect(mockedCache));

        getFromCache(cache, "k1", 10);
        assertCacheMetrics(metrics, expect(mockedCache).hits(10).misses(0));

        getFromCache(cache, "k2", 10);
        assertCacheMetrics(metrics, expect(mockedCache).hits(10).misses(10));

        cache.put("k2", "v2");
        getFromCache(cache, "k2", 70);
        getFromCache(cache, "k3", 10);
        assertCacheMetrics(metrics, expect(mockedCache).hits(80).misses(20));

        cache.clear();
        metrics.reset();
        assertCacheMetrics(metrics, expect(mockedCache));
    }

    private void getFromCache(InstrumentingCache<String,Object> cache, String key, int times)
    {
        for (int ix = 0; ix < times; ix++)
        {
            cache.get(key);
        }
    }

    private void assertCacheMetrics(CacheMetrics actual, CacheMetricsExpectation expectation)
    {
        // assuming meters/guagues (hits, misses, requests, and hitRate) will have correct one/five/fifteenMinute
        // calculations - applying some general assertions for hitRate calculations that essentially just smoke test
        // existence (i.e. NaN at initialization) since they are established by way of an inner class on CacheMetrics
        // itself.
        assertEquals(expectation.cacheSize.capacity(), actual.capacity.getValue().longValue());
        assertEquals(expectation.cacheSize.weightedSize(), actual.size.getValue().longValue());
        assertEquals(expectation.cacheSize.size(), actual.entries.getValue().intValue());
        assertEquals(expectation.hits, actual.hits.getCount());
        assertEquals(expectation.misses, actual.misses.getCount());
        assertEquals(expectation.requests(), actual.requests.getCount());
        assertEquals(expectation.hitRate(), actual.hitRate.getValue(), 0.001d);
        assertEquals(Double.NaN, actual.oneMinuteHitRate.getValue(), 0.001d);
        assertEquals(Double.NaN, actual.fiveMinuteHitRate.getValue(), 0.001d);
        assertEquals(Double.NaN, actual.fifteenMinuteHitRate.getValue(), 0.001d);
    }

    static CacheMetricsExpectation expect(CacheSize cacheSize)
    {
        return new CacheMetricsExpectation(cacheSize);
    }

    private static class MapMockedCache implements ICache<String,Object>
    {
        private final Map<String,Object> map = new HashMap<>();

        public void put(String key, Object value)
        {
            map.put(key, value);
        }

        public boolean putIfAbsent(String key, Object value)
        {
            return map.putIfAbsent(key, value) == null;
        }

        public boolean replace(String key, Object old, Object value)
        {
            return map.replace(key, old, value);
        }

        public Object get(String key)
        {
            return map.get(key);
        }

        public void remove(String key)
        {
            map.remove(key);
        }

        public void clear()
        {
            map.clear();
        }

        public Iterator<String> keyIterator()
        {
            return map.keySet().iterator();
        }

        public Iterator<String> hotKeyIterator(int n)
        {
            return map.keySet().iterator();
        }

        public boolean containsKey(String key)
        {
            return map.containsKey(key);
        }

        public long capacity()
        {
            // capacity in bytes but just using a fixed number here since since the validation is just to ensure
            // that this number is equivalent to the metric that publishes it.
            return capacity;
        }

        public void setCapacity(long capacity)
        {
            throw new UnsupportedOperationException("Not needed for testing");
        }

        public int size()
        {
            return map.size();
        }

        public long weightedSize()
        {
            // should be cache size in bytes, but no need to calculate that for tests since the validation
            // is just to ensure that this number is equivalent to the metric that publishes it.
            return map.size() * 8;
        }
    }

    private static class CacheMetricsExpectation
    {
        final CacheSize cacheSize;
        private long hits = 0;
        private long misses = 0;

        CacheMetricsExpectation(CacheSize cacheSize)
        {
            this.cacheSize = cacheSize;
        }

        public CacheMetricsExpectation hits(long hits)
        {
            this.hits = hits;
            return this;
        }

        public CacheMetricsExpectation misses(long misses)
        {
            this.misses = misses;
            return this;
        }

        public long requests()
        {
            return hits + misses;
        }

        public double hitRate()
        {
            return requests() == 0 ? Double.NaN : ((double) hits / (double) requests());
        }
    }
}
