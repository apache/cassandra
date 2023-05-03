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
package org.apache.cassandra.auth;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.UnavailableException;

import static org.apache.cassandra.config.CassandraRelevantProperties.AUTH_CACHE_WARMING_MAX_RETRIES;
import static org.apache.cassandra.config.CassandraRelevantProperties.AUTH_CACHE_WARMING_RETRY_INTERVAL_MS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthCacheTest
{
    private int loadCounter = 0;
    private int validity = 2000;
    private boolean isCacheEnabled = true;

    private final int MAX_ENTRIES = 10;

    @Test
    public void testCacheLoaderIsCalledOnFirst()
    {
        TestCache authCache = newCache();
        assertEquals(10, (int)authCache.get("10"));
        assertEquals(1, loadCounter);
    }

    @Test
    public void testCacheLoaderIsNotCalledOnSecond()
    {
        TestCache authCache = newCache();
        authCache.get("10");
        assertEquals(1, loadCounter);
        assertEquals(10, (int)authCache.get("10"));
        assertEquals(1, loadCounter);
    }

    @Test
    public void testCacheLoaderIsAlwaysCalledWhenDisabled()
    {
        isCacheEnabled = false;
        TestCache authCache = newCache();
        authCache.get("10");
        assertEquals(10, (int)authCache.get("10"));
        assertEquals(2, loadCounter);
    }

    @Test
    public void testCacheLoaderIsAlwaysCalledWhenValidityIsZero()
    {
        setValidity(0);
        TestCache authCache = newCache();
        authCache.get("10");
        assertEquals(10, (int)authCache.get("10"));
        assertEquals(2, loadCounter);
    }

    @Test
    public void testCacheLoaderIsCalledAfterFullInvalidate()
    {
        TestCache authCache = newCache();
        authCache.get("10");
        authCache.get("11");
        assertEquals(2, loadCounter);
        authCache.invalidate();
        assertEquals(10, (int)authCache.get("10"));
        assertEquals(11, (int)authCache.get("11"));
        assertEquals(4, loadCounter);
    }

    @Test
    public void testCacheLoaderIsCalledAfterInvalidateKey()
    {
        TestCache authCache = newCache();
        authCache.get("10");
        authCache.get("11"); // second key that should not be invalidated
        assertEquals(2, loadCounter);
        authCache.invalidate("10");
        assertEquals(10, (int)authCache.get("10"));
        assertEquals(11, (int)authCache.get("11"));
        assertEquals(3, loadCounter);
    }

    @Test
    public void testCacheLoaderIsCalledAfterReset()
    {
        TestCache authCache = newCache();
        authCache.get("10");

        authCache.cache = null;
        int result = authCache.get("10");

        assertEquals(10, result);
        assertEquals(2, loadCounter);
    }

    @Test
    public void testThatZeroValidityTurnOffCaching()
    {
        setValidity(0);
        TestCache authCache = newCache();
        authCache.get("10");
        int result = authCache.get("10");

        assertNull(authCache.cache);
        assertEquals(10, result);
        assertEquals(2, loadCounter);
    }

    @Test
    public void testThatRaisingValidityTurnOnCaching()
    {
        setValidity(0);
        TestCache authCache = newCache();
        authCache.setValidity(2000);
        authCache.cache = authCache.initCache(null);

        assertNotNull(authCache.cache);
    }

    @Test
    public void testDisableCache()
    {
        isCacheEnabled = false;
        TestCache authCache = newCache();
        assertNull(authCache.cache);
    }

    @Test
    public void testDynamicallyEnableCache()
    {
        isCacheEnabled = false;
        TestCache authCache = newCache();
        isCacheEnabled = true;
        authCache.cache = authCache.initCache(null);

        assertNotNull(authCache.cache);
    }

    @Test
    public void testDefaultPolicies()
    {
        TestCache authCache = newCache();
        assertTrue(authCache.cache.policy().expireAfterWrite().isPresent());
        assertTrue(authCache.cache.policy().refreshAfterWrite().isPresent());
        assertTrue(authCache.cache.policy().eviction().isPresent());
    }

    @Test(expected = UnavailableException.class)
    public void testCassandraExceptionPassThroughWhenCacheEnabled()
    {
        TestCache cache = newCache(s -> { throw UnavailableException.create(ConsistencyLevel.QUORUM, 3, 1); });
        cache.get("expect-exception");
    }

    @Test(expected = UnavailableException.class)
    public void testCassandraExceptionPassThroughWhenCacheDisable()
    {
        isCacheEnabled = false;
        TestCache cache = newCache(s -> { throw UnavailableException.create(ConsistencyLevel.QUORUM, 3, 1); });
        cache.get("expect-exception");
    }

    @Test
    public void testCassandraExceptionPassThroughWhenCacheRefreshed() throws InterruptedException
    {
        setValidity(50);
        TestCache cache = new TestCache(this::countingLoaderWithException, this::emptyBulkLoader, this::setValidity, () -> validity, () -> isCacheEnabled);
        cache.get("10");

        // wait until the cached record expires
        Thread.sleep(60);

        for (int i = 1; i <= 5; i++)
        {
            try
            {
                cache.get("10");
                fail("Did not get expected Exception on attempt " + i);
            }
            catch (UnavailableException expected)
            {
            }
        }
    }

    @Test
    public void warmCacheUsingEntryProvider()
    {
        AtomicBoolean provided = new AtomicBoolean(false);
        Supplier<Map<String, Integer>> bulkLoader = () -> {
            provided.set(true);
            return Collections.singletonMap("0", 0);
        };
        TestCache cache = newCache(bulkLoader);
        cache.warm();
        assertEquals(1, cache.getEstimatedSize());
        assertEquals(0, (int)cache.get("0")); // warmed entry
        assertEquals(0, loadCounter);
        assertEquals(10, (int)cache.get("10")); // cold entry
        assertEquals(1, loadCounter);
        assertTrue(provided.get());
    }

    @Test
    public void warmCacheIsSafeIfCachingIsDisabled()
    {
        isCacheEnabled = false;
        TestCache cache = newCache(() -> Collections.singletonMap("0", 0));
        cache.warm();
        assertEquals(0, cache.getEstimatedSize());
    }

    @Test
    public void providerSuppliesMoreEntriesThanCapacity()
    {
        Supplier<Map<String, Integer>> bulkLoader = () -> {
            Map<String, Integer> entries = new HashMap<>();
            for (int i = 0; i < MAX_ENTRIES * 2; i++)
                entries.put(Integer.toString(i), i);
            return entries;
        };
        TestCache cache = new TestCache(this::countingLoader,
                                        bulkLoader,
                                        this::setValidity,
                                        () -> validity,
                                        () -> isCacheEnabled);
        cache.warm();
        cache.cleanup(); // Force the cleanup task rather than waiting for it to be scheduled to get accurate count
        assertEquals(MAX_ENTRIES, cache.getEstimatedSize());
    }

    @Test
    public void handleProviderErrorDuringWarming()
    {
        AUTH_CACHE_WARMING_MAX_RETRIES.setInt(3);
        AUTH_CACHE_WARMING_RETRY_INTERVAL_MS.setLong(0);
        final AtomicInteger attempts = new AtomicInteger(0);

        Supplier<Map<String, Integer>> bulkLoader = () -> {
            if (attempts.incrementAndGet() < 3)
                throw new RuntimeException("BOOM");

            return Collections.singletonMap("0", 99);
        };

        TestCache cache = newCache(bulkLoader);
        cache.warm();
        assertEquals(1, cache.getEstimatedSize());
        assertEquals(99, (int)cache.get("0"));
        // We should have made 3 attempts to get the initial entries
        assertEquals(3, attempts.get());
    }

    @Test
    public void testCacheLoaderIsNotCalledOnGetAllWhenCacheIsDisabled()
    {
        isCacheEnabled = false;
        TestCache authCache = new TestCache(this::countingLoader, this::emptyBulkLoader, this::setValidity, () -> validity, () -> isCacheEnabled);
        authCache.get("10");
        Map<String, Integer> result = authCache.getAll();

        // even though the cache is disabled and nothing is cache we still use loadFunction on get operation, so
        // its counter has been incremented
        assertThat(result).isEmpty();
        assertEquals(1, loadCounter);
    }

    @Test
    public void testCacheLoaderIsNotCalledOnGetAllWhenCacheIsEmpty()
    {
        TestCache authCache = new TestCache(this::countingLoader, this::emptyBulkLoader, this::setValidity, () -> validity, () -> isCacheEnabled);

        Map<String, Integer> result = authCache.getAll();

        assertThat(result).isEmpty();
        assertEquals(0, loadCounter);
    }

    @Test
    public void testCacheLoaderIsNotCalledOnGetAllWhenCacheIsNotEmpty()
    {
        TestCache authCache = new TestCache(this::countingLoader, this::emptyBulkLoader, this::setValidity, () -> validity, () -> isCacheEnabled);
        authCache.get("10");
        Map<String, Integer> result = authCache.getAll();

        assertThat(result).hasSize(1);
        assertThat(result).containsEntry("10", 10);
        assertEquals(1, loadCounter);
    }

    private void setValidity(int validity)
    {
        this.validity = validity;
    }

    private Integer countingLoader(String s)
    {
        loadCounter++;
        return Integer.parseInt(s);
    }

    private Integer countingLoaderWithException(String s)
    {
        Integer loadedValue = countingLoader(s);

        if (loadCounter > 1)
            throw UnavailableException.create(ConsistencyLevel.QUORUM, 3, 1);

        return loadedValue;
    }

    private Map<String, Integer> emptyBulkLoader()
    {
        return Collections.emptyMap();
    }

    private TestCache newCache()
    {
        return new TestCache(this::countingLoader,
                             this::emptyBulkLoader,
                             this::setValidity,
                             () -> validity,
                             () -> isCacheEnabled);
    }

    private TestCache newCache(Function<String, Integer> loadFunction)
    {
        return new TestCache(loadFunction,
                             this::emptyBulkLoader,
                             this::setValidity,
                             () -> validity,
                             () -> isCacheEnabled);
    }

    private TestCache newCache(Supplier<Map<String, Integer>> bulkLoader)
    {
        return new TestCache(this::countingLoader,
                             bulkLoader,
                             this::setValidity,
                             () -> validity,
                             () -> isCacheEnabled);
    }

    private static class TestCache extends AuthCache<String, Integer>
    {
        private static int nameCounter = 0; // Allow us to create many instances of cache with same name prefix

        TestCache(Function<String, Integer> loadFunction,
                  Supplier<Map<String, Integer>> bulkLoadFunction,
                  IntConsumer setValidityDelegate,
                  IntSupplier getValidityDelegate,
                  BooleanSupplier cacheEnabledDelegate)
        {
            super("TestCache" + nameCounter++,
                  setValidityDelegate,
                  getValidityDelegate,
                  (updateInterval) -> {},               // set update interval
                  () -> 1000,                           // get update interval
                  (MAX_ENTRIES) -> {},                   // set max entries
                  () -> 10,                             // get max entries
                  (updateActiveUpdate) -> {},           // set active update enabled
                  () -> false,                          // get active update enabled
                  loadFunction,
                  bulkLoadFunction,
                  cacheEnabledDelegate);
        }

        void cleanup()
        {
            cache.cleanUp();
        }
    }
}
