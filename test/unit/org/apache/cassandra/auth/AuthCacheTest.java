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

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.UnavailableException;

import static org.junit.Assert.assertEquals;

public class AuthCacheTest
{
    private int loadCounter = 0;
    private int validity = 2000;
    private boolean isCacheEnabled = true;

    @Test
    public void testCacheLoaderIsCalledOnFirst()
    {
        TestCache<String, Integer> authCache = new TestCache<>(this::countingLoader, this::setValidity, () -> validity, () -> isCacheEnabled);

        int result = authCache.get("10");

        assertEquals(10, result);
        assertEquals(1, loadCounter);
    }

    @Test
    public void testCacheLoaderIsNotCalledOnSecond()
    {
        TestCache<String, Integer> authCache = new TestCache<>(this::countingLoader, this::setValidity, () -> validity, () -> isCacheEnabled);
        authCache.get("10");
        assertEquals(1, loadCounter);

        int result = authCache.get("10");

        assertEquals(10, result);
        assertEquals(1, loadCounter);
    }

    @Test
    public void testCacheLoaderIsAlwaysCalledWhenDisabled()
    {
        isCacheEnabled = false;
        TestCache<String, Integer> authCache = new TestCache<>(this::countingLoader, this::setValidity, () -> validity, () -> isCacheEnabled);

        authCache.get("10");
        int result = authCache.get("10");

        assertEquals(10, result);
        assertEquals(2, loadCounter);
    }

    @Test
    public void testCacheLoaderIsAlwaysCalledWhenValidityIsZero()
    {
        setValidity(0);
        TestCache<String, Integer> authCache = new TestCache<>(this::countingLoader, this::setValidity, () -> validity, () -> isCacheEnabled);

        authCache.get("10");
        int result = authCache.get("10");

        assertEquals(10, result);
        assertEquals(2, loadCounter);
    }

    @Test
    public void testCacheLoaderIsCalledAfterFullInvalidate()
    {
        TestCache<String, Integer> authCache = new TestCache<>(this::countingLoader, this::setValidity, () -> validity, () -> isCacheEnabled);
        authCache.get("10");

        authCache.invalidate();
        int result = authCache.get("10");

        assertEquals(10, result);
        assertEquals(2, loadCounter);
    }

    @Test
    public void testCacheLoaderIsCalledAfterInvalidateKey()
    {
        TestCache<String, Integer> authCache = new TestCache<>(this::countingLoader, this::setValidity, () -> validity, () -> isCacheEnabled);
        authCache.get("10");

        authCache.invalidate("10");
        int result = authCache.get("10");

        assertEquals(10, result);
        assertEquals(2, loadCounter);
    }

    @Test(expected = UnavailableException.class)
    public void testCassandraExceptionPassThroughWhenCacheEnabled()
    {
        TestCache<String, Integer> cache = new TestCache<>(s -> {
            throw new UnavailableException(ConsistencyLevel.QUORUM, 3, 1);
        }, this::setValidity, () -> validity, () -> isCacheEnabled);

        cache.get("expect-exception");
    }

    @Test(expected = UnavailableException.class)
    public void testCassandraExceptionPassThroughWhenCacheDisable()
    {
        isCacheEnabled = false;
        TestCache<String, Integer> cache = new TestCache<>(s -> {
            throw new UnavailableException(ConsistencyLevel.QUORUM, 3, 1);
        }, this::setValidity, () -> validity, () -> isCacheEnabled);

        cache.get("expect-exception");
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

    private static class TestCache<K, V> extends AuthCache<K, V>
    {
        private static int nameCounter = 0; // Allow us to create many instances of cache with same name prefix

        TestCache(Function<K, V> loadFunction, Consumer<Integer> setValidityDelegate, Supplier<Integer> getValidityDelegate, Supplier<Boolean> cacheEnabledDelegate)
        {
            super("TestCache" + nameCounter++,
                  setValidityDelegate,
                  getValidityDelegate,
                  (updateInterval) -> {
                  },
                  () -> 1000,
                  (maxEntries) -> {
                  },
                  () -> 10,
                  loadFunction,
                  cacheEnabledDelegate);
        }
    }
}
