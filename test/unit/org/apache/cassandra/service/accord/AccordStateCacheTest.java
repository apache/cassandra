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
package org.apache.cassandra.service.accord;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import accord.utils.async.AsyncChain;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.ManualExecutor;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.metrics.CacheAccessMetrics;
import org.apache.cassandra.service.accord.AccordCachingState.Status;

import static org.apache.cassandra.service.accord.AccordTestUtils.testLoad;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccordStateCacheTest
{
    private static final long DEFAULT_NODE_SIZE = nodeSize(0);
    private AccordStateCacheMetrics cacheMetrics;

    private static abstract class TestSafeState<T> implements AccordSafeState<T, T>
    {
        protected boolean invalidated = false;
        protected final AccordCachingState<T, T> global;
        private T original = null;

        public TestSafeState(AccordCachingState<T, T> global)
        {
            this.global = global;
        }

        public AccordCachingState<T, T> global()
        {
            return global;
        }

        @Override
        public T key()
        {
            return global.key();
        }

        @Override
        public T current()
        {
            return global.get();
        }

        @Override
        public void set(T update)
        {
            global.set(update);
        }

        @Override
        public T original()
        {
            return original;
        }

        @Override
        public void preExecute()
        {
            original = global.get();
        }

        @Override
        public Status globalStatus()
        {
            return global.status();
        }

        @Override
        public AsyncChain<?> loading()
        {
            return global.loading();
        }

        @Override
        public AsyncChain<?> saving()
        {
            return global.saving();
        }

        @Override
        public Throwable failure()
        {
            return global.failure();
        }

        @Override
        public void invalidate()
        {
            invalidated = true;
        }

        @Override
        public boolean invalidated()
        {
            return invalidated;
        }
    }

    private static class SafeString extends TestSafeState<String>
    {
        public SafeString(AccordCachingState<String, String> global)
        {
            super(global);
        }
    }

    private static class SafeInt extends TestSafeState<Integer>
    {
        public SafeInt(AccordCachingState<Integer, Integer> global)
        {
            super(global);
        }
    }

    private static long emptyNodeSize()
    {
        return AccordCachingState.EMPTY_SIZE;
    }

    private static long nodeSize(long itemSize)
    {
        return itemSize + emptyNodeSize();
    }

    private static void assertCacheState(AccordStateCache cache, int referenced, int total, long bytes)
    {
        Assert.assertEquals(referenced, cache.numReferencedEntries());
        Assert.assertEquals(total, cache.size());
        Assert.assertEquals(bytes, cache.weightedSize());
    }

    private void assertCacheMetrics(CacheAccessMetrics metrics, int hits, int misses, int requests)
    {
        Assert.assertEquals(hits, metrics.hits.getCount());
        Assert.assertEquals(misses, metrics.misses.getCount());
        Assert.assertEquals(requests, metrics.requests.getCount());
        if (metrics instanceof AccordStateCacheMetrics)
        {
            AccordStateCacheMetrics ascMetrics = (AccordStateCacheMetrics) metrics;
            Assert.assertEquals(misses, ascMetrics.objectSize.getCount());
            assertThat(ascMetrics.objectSize.getSnapshot().getMax()).isGreaterThanOrEqualTo(DEFAULT_NODE_SIZE);
        }
    }

    @Before
    public void before()
    {
        String type = String.format("%s-%s", AccordCommandStores.ACCORD_STATE_CACHE, UUID.randomUUID());
        cacheMetrics = new AccordStateCacheMetrics(type);
    }

    @Test
    public void testAcquisitionAndRelease()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, 500, cacheMetrics);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, SafeString.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString1 = instance.acquire("1");
        assertCacheState(cache, 1, 1, emptyNodeSize());
        testLoad(executor, safeString1, "1");
        Assert.assertTrue(cache.isEmpty());

        instance.release(safeString1);
        assertCacheState(cache, 0, 1, nodeSize(1));
        Assert.assertSame(safeString1.global, cache.head());
        Assert.assertSame(safeString1.global, cache.tail());

        SafeString safeString2 = instance.acquire("2");
        assertCacheState(cache, 1, 2, DEFAULT_NODE_SIZE + nodeSize(1));
        testLoad(executor, safeString2, "2");
        instance.release(safeString2);
        assertCacheState(cache, 0, 2, nodeSize(1) + nodeSize(1));

        Assert.assertSame(safeString1.global, cache.head());
        Assert.assertSame(safeString2.global, cache.tail());

        assertCacheMetrics(cache.metrics, 0, 2, 2);
        assertCacheMetrics(instance.instanceMetrics, 0, 2, 2);
    }

    @Test
    public void testCachingMetricsWithTwoInstances()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, 500, cacheMetrics);
        AccordStateCache.Instance<String, String, SafeString> stringInstance =
            cache.instance(String.class, SafeString.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true,String::length);
        AccordStateCache.Instance<Integer, Integer, SafeInt> intInstance =
            cache.instance(Integer.class, SafeInt.class, SafeInt::new, key -> key, (original, current) -> null, (k, v) -> true,ignored -> Integer.BYTES);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString1 = stringInstance.acquire("1");
        testLoad(executor, safeString1, "1");
        stringInstance.release(safeString1);
        SafeString safeString2 = stringInstance.acquire("2");
        testLoad(executor, safeString2, "2");
        stringInstance.release(safeString2);

        SafeInt safeInt1 = intInstance.acquire(3);
        testLoad(executor, safeInt1, 3);
        intInstance.release(safeInt1);
        SafeInt safeInt2 = intInstance.acquire(4);
        testLoad(executor, safeInt2, 4);
        intInstance.release(safeInt2);
        SafeInt safeInt3 = intInstance.acquire(5);
        testLoad(executor, safeInt3, 5);
        intInstance.release(safeInt3);

        assertCacheState(cache, 0, 5, nodeSize(Integer.BYTES) * 3 + nodeSize(1) * 2);
        assertThat(stringInstance.size()).isEqualTo(2);
        assertThat(stringInstance.weightedSize()).isEqualTo(nodeSize(1) * 2);
        assertThat(stringInstance.capacity()).isEqualTo(cache.capacity());
        assertThat(intInstance.size()).isEqualTo(3);
        assertThat(intInstance.weightedSize()).isEqualTo(nodeSize(Integer.BYTES) * 3);
        assertThat(intInstance.capacity()).isEqualTo(cache.capacity());

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> stringInstance.setCapacity(123));
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> intInstance.setCapacity(123));
    }

    @Test
    public void testRotation()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, DEFAULT_NODE_SIZE * 5, cacheMetrics);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, SafeString.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[3];
        for (int i=0; i<3; i++)
        {
            SafeString safeString = instance.acquire(Integer.toString(i));
            items[i] = safeString;
            Assert.assertNotNull(safeString);
            testLoad(executor, safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
            instance.release(safeString);
        }

        Assert.assertSame(items[0].global, cache.head());
        Assert.assertSame(items[2].global, cache.tail());
        assertCacheState(cache, 0, 3, nodeSize(1) * 3);
        assertCacheMetrics(cache.metrics, 0, 3, 3);
        assertCacheMetrics(instance.instanceMetrics, 0, 3, 3);

        SafeString safeString = instance.acquire("1");
        Assert.assertEquals(Status.LOADED, safeString.globalStatus());

        assertCacheState(cache, 1, 3, nodeSize(1) * 3);
        assertCacheMetrics(cache.metrics, 1, 3, 4);
        assertCacheMetrics(instance.instanceMetrics, 1, 3, 4);

        // releasing item should return it to the tail
        instance.release(safeString);
        assertCacheState(cache, 0, 3, nodeSize(1) * 3);
        Assert.assertSame(items[0].global, cache.head());
        Assert.assertSame(items[1].global, cache.tail());
    }

    @Test
    public void testEvictionOnAcquire()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, nodeSize(1) * 5, cacheMetrics);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, SafeString.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[5];
        for (int i=0; i<5; i++)
        {
            SafeString safeString = instance.acquire(Integer.toString(i));
            items[i] = safeString;
            testLoad(executor, safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
            instance.release(safeString);
        }

        assertCacheState(cache, 0, 5, nodeSize(1) * 5);
        Assert.assertSame(items[0].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
        assertCacheMetrics(cache.metrics, 0, 5, 5);
        assertCacheMetrics(instance.instanceMetrics, 0, 5, 5);

        SafeString safeString = instance.acquire("5");
        Assert.assertTrue(instance.isReferenced(safeString.key()));

        // since it's not loaded, only the node size is counted here
        assertCacheState(cache, 1, 5, nodeSize(1) * 4 + nodeSize(0));
        Assert.assertSame(items[1].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
        Assert.assertFalse(instance.keyIsCached("0", SafeString.class));
        Assert.assertFalse(instance.keyIsReferenced("0", SafeString.class));
        assertCacheMetrics(cache.metrics, 0, 6, 6);
        assertCacheMetrics(instance.instanceMetrics, 0, 6, 6);

        testLoad(executor, safeString, "5");
        instance.release(safeString);
        assertCacheState(cache, 0, 5, nodeSize(1) * 5);
        Assert.assertSame(items[1].global, cache.head());
        Assert.assertSame(safeString.global, cache.tail());
        assertCacheMetrics(cache.metrics, 0, 6, 6);
        assertCacheMetrics(instance.instanceMetrics, 0, 6, 6);
    }

    @Test
    public void testEvictionOnRelease()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, nodeSize(1) * 4, cacheMetrics);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, SafeString.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[5];
        for (int i=0; i<5; i++)
        {
            SafeString safeString = instance.acquire(Integer.toString(i));
            items[i] = safeString;
            testLoad(executor, safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
        }

        assertCacheState(cache, 5, 5, nodeSize(0) * 5);
        assertCacheMetrics(cache.metrics, 0, 5, 5);
        assertCacheMetrics(instance.instanceMetrics, 0, 5, 5);
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        instance.release(items[2]);
        assertCacheState(cache, 4, 4, nodeSize(0) * 4);
        assertCacheMetrics(cache.metrics, 0, 5, 5);
        assertCacheMetrics(instance.instanceMetrics, 0, 5, 5);
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        instance.release(items[4]);
        assertCacheState(cache, 3, 4, nodeSize(0) * 3 + nodeSize(1));
        assertCacheMetrics(cache.metrics, 0, 5, 5);
        assertCacheMetrics(instance.instanceMetrics, 0, 5, 5);
        Assert.assertSame(items[4].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
    }

    @Test
    public void testMultiAcquireRelease()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, DEFAULT_NODE_SIZE * 4, cacheMetrics);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, SafeString.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString1 = instance.acquire("0");
        testLoad(executor, safeString1, "0");
        Assert.assertEquals(Status.LOADED, safeString1.globalStatus());
        assertCacheMetrics(cache.metrics, 0, 1, 1);
        assertCacheMetrics(instance.instanceMetrics, 0, 1, 1);

        Assert.assertEquals(1, instance.references("0", SafeString.class));
        assertCacheState(cache, 1, 1, nodeSize(0));

        SafeString safeString2 = instance.acquire("0");
        Assert.assertEquals(Status.LOADED, safeString1.globalStatus());
        Assert.assertEquals(2, instance.references("0", SafeString.class));
        assertCacheState(cache, 1, 1, nodeSize(0));
        assertCacheMetrics(cache.metrics, 1, 1, 2);
        assertCacheMetrics(instance.instanceMetrics, 1, 1, 2);

        instance.release(safeString1);
        assertCacheState(cache, 1, 1, nodeSize(1));
        instance.release(safeString2);
        assertCacheState(cache, 0, 1, nodeSize(1));
    }

    @Test
    public void evictionBlockedOnSaving()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, nodeSize(1) * 3 + nodeSize(3), cacheMetrics);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, SafeString.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString item = instance.acquire(Integer.toString(0));
        testLoad(executor, item, Integer.toString(0));
        item.set("0*");
        Assert.assertTrue(instance.isReferenced(item.key()));
        instance.release(item);

        for (int i=1; i<4; i++)
        {
            item = instance.acquire(Integer.toString(i));
            testLoad(executor, item, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(item.key()));
            instance.release(item);
        }

        assertCacheState(cache, 0, 4, nodeSize(1) * 3 + nodeSize(3));
        assertCacheMetrics(cache.metrics, 0, 4, 4);
        assertCacheMetrics(instance.instanceMetrics, 0, 4, 4);

        // force cache eviction
        cache.setCapacity(0);

        // all should have been evicted except 0
        assertCacheState(cache, 0, 1, nodeSize(2));

        Assert.assertTrue(instance.keyIsCached("0", SafeString.class));
        Assert.assertFalse(instance.keyIsCached("1", SafeString.class));
        Assert.assertFalse(instance.keyIsCached("2", SafeString.class));
        Assert.assertFalse(instance.keyIsCached("3", SafeString.class));
    }

    @Test
    public void testUpdates()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, 500, cacheMetrics);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, SafeString.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString = instance.acquire("1");
        testLoad(executor, safeString, "1");
        assertCacheState(cache, 1, 1, emptyNodeSize());
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        Assert.assertTrue(instance.isReferenced(safeString.key()));
        assertCacheState(cache, 1, 1, emptyNodeSize());

        safeString.set("11");
        instance.release(safeString);
        assertCacheState(cache, 0, 1, nodeSize(3));
        Assert.assertSame(safeString.global, cache.head());
        Assert.assertSame(safeString.global, cache.tail());

        assertCacheMetrics(cache.metrics, 0, 1, 1);
        assertCacheMetrics(instance.instanceMetrics, 0, 1, 1);
    }

    private CacheSize mockCacheSize(long capacity, long size, int entries)
    {
        CacheSize cacheSize = mock(CacheSize.class);
        when(cacheSize.capacity()).thenReturn(capacity);
        when(cacheSize.weightedSize()).thenReturn(size);
        when(cacheSize.size()).thenReturn(entries);
        return cacheSize;
    }

    @Test
    public void testAccorStateCacheMetrics()
    {
        CacheAccessMetrics stringInstance1 = cacheMetrics.forInstance(String.class);
        CacheAccessMetrics stringInstance1Dup = cacheMetrics.forInstance(String.class);
        CacheAccessMetrics stringInstance2 = cacheMetrics.forInstance(String.class);
        CacheAccessMetrics integerInstance1 = cacheMetrics.forInstance(Integer.class);
        CacheAccessMetrics integerInstance2 = cacheMetrics.forInstance(Integer.class);

        assertThat(stringInstance1).isSameAs(stringInstance1Dup);
        assertThat(stringInstance1).isSameAs(stringInstance2);
        assertThat(integerInstance1).isSameAs(integerInstance2);
        assertThat(stringInstance1).isNotSameAs(integerInstance1);
    }
}
