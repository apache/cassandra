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
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import accord.utils.async.Cancellable;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ManualExecutor;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.metrics.CacheAccessMetrics;
import org.apache.cassandra.service.accord.AccordCacheEntry.OnSaved;
import org.apache.cassandra.service.accord.AccordCacheEntry.Status;

import static org.apache.cassandra.service.accord.AccordTestUtils.testLoad;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccordCacheTest
{
    private static final long DEFAULT_NODE_SIZE = nodeSize(0);
    private AccordCacheMetrics cacheMetrics;

    private static abstract class TestSafeState<T> implements AccordSafeState<T, T>
    {
        protected boolean invalidated = false;
        protected final AccordCacheEntry<T, T> global;
        private T original = null;

        public TestSafeState(AccordCacheEntry<T, T> global)
        {
            this.global = global;
        }

        public AccordCacheEntry<T, T> global()
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
            return global.getExclusive();
        }

        @Override
        public void set(T update)
        {
            global.setExclusive(update);
        }

        @Override
        public T original()
        {
            return original;
        }

        @Override
        public void preExecute()
        {
            original = global.getExclusive();
        }

        @Override
        public Cancellable saving()
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
        public SafeString(AccordCacheEntry<String, String> global)
        {
            super(global);
        }
    }

    private static class SafeInt extends TestSafeState<Integer>
    {
        public SafeInt(AccordCacheEntry<Integer, Integer> global)
        {
            super(global);
        }
    }

    private static long emptyNodeSize()
    {
        return AccordCacheEntry.EMPTY_SIZE;
    }

    private static long nodeSize(long itemSize)
    {
        return itemSize + emptyNodeSize();
    }

    private static void assertCacheState(AccordCache cache, int referenced, int total, long bytes)
    {
        Assert.assertEquals(referenced, cache.numReferencedEntries());
        Assert.assertEquals(total, cache.size());
        Assert.assertEquals(bytes, cache.weightedSize());
    }

    private void assertCacheMetrics(CacheAccessMetrics metrics, int hits, int misses, int requests, int sizes)
    {
        Assert.assertEquals(hits, metrics.hits.getCount());
        Assert.assertEquals(misses, metrics.misses.getCount());
        Assert.assertEquals(requests, metrics.requests.getCount());
        if (metrics instanceof AccordCacheMetrics)
        {
            AccordCacheMetrics ascMetrics = (AccordCacheMetrics) metrics;
            Assert.assertEquals(sizes, ascMetrics.objectSize.getCount());
            assertThat(ascMetrics.objectSize.getSnapshot().getMax()).isGreaterThanOrEqualTo(DEFAULT_NODE_SIZE);
        }
    }

    @Before
    public void before()
    {
        String type = String.format("%s-%s", AccordCommandStores.ACCORD_STATE_CACHE, UUID.randomUUID());
        cacheMetrics = new AccordCacheMetrics(type);
    }
    
    @Test
    public void testAcquisitionAndRelease()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(wrap(executor), OnSaved.immediate(), 500, cacheMetrics);
        AccordCache.Type<String, String, SafeString> type =
            cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString1 = instance.acquire("1");
        assertCacheState(cache, 1, 1, emptyNodeSize());
        testLoad(executor, instance, safeString1, "1");
        Assert.assertTrue(!cache.evictionQueue().iterator().hasNext());

        instance.release(safeString1, null);
        assertCacheState(cache, 0, 1, nodeSize(1));
        Assert.assertSame(safeString1.global, cache.head());
        Assert.assertSame(safeString1.global, cache.tail());

        SafeString safeString2 = instance.acquire("2");
        assertCacheState(cache, 1, 2, DEFAULT_NODE_SIZE + nodeSize(1));
        testLoad(executor, instance, safeString2, "2");
        instance.release(safeString2, null);
        assertCacheState(cache, 0, 2, nodeSize(1) + nodeSize(1));

        Assert.assertSame(safeString1.global, cache.head());
        Assert.assertSame(safeString2.global, cache.tail());

        assertCacheMetrics(cache.metrics, 0, 2, 2, 2);
        assertCacheMetrics(type.typeMetrics, 0, 2, 2, 2);
    }

    @Test
    public void testCachingMetricsWithTwoInstances()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(wrap(executor), OnSaved.immediate(), 500, cacheMetrics);
        AccordCache.Type<String, String, SafeString> stringType =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new);
        AccordCache.Type<String, String, SafeString>.Instance stringInstance = stringType.newInstance(null);
        AccordCache.Type<Integer, Integer, SafeInt> intType =
        cache.newType(Integer.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, ignore -> Integer.BYTES, SafeInt::new);
        assertCacheState(cache, 0, 0, 0);
        AccordCache.Type<Integer, Integer, SafeInt>.Instance intInstance = intType.newInstance(null);

        SafeString safeString1 = stringInstance.acquire("1");
        testLoad(executor, stringInstance, safeString1, "1");
        stringInstance.release(safeString1, null);
        SafeString safeString2 = stringInstance.acquire("2");
        testLoad(executor, stringInstance, safeString2, "2");
        stringInstance.release(safeString2, null);

        SafeInt safeInt1 = intInstance.acquire(3);
        testLoad(executor, intInstance, safeInt1, 3);
        intInstance.release(safeInt1, null);
        SafeInt safeInt2 = intInstance.acquire(4);
        testLoad(executor, intInstance, safeInt2, 4);
        intInstance.release(safeInt2, null);
        SafeInt safeInt3 = intInstance.acquire(5);
        testLoad(executor, intInstance, safeInt3, 5);
        intInstance.release(safeInt3, null);

        assertCacheState(cache, 0, 5, nodeSize(Integer.BYTES) * 3 + nodeSize(1) * 2);
        assertThat(stringType.size()).isEqualTo(2);
        assertThat(stringType.weightedSize()).isEqualTo(nodeSize(1) * 2);
        assertThat(stringType.capacity()).isEqualTo(cache.capacity());
        assertThat(intType.size()).isEqualTo(3);
        assertThat(intType.weightedSize()).isEqualTo(nodeSize(Integer.BYTES) * 3);
        assertThat(intType.capacity()).isEqualTo(cache.capacity());

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> stringType.setCapacity(123));
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> intType.setCapacity(123));
    }

    @Test
    public void testRotation()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(wrap(executor), OnSaved.immediate(), DEFAULT_NODE_SIZE * 5, cacheMetrics);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new);
        assertCacheState(cache, 0, 0, 0);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);

        SafeString[] items = new SafeString[3];
        for (int i=0; i<3; i++)
        {
            SafeString safeString = instance.acquire(Integer.toString(i));
            items[i] = safeString;
            Assert.assertNotNull(safeString);
            testLoad(executor, instance, safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
            instance.release(safeString, null);
        }

        Assert.assertSame(items[0].global, cache.head());
        Assert.assertSame(items[2].global, cache.tail());
        assertCacheState(cache, 0, 3, nodeSize(1) * 3);
        assertCacheMetrics(cache.metrics, 0, 3, 3, 3);
        assertCacheMetrics(type.typeMetrics, 0, 3, 3, 3);

        SafeString safeString = instance.acquire("1");
        Assert.assertEquals(Status.LOADED, safeString.global.status());

        assertCacheState(cache, 1, 3, nodeSize(1) * 3);
        assertCacheMetrics(cache.metrics, 1, 3, 4, 3);
        assertCacheMetrics(type.typeMetrics, 1, 3, 4, 3);

        // releasing item should return it to the tail
        instance.release(safeString, null);
        assertCacheState(cache, 0, 3, nodeSize(1) * 3);
        Assert.assertSame(items[0].global, cache.head());
        Assert.assertSame(items[1].global, cache.tail());
    }

    @Test
    public void testEvictionOnAcquire()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(wrap(executor), OnSaved.immediate(), nodeSize(1) * 5, cacheMetrics);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[5];
        for (int i=0; i<5; i++)
        {
            SafeString safeString = instance.acquire(Integer.toString(i));
            items[i] = safeString;
            testLoad(executor, instance, safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
            instance.release(safeString, null);
        }

        assertCacheState(cache, 0, 5, nodeSize(1) * 5);
        Assert.assertSame(items[0].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
        assertCacheMetrics(cache.metrics, 0, 5, 5, 5);
        assertCacheMetrics(type.typeMetrics, 0, 5, 5, 5);

        SafeString safeString = instance.acquire("5");
        Assert.assertTrue(instance.isReferenced(safeString.key()));

        // since it's not loaded, only the node size is counted here
        assertCacheState(cache, 1, 5, nodeSize(1) * 4 + nodeSize(0));
        Assert.assertSame(items[1].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
        Assert.assertFalse(instance.keyIsCached("0", SafeString.class));
        Assert.assertFalse(instance.keyIsReferenced("0", SafeString.class));
        assertCacheMetrics(cache.metrics, 0, 6, 6, 5);
        assertCacheMetrics(type.typeMetrics, 0, 6, 6, 5);

        testLoad(executor, instance, safeString, "5");
        instance.release(safeString, null);
        assertCacheState(cache, 0, 5, nodeSize(1) * 5);
        Assert.assertSame(items[1].global, cache.head());
        Assert.assertSame(safeString.global, cache.tail());
        assertCacheMetrics(cache.metrics, 0, 6, 6, 6);
        assertCacheMetrics(type.typeMetrics, 0, 6, 6, 6);
    }

    @Test
    public void testEvictionOnRelease()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(wrap(executor), OnSaved.immediate(), nodeSize(1) * 4, cacheMetrics);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[5];
        for (int i=0; i<5; i++)
        {
            SafeString safeString = instance.acquire(Integer.toString(i));
            items[i] = safeString;
            testLoad(executor, instance, safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
        }

        assertCacheState(cache, 5, 5, nodeSize(1) * 5);
        assertCacheMetrics(cache.metrics, 0, 5, 5, 5);
        assertCacheMetrics(type.typeMetrics, 0, 5, 5, 5);
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        instance.release(items[2], null);
        assertCacheState(cache, 4, 4, nodeSize(1) * 4);
        assertCacheMetrics(cache.metrics, 0, 5, 5, 5);
        assertCacheMetrics(type.typeMetrics, 0, 5, 5, 5);
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        instance.release(items[4], null);
        assertCacheState(cache, 3, 4, nodeSize(1) * 4);
        assertCacheMetrics(cache.metrics, 0, 5, 5, 5);
        assertCacheMetrics(type.typeMetrics, 0, 5, 5, 5);
        Assert.assertSame(items[4].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
    }

    @Test
    public void testMultiAcquireRelease()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(wrap(executor), OnSaved.immediate(), DEFAULT_NODE_SIZE * 4, cacheMetrics);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString1 = instance.acquire("0");
        testLoad(executor, instance, safeString1, "0");
        Assert.assertEquals(Status.LOADED, safeString1.global.status());
        assertCacheMetrics(cache.metrics, 0, 1, 1, 1);
        assertCacheMetrics(type.typeMetrics, 0, 1, 1, 1);

        Assert.assertEquals(1, instance.references("0", SafeString.class));
        assertCacheState(cache, 1, 1, nodeSize(1));

        SafeString safeString2 = instance.acquire("0");
        Assert.assertEquals("0", safeString2.current());
        Assert.assertEquals(Status.LOADED, safeString1.global.status());
        Assert.assertEquals(2, instance.references("0", SafeString.class));
        assertCacheState(cache, 1, 1, nodeSize(1));
        assertCacheMetrics(cache.metrics, 1, 1, 2, 1);
        assertCacheMetrics(type.typeMetrics, 1, 1, 2, 1);

        instance.release(safeString1, null);
        assertCacheState(cache, 1, 1, nodeSize(1));
        instance.release(safeString2, null);
        assertCacheState(cache, 0, 1, nodeSize(1));
    }

    @Test
    public void evictionBlockedOnSaving()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(wrap(executor), OnSaved.immediate(), nodeSize(1) * 3 + nodeSize(3), cacheMetrics);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString item = instance.acquire(Integer.toString(0));
        testLoad(executor, instance, item, Integer.toString(0));
        item.set("0*");
        Assert.assertTrue(instance.isReferenced(item.key()));
        instance.release(item, null);

        for (int i=1; i<4; i++)
        {
            item = instance.acquire(Integer.toString(i));
            testLoad(executor, instance, item, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(item.key()));
            instance.release(item, null);
        }

        assertCacheState(cache, 0, 4, nodeSize(1) * 3 + nodeSize(2));
        assertCacheMetrics(cache.metrics, 0, 4, 4, 5);
        assertCacheMetrics(type.typeMetrics, 0, 4, 4, 5);

        // force cache eviction
        instance.acquire(Integer.toString(0));
        cache.setCapacity(0);

        // all should have been evicted except 0
        assertCacheState(cache, 1, 1, nodeSize(2));

        Assert.assertTrue(instance.keyIsCached("0", SafeString.class));
        Assert.assertFalse(instance.keyIsCached("1", SafeString.class));
        Assert.assertFalse(instance.keyIsCached("2", SafeString.class));
        Assert.assertFalse(instance.keyIsCached("3", SafeString.class));
    }

    @Test
    public void testUpdates()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(wrap(executor), OnSaved.immediate(), 500, cacheMetrics);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString = instance.acquire("1");
        testLoad(executor, instance, safeString, "1");
        assertCacheState(cache, 1, 1, nodeSize(1));
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        Assert.assertTrue(instance.isReferenced(safeString.key()));
        assertCacheState(cache, 1, 1, nodeSize(1));

        safeString.set("11");
        instance.release(safeString, null);
        assertCacheState(cache, 0, 1, nodeSize(2));
        Assert.assertSame(safeString.global, cache.head());
        Assert.assertSame(safeString.global, cache.tail());

        assertCacheMetrics(cache.metrics, 0, 1, 1, 2);
        assertCacheMetrics(type.typeMetrics, 0, 1, 1, 2);
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
    
    private static Function<Runnable, Cancellable> wrap(ExecutorPlus executor)
    {
        return r -> AccordExecutor.wrap(executor.submit(r));
    }
}
