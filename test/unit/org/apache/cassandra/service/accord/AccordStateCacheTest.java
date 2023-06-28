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

import org.junit.Assert;
import org.junit.Test;

import accord.utils.async.AsyncChain;
import org.apache.cassandra.concurrent.ManualExecutor;
import org.apache.cassandra.service.accord.AccordCachingState.Status;

import static org.apache.cassandra.service.accord.AccordTestUtils.testLoad;

public class AccordStateCacheTest
{
    private static final long DEFAULT_NODE_SIZE = nodeSize(0);

    private static class SafeString implements AccordSafeState<String, String>
    {
        private boolean invalidated = false;
        private final AccordCachingState<String, String> global;
        private String original = null;

        public SafeString(AccordCachingState<String, String> global)
        {
            this.global = global;
        }

        public AccordCachingState<String, String> global()
        {
            return global;
        }

        @Override
        public String key()
        {
            return global.key();
        }

        @Override
        public String current()
        {
            return global.get();
        }

        @Override
        public void set(String update)
        {
            global.set(update);
        }

        @Override
        public String original()
        {
            return original;
        }

        @Override
        public void preExecute()
        {
            original = global.get();
        }

        @Override
        public void postExecute()
        {
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
        Assert.assertEquals(total, cache.totalNumEntries());
        Assert.assertEquals(bytes, cache.bytesCached());
    }

    @Test
    public void testAcquisitionAndRelease()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor,500);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, String.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
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
    }

    @Test
    public void testRotation()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, DEFAULT_NODE_SIZE * 5);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, String.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
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

        SafeString safeString = instance.acquire("1");
        Assert.assertEquals(Status.LOADED, safeString.globalStatus());

        assertCacheState(cache, 1, 3, nodeSize(1) * 3);

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
        AccordStateCache cache = new AccordStateCache(executor, executor, nodeSize(1) * 5);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, String.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
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

        SafeString safeString = instance.acquire("5");
        Assert.assertTrue(instance.isReferenced(safeString.key()));

        // since it's not loaded, only the node size is counted here
        assertCacheState(cache, 1, 5, nodeSize(1) * 4 + nodeSize(0));
        Assert.assertSame(items[1].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
        Assert.assertFalse(cache.keyIsCached("0"));
        Assert.assertFalse(cache.keyIsReferenced("0"));

        testLoad(executor, safeString, "5");
        instance.release(safeString);
        assertCacheState(cache, 0, 5, nodeSize(1) * 5);
        Assert.assertSame(items[1].global, cache.head());
        Assert.assertSame(safeString.global, cache.tail());
    }

    @Test
    public void testEvictionOnRelease()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, nodeSize(1) * 4);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, String.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
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
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        instance.release(items[2]);
        assertCacheState(cache, 4, 4, nodeSize(0) * 4);
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        instance.release(items[4]);
        assertCacheState(cache, 3, 4, nodeSize(0) * 3 + nodeSize(1));
        Assert.assertSame(items[4].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
    }

    @Test
    public void testMultiAcquireRelease()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, DEFAULT_NODE_SIZE * 4);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, String.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString1 = instance.acquire("0");
        testLoad(executor, safeString1, "0");
        Assert.assertEquals(Status.LOADED, safeString1.globalStatus());

        Assert.assertEquals(1, cache.references("0"));
        assertCacheState(cache, 1, 1, nodeSize(0));

        SafeString safeString2 = instance.acquire("0");
        Assert.assertEquals(Status.LOADED, safeString1.globalStatus());
        Assert.assertEquals(2, cache.references("0"));
        assertCacheState(cache, 1, 1, nodeSize(0));

        instance.release(safeString1);
        assertCacheState(cache, 1, 1, nodeSize(1));
        instance.release(safeString2);
        assertCacheState(cache, 0, 1, nodeSize(1));
    }

    @Test
    public void evictionBlockedOnSaving()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, nodeSize(1) * 3 + nodeSize(3));
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, String.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
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

        // force cache eviction
        cache.setMaxSize(0);

        // all should have been evicted except 0
        assertCacheState(cache, 0, 1, nodeSize(2));

        Assert.assertTrue(cache.keyIsCached("0"));
        Assert.assertFalse(cache.keyIsCached("1"));
        Assert.assertFalse(cache.keyIsCached("2"));
        Assert.assertFalse(cache.keyIsCached("3"));
    }

    @Test
    public void testUpdates()
    {
        ManualExecutor executor = new ManualExecutor();
        AccordStateCache cache = new AccordStateCache(executor, executor, 500);
        AccordStateCache.Instance<String, String, SafeString> instance =
            cache.instance(String.class, String.class, SafeString::new, key -> key, (original, current) -> null, (k, v) -> true, String::length);
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
    }
}
