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

import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.service.accord.AccordLoadingState.LoadingState;

import static org.apache.cassandra.service.accord.AccordTestUtils.testLoad;

public class AccordStateCacheTest
{
    private static final long DEFAULT_NODE_SIZE = nodeSize(0);

    private static class SafeString implements AccordSafeState<String, String>
    {
        private boolean invalidated = false;
        private final AccordLoadingState<String, String> global;
        private String original = null;

        public SafeString(AccordLoadingState<String, String> global)
        {
            this.global = global;
        }

        public AccordLoadingState<String, String> global()
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
            return global.value();
        }

        @Override
        public void set(String update)
        {
            global.value(update);
        }

        @Override
        public String original()
        {
            return original;
        }

        @Override
        public void preExecute()
        {
            original = global.value();
        }

        @Override
        public void postExecute()
        {

        }

        @Override
        public LoadingState loadingState()
        {
            return global.state();
        }

        @Override
        public AsyncResults.RunnableResult<String> load(Function<String, String> loadFunction)
        {
            return global.load(loadFunction);
        }

        @Override
        public AsyncChain<?> listen()
        {
            return global.listen();
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
        return AccordStateCache.Node.EMPTY_SIZE;
    }

    private static long nodeSize(long itemSize)
    {
        return itemSize + emptyNodeSize();
    }

    private static void assertCacheState(AccordStateCache cache, int referencd, int total, long bytes)
    {
        Assert.assertEquals(referencd, cache.numReferencedEntries());
        Assert.assertEquals(total, cache.totalNumEntries());
        Assert.assertEquals(bytes, cache.bytesCached());
    }

    @Test
    public void testAcquisitionAndRelease()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<String, String, SafeString> instance = cache.instance(String.class, String.class, SafeString::new, String::length);
        assertCacheState(cache, 0, 0, 0);


        SafeString safeString1 = instance.reference("1");
        assertCacheState(cache, 1, 1, emptyNodeSize());
        testLoad(safeString1, "1");
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        instance.release(safeString1);
        assertCacheState(cache, 0, 1, nodeSize(1));
        Assert.assertSame(safeString1.global, cache.tail);
        Assert.assertSame(safeString1.global, cache.head);

        SafeString safeString2 = instance.reference("2");
        assertCacheState(cache, 1, 2, DEFAULT_NODE_SIZE + nodeSize(1));
        testLoad(safeString2, "2");
        instance.release(safeString2);
        assertCacheState(cache, 0, 2, nodeSize(1) + nodeSize(1));

        Assert.assertSame(safeString1.global, cache.tail);
        Assert.assertSame(safeString2.global, cache.head);
    }

    @Test
    public void testRotation()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 5);
        AccordStateCache.Instance<String, String, SafeString> instance = cache.instance(String.class, String.class, SafeString::new, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[3];
        for (int i=0; i<3; i++)
        {
            SafeString safeString = instance.reference(Integer.toString(i));
            items[i] = safeString;
            Assert.assertNotNull(safeString);
            testLoad(safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
            instance.release(safeString);
        }

        Assert.assertSame(items[0].global, cache.tail);
        Assert.assertSame(items[2].global, cache.head);
        assertCacheState(cache, 0, 3, nodeSize(1) * 3);

        SafeString safeString = instance.reference("1");
        Assert.assertEquals(LoadingState.LOADED, safeString.loadingState());

        assertCacheState(cache, 1, 3, nodeSize(1) * 3);

        // releasing item should return it to the head
        instance.release(safeString);
        assertCacheState(cache, 0, 3, nodeSize(1) * 3);
        Assert.assertSame(items[0].global, cache.tail);
        Assert.assertSame(items[1].global, cache.head);
    }

    @Test
    public void testEvictionOnAcquire()
    {
        AccordStateCache cache = new AccordStateCache(nodeSize(1) * 5);
        AccordStateCache.Instance<String, String, SafeString> instance = cache.instance(String.class, String.class, SafeString::new, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[5];
        for (int i=0; i<5; i++)
        {
            SafeString safeString = instance.reference(Integer.toString(i));
            items[i] = safeString;
            testLoad(safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
            instance.release(safeString);
        }

        assertCacheState(cache, 0, 5, nodeSize(1) * 5);
        Assert.assertSame(items[0].global, cache.tail);
        Assert.assertSame(items[4].global, cache.head);

        SafeString safeString = instance.reference("5");
        Assert.assertTrue(instance.isReferenced(safeString.key()));

        // since it's not loaded, only the node size is counted here
        assertCacheState(cache, 1, 5, nodeSize(1) * 4 + nodeSize(0));
        Assert.assertSame(items[1].global, cache.tail);
        Assert.assertSame(items[4].global, cache.head);
        Assert.assertFalse(cache.keyIsCached("0"));
        Assert.assertFalse(cache.keyIsReferenced("0"));

        testLoad(safeString, "5");
        instance.release(safeString);
        assertCacheState(cache, 0, 5, nodeSize(1) * 5);
        Assert.assertSame(items[1].global, cache.tail);
        Assert.assertSame(safeString.global, cache.head);
    }

    @Test
    public void testEvictionOnRelease()
    {
        AccordStateCache cache = new AccordStateCache(nodeSize(1) * 4);
        AccordStateCache.Instance<String, String, SafeString> instance = cache.instance(String.class, String.class, SafeString::new, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[5];
        for (int i=0; i<5; i++)
        {
            SafeString safeString = instance.reference(Integer.toString(i));
            items[i] = safeString;
            testLoad(safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
        }

        assertCacheState(cache, 5, 5, nodeSize(0) * 5);
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        instance.release(items[2]);
        assertCacheState(cache, 4, 4, nodeSize(0) * 4);
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        instance.release(items[4]);
        assertCacheState(cache, 3, 4, nodeSize(0) * 3 + nodeSize(1));
        Assert.assertSame(items[4].global, cache.tail);
        Assert.assertSame(items[4].global, cache.head);
    }

    @Test
    public void testMultiAcquireRelease()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 4);
        AccordStateCache.Instance<String, String, SafeString> instance = cache.instance(String.class, String.class, SafeString::new, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString1 = instance.reference("0");
        testLoad(safeString1, "0");
        Assert.assertEquals(LoadingState.LOADED, safeString1.loadingState());

        Assert.assertEquals(1, cache.references("0"));
        assertCacheState(cache, 1, 1, nodeSize(0));

        SafeString safeString2 = instance.reference("0");
        Assert.assertEquals(LoadingState.LOADED, safeString1.loadingState());
        Assert.assertEquals(2, cache.references("0"));
        assertCacheState(cache, 1, 1, nodeSize(0));

        instance.release(safeString1);
        assertCacheState(cache, 1, 1, nodeSize(1));
        instance.release(safeString2);
        assertCacheState(cache, 0, 1, nodeSize(1));
    }

    @Test
    public void evictionBlockedOnSaveFuture()
    {
        AccordStateCache cache = new AccordStateCache(nodeSize(1) * 4);
        AccordStateCache.Instance<String, String, SafeString> instance = cache.instance(String.class, String.class, SafeString::new, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[4];
        for (int i=0; i<4; i++)
        {
            SafeString item = instance.reference(Integer.toString(i));
            testLoad(item, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(item.key()));
            instance.release(item);
        }

        assertCacheState(cache, 0, 4, nodeSize(1) * 4);

        AsyncResult<Void> saveFuture = AsyncResults.settable();
        instance.addSaveResult("0", saveFuture);
        cache.setMaxSize(0);

        // all should have been evicted except 0
        assertCacheState(cache, 0, 1, nodeSize(1));
        Assert.assertTrue(cache.keyIsCached("0"));
        Assert.assertFalse(cache.keyIsCached("1"));
        Assert.assertFalse(cache.keyIsCached("2"));
        Assert.assertFalse(cache.keyIsCached("3"));
    }

    // if a future is added and another one exists for the same key, they should be merged
    @Test
    public void testFutureMerging()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<String, String, SafeString> instance = cache.instance(String.class, String.class, SafeString::new, String::length);
        AsyncResult.Settable<Void> promise1 = AsyncResults.settable();
        AsyncResult.Settable<Void> promise2 = AsyncResults.settable();
        instance.addSaveResult("5", promise1);
        instance.addSaveResult("5", promise2);

        AsyncResult<?> future = instance.getSaveResult("5");
        Assert.assertNotSame(future, promise1);
        Assert.assertNotSame(future, promise2);

        Assert.assertFalse(future.isDone());

        promise1.setSuccess(null);
        Assert.assertFalse(future.isDone());

        promise2.setSuccess(null);
        Assert.assertTrue(future.isDone());
    }

    @Test
    public void testUpdates()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<String, String, SafeString> instance = cache.instance(String.class, String.class, SafeString::new, String::length);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString = instance.reference("1");
        testLoad(safeString, "1");
        assertCacheState(cache, 1, 1, emptyNodeSize());
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        Assert.assertTrue(instance.isReferenced(safeString.key()));
        assertCacheState(cache, 1, 1, nodeSize(0));

        safeString.set("11");
        instance.release(safeString);
        assertCacheState(cache, 0, 1, nodeSize(2));
        Assert.assertSame(safeString.global, cache.tail);
        Assert.assertSame(safeString.global, cache.head);

    }
}
