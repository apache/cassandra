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
package org.apache.cassandra.service.accord.execution;

import java.util.function.Function;

import org.agrona.concurrent.NoOpLock;
import org.junit.Assert;
import org.junit.Test;

import accord.local.ExecutionContext;
import accord.local.SafeState;

import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ManualExecutor;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.SaveExecutor;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status;

import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.RELEASE_QUEUE;
import static org.apache.cassandra.service.accord.execution.AccordExecutionTestUtils.testLoad;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccordCacheTest
{
    @org.junit.After
    public void clearOwners()
    {
        // AccordExecutionTestUtils.owners is static, so without this it retains every safe state and task this JVM created
        AccordExecutionTestUtils.clearOwners();
    }

    private static final long DEFAULT_NODE_SIZE = nodeSize(0);

    private static abstract class TestSafeState<T, S extends SafeState<T> & SaferState<T, T, S>> extends SafeState<T> implements SaferState<T, T, S>
    {
        protected final AccordCacheEntry<T, T, S> global;

        public TestSafeState(AccordCacheEntry<T, T, S> global)
        {
            this.global = global;
        }

        public AccordCacheEntry<T, T, S> global()
        {
            return global;
        }

        public final T key() { return global.key(); }

        public void preExecute(SafeTask<?> owner, LockMode lockMode)
        {
            requireUninitialised();
            current = global.lockExclusive(owner, lockMode);
            setSafe();
        }
    }

    private static class SafeString extends TestSafeState<String, SafeString>
    {
        public SafeString(AccordCacheEntry<String, String, SafeString> global)
        {
            super(global);
        }

        @Override
        public void postExecute(SafeTask<?> owner)
        {
            global.releaseExclusive(this, owner);
        }
    }

    private static class SafeInt extends TestSafeState<Integer, SafeInt>
    {
        public SafeInt(AccordCacheEntry<Integer, Integer, SafeInt> global)
        {
            super(global);
        }

        @Override
        public void postExecute(SafeTask<?> owner)
        {
            global.releaseExclusive(this, owner);
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

    private static int nextMetricId;
    private static String nextMetricId()
    {
        return Integer.toString(++nextMetricId);
    }

    private static void assertCacheState(AccordCache cache, int referenced, int total, long bytes)
    {
        Assert.assertEquals(referenced, cache.numReferencedEntries());
        Assert.assertEquals(total, cache.size());
        Assert.assertEquals(bytes, cache.weightedSize());
    }

    private void assertCacheMetrics(AccordCacheMetrics metrics, int hits, int misses, int requests, int sizes)
    {
        metrics.hitRate.refresh();
        Assert.assertEquals(hits, metrics.hits.getValue().intValue());
        Assert.assertEquals(misses, metrics.misses.getValue().intValue());
        Assert.assertEquals(requests, metrics.requests.getValue().intValue());
        Assert.assertEquals(sizes, metrics.objectSize.getCount());
    }

    @Test
    public void testAcquisitionAndRelease()
    {
        AccordCacheMetrics cacheMetrics = new AccordCacheMetrics(nextMetricId());
        AccordCacheMetrics.Shard shard = cacheMetrics.newShard(new NoOpLock());

        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(saveExecutor(executor), 500);
        AccordCache.Type<String, String, SafeString> type =
            cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new, shard);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString1 = instance.acquire("1");
        assertCacheState(cache, 1, 1, emptyNodeSize());
        testLoad(executor, safeString1, "1");
        Assert.assertTrue(!cache.evictionQueue().iterator().hasNext());

        instance.release(safeString1, AccordExecutionTestUtils.owner(safeString1));
        assertCacheState(cache, 0, 1, nodeSize(1));
        Assert.assertSame(safeString1.global, cache.head());
        Assert.assertSame(safeString1.global, cache.tail());

        SafeString safeString2 = instance.acquire("2");
        assertCacheState(cache, 1, 2, DEFAULT_NODE_SIZE + nodeSize(1));
        testLoad(executor, safeString2, "2");
        instance.release(safeString2, AccordExecutionTestUtils.owner(safeString2));
        assertCacheState(cache, 0, 2, nodeSize(1) + nodeSize(1));

        Assert.assertSame(safeString1.global, cache.head());
        Assert.assertSame(safeString2.global, cache.tail());

        assertCacheMetrics(cacheMetrics, 0, 2, 2, 2);
    }

    @Test
    public void testCachingMetricsWithTwoInstances()
    {
        AccordCacheMetrics cacheMetrics = new AccordCacheMetrics(nextMetricId());
        AccordCacheMetrics.Shard shard = cacheMetrics.newShard(new NoOpLock());
        AccordCacheMetrics.Shard shard2 = cacheMetrics.newShard(new NoOpLock());

        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(saveExecutor(executor), 500);
        AccordCache.Type<String, String, SafeString> stringType =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new, shard);
        AccordCache.Type<String, String, SafeString>.Instance stringInstance = stringType.newInstance(null);
        AccordCache.Type<Integer, Integer, SafeInt> intType =
        cache.newType(Integer.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, ignore -> Integer.BYTES, SafeInt::new, shard2);
        assertCacheState(cache, 0, 0, 0);
        AccordCache.Type<Integer, Integer, SafeInt>.Instance intInstance = intType.newInstance(null);

        SafeString safeString1 = stringInstance.acquire("1");
        testLoad(executor, safeString1, "1");
        stringInstance.release(safeString1, AccordExecutionTestUtils.owner(safeString1));
        SafeString safeString2 = stringInstance.acquire("2");
        testLoad(executor, safeString2, "2");
        stringInstance.release(safeString2, AccordExecutionTestUtils.owner(safeString2));

        SafeInt safeInt1 = intInstance.acquire(3);
        testLoad(executor, safeInt1, 3);
        intInstance.release(safeInt1, AccordExecutionTestUtils.owner(safeInt1));
        SafeInt safeInt2 = intInstance.acquire(4);
        testLoad(executor, safeInt2, 4);
        intInstance.release(safeInt2, AccordExecutionTestUtils.owner(safeInt2));
        SafeInt safeInt3 = intInstance.acquire(5);
        testLoad(executor, safeInt3, 5);
        intInstance.release(safeInt3, AccordExecutionTestUtils.owner(safeInt3));

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
        AccordCacheMetrics cacheMetrics = new AccordCacheMetrics(nextMetricId());
        AccordCacheMetrics.Shard shard = cacheMetrics.newShard(new NoOpLock());

        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(saveExecutor(executor), DEFAULT_NODE_SIZE * 5);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new, shard);
        assertCacheState(cache, 0, 0, 0);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);

        SafeString[] items = new SafeString[3];
        for (int i=0; i<3; i++)
        {
            SafeString safeString = instance.acquire(Integer.toString(i));
            items[i] = safeString;
            Assert.assertNotNull(safeString);
            testLoad(executor, safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
            instance.release(safeString, AccordExecutionTestUtils.owner(safeString));
        }

        Assert.assertSame(items[0].global, cache.head());
        Assert.assertSame(items[2].global, cache.tail());
        assertCacheState(cache, 0, 3, nodeSize(1) * 3);
        assertCacheMetrics(cacheMetrics, 0, 3, 3, 3);

        SafeString safeString = instance.acquire("1");
        AccordExecutionTestUtils.preExecute(safeString);
        Assert.assertEquals(Status.LOADED, safeString.global.status());

        assertCacheState(cache, 1, 3, nodeSize(1) * 3);
        assertCacheMetrics(cacheMetrics, 1, 3, 4, 3);

        // releasing item should return it to the tail
        instance.release(safeString, AccordExecutionTestUtils.owner(safeString));
        assertCacheState(cache, 0, 3, nodeSize(1) * 3);
        Assert.assertSame(items[0].global, cache.head());
        Assert.assertSame(items[1].global, cache.tail());
    }

    @Test
    public void testEvictionOnAcquire()
    {
        AccordCacheMetrics cacheMetrics = new AccordCacheMetrics(nextMetricId());
        NoOpLock lock = new NoOpLock();
        AccordCacheMetrics.Shard shard = cacheMetrics.newShard(lock);

        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(saveExecutor(executor), nodeSize(1) * 5);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new, shard);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[5];
        for (int i=0; i<5; i++)
        {
            SafeString safeString = instance.acquire(Integer.toString(i));
            items[i] = safeString;
            testLoad(executor, safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
            instance.release(safeString, AccordExecutionTestUtils.owner(safeString));
            cache.tryShrinkOrEvict(lock);
        }

        assertCacheState(cache, 0, 5, nodeSize(1) * 5);
        Assert.assertSame(items[0].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
        assertCacheMetrics(cacheMetrics, 0, 5, 5, 5);

        SafeString safeString = instance.acquire("5");
        cache.tryShrinkOrEvict(lock);
        Assert.assertTrue(instance.isReferenced(safeString.key()));

        // since it's not loaded, only the node size is counted here
        assertCacheState(cache, 1, 5, nodeSize(1) * 4 + nodeSize(0));
        Assert.assertSame(items[1].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
        Assert.assertFalse(instance.keyIsCached("0", SafeString.class));
        Assert.assertFalse(instance.keyIsReferenced("0", SafeString.class));
        assertCacheMetrics(cacheMetrics, 0, 6, 6, 5);

        testLoad(executor, safeString, "5");
        instance.release(safeString, AccordExecutionTestUtils.owner(safeString));
        assertCacheState(cache, 0, 5, nodeSize(1) * 5);
        Assert.assertSame(items[1].global, cache.head());
        Assert.assertSame(safeString.global, cache.tail());
        assertCacheMetrics(cacheMetrics, 0, 6, 6, 5);
    }

    @Test
    public void testEvictionOnRelease()
    {
        AccordCacheMetrics cacheMetrics = new AccordCacheMetrics(nextMetricId());
        NoOpLock lock = new NoOpLock();
        AccordCacheMetrics.Shard shard = cacheMetrics.newShard(lock);

        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(saveExecutor(executor), nodeSize(1) * 4);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new, shard);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString[] items = new SafeString[5];
        for (int i=0; i<5; i++)
        {
            SafeString safeString = instance.acquire(Integer.toString(i));
            items[i] = safeString;
            testLoad(executor, safeString, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(safeString.key()));
            cache.tryShrinkOrEvict(lock);
        }

        assertCacheState(cache, 5, 5, nodeSize(1) * 5);
        assertCacheMetrics(cacheMetrics, 0, 5, 5, 5);
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        instance.release(items[2], AccordExecutionTestUtils.owner(items[2]));
        cache.tryShrinkOrEvict(lock);
        assertCacheState(cache, 4, 4, nodeSize(1) * 4);
        assertCacheMetrics(cacheMetrics, 0, 5, 5, 5);
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        instance.release(items[4], AccordExecutionTestUtils.owner(items[4]));
        cache.tryShrinkOrEvict(lock);
        assertCacheState(cache, 3, 4, nodeSize(1) * 4);
        assertCacheMetrics(cacheMetrics, 0, 5, 5, 5);
        Assert.assertSame(items[4].global, cache.head());
        Assert.assertSame(items[4].global, cache.tail());
    }

    @Test
    public void testMultiAcquireRelease()
    {
        AccordCacheMetrics cacheMetrics = new AccordCacheMetrics(nextMetricId());
        AccordCacheMetrics.Shard shard = cacheMetrics.newShard(new NoOpLock());

        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(saveExecutor(executor), DEFAULT_NODE_SIZE * 4);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new, shard);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString1 = instance.acquire("0");
        testLoad(executor, safeString1, "0");
        Assert.assertEquals(Status.LOADED, safeString1.global.status());
        assertCacheMetrics(cacheMetrics, 0, 1, 1, 1);

        Assert.assertEquals(1, instance.references("0", SafeString.class));
        assertCacheState(cache, 1, 1, nodeSize(1));

        SafeString safeString2 = instance.acquire("0");
        // a second reference sees the same loaded value, but cannot lock the entry while the first reference holds it
        Assert.assertEquals("0", safeString2.global.getExclusive());
        Assert.assertEquals(Status.LOADED, safeString1.global.status());
        Assert.assertEquals(2, instance.references("0", SafeString.class));
        assertCacheState(cache, 1, 1, nodeSize(1));
        assertCacheMetrics(cacheMetrics, 1, 1, 2, 1);

        // release the locked reference first: an entry cannot be released through another reference while it is locked
        instance.release(safeString1, AccordExecutionTestUtils.owner(safeString1));
        assertCacheState(cache, 1, 1, nodeSize(1));
        // safeString2 never locked the entry - it could not, while safeString1 held it - so it has no owner to release
        // it as. This is the one release here that is legitimately owner-less; owner() throws rather than returning null
        // so that a forgotten preExecute elsewhere cannot silently become this case.
        instance.release(safeString2, null);
        assertCacheState(cache, 0, 1, nodeSize(1));
    }

    @Test
    public void evictionBlockedOnSaving()
    {
        AccordCacheMetrics cacheMetrics = new AccordCacheMetrics(nextMetricId());
        NoOpLock lock = new NoOpLock();
        AccordCacheMetrics.Shard shard = cacheMetrics.newShard(lock);

        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(saveExecutor(executor), nodeSize(1) * 3 + nodeSize(3));
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new, shard);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString item = instance.acquire(Integer.toString(0));
        testLoad(executor, item, Integer.toString(0));
        item.set("0*");
        Assert.assertTrue(instance.isReferenced(item.key()));
        instance.release(item, AccordExecutionTestUtils.owner(item));

        for (int i=1; i<4; i++)
        {
            item = instance.acquire(Integer.toString(i));
            testLoad(executor, item, Integer.toString(i));
            Assert.assertTrue(instance.isReferenced(item.key()));
            instance.release(item, AccordExecutionTestUtils.owner(item));
            cache.tryShrinkOrEvict(lock);
        }

        assertCacheState(cache, 0, 4, nodeSize(1) * 3 + nodeSize(2));
        assertCacheMetrics(cacheMetrics, 0, 4, 4, 4);

        // force cache eviction
        instance.acquire(Integer.toString(0));
        cache.setCapacity(0);
        cache.tryShrinkOrEvict(lock);

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
        AccordCacheMetrics cacheMetrics = new AccordCacheMetrics(nextMetricId());
        AccordCacheMetrics.Shard shard = cacheMetrics.newShard(new NoOpLock());

        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(saveExecutor(executor), 500);
        AccordCache.Type<String, String, SafeString> type =
        cache.newType(String.class, (s, k) -> k, (s, k, c, o) -> null, Function.identity(), (s, k, v) -> true, String::length, SafeString::new, shard);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);
        assertCacheState(cache, 0, 0, 0);

        SafeString safeString = instance.acquire("1");
        testLoad(executor, safeString, "1");
        assertCacheState(cache, 1, 1, nodeSize(1));
        Assert.assertNull(cache.head());
        Assert.assertNull(cache.tail());

        Assert.assertTrue(instance.isReferenced(safeString.key()));
        assertCacheState(cache, 1, 1, nodeSize(1));

        safeString.set("11");
        instance.release(safeString, AccordExecutionTestUtils.owner(safeString));
        assertCacheState(cache, 0, 1, nodeSize(2));
        Assert.assertSame(safeString.global, cache.head());
        Assert.assertSame(safeString.global, cache.tail());

        assertCacheMetrics(cacheMetrics, 0, 1, 1, 1);
    }

    /**
     * This test has been authored entirely by Claude.
     *
     * A save the adapter refuses (CommandsForKeyAdapter refuses while the value isLoadingPruned) parks the entry in
     * WAITING_TO_SAVE. tryEvict must then unlink it without evicting it - it holds an unsaved modification - so the
     * entry is off the eviction queue with no reference, and only the durability path revisits it. This asserts that
     * path retries the deferred save and that the entry is returned to the eviction queue when it resolves, including
     * when it resolves inline (a null mutation, so no SAVING episode and no AccordCache.saved to do it).
     */
    @Test
    public void deferredSaveIsRetriedAndReturnedToEvictQueue()
    {
        AccordCacheMetrics cacheMetrics = new AccordCacheMetrics(nextMetricId());
        NoOpLock lock = new NoOpLock();
        AccordCacheMetrics.Shard shard = cacheMetrics.newShard(lock);

        ManualExecutor executor = new ManualExecutor();
        AccordCache cache = new AccordCache(saveExecutor(executor), nodeSize(1));
        boolean[] canSave = new boolean[] { false };
        // the save function returns a null Runnable, i.e. the "null mutation -> no change on disk" resolution
        AccordCache.Adapter<String, String, SafeString> adapter =
            new AccordCache.FunctionalAdapter<String, String, SafeString>((s, k) -> k, (s, k, v, o) -> null,
                                                                          Function.identity(), (k, v) -> null, (s, k, o) -> null,
                                                                          (s, k, v) -> true, String::length, o -> 0,
                                                                          SafeString::new, AccordCacheEntry::createReadyToLoad)
            {
                @Override public boolean canSave(String value, Object shrunk) { return canSave[0]; }
            };
        AccordCache.Type<String, String, SafeString> type = cache.newType(String.class, adapter, shard);
        AccordCache.Type<String, String, SafeString>.Instance instance = type.newInstance(null);

        SafeString safeString = instance.acquire("0");
        testLoad(executor, safeString, "0");
        safeString.set("modified");
        instance.release(safeString, AccordExecutionTestUtils.owner(safeString));
        AccordCacheEntry<String, String, SafeString> entry = safeString.global();
        Assert.assertEquals(Status.MODIFIED, entry.status());
        Assert.assertSame(entry, cache.head());

        // the durability path asks for a save the adapter refuses
        int[] saved = new int[] { 0 };
        cache.saveWhenReadyExclusive(entry, () -> ++saved[0]);
        Assert.assertEquals(Status.WAITING_TO_SAVE, entry.status());
        Assert.assertEquals(0, saved[0]);

        // over capacity, so tryEvict sees it: not evictable, and unlinked
        cache.tryShrinkOrEvict(lock);
        Assert.assertEquals(Status.WAITING_TO_SAVE, entry.status());
        Assert.assertNull(cache.head());
        Assert.assertTrue(entry.isUnqueued());
        Assert.assertEquals(0, entry.references());

        // the reason the adapter refused has cleared. The next durability pass (which iterates every modified entry,
        // not the eviction queue, so it still sees this one) must retry, resolve, and re-queue it
        canSave[0] = true;
        cache.saveWhenReadyExclusive(entry, () -> ++saved[0]);
        Assert.assertEquals(Status.LOADED, entry.status());
        Assert.assertEquals(2, saved[0]);
        Assert.assertSame(entry, cache.head());
        Assert.assertFalse(entry.isUnqueued());
    }

    private static SaveExecutor saveExecutor(ExecutorPlus executor)
    {
        return (saving, identity, save) -> {
            executor.submit(() -> {
                try { save.run(); }
                catch (Throwable t) { saving.saved(identity, t); throw t; }
                saving.saved(identity, null);
            });
            return null;
        };
    }
}
