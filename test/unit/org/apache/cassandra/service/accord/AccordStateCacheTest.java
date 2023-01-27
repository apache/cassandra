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

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

public class AccordStateCacheTest
{
    private static final long DEFAULT_ITEM_SIZE = 100;
    private static final long KEY_SIZE = 4;
    private static final long DEFAULT_NODE_SIZE = nodeSize(DEFAULT_ITEM_SIZE);

    private static class Item implements AccordState<Integer>
    {
        long size = DEFAULT_ITEM_SIZE;

        final Integer key;
        boolean modified = false;
        boolean initialized = false;

        public Item(Integer key)
        {
            this.key = key;
        }

        @Override
        public boolean isEmpty()
        {
            return initialized;
        }

        @Override
        public Integer key()
        {
            return key;
        }

        @Override
        public boolean hasModifications()
        {
            return modified;
        }

        @Override
        public void clearModifiedFlag()
        {
            modified = false;
        }

        @Override
        public boolean isLoaded()
        {
            return true;
        }

        @Override
        public long estimatedSizeOnHeap()
        {
            return size + KEY_SIZE;
        }
    }

    private static long nodeSize(long itemSize)
    {
        return itemSize + KEY_SIZE + AccordStateCache.Node.EMPTY_SIZE;
    }

    private static void assertCacheState(AccordStateCache cache, int active, int cached, long bytes)
    {
        Assert.assertEquals(active, cache.numActiveEntries());
        Assert.assertEquals(cached, cache.numCachedEntries());
        Assert.assertEquals(bytes, cache.bytesCached());
    }

    @Test
    public void testAcquisitionAndRelease()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class, Item::new);
        assertCacheState(cache, 0, 0, 0);

        Item item1 = instance.getOrCreate(1);
        assertCacheState(cache, 1, 0, DEFAULT_NODE_SIZE);
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        item1.size = 110;
        item1.modified = true;
        instance.release(item1);
        assertCacheState(cache, 0, 1, nodeSize(110));
        Assert.assertSame(item1, cache.tail.value);
        Assert.assertSame(item1, cache.head.value);

        Item item2 = instance.getOrCreate(2);
        assertCacheState(cache, 1, 1, DEFAULT_NODE_SIZE + nodeSize(110));
        instance.release(item2);
        assertCacheState(cache, 0, 2, DEFAULT_NODE_SIZE + nodeSize(110));

        Assert.assertSame(item1, cache.tail.value);
        Assert.assertSame(item2, cache.head.value);
    }

    @Test
    public void testRotation()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 5);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class, Item::new);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[3];
        for (int i=0; i<3; i++)
        {
            Item item = instance.getOrCreate(i);
            items[i] = item;
            instance.release(item);
        }

        Assert.assertSame(items[0], cache.tail.value);
        Assert.assertSame(items[2], cache.head.value);
        assertCacheState(cache, 0, 3, DEFAULT_NODE_SIZE * 3);

        Item item = instance.getOrCreate(1);
        assertCacheState(cache, 1, 2, DEFAULT_NODE_SIZE * 3);

        // releasing item should return it to the head
        instance.release(item);
        assertCacheState(cache, 0, 3, DEFAULT_NODE_SIZE * 3);
        Assert.assertSame(items[0], cache.tail.value);
        Assert.assertSame(items[1], cache.head.value);
    }

    @Test
    public void testEvictionOnAcquire()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 5);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class, Item::new);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[5];
        for (int i=0; i<5; i++)
        {
            Item item = instance.getOrCreate(i);
            items[i] = item;
            instance.release(item);
        }

        assertCacheState(cache, 0, 5, DEFAULT_NODE_SIZE * 5);
        Assert.assertSame(items[0], cache.tail.value);
        Assert.assertSame(items[4], cache.head.value);

        instance.getOrCreate(5);
        assertCacheState(cache, 1, 4, DEFAULT_NODE_SIZE * 5);
        Assert.assertSame(items[1], cache.tail.value);
        Assert.assertSame(items[4], cache.head.value);
        Assert.assertFalse(cache.keyIsCached(0));
        Assert.assertFalse(cache.keyIsActive(0));
    }

    @Test
    public void testEvictionOnRelease()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 4);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class, Item::new);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[5];
        for (int i=0; i<5; i++)
        {
            Item item = instance.getOrCreate(i);
            items[i] = item;
        }

        assertCacheState(cache, 5, 0, DEFAULT_NODE_SIZE * 5);
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        instance.release(items[2]);
        assertCacheState(cache, 4, 0, DEFAULT_NODE_SIZE * 4);
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        instance.release(items[4]);
        assertCacheState(cache, 3, 1, DEFAULT_NODE_SIZE * 4);
        Assert.assertSame(items[4], cache.tail.value);
        Assert.assertSame(items[4], cache.head.value);
    }

    @Test
    public void testMultiAcquireRelease()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 4);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class, Item::new);
        assertCacheState(cache, 0, 0, 0);

        Item item = instance.getOrCreate(0);
        Assert.assertNotNull(item);
        Assert.assertEquals(1, cache.references(0));
        assertCacheState(cache, 1, 0, DEFAULT_NODE_SIZE);

        Assert.assertNotNull(instance.getOrCreate(0));
        Assert.assertEquals(2, cache.references(0));
        assertCacheState(cache, 1, 0, DEFAULT_NODE_SIZE);

        instance.release(item);
        assertCacheState(cache, 1, 0, DEFAULT_NODE_SIZE);
        instance.release(item);
        assertCacheState(cache, 0, 1, DEFAULT_NODE_SIZE);
    }

    @Test
    public void evictionBlockedOnSaveFuture()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 4);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class, Item::new);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[4];
        for (int i=0; i<4; i++)
        {
            Item item = instance.getOrCreate(i);
            items[i] = item;
            instance.release(item);
        }

        assertCacheState(cache, 0, 4, DEFAULT_NODE_SIZE * 4);

        AsyncResult<Void> saveFuture = AsyncResults.settable();
        instance.addSaveResult(0, saveFuture);
        cache.setMaxSize(0);

        // all should have been evicted except 0
        assertCacheState(cache, 0, 1, DEFAULT_NODE_SIZE);
        Assert.assertTrue(cache.keyIsCached(0));
        Assert.assertFalse(cache.keyIsCached(1));
        Assert.assertFalse(cache.keyIsCached(2));
        Assert.assertFalse(cache.keyIsCached(3));
    }

    static class SetItem implements AccordState<Integer>
    {
        final Integer key;
        final Set<Integer> set = new HashSet<>();
        boolean modified = false;
        boolean initialized = false;

        static class WriteOnly extends SetItem implements AccordState.WriteOnly<Integer, SetItem>
        {
            AsyncResult.Settable<Void> promise = null;
            final Set<Integer> added = new HashSet<>();
            final Set<Integer> remove = new HashSet<>();

            public WriteOnly(Integer key)
            {
                super(key);
            }

            @Override
            public void asyncResult(AsyncResult<Void> notifier)
            {
                Preconditions.checkArgument(notifier instanceof AsyncResult.Settable);
                this.promise = (AsyncResult.Settable<Void>) notifier;
            }

            @Override
            public AsyncResult<Void> asyncResult()
            {
                return promise;
            }

            @Override
            public void applyChanges(SetItem instance)
            {
                instance.set.addAll(added);
                instance.set.removeAll(remove);
            }
        }


        public SetItem(Integer key)
        {
            this.key = key;
        }

        @Override
        public boolean isEmpty()
        {
            return initialized;
        }

        @Override
        public Integer key()
        {
            return key;
        }

        @Override
        public boolean hasModifications()
        {
            return modified;
        }

        @Override
        public void clearModifiedFlag()
        {
            this.modified = false;
        }

        @Override
        public boolean isLoaded()
        {
            return true;
        }

        @Override
        public long estimatedSizeOnHeap()
        {
            return set.size() * 100L;
        }
    }

    @Test
    public void writeOnlyCycle()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<Integer, SetItem> instance = cache.instance(Integer.class, SetItem.class, SetItem::new);
        SetItem onDisk = new SetItem(5);
        onDisk.set.addAll(ImmutableSet.of(1, 2, 3));
        Assert.assertEquals(0, instance.pendingWriteOnlyOperations(5));

        SetItem.WriteOnly writeOnly1 = new SetItem.WriteOnly(5);
        writeOnly1.added.addAll(ImmutableSet.of(4, 5));
        writeOnly1.asyncResult(AsyncResults.settable());
        instance.addWriteOnly(writeOnly1);
        Assert.assertEquals(1, instance.pendingWriteOnlyOperations(5));

        SetItem.WriteOnly writeOnly2 = new SetItem.WriteOnly(5);
        writeOnly2.remove.addAll(ImmutableSet.of(2, 4));
        writeOnly2.asyncResult(AsyncResults.settable());
        instance.addWriteOnly(writeOnly2);
        Assert.assertEquals(2, instance.pendingWriteOnlyOperations(5));

        Assert.assertNull(instance.getSaveResult(5));
        Assert.assertFalse(instance.writeOnlyGroupIsLocked(5));

        instance.lockWriteOnlyGroupIfExists(5);
        Assert.assertTrue(instance.writeOnlyGroupIsLocked(5));
        Assert.assertEquals(ImmutableSet.of(1, 2, 3), onDisk.set);
        Assert.assertTrue(instance.canEvict(5));

        instance.applyAndRemoveWriteOnlyGroup(onDisk);
        Assert.assertFalse(instance.writeOnlyGroupIsLocked(5));
        Assert.assertEquals(ImmutableSet.of(1, 3, 5), onDisk.set);

        // write only futures should have been merged and promoted to normal save futures, which would
        // prevent the cached object from being purged until they were completed
        AsyncResult<?> saveFuture = instance.getSaveResult(5);
        Assert.assertNotNull(saveFuture);
        Assert.assertFalse(saveFuture.isDone());
        Assert.assertFalse(instance.canEvict(5));

        writeOnly1.promise.setSuccess(null);
        Assert.assertFalse(saveFuture.isDone());
        Assert.assertFalse(instance.canEvict(5));

        writeOnly2.promise.setSuccess(null);
        Assert.assertTrue(saveFuture.isDone());
        Assert.assertTrue(instance.canEvict(5));
    }

    // write only operations should not be purged out of order
    @Test
    public void writeOnlyPurging()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<Integer, SetItem> instance = cache.instance(Integer.class, SetItem.class, SetItem::new);
        SetItem.WriteOnly[] writeOnly = new SetItem.WriteOnly[4];
        for (int i=0; i<writeOnly.length; i++)
        {
            SetItem.WriteOnly item = new SetItem.WriteOnly(5);
            item.added.add(i);
            item.asyncResult(AsyncResults.settable());
            instance.addWriteOnly(item);
            writeOnly[i] = item;
        }

        Assert.assertEquals(4, instance.pendingWriteOnlyOperations(5));

        // finishing the first item should cause it to be purged
        writeOnly[0].promise.setSuccess(null);
        instance.purgeWriteOnly(5);
        Assert.assertEquals(3, instance.pendingWriteOnlyOperations(5));

        // finishing the second item should not, since the (now) first item has not completed
        writeOnly[2].promise.setSuccess(null);
        instance.purgeWriteOnly(5);
        Assert.assertEquals(3, instance.pendingWriteOnlyOperations(5));

        // then finishing the first item should cause both items to be purged
        writeOnly[1].promise.setSuccess(null);
        instance.purgeWriteOnly(5);
        Assert.assertEquals(1, instance.pendingWriteOnlyOperations(5));
    }

    @Test
    public void writeOnlyPurgedLock()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<Integer, SetItem> instance = cache.instance(Integer.class, SetItem.class, SetItem::new);

        SetItem.WriteOnly item = new SetItem.WriteOnly(5);
        item.added.add(0);
        item.asyncResult(AsyncResults.settable());
        instance.addWriteOnly(item);

        instance.lockWriteOnlyGroupIfExists(5);

        // the write only item should not be purged, even though it's complete
        item.promise.setSuccess(null);
        instance.purgeWriteOnly(5);
        Assert.assertEquals(1, instance.pendingWriteOnlyOperations(5));
    }

    // if a load future exists for the key we're creating a write group for, we need to lock
    // the group so the loading instance gets changes applied when it finishes loading
    @Test
    public void testLoadFutureAutoLocksWriteOnlyInstances()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<Integer, SetItem> instance = cache.instance(Integer.class, SetItem.class, SetItem::new);

        AsyncResult<Void> loadfuture = AsyncResults.settable();
        instance.setLoadResult(5, loadfuture);

        Assert.assertFalse(instance.writeOnlyGroupIsLocked(5));
        Assert.assertEquals(0, instance.pendingWriteOnlyOperations(5));

        // adding a write only object should immediately lock the group, since there's an existing load future
        SetItem.WriteOnly item = new SetItem.WriteOnly(5);
        item.added.add(0);
        item.asyncResult(AsyncResults.settable());
        instance.addWriteOnly(item);

        Assert.assertTrue(instance.writeOnlyGroupIsLocked(5));
        Assert.assertEquals(1, instance.pendingWriteOnlyOperations(5));
    }

    // if a future is added and another one exists for the same key, they should be merged
    @Test
    public void testFutureMerging()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<Integer, SetItem> instance = cache.instance(Integer.class, SetItem.class, SetItem::new);
        AsyncResult.Settable<Void> promise1 = AsyncResults.settable();
        AsyncResult.Settable<Void> promise2 = AsyncResults.settable();
        instance.addSaveResult(5, promise1);
        instance.addSaveResult(5, promise2);

        AsyncResult<?> future = instance.getSaveResult(5);
        Assert.assertNotSame(future, promise1);
        Assert.assertNotSame(future, promise2);

        Assert.assertFalse(future.isDone());

        promise1.setSuccess(null);
        Assert.assertFalse(future.isDone());

        promise2.setSuccess(null);
        Assert.assertTrue(future.isDone());
    }
}
