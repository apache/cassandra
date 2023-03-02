/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref.Visitor;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings({"unused", "unchecked", "rawtypes"})
public class RefCountedTest
{
    static
    {
        if (Ref.STRONG_LEAK_DETECTOR != null)
            Ref.STRONG_LEAK_DETECTOR.submit(() -> { Thread.sleep(Integer.MAX_VALUE); return null; });
    }

    private static final class Tidier implements RefCounted.Tidy
    {
        boolean tidied;

        public void tidy()
        {
            tidied = true;
        }

        public String name()
        {
            return "test tidy";
        }
    }

    @Test
    public void testLeak() throws InterruptedException
    {
        Tidier tidier = new Tidier();
        Ref<?> obj = new Ref(null, tidier);
        obj.tryRef();
        obj.release();
        System.gc();
        System.gc();
        Thread.sleep(1000);
        Assert.assertTrue(tidier.tidied);
    }

    @Test
    public void testSeriousLeak() throws InterruptedException
    {
        Tidier tidier = new Tidier();
        new Ref(null, tidier);
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        Thread.sleep(1000);
        Assert.assertTrue(tidier.tidied);
    }

    @Test
    public void testDoubleRelease() throws InterruptedException
    {
        Tidier tidier = null;
        try
        {
            tidier = new Tidier();
            Ref<?> obj = new Ref(null, tidier);
            obj.release();
            obj.release();
            Assert.assertTrue(false);
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testMemoryLeak()
    {
        Tidier tidier = new Tidier();
        Ref<Object> ref = new Ref(null, tidier);
        long initialSize = ObjectSizes.measureDeep(ref);
        for (int i = 0 ; i < 1000 ; i++)
            ref.ref().release();
        long finalSize = ObjectSizes.measureDeep(ref);
        if (finalSize > initialSize * 2)
            throw new AssertionError();
        ref.release();
    }

    static final int entryCount = 1000000;
    static final int fudgeFactor = 20;

    @Test
    public void testLinkedList()
    {
        final List<Object> iterable = new LinkedList<Object>();
        Pair<Object, Object> p = Pair.create(iterable, iterable);
        RefCounted.Tidy tidier = new RefCounted.Tidy() {
            Object ref = iterable;
            @Override
            public void tidy() throws Exception
            {
            }

            @Override
            public String name()
            {
                return "42";
            }
        };
        Ref<Object> ref = new Ref(new AtomicReference<List<Object>>(iterable), tidier);
        for (int i = 0; i < entryCount; i++)
        {
            iterable.add(p);
        }
        Visitor visitor = new Visitor();
        visitor.run();
        ref.close();

        System.out.println("LinkedList visited " + visitor.lastVisitedCount + " iterations " + visitor.iterations);
        //Should visit a lot of list nodes, but no more since there is only one object stored in the list
        Assert.assertTrue(visitor.lastVisitedCount > entryCount && visitor.lastVisitedCount < entryCount + fudgeFactor);
        //Should have a lot of iterations to walk the list, but linear to the number of entries
        Assert.assertTrue(visitor.iterations > (entryCount * 3) && visitor.iterations < (entryCount * 3) + fudgeFactor);
    }

    /*
     * There was a traversal error terminating traversal for an object upon encountering a null
     * field. Test for the bug here using CLQ.
     */
    @Test
    public void testCLQBug()
    {
        Ref.concurrentIterables.remove(ConcurrentLinkedQueue.class);
        try
        {
            testConcurrentLinkedQueueImpl(true);
        }
        finally
        {
            Ref.concurrentIterables.add(ConcurrentLinkedQueue.class);
        }
    }

    private void testConcurrentLinkedQueueImpl(boolean bugTest)
    {
        final Queue<Object> iterable = new ConcurrentLinkedQueue<Object>();
        Pair<Object, Object> p = Pair.create(iterable, iterable);
        RefCounted.Tidy tidier = new RefCounted.Tidy() {
            Object ref = iterable;
            @Override
            public void tidy() throws Exception
            {
            }

            @Override
            public String name()
            {
                return "42";
            }
        };
        Ref<Object> ref = new Ref(new AtomicReference<Queue<Object>>(iterable), tidier);
        for (int i = 0; i < entryCount; i++)
        {
            iterable.add(p);
        }
        Visitor visitor = new Visitor();
        visitor.run();
        ref.close();

        System.out.println("ConcurrentLinkedQueue visited " + visitor.lastVisitedCount + " iterations " + visitor.iterations + " bug test " + bugTest);

        if (bugTest)
        {
            //Should have to visit a lot of queue nodes
            Assert.assertTrue(visitor.lastVisitedCount > entryCount && visitor.lastVisitedCount < entryCount + fudgeFactor);
            //Should have a lot of iterations to walk the queue, but linear to the number of entries
            Assert.assertTrue(visitor.iterations > (entryCount * 2) && visitor.iterations < (entryCount * 2) + fudgeFactor);
        }
        else
        {
            //There are almost no objects in this linked list once it's iterated as a collection so visited count
            //should be small
            Assert.assertTrue(visitor.lastVisitedCount < 10);
            //Should have a lot of iterations to walk the collection, but linear to the number of entries
            Assert.assertTrue(visitor.iterations > entryCount && visitor.iterations < entryCount + fudgeFactor);
        }
    }

    @Test
    public void testConcurrentLinkedQueue()
    {
        testConcurrentLinkedQueueImpl(false);
    }

    @Test
    public void testBlockingQueue()
    {
        final BlockingQueue<Object> iterable = new LinkedBlockingQueue<Object>();
        Pair<Object, Object> p = Pair.create(iterable, iterable);
        RefCounted.Tidy tidier = new RefCounted.Tidy() {
            Object ref = iterable;
            @Override
            public void tidy() throws Exception
            {
            }

            @Override
            public String name()
            {
                return "42";
            }
        };
        Ref<Object> ref = new Ref(new AtomicReference<BlockingQueue<Object>>(iterable), tidier);
        for (int i = 0; i < entryCount; i++)
        {
            iterable.add(p);
        }
        Visitor visitor = new Visitor();
        visitor.run();
        ref.close();

        System.out.println("BlockingQueue visited " + visitor.lastVisitedCount + " iterations " + visitor.iterations);
        //There are almost no objects in this queue once it's iterated as a collection so visited count
        //should be small
        Assert.assertTrue(visitor.lastVisitedCount < 10);
        //Should have a lot of iterations to walk the collection, but linear to the number of entries
        Assert.assertTrue(visitor.iterations > entryCount && visitor.iterations < entryCount + fudgeFactor);
    }

    @Test
    public void testConcurrentMap()
    {
        final Map<Object, Object> map = new ConcurrentHashMap<Object, Object>();
        RefCounted.Tidy tidier = new RefCounted.Tidy() {
            Object ref = map;
            @Override
            public void tidy() throws Exception
            {
            }

            @Override
            public String name()
            {
                return "42";
            }
        };
        Ref<Object> ref = new Ref(new AtomicReference<Map<Object, Object>>(map), tidier);

        Object o = new Object();
        for (int i = 0; i < entryCount; i++)
        {
            map.put(new Object(), o);
        }
        Visitor visitor = new Visitor();
        visitor.run();
        ref.close();

        System.out.println("ConcurrentHashMap visited " + visitor.lastVisitedCount + " iterations " + visitor.iterations);

        //Should visit roughly the same number of objects as entries because the value object is constant
        //Map.Entry objects shouldn't be counted since it is iterated as a collection
        Assert.assertTrue(visitor.lastVisitedCount > entryCount && visitor.lastVisitedCount < entryCount + fudgeFactor);
        //Should visit 2x the number of entries since we have to traverse the key and value separately
        Assert.assertTrue(visitor.iterations > entryCount * 2 && visitor.iterations < entryCount * 2 + fudgeFactor);
    }

    @Test
    public void testHashMap()
    {
        final Map<Object, Object> map = new HashMap<Object, Object>();
        RefCounted.Tidy tidier = new RefCounted.Tidy() {
            Object ref = map;
            @Override
            public void tidy() throws Exception
            {
            }

            @Override
            public String name()
            {
                return "42";
            }
        };
        Ref<Object> ref = new Ref(new AtomicReference<Map<Object, Object>>(map), tidier);

        Object o = new Object();
        for (int i = 0; i < entryCount; i++)
        {
            map.put(new Object(), o);
        }
        Visitor visitor = new Visitor();
        visitor.run();
        ref.close();

        System.out.println("HashMap visited " + visitor.lastVisitedCount + " iterations " + visitor.iterations);

        //Should visit 2x  the number of entries because of the wrapper Map.Entry objects
        Assert.assertTrue(visitor.lastVisitedCount > (entryCount * 2) && visitor.lastVisitedCount < (entryCount * 2) + fudgeFactor);
        //Should iterate 3x the number of entries since we have to traverse the key and value separately
        Assert.assertTrue(visitor.iterations > (entryCount * 3) && visitor.iterations < (entryCount * 3) + fudgeFactor);
    }

    @Test
    public void testArray() throws Exception
    {
        final Object objects[] = new Object[entryCount];
        for (int i = 0; i < entryCount; i += 2)
            objects[i] = new Object();

        File f = FileUtils.createTempFile("foo", "bar");
        RefCounted.Tidy tidier = new RefCounted.Tidy() {
            Object ref = objects;
            //Checking we don't get an infinite loop out of traversing file refs
            File fileRef = f;

            @Override
            public void tidy() throws Exception
            {
            }

            @Override
            public String name()
            {
                return "42";
            }
        };
        Ref<Object> ref = new Ref(new AtomicReference<Object[]>(objects), tidier);

        Visitor visitor = new Visitor();
        visitor.run();
        ref.close();

        System.out.println("Array visited " + visitor.lastVisitedCount + " iterations " + visitor.iterations);
        //Should iterate the elements in the array and get a unique object from every other one
        Assert.assertTrue(visitor.lastVisitedCount > (entryCount / 2) && visitor.lastVisitedCount < (entryCount / 2) + fudgeFactor);
        //Should iterate over the array touching roughly the same number of objects as entries
        Assert.assertTrue(visitor.iterations > (entryCount / 2) && visitor.iterations < (entryCount / 2) + fudgeFactor);
    }

    //Make sure a weak ref is ignored by the visitor looking for strong ref leaks
    @Test
    public void testWeakRef() throws Exception
    {
        AtomicReference dontRefMe = new AtomicReference();

        WeakReference<Object> weakRef = new WeakReference(dontRefMe);

        RefCounted.Tidy tidier = new RefCounted.Tidy() {
            WeakReference<Object> ref = weakRef;

            @Override
            public void tidy() throws Exception
            {
            }

            @Override
            public String name()
            {
                return "42";
            }
        };

        Ref<Object> ref = new Ref(dontRefMe, tidier);
        dontRefMe.set(ref);

        Visitor visitor = new Visitor();
        visitor.haveLoops = new HashSet<>();
        visitor.run();
        ref.close();

        Assert.assertTrue(visitor.haveLoops.isEmpty());
    }

    static class LambdaTestClassTidier implements RefCounted.Tidy
    {
        Runnable runOnClose;

        @Override
        public void tidy() throws Exception
        {
            runOnClose.run();
        }

        @Override
        public String name()
        {
            return "42";
        }
    }

    static class LambdaTestClass
    {
        String a = "x";
        Ref<Object> ref;

        Runnable getRunOnCloseLambda()
        {
            return () -> System.out.println("aaa");
        }

        Runnable getRunOnCloseInner()
        {
            return new Runnable()
            {
                @Override
                public void run()
                {
                    System.out.println("aaa");
                }
            };
        }

        Runnable getRunOnCloseLambdaWithThis()
        {
            return () -> System.out.println(a);
        }
    }

    private Set<Ref.GlobalState> testCycles(Function<LambdaTestClass, Runnable> runOnCloseSupplier)
    {
        LambdaTestClass test = new LambdaTestClass();
        Runnable weakRef = runOnCloseSupplier.apply(test);
        RefCounted.Tidy tidier = new RefCounted.Tidy()
        {
            Runnable ref = weakRef;

            public void tidy()
            {
            }

            public String name()
            {
                return "42";
            }
        };

        Ref<Object> ref = new Ref(test, tidier);
        test.ref = ref;

        Visitor visitor = new Visitor();
        visitor.haveLoops = new HashSet<>();
        visitor.run();
        ref.close();

        return visitor.haveLoops;
    }

    /**
     * The intention of this test is to confirm that lambda without a reference to `this`, does not implicitly
     * include a reference to the enclosing class. If it did, we would detect cycles, as we do when we deal with
     * anonymous inner classes or lamba which references `this` explicitly (see the sanity checks).
     * <p>
     * This test aims to confirm that JVM works as we assumed because we couldn't find that in the specification.
     */
    @Test
    public void testCycles()
    {
        assertThat(testCycles(LambdaTestClass::getRunOnCloseLambdaWithThis)).isNotEmpty(); // sanity test
        assertThat(testCycles(LambdaTestClass::getRunOnCloseInner)).isNotEmpty(); // sanity test

        assertThat(testCycles(LambdaTestClass::getRunOnCloseLambda)).isEmpty();
    }

    @Test
    public void testSSTableReaderLeakIsDetected()
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        Descriptor descriptor = Descriptor.fromFileWithComponent(new File("test/data/legacy-sstables/nb/legacy_tables/legacy_nb_simple/nb-1-big-Data.db"), false).left;
        TableMetadata tm = TableMetadata.builder("legacy_tables", "legacy_nb_simple").addPartitionKeyColumn("pk", UTF8Type.instance).addRegularColumn("val", UTF8Type.instance).build();
        AtomicBoolean leakDetected = new AtomicBoolean();
        AtomicBoolean runOnCloseExecuted1 = new AtomicBoolean();
        AtomicBoolean runOnCloseExecuted2 = new AtomicBoolean();
        Ref.OnLeak prevOnLeak = Ref.ON_LEAK;

        try
        {
            Ref.ON_LEAK = state -> {
                leakDetected.set(true);
            };
            {
                SSTableReader reader = SSTableReader.openNoValidation(null, descriptor, TableMetadataRef.forOfflineTools(tm));
                reader.runOnClose(() -> runOnCloseExecuted1.set(true));
                reader.runOnClose(() -> runOnCloseExecuted2.set(true)); // second time to actually create lambda referencing to lambda, see runOnClose impl
                //noinspection UnusedAssignment
                reader = null; // this is required, otherwise GC will not attempt to collect the created reader
            }
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollDelay(Duration.ofSeconds(1)).untilAsserted(() -> {
                System.gc();
                System.gc();
                System.gc();
                System.gc();
                assertThat(leakDetected.get()).isTrue();
            });
            assertThat(runOnCloseExecuted1.get()).isTrue();
            assertThat(runOnCloseExecuted2.get()).isTrue();
        }
        finally
        {
            Ref.ON_LEAK = prevOnLeak;
        }
    }
}
