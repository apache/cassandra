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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabAllocator;
import org.apache.cassandra.utils.memory.SlabPool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusteringPrefixTest
{
    @Test
    public void arrayTopAndBottom()
    {
        Assert.assertTrue(ArrayClusteringBound.BOTTOM.isBottom());
        Assert.assertFalse(ArrayClusteringBound.BOTTOM.isTop());
        Assert.assertTrue(ArrayClusteringBound.TOP.isTop());
        Assert.assertFalse(ArrayClusteringBound.TOP.isBottom());
    }

    @Test
    public void bufferTopAndBottom()
    {
        Assert.assertTrue(BufferClusteringBound.BOTTOM.isBottom());
        Assert.assertFalse(BufferClusteringBound.BOTTOM.isTop());
        Assert.assertTrue(BufferClusteringBound.TOP.isTop());
        Assert.assertFalse(BufferClusteringBound.TOP.isBottom());
    }

    @Test
    public void testRetainableArray()
    {
        testRetainable(ByteArrayAccessor.instance.factory(), x -> new byte[][] {x.getBytes(StandardCharsets.UTF_8)});
    }

    @Test
    public void testRetainableOnHeap()
    {
        testRetainable(ByteBufferAccessor.instance.factory(), x -> new ByteBuffer[] {ByteBufferUtil.bytes(x)});
    }

    @Test
    public void testRetainableOnHeapSliced()
    {
        for (int prepend = 0; prepend < 3; ++prepend)
        {
            for (int append = 0; append < 3; ++append)
            {
                testRetainable(ByteBufferAccessor.instance.factory(),
                               slicingAllocator(prepend, append));
            }
        }
    }

    private Function<String, ByteBuffer[]> slicingAllocator(int prepend, int append)
    {
        return x ->
        {
            ByteBuffer bytes = ByteBufferUtil.bytes(x);
            ByteBuffer sliced = ByteBuffer.allocate(bytes.remaining() + prepend + append);
            for (int i = 0; i < prepend; ++i)
                sliced.put((byte) ThreadLocalRandom.current().nextInt());
            sliced.put(bytes);
            bytes.flip();
            for (int i = 0; i < append; ++i)
                sliced.put((byte) ThreadLocalRandom.current().nextInt());
            sliced.position(prepend).limit(prepend + bytes.remaining());
            return new ByteBuffer[]{ sliced.slice() };
        };
    }

    @Test
    public void testRetainableOffHeap()
    {
        testRetainable(ByteBufferAccessor.instance.factory(), x ->
        {
            ByteBuffer h = ByteBufferUtil.bytes(x);
            ByteBuffer v = ByteBuffer.allocateDirect(h.remaining());
            v.put(h);
            v.flip();
            return new ByteBuffer[] {v};
        });
    }

    @Test
    public void testRetainableOnHeapSlab() throws InterruptedException, TimeoutException
    {
        testRetainableSlab(true);
    }

    @Test
    public void testRetainableOffHeapSlab() throws InterruptedException, TimeoutException
    {
        testRetainableSlab(false);
    }

    public void testRetainableSlab(boolean onHeap) throws InterruptedException, TimeoutException
    {
        MemtablePool pool = new SlabPool(1L << 24, onHeap ? 0 : 1L << 24, 1.0f, () -> ImmediateFuture.success(false));
        SlabAllocator allocator = ((SlabAllocator) pool.newAllocator("test"));
        assert !allocator.allocate(1).isDirect() == onHeap;
        try
        {
            testRetainable(ByteBufferAccessor.instance.factory(), x ->
            {
                ByteBuffer h = ByteBufferUtil.bytes(x);
                ByteBuffer v = allocator.allocate(h.remaining());
                v.put(h);
                v.flip();
                return new ByteBuffer[] {v};
            });
        }
        finally
        {
            pool.shutdownAndWait(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRetainableNative() throws InterruptedException, TimeoutException
    {
        MemtablePool pool = new NativePool(1L << 24,1L << 24, 1.0f, () -> ImmediateFuture.success(false));
        NativeAllocator allocator = (NativeAllocator) pool.newAllocator("test");
        try
        {
            testRetainable(ByteBufferAccessor.instance.factory(),
                           x -> new ByteBuffer[] {ByteBufferUtil.bytes(x)},
                           x -> x.kind() == ClusteringPrefix.Kind.CLUSTERING
                                ? new NativeClustering(allocator, null, (Clustering<?>) x)
                                : x);
        }
        finally
        {
            pool.shutdownAndWait(10, TimeUnit.SECONDS);
        }
    }

    public <V> void testRetainable(ValueAccessor.ObjectFactory<V> factory,
                                   Function<String, V[]> allocator)
    {
        testRetainable(factory, allocator, null);
    }

    public <V> void testRetainable(ValueAccessor.ObjectFactory<V> factory,
                                   Function<String, V[]> allocator,
                                   Function<ClusteringPrefix<V>, ClusteringPrefix<V>> mapper)
    {
        ClusteringPrefix<V>[] clusterings = new ClusteringPrefix[]
        {
            factory.clustering(),
            factory.staticClustering(),
            factory.clustering(allocator.apply("test")),
            factory.bound(ClusteringPrefix.Kind.INCL_START_BOUND, allocator.apply("testA")),
            factory.bound(ClusteringPrefix.Kind.INCL_END_BOUND, allocator.apply("testB")),
            factory.bound(ClusteringPrefix.Kind.EXCL_START_BOUND, allocator.apply("testC")),
            factory.bound(ClusteringPrefix.Kind.EXCL_END_BOUND, allocator.apply("testD")),
            factory.boundary(ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY, allocator.apply("testE")),
            factory.boundary(ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY, allocator.apply("testF")),
        };

        if (mapper != null)
            clusterings = Arrays.stream(clusterings)
                                .map(mapper)
                                .toArray(ClusteringPrefix[]::new);

        testRetainable(clusterings);
    }

    public void testRetainable(ClusteringPrefix<?>[] clusterings)
    {
        for (ClusteringPrefix<?> clustering : clusterings)
        {
            ClusteringPrefix<?> retainable = clustering.retainable();
            assertEquals(clustering, retainable);
            assertClusteringIsRetainable(retainable);
        }
    }


    public static void assertClusteringIsRetainable(ClusteringPrefix<?> clustering)
    {
        if (clustering instanceof AbstractArrayClusteringPrefix)
            return; // has to be on-heap and minimized

        assertTrue(clustering instanceof AbstractBufferClusteringPrefix);
        AbstractBufferClusteringPrefix abcf = (AbstractBufferClusteringPrefix) clustering;
        ByteBuffer[] buffers = abcf.getBufferArray();
        for (ByteBuffer b : buffers)
        {
            assertFalse(b.isDirect());
            assertTrue(b.hasArray());
            assertEquals(b.capacity(), b.remaining());
            assertEquals(0, b.arrayOffset());
            assertEquals(b.capacity(), b.array().length);
        }
    }
}
