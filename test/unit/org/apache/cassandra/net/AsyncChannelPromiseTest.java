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
package org.apache.cassandra.net;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.utils.concurrent.AbstractTestAsyncPromise;
import org.apache.cassandra.utils.concurrent.Promise;

public class AsyncChannelPromiseTest extends AbstractTestAsyncPromise
{
    @After
    public void shutdown()
    {
        exec.shutdownNow();
    }

    private static List<Supplier<Promise<Void>>> suppliers(AtomicInteger listeners, boolean includedUncancellable)
    {
        List<Supplier<Promise<Void>>> cancellable = ImmutableList.of(
            () -> new AsyncChannelPromise(new EmbeddedChannel()),
            () -> AsyncChannelPromise.withListener(new EmbeddedChannel(), f -> listeners.incrementAndGet())
        );

        if (!includedUncancellable)
            return cancellable;

        return ImmutableList.<Supplier<Promise<Void>>>builder()
               .addAll(cancellable)
               .addAll(cancellable.stream().map(s -> (Supplier<Promise<Void>>) () -> cancelSuccess(s.get())).collect(Collectors.toList()))
               .build();
    }

    @Test
    public void testSuccess()
    {
        final AtomicInteger initialListeners = new AtomicInteger();
        List<Supplier<Promise<Void>>> suppliers = suppliers(initialListeners, true);
        for (boolean tryOrSet : new boolean[]{ false, true })
                for (Supplier<Promise<Void>> supplier : suppliers)
                    testOneSuccess(supplier.get(), tryOrSet, null, null);
        Assert.assertEquals(2 * 2, initialListeners.get());
    }

    @Test
    public void testFailure()
    {
        final AtomicInteger initialListeners = new AtomicInteger();
        List<Supplier<Promise<Void>>> suppliers = suppliers(initialListeners, true);
        for (boolean tryOrSet : new boolean[]{ false, true })
            for (Throwable v : new Throwable[] { null, new NullPointerException() })
                for (Supplier<Promise<Void>> supplier : suppliers)
                    testOneFailure(supplier.get(), tryOrSet, v, null);
        Assert.assertEquals(2 * 2 * 2, initialListeners.get());
    }


    @Test
    public void testCancellation()
    {
        final AtomicInteger initialListeners = new AtomicInteger();
        List<Supplier<Promise<Void>>> suppliers = suppliers(initialListeners, false);
        for (boolean interruptIfRunning : new boolean[] { true, false })
            for (Supplier<Promise<Void>> supplier : suppliers)
                testOneCancellation(supplier.get(), interruptIfRunning, null);
        Assert.assertEquals(2, initialListeners.get());
    }


    @Test
    public void testTimeout()
    {
        final AtomicInteger initialListeners = new AtomicInteger();
        List<Supplier<Promise<Void>>> suppliers = suppliers(initialListeners, true);
        for (Supplier<Promise<Void>> supplier : suppliers)
            testOneTimeout(supplier.get());
        Assert.assertEquals(0, initialListeners.get());
    }

}
