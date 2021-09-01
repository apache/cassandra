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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.shared.ThrowingRunnable;

public class ImmediateFutureTest
{

    private void testSimple(ImmediateFuture<Boolean> p, boolean isCancelled) throws InterruptedException
    {
        Assert.assertEquals(p, p.await());
        Assert.assertEquals(p, p.awaitUninterruptibly());
        Assert.assertEquals(p, p.awaitThrowUncheckedOnInterrupt());
        Assert.assertTrue(p.await(1L, TimeUnit.MILLISECONDS));
        Assert.assertTrue(p.await(1L));
        Assert.assertTrue(p.awaitUntil(Long.MAX_VALUE));
        Assert.assertTrue(p.awaitUntilUninterruptibly(Long.MAX_VALUE));
        Assert.assertTrue(p.awaitUntilThrowUncheckedOnInterrupt(Long.MAX_VALUE));
        Assert.assertTrue(p.isDone());
        Assert.assertFalse(p.isCancellable());
        Assert.assertEquals(isCancelled, p.isCancelled());
        Assert.assertEquals(!isCancelled, p.setUncancellable());
        Assert.assertFalse(p.setUncancellableExclusive());
        Assert.assertFalse(p.cancel(true));
        Assert.assertFalse(p.cancel(false));
        Assert.assertFalse(p.trySuccess(false));
        Assert.assertFalse(p.tryFailure(new InterruptedException()));
    }

    @Test
    public void testSucceeded() throws InterruptedException, ExecutionException, TimeoutException
    {
        ImmediateFuture<Boolean> p = ImmediateFuture.success(true);
        Assert.assertTrue(p.getNow());
        Assert.assertTrue(p.get());
        Assert.assertTrue(p.get(1L, TimeUnit.MILLISECONDS));
        Assert.assertEquals(p, p.sync());
        Assert.assertEquals(p, p.syncUninterruptibly());
        Assert.assertFalse(p.isCancelled());
        testSimple(p, false);
    }

    @Test
    public void testFailed() throws InterruptedException
    {
        testFailed(ImmediateFuture.failure(new RuntimeException()), false, t -> t instanceof ExecutionException, t -> t instanceof RuntimeException);
    }

    @Test
    public void testCancelled() throws InterruptedException
    {
        testFailed(ImmediateFuture.cancelled(), true, t -> t instanceof CancellationException, t -> t instanceof CancellationException);
    }

    private void testFailed(ImmediateFuture<Boolean> p, boolean isCancelled, Predicate<Throwable> get, Predicate<Throwable> sync) throws InterruptedException
    {
        Assert.assertNull(p.getNow());
        assertFailure(p::get, get);
        assertFailure(() -> p.get(1L, TimeUnit.MILLISECONDS), get);
        assertFailure(p::sync, sync);
        assertFailure(p::syncUninterruptibly, sync);
        testSimple(p, isCancelled);
    }

    private static void assertFailure(ThrowingRunnable run, Predicate<Throwable> test)
    {
        Throwable failure = null;
        try
        {
            run.run();
        }
        catch (Throwable t)
        {
            failure = t;
        }
        if (failure == null || !test.test(failure))
            Assert.fail();
    }

}
