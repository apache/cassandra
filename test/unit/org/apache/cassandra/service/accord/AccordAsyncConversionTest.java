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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.junit.Assert;
import org.junit.Test;

import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;

import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

public class AccordAsyncConversionTest
{
    private static final String SUCCESS = "test-success";
    private static final RuntimeException EXCEPTION = new RuntimeException("test-failure");

    @Test
    public void testAsyncChainToFutureSuccess()
    {
        AsyncChain<String> chain = AsyncChains.success(SUCCESS);
        Future<String> future = AccordService.toFuture(chain);
        
        Assert.assertTrue("should be completed", future.isDone());
        Assert.assertTrue("should be successful", future.isSuccess());
        Assert.assertEquals("should contain expected value", SUCCESS, future.getNow());
    }

    @Test
    public void testAsyncChainToFutureFailure()
    {
        AsyncChain<String> chain = AsyncChains.failure(EXCEPTION);
        Future<String> future = AccordService.toFuture(chain);
        
        Assert.assertTrue("should be completed", future.isDone());
        Assert.assertFalse("should be failed", future.isSuccess());
        Assert.assertEquals("should contain expected exception", EXCEPTION, future.cause());
    }

    @Test
    public void testAsyncChainToFutureAsyncCompletion() throws InterruptedException
    {
        CountDownLatch startLatch = new CountDownLatch(1);
        AsyncChain<String> chain = new AsyncChains.Head<String>()
        {
            @Override
            protected accord.utils.async.Cancellable start(BiConsumer<? super String, Throwable> callback)
            {
                Thread t = new Thread(() -> {
                    try
                    {
                        startLatch.await();
                        callback.accept(SUCCESS, null);
                    }
                    catch (InterruptedException e)
                    {
                        callback.accept(null, e);
                    }
                });
                t.start();
                return () -> t.interrupt();
            }
        };
        
        Future<String> future = AccordService.toFuture(chain);
        
        Assert.assertFalse("should not be completed initially", future.isDone());
        
        startLatch.countDown();
        
        Assert.assertEquals("should complete with expected value", SUCCESS, future.syncUninterruptibly().getNow());
    }

    @Test
    public void testAsyncResultToFutureWhenAlreadyFuture()
    {
        AsyncFuture<String> original = ImmediateFuture.success(SUCCESS);
        Future<String> converted = AccordService.toFuture(original);
        
        Assert.assertSame("Should return the same instance when already a Future", original, converted);
    }

    @Test
    public void testAsyncResultToFutureSuccess()
    {
        AsyncResult<String> result = new TestAsyncResult<>(SUCCESS, null);
        Future<String> future = AccordService.toFuture(result);
        
        Assert.assertTrue("should be completed", future.isDone());
        Assert.assertTrue("should be successful", future.isSuccess());
        Assert.assertEquals("should contain expected value", SUCCESS, future.getNow());
    }

    @Test
    public void testAsyncResultToFutureFailure()
    {
        AsyncResult<String> result = new TestAsyncResult<>(null, EXCEPTION);
        Future<String> future = AccordService.toFuture(result);
        
        Assert.assertTrue("should be completed", future.isDone());
        Assert.assertFalse("should be failed", future.isSuccess());
        Assert.assertEquals("should contain expected exception", EXCEPTION, future.cause());
    }

    @Test
    public void testGetBlockingAsyncChainSuccess()
    {
        AsyncChain<String> chain = AsyncChains.success(SUCCESS);
        String result = AccordService.getBlocking(chain);
        
        Assert.assertEquals("Should return expected value", SUCCESS, result);
    }

    @Test(expected = RuntimeException.class)
    public void testGetBlockingAsyncChainFailure()
    {
        AsyncChain<String> chain = AsyncChains.failure(EXCEPTION);
        AccordService.getBlocking(chain);
    }

    @Test
    public void testGetBlockingAsyncResultSuccess()
    {
        AsyncResult<String> result = new TestAsyncResult<>(SUCCESS, null);
        String value = AccordService.getBlocking(result);
        
        Assert.assertEquals("Should return expected value", SUCCESS, value);
    }

    @Test(expected = RuntimeException.class)
    public void testGetBlockingAsyncResultFailure()
    {
        AsyncResult<String> result = new TestAsyncResult<>(null, EXCEPTION);
        AccordService.getBlocking(result);
    }

    @Test
    public void testAsyncChainCancellation() throws InterruptedException
    {
        AtomicBoolean cancelled = new AtomicBoolean(false);
        CountDownLatch startLatch = new CountDownLatch(1);
        
        AsyncChain<String> chain = new AsyncChains.Head<String>()
        {
            @Override
            protected accord.utils.async.Cancellable start(BiConsumer<? super String, Throwable> callback)
            {
                Thread t = new Thread(() -> {
                    try
                    {
                        startLatch.await();
                        Thread.sleep(1000);
                        callback.accept(SUCCESS, null);
                    }
                    catch (InterruptedException e)
                    {
                        cancelled.set(true);
                        callback.accept(null, e);
                    }
                });
                t.start();
                return () -> {
                    cancelled.set(true);
                    t.interrupt();
                };
            }
        };
        
        Future<String> future = AccordService.toFuture(chain);
        startLatch.countDown();
        
        boolean cancelResult = future.cancel(true);
        Thread.sleep(100);
        
        Assert.assertTrue("should be cancellable", cancelResult);
        Assert.assertTrue("should be cancelled", future.isCancelled());
    }

    @Test
    public void testMultipleCallbacksOnAsyncResult() throws InterruptedException
    {
        AtomicReference<String> callback1Result = new AtomicReference<>();
        AtomicReference<String> callback2Result = new AtomicReference<>();
        CountDownLatch callbackLatch = new CountDownLatch(2);
        
        AsyncResult<String> result = new TestAsyncResult<>(SUCCESS, null);
        
        result.invoke((value, throwable) -> {
            callback1Result.set(value);
            callbackLatch.countDown();
        });
        
        result.invoke((value, throwable) -> {
            callback2Result.set(value);
            callbackLatch.countDown();
        });
        
        Assert.assertTrue("Both callbacks should be invoked", callbackLatch.await(1, TimeUnit.SECONDS));
        Assert.assertEquals("First callback should receive correct value", SUCCESS, callback1Result.get());
        Assert.assertEquals("Second callback should receive correct value", SUCCESS, callback2Result.get());
    }

    @Test
    public void testNullAsyncResult()
    {
        try
        {
            AccordService.toFuture((AsyncResult<String>) null);
            Assert.fail("Should throw NullPointerException for null AsyncResult");
        }
        catch (NullPointerException e)
        {
        }
    }

    @Test
    public void testNullAsyncChain()
    {
        try
        {
            AccordService.toFuture((AsyncChain<String>) null);
            Assert.fail("Should throw NullPointerException for null AsyncChain");
        }
        catch (NullPointerException e)
        {
            // ignore
        }
    }

    @Test
    public void testFutureInterfaceCompliance()
    {
        AsyncChain<String> chain = AsyncChains.success(SUCCESS);
        Future<String> future = AccordService.toFuture(chain);
        
        Assert.assertTrue("should implement AsyncResult", future instanceof AsyncResult);
        Assert.assertTrue("should be completion-aware", future.isDone());
        Assert.assertNotNull("should have a result", future.getNow());
    }

    @Test
    public void testConcurrentConversion() throws InterruptedException
    {
        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threadCount);
        AtomicReference<Exception> exception = new AtomicReference<>();
        
        for (int i = 0; i < threadCount; i++)
        {
            final int threadIndex = i;
            Thread t = new Thread(() -> {
                try
                {
                    startLatch.await();
                    AsyncChain<Integer> chain = AsyncChains.success(threadIndex);
                    Future<Integer> future = AccordService.toFuture(chain);
                    Assert.assertEquals("Thread " + threadIndex + " should get correct result",
                                      Integer.valueOf(threadIndex), future.getNow());
                }
                catch (Exception e)
                {
                    exception.set(e);
                }
                finally
                {
                    completeLatch.countDown();
                }
            });
            t.start();
        }
        
        startLatch.countDown();
        Assert.assertTrue("All threads should complete", completeLatch.await(5, TimeUnit.SECONDS));
        Assert.assertNull("No exceptions should occur during concurrent conversion", exception.get());
    }

    private static class TestAsyncResult<V> implements AsyncResult<V>
    {
        private final V successValue;
        private final Throwable failureValue;

        public TestAsyncResult(V successValue, Throwable failureValue)
        {
            this.successValue = successValue;
            this.failureValue = failureValue;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public boolean isSuccess()
        {
            return false;
        }

        @Override
        public AsyncResult<V> invoke(BiConsumer<? super V, Throwable> callback)
        {
            callback.accept(successValue, failureValue);
            return null;
        }
    }
}