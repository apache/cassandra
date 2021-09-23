package org.apache.cassandra.concurrent;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.TraceStateImpl;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.WrappedRunnable;

public class DebuggableThreadPoolExecutorTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSerialization()
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        WrappedRunnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws InterruptedException
            {
                Thread.sleep(50);
            }
        };
        long start = System.nanoTime();
        for (int i = 0; i < 10; i++)
        {
            executor.execute(runnable);
        }
        assert q.size() > 0 : q.size();
        while (executor.getCompletedTaskCount() < 10)
            continue;
        long delta = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assert delta >= 9 * 50 : delta;
    }

    @Test
    public void testExecuteFutureTaskWhileTracing()
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        Runnable test = () -> executor.execute(failingTask());
        try
        {
            // make sure the non-tracing case works
            Throwable cause = catchUncaughtExceptions(test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());

            // tracing should have the same semantics
            cause = catchUncaughtExceptions(() -> withTracing(test));
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    public void testSubmitFutureTaskWhileTracing()
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        FailingRunnable test = () -> executor.submit(failingTask()).get();
        try
        {
            // make sure the non-tracing case works
            Throwable cause = catchUncaughtExceptions(test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());

            // tracing should have the same semantics
            cause = catchUncaughtExceptions(() -> withTracing(test));
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    public void testSubmitWithResultFutureTaskWhileTracing()
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        FailingRunnable test = () -> executor.submit(failingTask(), 42).get();
        try
        {
            Throwable cause = catchUncaughtExceptions(test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
            cause = catchUncaughtExceptions(() -> withTracing(test));
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
        }
        finally
        {
            executor.shutdown();
        }
    }

    private static void withTracing(Runnable fn)
    {
        TraceState state = Tracing.instance.get();
        try {
            Tracing.instance.set(new TraceStateImpl(ClientState.forInternalCalls(), InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.0.1")), UUID.randomUUID(), Tracing.TraceType.NONE));
            fn.run();
        }
        finally
        {
            Tracing.instance.set(state);
        }
    }

    private static Throwable catchUncaughtExceptions(Runnable fn)
    {
        Thread.UncaughtExceptionHandler defaultHandler = Thread.getDefaultUncaughtExceptionHandler();
        try
        {
            AtomicReference<Throwable> ref = new AtomicReference<>(null);
            CountDownLatch latch = new CountDownLatch(1);
            Thread.setDefaultUncaughtExceptionHandler((thread, cause) -> {
                ref.set(cause);
                latch.countDown();
            });
            fn.run();
            try
            {
                latch.await(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            return ref.get();
        }
        finally
        {
            Thread.setDefaultUncaughtExceptionHandler(defaultHandler);
        }
    }

    private static String failingFunction()
    {
        throw new DebuggingThrowsException();
    }

    private static RunnableFuture<String> failingTask()
    {
        return ListenableFutureTask.create(DebuggableThreadPoolExecutorTest::failingFunction);
    }

    private static final class DebuggingThrowsException extends RuntimeException {

    }

    // REVIEWER : I know this is the same as WrappedRunnable, but that doesn't support lambda...
    private interface FailingRunnable extends Runnable
    {
        void doRun() throws Throwable;

        default void run()
        {
            try
            {
                doRun();
            }
            catch (Throwable t)
            {
                Throwables.throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
        }
    }
}
