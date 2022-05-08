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


import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.TraceStateImpl;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FailingRunnable;
import org.apache.cassandra.utils.WrappedRunnable;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

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
        ExecutorPlus executor = executorFactory().configureSequential("TEST").withQueueLimit(1).build();
        WrappedRunnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws InterruptedException
            {
                Thread.sleep(50);
            }
        };
        long start = nanoTime();
        for (int i = 0; i < 10; i++)
        {
            executor.execute(runnable);
        }
        assert executor.getPendingTaskCount() > 0 : executor.getPendingTaskCount();
        while (executor.getCompletedTaskCount() < 10)
            continue;
        long delta = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);
        assert delta >= 9 * 50 : delta;
    }

    @Test
    public void testLocalStatePropagation()
    {
        ExecutorPlus executor = executorFactory().localAware().sequential("TEST");
        assertThat(executor).isInstanceOf(LocalAwareExecutorPlus.class);
        try
        {
            checkLocalStateIsPropagated(executor);
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    public void testNoLocalStatePropagation() throws InterruptedException
    {
        ExecutorPlus executor = executorFactory().sequential("TEST");
        assertThat(executor).isNotInstanceOf(LocalAwareExecutorPlus.class);
        try
        {
            checkLocalStateIsPropagated(executor);
        }
        finally
        {
            executor.shutdown();
        }
    }

    public static void checkLocalStateIsPropagated(ExecutorPlus executor)
    {
        checkClientWarningsArePropagated(executor, () -> executor.execute(() -> ClientWarn.instance.warn("msg")));
        checkClientWarningsArePropagated(executor, () -> executor.submit(() -> ClientWarn.instance.warn("msg")));
        checkClientWarningsArePropagated(executor, () -> executor.submit(() -> ClientWarn.instance.warn("msg"), null));
        checkClientWarningsArePropagated(executor, () -> executor.submit((Callable<Void>) () -> {
            ClientWarn.instance.warn("msg");
            return null;
        }));

        checkTracingIsPropagated(executor, () -> executor.execute(() -> Tracing.trace("msg")));
        checkTracingIsPropagated(executor, () -> executor.submit(() -> Tracing.trace("msg")));
        checkTracingIsPropagated(executor, () -> executor.submit(() -> Tracing.trace("msg"), null));
        checkTracingIsPropagated(executor, () -> executor.submit((Callable<Void>) () -> {
            Tracing.trace("msg");
            return null;
        }));
    }

    public static void checkClientWarningsArePropagated(ExecutorPlus executor, Runnable schedulingTask) {
        ClientWarn.instance.captureWarnings();
        assertThat(ClientWarn.instance.getWarnings()).isNullOrEmpty();

        ClientWarn.instance.warn("msg0");
        long initCompletedTasks = executor.getCompletedTaskCount();
        schedulingTask.run();
        while (executor.getCompletedTaskCount() == initCompletedTasks) Uninterruptibles.sleepUninterruptibly(10, MILLISECONDS);
        ClientWarn.instance.warn("msg1");

        if (executor instanceof LocalAwareExecutorPlus)
            assertThat(ClientWarn.instance.getWarnings()).containsExactlyInAnyOrder("msg0", "msg", "msg1");
        else
            assertThat(ClientWarn.instance.getWarnings()).containsExactlyInAnyOrder("msg0", "msg1");
    }

    public static void checkTracingIsPropagated(ExecutorPlus executor, Runnable schedulingTask) {
        ClientWarn.instance.captureWarnings();
        assertThat(ClientWarn.instance.getWarnings()).isNullOrEmpty();

        ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<>();
        Tracing.instance.set(new TraceState(FBUtilities.getLocalAddressAndPort(), nextTimeUUID(), Tracing.TraceType.NONE)
        {
            @Override
            protected void traceImpl(String message)
            {
                q.add(message);
            }
        });
        Tracing.trace("msg0");
        long initCompletedTasks = executor.getCompletedTaskCount();
        schedulingTask.run();
        while (executor.getCompletedTaskCount() == initCompletedTasks) Uninterruptibles.sleepUninterruptibly(10, MILLISECONDS);
        Tracing.trace("msg1");

        if (executor instanceof LocalAwareExecutorPlus)
            assertThat(q.toArray()).containsExactlyInAnyOrder("msg0", "msg", "msg1");
        else
            assertThat(q.toArray()).containsExactlyInAnyOrder("msg0", "msg1");
    }

    @Test
    public void testExecuteFutureTaskWhileTracing()
    {
        SettableUncaughtExceptionHandler ueh = new SettableUncaughtExceptionHandler();
        ExecutorPlus executor = executorFactory()
                                .localAware()
                                .configureSequential("TEST")
                                .withUncaughtExceptionHandler(ueh)
                                .withQueueLimit(1).build();
        Runnable test = () -> executor.execute(failingTask());
        try
        {
            // make sure the non-tracing case works
            Throwable cause = catchUncaughtExceptions(ueh, test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());

            // tracing should have the same semantics
            cause = catchUncaughtExceptions(ueh, () -> withTracing(test));
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
        SettableUncaughtExceptionHandler ueh = new SettableUncaughtExceptionHandler();
        ExecutorPlus executor = executorFactory().localAware()
                                                 .configureSequential("TEST")
                                                 .withUncaughtExceptionHandler(ueh)
                                                 .withQueueLimit(1).build();
        FailingRunnable test = () -> executor.submit(failingTask()).get();
        try
        {
            // make sure the non-tracing case works
            Throwable cause = catchUncaughtExceptions(ueh, test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());

            // tracing should have the same semantics
            cause = catchUncaughtExceptions(ueh, () -> withTracing(test));
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
        SettableUncaughtExceptionHandler ueh = new SettableUncaughtExceptionHandler();
        ExecutorPlus executor = executorFactory().localAware()
                                                 .configureSequential("TEST")
                                                 .withUncaughtExceptionHandler(ueh)
                                                 .withQueueLimit(1).build();
        FailingRunnable test = () -> executor.submit(failingTask(), 42).get();
        try
        {
            Throwable cause = catchUncaughtExceptions(ueh, test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
            cause = catchUncaughtExceptions(ueh, () -> withTracing(test));
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
            Tracing.instance.set(new TraceStateImpl(InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.0.1")), nextTimeUUID(), Tracing.TraceType.NONE));
            fn.run();
        }
        finally
        {
            Tracing.instance.set(state);
        }
    }

    private static class SettableUncaughtExceptionHandler implements UncaughtExceptionHandler
    {
        volatile Supplier<UncaughtExceptionHandler> cur;

        @Override
        public void uncaughtException(Thread t, Throwable e)
        {
            cur.get().uncaughtException(t, e);
        }

        void set(Supplier<UncaughtExceptionHandler> set)
        {
            cur = set;
        }

        void clear()
        {
            cur = Thread::getDefaultUncaughtExceptionHandler;
        }
    }

    private static Throwable catchUncaughtExceptions(SettableUncaughtExceptionHandler ueh, Runnable fn)
    {
        try
        {
            AtomicReference<Throwable> ref = new AtomicReference<>(null);
            CountDownLatch latch = new CountDownLatch(1);
            ueh.set(() -> (thread, cause) -> {
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
            ueh.clear();
        }
    }

    private static String failingFunction()
    {
        throw new DebuggingThrowsException();
    }

    private static RunnableFuture<String> failingTask()
    {
        return new FutureTask<>(DebuggableThreadPoolExecutorTest::failingFunction);
    }

    private static final class DebuggingThrowsException extends RuntimeException {

    }
}
