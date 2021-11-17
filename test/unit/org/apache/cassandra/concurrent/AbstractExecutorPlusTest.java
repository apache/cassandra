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

package org.apache.cassandra.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Ignore;

import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Semaphore;

import static org.apache.cassandra.utils.concurrent.Semaphore.newSemaphore;

@Ignore
public abstract class AbstractExecutorPlusTest
{
    interface Verify<V>
    {
        void test(V test) throws Throwable;
    }

    static <V> Verify<V> ignoreNull(Verify<V> verify)
    {
        return test -> { if (test != null) verify.test(test); };
    }

    public <E extends ExecutorPlus> void testPooled(Supplier<ExecutorBuilder<? extends E>> builders) throws Throwable
    {
        testSuccess(builders);
        testFailure(builders);
    }

    public <E extends SequentialExecutorPlus> void testSequential(Supplier<ExecutorBuilder<? extends E>> builders) throws Throwable
    {
        testSuccess(builders);
        testFailure(builders);
        testAtLeastOnce(builders);
    }

    Runnable wrapSubmit(Runnable submit)
    {
        return submit;
    }

    public <E extends ExecutorPlus> void testSuccess(Supplier<ExecutorBuilder<? extends E>> builders) throws Throwable
    {
        testExecution(builders.get().build(), wrapSubmit(() -> {}), ignoreNull(Future::get));
        testExecution(builders.get().build(), WithResources.none(), wrapSubmit(() -> {}), ignoreNull(Future::get));
    }

    public <E extends ExecutorPlus> void testFailure(Supplier<ExecutorBuilder<? extends E>> builders) throws Throwable
    {
        ExecutorBuilder<? extends E> builder = builders.get();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread.UncaughtExceptionHandler ueh = (thread, f) -> failure.set(f);
        builder.withUncaughtExceptionHandler(ueh);
        Verify<Future<?>> verify = f -> {
            int c = 0;
            while (f == null && failure.get() == null && c++ < 100000)
                Thread.yield();
            Assert.assertTrue(failure.get() instanceof OutOfMemoryError);
            if (f != null)
                Assert.assertTrue(f.cause() instanceof OutOfMemoryError);
            failure.set(null);
        };
        Runnable submit = wrapSubmit(() -> { throw new OutOfMemoryError(); });
        testExecution(builder.build(), submit, verify);
        testExecution(builder.build(), WithResources.none(), submit, verify);
        testFailGetWithResources(builder.build(), () -> { throw new OutOfMemoryError(); }, verify);
        testFailCloseWithResources(builder.build(), () -> () -> { throw new OutOfMemoryError(); }, verify);
    }

    public <E extends SequentialExecutorPlus> void testAtLeastOnce(Supplier<ExecutorBuilder<? extends E>> builders) throws Throwable
    {
        ExecutorBuilder<? extends E> builder = builders.get();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread.UncaughtExceptionHandler ueh = (thread, f) -> failure.set(f);
        builder.withUncaughtExceptionHandler(ueh);

        SequentialExecutorPlus exec = builder.build();

        Semaphore enter = newSemaphore(0);
        Semaphore exit = newSemaphore(0);
        Semaphore runAfter = newSemaphore(0);
        SequentialExecutorPlus.AtLeastOnceTrigger trigger;
        trigger = exec.atLeastOnceTrigger(() -> { enter.release(1); exit.acquireThrowUncheckedOnInterrupt(1); });

        // check runAfter runs immediately
        trigger.runAfter(() -> runAfter.release(1));
        Assert.assertTrue(runAfter.tryAcquire(1, 1L, TimeUnit.SECONDS));

        Assert.assertTrue(trigger.trigger());
        enter.acquire(1);
        Assert.assertTrue(trigger.trigger());
        Assert.assertFalse(trigger.trigger());
        trigger.runAfter(() -> runAfter.release(1));
        Assert.assertFalse(runAfter.tryAcquire(1, 10L, TimeUnit.MILLISECONDS));
        exit.release(1);
        enter.acquire(1);
        Assert.assertFalse(runAfter.tryAcquire(1, 10L, TimeUnit.MILLISECONDS));
        Assert.assertTrue(trigger.trigger());
        Assert.assertFalse(trigger.trigger());
        exit.release(1);
        Assert.assertTrue(runAfter.tryAcquire(1, 1L, TimeUnit.SECONDS));
        exit.release(1);

        trigger = exec.atLeastOnceTrigger(() -> { throw new OutOfMemoryError(); });
        trigger.trigger();
        trigger.sync();
        Assert.assertTrue(failure.get() instanceof OutOfMemoryError);
    }

    void testExecution(ExecutorPlus e, WithResources withResources, Runnable submit, Verify<Future<?>> verify) throws Throwable
    {
        AtomicInteger i = new AtomicInteger();
        e.execute(() -> { i.incrementAndGet(); return withResources.get(); } , () -> { i.incrementAndGet(); submit.run(); });
        while (i.get() < 2) Thread.yield();
        verify.test(null);
        verify.test(e.submit(() -> { i.incrementAndGet(); return withResources.get(); }, () -> { i.incrementAndGet(); submit.run(); return null; }).await());
        Assert.assertEquals(4, i.get());
        verify.test(e.submit(() -> { i.incrementAndGet(); return withResources.get(); }, () -> { i.incrementAndGet(); submit.run(); }).await());
        Assert.assertEquals(6, i.get());
        verify.test(e.submit(() -> { i.incrementAndGet(); return withResources.get(); }, () -> { i.incrementAndGet(); submit.run(); }, null).await());
        Assert.assertEquals(8, i.get());
    }

    void testExecution(ExecutorPlus e, Runnable submit, Verify<Future<?>> verify) throws Throwable
    {
        AtomicInteger i = new AtomicInteger();
        e.execute(() -> { i.incrementAndGet(); submit.run(); });
        while (i.get() < 1) Thread.yield();
        verify.test(null);
        e.maybeExecuteImmediately(() -> { i.incrementAndGet(); submit.run(); });
        while (i.get() < 2) Thread.yield();
        verify.test(null);
        verify.test(e.submit(() -> { i.incrementAndGet(); submit.run(); return null; }).await());
        Assert.assertEquals(3, i.get());
        verify.test(e.submit(() -> { i.incrementAndGet(); submit.run(); }).await());
        Assert.assertEquals(4, i.get());
        verify.test(e.submit(() -> { i.incrementAndGet(); submit.run(); }, null).await());
        Assert.assertEquals(5, i.get());
    }

    void testFailGetWithResources(ExecutorPlus e, WithResources withResources, Verify<Future<?>> verify) throws Throwable
    {
        AtomicInteger i = new AtomicInteger();
        WithResources countingOnGetResources = () -> { i.incrementAndGet(); return withResources.get(); };
        AtomicBoolean executed = new AtomicBoolean();
        e.execute(countingOnGetResources, () -> executed.set(true));
        while (i.get() < 1) Thread.yield();
        verify.test(null);
        Assert.assertFalse(executed.get());
        verify.test(e.submit(countingOnGetResources, () -> { executed.set(true); return null; } ).await());
        Assert.assertEquals(2, i.get());
        Assert.assertFalse(executed.get());
        verify.test(e.submit(countingOnGetResources, () -> { executed.set(true); }).await());
        Assert.assertEquals(3, i.get());
        Assert.assertFalse(executed.get());
        verify.test(e.submit(countingOnGetResources, () -> { executed.set(true); }, null).await());
        Assert.assertEquals(4, i.get());
        Assert.assertFalse(executed.get());
    }

    void testFailCloseWithResources(ExecutorPlus e, WithResources withResources, Verify<Future<?>> verify) throws Throwable
    {
        AtomicInteger i = new AtomicInteger();
        WithResources countingOnCloseResources = () -> { Closeable close = withResources.get(); return () -> { i.incrementAndGet(); close.close(); }; };
        e.execute(countingOnCloseResources, i::incrementAndGet);
        while (i.get() < 2) Thread.yield();
        verify.test(null);
        verify.test(e.submit(countingOnCloseResources, () -> { i.incrementAndGet(); return null; } ).await());
        Assert.assertEquals(4, i.get());
        verify.test(e.submit(countingOnCloseResources, () -> { i.incrementAndGet(); }).await());
        Assert.assertEquals(6, i.get());
        verify.test(e.submit(countingOnCloseResources, () -> { i.incrementAndGet(); }, null).await());
        Assert.assertEquals(8, i.get());
    }

}
