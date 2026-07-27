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

package org.apache.cassandra.simulator.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.junit.Test;

import accord.api.AsyncExecutor;
import accord.api.ExclusiveAsyncExecutor;
import accord.utils.async.Cancellable;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableSupplier;
import org.apache.cassandra.service.accord.execution.AccordExecutor;
import org.apache.cassandra.service.accord.execution.AccordExecutorAsyncSubmit;
import org.apache.cassandra.service.accord.execution.AccordExecutorSignalLoop;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.simulator.test.AccordExecutorTest.Submitted.Consequences;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.SignalLock;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;

// TODO (required): have simulator intercept ReentantLock so we can test SyncSubmit and SemiSyncSubmit
public class AccordExecutorTest extends SimulationTestBase
{
    @Test
    public void signalLoopTest()
    {
        executorTest(() -> new AccordExecutorSignalLoop(1, RUN_WITHOUT_LOCK, SignalLock.MAX_THREADS, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new AccordAgent()),
                     16);
    }

    @Test
    public void signalSpinLoopTest()
    {
        executorTest(() -> new AccordExecutorSignalLoop(1, RUN_WITHOUT_LOCK, SignalLock.MAX_THREADS, 10, 100, TimeUnit.MICROSECONDS, i ->"Loop" + i, new AccordAgent()),
                     16);
    }

    @Test
    public void ayncSubmitTest()
    {
        int threads = 32;
        executorTest(() -> new AccordExecutorAsyncSubmit(1, RUN_WITHOUT_LOCK, threads, i -> "Loop" + i, new AccordAgent()),
                     16);
    }

    static class Submitted
    {
        final AtomicInteger nextId = new AtomicInteger();
        final AtomicInteger doneBefore = new AtomicInteger();
        final ConcurrentHashMap<Integer, Collection<Future<?>>> consequences = new ConcurrentHashMap<>();

        boolean isDone()
        {
            return isDoneBetween(0, nextId.get());
        }

        boolean isDoneBefore(int before)
        {
            if (!isDoneBetween(doneBefore.get(), before))
                return false;
            doneBefore.accumulateAndGet(before, Integer::max);
            return true;
        }

        boolean isDoneBetween(int from, int before)
        {
            for (int id = from ; id < before ; ++id)
            {
                for (Future<?> future : consequences.get(id))
                {
                    if (!future.isDone())
                        return false;
                }
            }
            return true;
        }

        class Consequences extends ConcurrentLinkedQueue<Future<?>>
        {
            volatile boolean started;

            void ensureStarted()
            {
                if (started)
                    return;

                synchronized (this)
                {
                    if (started)
                        return;

                    while (true)
                    {
                        int id = nextId.get();
                        Object prev = consequences.putIfAbsent(id, this);
                        nextId.compareAndSet(id, id + 1);
                        if (prev == null)
                            break;
                    }

                    started = true;
                }
            }
        }

        Consequences allocate()
        {
            return new Consequences();
        }
    }

    static class Control extends ConcurrentLinkedQueue<Cancellable>
    {
        final Submitted submitted;
        final AtomicInteger count = new AtomicInteger();
        final float cancelChance;
        float processChance;

        Control(float cancelChance, Submitted submitted)
        {
            this(submitted, cancelChance, ThreadLocalRandom.current().nextFloat() * 0.5f);
        }

        Control(Submitted submitted, float cancelChance, float processChance)
        {
            this.submitted = submitted;
            this.cancelChance = cancelChance;
            this.processChance = processChance;
        }

        void submit(AsyncExecutor executor, Consequences consequences, Consumer<Consequences> run)
        {
            AsyncPromise<?> future = new AsyncPromise<>();
            consequences.add(future);
            Cancellable cancel = executor.chain(() -> run.accept(consequences)).begin((success, fail) -> {
                if (fail == null) future.trySuccess(null);
                else
                {
                    future.tryFailure(fail);
                    if (fail instanceof CancellationException)
                        run.accept(submitted.allocate());
                }
            });
            consequences.ensureStarted();

            if (cancel != null && ThreadLocalRandom.current().nextFloat() <= cancelChance)
            {
                add(cancel);
                count.incrementAndGet();
            }

            if (count.get() > 0 && ThreadLocalRandom.current().nextFloat() <= processChance)
            {
                int cancelCount = 0;
                do
                {
                    ++cancelCount;

                    float delta = ThreadLocalRandom.current().nextFloat() - 0.5f;
                    if (delta < 0) processChance /= delta;
                    else processChance *= -delta;
                    if (processChance < 0.001f || processChance > 0.999f)
                        processChance = cancelChance;
                } while (count.decrementAndGet() > 0 && ThreadLocalRandom.current().nextFloat() <= processChance);

                // do outside of loop to avoid reentry
                while (cancelCount-- > 0)
                    remove().cancel();
            }
        }
    }

    public void executorTest(SerializableSupplier<AccordExecutor> supplier, int submissionThreads)
    {
        simulate(arr(() -> {
                     try
                     {
                         DatabaseDescriptor.daemonInitialization();
                         ExecutorPlus submit = executorFactory().pooled("submit-test", submissionThreads);
                         AccordExecutor executor = supplier.get();
                         Lock lock = executor.unsafeLock();
                         ExclusiveAsyncExecutor sequentialExecutor = executor.newExclusiveExecutor();
                         Executor lockExecutor = executorFactory().sequential("lock");

                         for (float sleepChance : new float[] { 0f, 0.01f, 0.1f })
                         {
                             for (float lockChance : new float[] { 0f, 0.01f, 0.1f })
                             {
                                 for (float cancelChance : new float[] { 0f, 0.01f, 0.1f })
                                 {
                                     System.out.println(String.format("sleepChance %.2f, lockChance %.2f, cancelChance %.2f", sleepChance, lockChance, cancelChance));
                                     List<Future<?>> done = new ArrayList<>();
                                     Submitted submitted = new Submitted();
                                     for (int i = 0; i < submissionThreads; ++i)
                                     {
                                         int id = i;
                                         done.add(submit.submit(() -> {
                                             try
                                             {
                                                 submitLoop(id, lock, executor, sequentialExecutor, lockExecutor, 20, 10, sleepChance, lockChance, new Control(cancelChance, submitted), submitted);
                                             }
                                             catch (ExecutionException | InterruptedException e)
                                             {
                                                 throw new RuntimeException(e);
                                             }
                                         }));
                                     }
                                     for (Future<?> f : done)
                                         f.get();

                                     if (!submitted.isDone())
                                         throw new AssertionError();
                                 }
                             }
                         }
                     }
                     catch (Throwable t)
                     {
                         throw new RuntimeException(t);
                     }
                 }),
                 () -> {}, 1L);
    }

    private static void submitLoop(int id, Lock lock, AccordExecutor executor, ExclusiveAsyncExecutor sequentialExecutor, Executor lockExecutor, int outerLoop, int innerLoop, float sleepChance, float lockChance, Control control, Submitted submitted) throws ExecutionException, InterruptedException
    {
        ConcurrentLinkedQueue<Future<?>> awaitConsequences = new ConcurrentLinkedQueue<>();
        while (outerLoop-- > 0)
        {
            List<Collection<Future<?>>> allAwaitSubmitted = new ArrayList<>();
            for (int i = 0; i < innerLoop; ++i)
            {
                Consequences awaitSubmitted = submitted.allocate();
                allAwaitSubmitted.add(awaitSubmitted);
                submitRecursive(lock, executor, sequentialExecutor, 1 + i, awaitSubmitted, awaitConsequences, submitted, sleepChance, lockChance, control);
            }

            AtomicBoolean done = new AtomicBoolean();
            submitUntil(lock, lockExecutor, sleepChance, done::get);
            for (Collection<Future<?>> awaitSubmitted : allAwaitSubmitted)
                await(awaitSubmitted, CancellationException.class);
            await(awaitConsequences, null);
            done.set(true);
            System.out.println("Loop " + id + '.' + (1 + outerLoop));
        }
    }

    private static void submitRecursive(Lock lock, AccordExecutor executor, ExclusiveAsyncExecutor sequentialExecutor, int count, Consequences consequences, Collection<Future<?>> awaitConsequences, Submitted submitted, float sleepChance, float lockChance, Control control)
    {
        AsyncExecutor submitTo = ThreadLocalRandom.current().nextBoolean() ? executor : sequentialExecutor;

        control.submit(submitTo, consequences, nextConsequences -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            boolean locked = false;
            if (rnd.nextFloat() < lockChance)
            {
                if (rnd.nextBoolean()) locked = lock.tryLock();
                else { locked = true; lock.lock(); }
            }
            if (ThreadLocalRandom.current().nextFloat() < 0.01f)
            {
                int expectDoneBefore = submitted.nextId.get();
                AsyncPromise<Void> afterConsequences = new AsyncPromise<>();
                executor.afterSubmittedAndConsequences(() -> {
                    if (!submitted.isDoneBefore(expectDoneBefore))
                        throw new AssertionError();
                    afterConsequences.setSuccess(null);
                });
                awaitConsequences.add(afterConsequences);
            }
            try
            {
                if (count > 1)
                    submitRecursive(lock, executor, sequentialExecutor, count -1, nextConsequences, awaitConsequences, submitted, sleepChance, lockChance, control);
                if (rnd.nextFloat() < sleepChance)
                    LockSupport.parkNanos(rnd.nextInt(10000, 100000));
            }
            finally
            {
                if (locked)
                    lock.unlock();
            }
        });
    }

    private static void submitUntil(Lock lock, Executor executor, float sleepChance, BooleanSupplier done)
    {
        if (done.getAsBoolean())
            return;

        executor.execute(() -> {

            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            boolean tryLock = rnd.nextBoolean();
            boolean locked = !tryLock;
            if (tryLock) locked = lock.tryLock();
            else lock.lock();
            try
            {
                if (rnd.nextFloat() < sleepChance)
                    LockSupport.parkNanos(rnd.nextInt(10000, 100000));

                submitUntil(lock, executor, sleepChance, done);
            }
            finally
            {
                if (locked)
                    lock.unlock();
            }
        });
    }

    private static void await(Collection<Future<?>> await, @Nullable Class<? extends Throwable> ignore) throws InterruptedException, ExecutionException
    {
        for (Future<?> future : await)
        {
            try { future.get(); }
            catch (ExecutionException e)
            {
                if (ignore == null || !(ignore.isInstance(e.getCause())))
                    throw e;
            }
        }
    }
}
