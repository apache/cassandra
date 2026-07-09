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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import accord.api.AsyncExecutor;
import accord.local.ExclusiveAsyncExecutor;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableSupplier;
import org.apache.cassandra.service.accord.AccordExecutor;
import org.apache.cassandra.service.accord.AccordExecutorAsyncSubmit;
import org.apache.cassandra.service.accord.AccordExecutorSignalLoop;
import org.apache.cassandra.service.accord.api.AccordAgent;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.apache.cassandra.service.accord.AccordService.toFuture;

// TODO (required): have simulator intercept ReentantLock so we can test SyncSubmit and SemiSyncSubmit
public class AccordExecutorTest extends SimulationTestBase
{
    static final int EXECUTOR_THREAD_COUNT = 44;

    @Test
    public void signalLoopTest()
    {
        executorTest(() -> new AccordExecutorSignalLoop(1, RUN_WITHOUT_LOCK, EXECUTOR_THREAD_COUNT, -1, -1, TimeUnit.MICROSECONDS, i ->"Loop" + i, new AccordAgent()),
                     16);
    }

    @Test
    public void signalSpinLoopTest()
    {
        executorTest(() -> new AccordExecutorSignalLoop(1, RUN_WITHOUT_LOCK, EXECUTOR_THREAD_COUNT, 10, 100, TimeUnit.MICROSECONDS, i ->"Loop" + i, new AccordAgent()),
                     16);
    }

    @Test
    public void ayncSubmitTest()
    {
        executorTest(() -> new AccordExecutorAsyncSubmit(1, RUN_WITHOUT_LOCK, EXECUTOR_THREAD_COUNT, i -> "Loop" + i, new AccordAgent()),
                     16);
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
                         ExclusiveAsyncExecutor sequentialExecutor = executor.newSequentialExecutor();
                         Executor lockExecutor = executorFactory().sequential("lock");

                         for (float sleepChance : new float[] { 0f, 0.01f, 0.1f })
                         {
                             for (float lockChance : new float[] { 0f, 0.01f, 0.1f })
                             {
                                 List<Future<?>> done = new ArrayList<>();
                                 for (int i = 0 ; i < submissionThreads ; ++i)
                                 {
                                     int id = i;
                                     done.add(submit.submit(() -> {
                                         try
                                         {
                                             submitLoop(id, lock, executor, sequentialExecutor, lockExecutor, 20, 10, sleepChance, lockChance);
                                         }
                                         catch (ExecutionException | InterruptedException e)
                                         {
                                             throw new RuntimeException(e);
                                         }
                                     }));
                                 }
                                 for (Future<?> f : done)
                                     f.get();
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

    private static void submitLoop(int id, Lock lock, AccordExecutor executor, ExclusiveAsyncExecutor sequentialExecutor, Executor lockExecutor, int outerLoop, int innerLoop, float sleepChance, float lockChance) throws ExecutionException, InterruptedException
    {
        ConcurrentLinkedQueue<Future<?>> await = new ConcurrentLinkedQueue<>();
        while (outerLoop-- > 0)
        {
            for (int i = 0; i < innerLoop; ++i)
                submitRecursive(lock, executor, sequentialExecutor, 1 + i, await, sleepChance, lockChance);

            AtomicBoolean done = new AtomicBoolean();
            submitUntil(lock, lockExecutor, sleepChance, done::get);
            while (!await.isEmpty())
                await.poll().get();
            done.set(true);
            System.out.println("Loop " + id + '.' + (1 + outerLoop));
        }
    }

    private static void submitRecursive(Lock lock, AccordExecutor executor, ExclusiveAsyncExecutor sequentialExecutor, int count, Collection<Future<?>> await, float sleepChance, float lockChance)
    {
        AsyncExecutor submitTo = ThreadLocalRandom.current().nextBoolean() ? executor : sequentialExecutor;
        await.add(toFuture(submitTo.chain(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            boolean locked = false;
            if (rnd.nextFloat() < lockChance)
            {
                if (rnd.nextBoolean()) locked = lock.tryLock();
                else { locked = true; lock.lock(); }
            }
            try
            {
                if (count > 1)
                    submitRecursive(lock, executor, sequentialExecutor, count -1, await, sleepChance, lockChance);
                if (rnd.nextFloat() < sleepChance)
                    LockSupport.parkNanos(rnd.nextInt(10000, 100000));
            }
            finally
            {
                if (locked)
                    lock.unlock();
            }
        }).beginAsResult()));
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
}
