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

package org.apache.cassandra.repair;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.cassandra.utils.Pair;

/**
 * Task scheduler that limits the number of concurrent tasks across multiple executors.
 */
public interface Scheduler
{
    default <T> ListenableFuture<T> schedule(Supplier<ListenableFuture<T>> task, Executor executor)
    {
        return schedule(new Task<>(task, executor), executor);
    }

    <T> Task<T> schedule(Task<T> task, Executor executor);

    static Scheduler build(int concurrentValidations)
    {
        return concurrentValidations <= 0
               ? new NoopScheduler()
               : new LimitedConcurrentScheduler(concurrentValidations);
    }

    final class NoopScheduler implements Scheduler
    {
        @Override
        public <T> Task<T> schedule(Task<T> task, Executor executor)
        {
            executor.execute(task);
            return task;
        }
    }

    final class LimitedConcurrentScheduler implements Scheduler
    {
        private final int concurrentValidations;
        @GuardedBy("this")
        private int inflight = 0;
        @GuardedBy("this")
        private final Queue<Pair<Task<?>, Executor>> tasks = new LinkedList<>();

        LimitedConcurrentScheduler(int concurrentValidations)
        {
            this.concurrentValidations = concurrentValidations;
        }

        @Override
        public synchronized <T> Task<T> schedule(Task<T> task, Executor executor)
        {
            tasks.offer(Pair.create(task, executor));
            maybeSchedule();
            return task;
        }

        private synchronized void onDone()
        {
            inflight--;
            maybeSchedule();
        }

        private void maybeSchedule()
        {
            if (inflight == concurrentValidations || tasks.isEmpty())
                return;
            inflight++;
            Pair<Task<?>, Executor> pair = tasks.poll();
            Futures.addCallback(pair.left, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object result)
                {
                    onDone();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    onDone();
                }
            }, pair.right);
            pair.right.execute(pair.left);
        }
    }

    class Task<T> extends AbstractFuture<T> implements Runnable
    {
        private final Supplier<ListenableFuture<T>> supplier;
        private final Executor executor;

        public Task(Supplier<ListenableFuture<T>> supplier, Executor executor)
        {
            this.supplier = supplier;
            this.executor = executor;
        }

        @Override
        public void run()
        {
            Futures.addCallback(supplier.get(), new FutureCallback<T>() {
                @Override
                public void onSuccess(T result)
                {
                    set(result);
                }

                @Override
                public void onFailure(Throwable t)
                {
                    setException(t);
                }
            }, executor);
        }
    }
}