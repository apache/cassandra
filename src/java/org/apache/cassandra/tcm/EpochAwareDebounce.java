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

package org.apache.cassandra.tcm;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * When debouncing from a replica we know exactly which epoch we need, so to avoid retries we
 * keep track of which epoch we are currently debouncing, and if a request for a newer epoch
 * comes in, we create a new future. If a request for a newer epoch comes in, we simply
 * swap out the current future reference for a new one which is requesting the newer epoch.
 */
public class EpochAwareDebounce<T>
{
    public static final EpochAwareDebounce<ClusterMetadata> instance = new EpochAwareDebounce<>();

    private final AtomicReference<EpochAwareAsyncPromise<T>> currentFuture = new AtomicReference<>();
    private final ExecutorPlus executor;

    private EpochAwareDebounce()
    {
        // 2 threads since we might start a new debounce for a newer epoch while the old one is executing
        this.executor = ExecutorFactory.Global.executorFactory().pooled("debounce", 2);
    }

    public Future<T> getAsync(Callable<T> get, Epoch epoch)
    {
        while (true)
        {
            EpochAwareAsyncPromise<T> running = currentFuture.get();
            if (running != null && !running.isDone() && running.epoch.isEqualOrAfter(epoch))
                return running;

            EpochAwareAsyncPromise<T> promise = new EpochAwareAsyncPromise<>(epoch);
            if (currentFuture.compareAndSet(running, promise))
            {
                executor.submit(() -> {
                    try
                    {
                        promise.setSuccess(get.call());
                    }
                    catch (Throwable t)
                    {
                        promise.setFailure(t);
                    }
                });
                return promise;
            }
        }
    }

    private static class EpochAwareAsyncPromise<T> extends AsyncPromise<T>
    {
        private final Epoch epoch;
        public EpochAwareAsyncPromise(Epoch epoch)
        {
            this.epoch = epoch;
        }
    }

    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(timeout, unit, executor);
    }
}
