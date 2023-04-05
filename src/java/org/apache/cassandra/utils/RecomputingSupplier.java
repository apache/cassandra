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

package org.apache.cassandra.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

/**
 * Supplier that caches the last computed value until it is reset, forcing every caller of
 * {@link RecomputingSupplier#get(long, TimeUnit)} to wait until this value is computed if
 * it was not computed yet.
 *
 * Calling {@link RecomputingSupplier#recompute()} won't reset value for the already
 * waiting consumers, but instead will schedule one recomputation as soon as current one is done.
 */
public class RecomputingSupplier<T>
{
    private final Supplier<T> supplier;
    private final AtomicReference<Future<T>> cached = new AtomicReference<>(null);
    private final AtomicBoolean workInProgress = new AtomicBoolean(false);
    private final ExecutorService executor;

    public RecomputingSupplier(Supplier<T> supplier, ExecutorService executor)
    {
        this.supplier = supplier;
        this.executor = executor;
    }

    public void recompute()
    {
        Future<T> current = cached.get();
        boolean origWip = workInProgress.get();

        if (origWip || (current != null && !current.isDone()))
        {
            if (cached.get() != current)
                executor.submit(this::recompute);
            return; // if work is has not started yet, schedule task for the future
        }

        assert current == null || current.isDone();

        // The work is not in progress, and current future is done. Try to submit a new task.
        Promise<T> lazyValue = new AsyncPromise<>();
        if (cached.compareAndSet(current, lazyValue))
            executor.submit(() -> doWork(lazyValue));
        else
            executor.submit(this::recompute); // Lost CAS, resubmit
    }

    private void doWork(Promise<T> lazyValue)
    {
        T value = null;
        Throwable err = null;
        try
        {
            sanityCheck(workInProgress.compareAndSet(false, true));
            value = supplier.get();
        }
        catch (Throwable t)
        {
            err = t;
        }
        finally
        {
            sanityCheck(workInProgress.compareAndSet(true, false));
        }

        if (err == null)
            lazyValue.trySuccess(value);
        else
            lazyValue.tryFailure(err);
    }

    private static void sanityCheck(boolean check)
    {
        assert check : "At most one task should be executing using this executor";
    }

    public T get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException
    {
        Future<T> lazyValue = cached.get();

        // recompute was never called yet, return null.
        if (lazyValue == null)
            return null;

        return lazyValue.get(timeout, timeUnit);
    }

    public String toString()
    {
        return "RecomputingSupplier{" +
               "supplier=" + supplier +
               ", cached=" + cached +
               ", workInProgress=" + workInProgress +
               ", executor=" + executor +
               '}';
    }
}