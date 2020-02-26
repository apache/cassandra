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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntToLongFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.cassandra.concurrent.NamedThreadFactory;

public final class Retry
{
    private static final ScheduledExecutorService SCHEDULED = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("RetryScheduler"));

    private Retry()
    {

    }

    public static CompletableFuture<Void> schedule(final Duration duration, final Runnable fn)
    {
        return schedule(duration, ForkJoinPool.commonPool(), fn);
    }

    public static CompletableFuture<Void> schedule(final Duration duration, final Executor executor, final Runnable fn)
    {
        long nanos = duration.toNanos();
        CompletableFuture<Void> future = new CompletableFuture<>();
        SCHEDULED.schedule(() -> run0(executor, future, fn), nanos, TimeUnit.NANOSECONDS);
        return future;
    }

    private static void run0(final Executor executor, final CompletableFuture<Void> future, final Runnable fn)
    {
        try
        {
            executor.execute(() -> {
                try
                {
                    fn.run();
                    future.complete(null);
                }
                catch (Exception e)
                {
                    future.completeExceptionally(e);
                }
            });
        }
        catch (Exception e)
        {
            future.completeExceptionally(e);
        }
    }

    public static <A> CompletableFuture<A> retryWithBackoff(final int maxRetries,
                                                            final Supplier<CompletableFuture<A>> fn,
                                                            final Predicate<Throwable> retryableException)
    {
        CompletableFuture<A> future = new CompletableFuture<>();
        retryWithBackoff0(future, 0, maxRetries, fn, retryableException, retryCount -> computeSleepTimeMillis(retryCount, 50, 1000));
        return future;
    }

    public static <A> A retryWithBackoffBlocking(final int maxRetries,
                                                 final Supplier<A> fn)
    {
        return retryWithBackoffBlocking(maxRetries, fn, (ignore) -> true);
    }

    public static <A> A retryWithBackoffBlocking(final int maxRetries,
                                                 final Supplier<A> fn,
                                                 final Predicate<Throwable> retryableException)
    {
        return retryWithBackoff(maxRetries, () -> CompletableFuture.completedFuture(fn.get()), retryableException).join();
    }

    private static <A> void retryWithBackoff0(final CompletableFuture<A> result,
                                              final int retryCount,
                                              final int maxRetry,
                                              final Supplier<CompletableFuture<A>> body,
                                              final Predicate<Throwable> retryableException,
                                              final IntToLongFunction completeSleep)
    {
        try
        {
            Consumer<Throwable> attemptRetry = cause -> {
                if (retryCount >= maxRetry || !retryableException.test(cause))
                {
                    // too many attempts or exception isn't retryable, so fail
                    result.completeExceptionally(cause);
                }
                else
                {
                    long sleepMillis = completeSleep.applyAsLong(retryCount);
                    schedule(Duration.ofMillis(sleepMillis), () -> {
                        retryWithBackoff0(result, retryCount + 1, maxRetry, body, retryableException, completeSleep);
                    });
                }
            };

            // sanity check that the future isn't filled
            // the most likely cause for this is when the future is composed with other futures (such as .successAsList);
            // the failure of a different future may cancel this one, so stop running
            if (result.isDone())
            {
                if (!(result.isCancelled() || result.isCompletedExceptionally()))
                {
                    // the result is success!  But we didn't fill it...
                    new RuntimeException("Attempt to retry but found future was successful... aborting " + body).printStackTrace();
                }
                return;
            }

            CompletableFuture<A> future;
            try
            {
                future = body.get();
            }
            catch (Exception e)
            {
                attemptRetry.accept(e);
                return;
            }

            future.whenComplete((success, failure) -> {
                if (failure == null)
                {
                    result.complete(success);
                }
                else
                {
                    attemptRetry.accept(failure instanceof CompletionException ? failure.getCause() : failure);
                }
            });
        }
        catch (Exception e)
        {
            result.completeExceptionally(e);
        }
    }

    private static long computeSleepTimeMillis(int retryCount, long baseSleepTimeMillis, long maxSleepMillis)
    {
        long baseTime = baseSleepTimeMillis * (1L << retryCount);
        // its possible that this overflows, so fall back to max;
        if (baseTime <= 0)
        {
            baseTime = maxSleepMillis;
        }
        // now make sure this is capped to target max
        baseTime = Math.min(baseTime, maxSleepMillis);

        return (long) (baseTime * (ThreadLocalRandom.current().nextDouble() + 0.5));
    }
}
