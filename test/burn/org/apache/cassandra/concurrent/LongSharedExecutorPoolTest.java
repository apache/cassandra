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

import java.util.BitSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.math3.distribution.WeibullDistribution;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class LongSharedExecutorPoolTest
{

    private static final class WaitTask implements Runnable
    {
        final long nanos;

        private WaitTask(long nanos)
        {
            this.nanos = nanos;
        }

        public void run()
        {
            LockSupport.parkNanos(nanos);
        }
    }

    private static final class Result implements Comparable<Result>
    {
        final Future<?> future;
        final long forecastedCompletion;

        private Result(Future<?> future, long forecastedCompletion)
        {
            this.future = future;
            this.forecastedCompletion = forecastedCompletion;
        }

        public int compareTo(Result that)
        {
            int c = Long.compare(this.forecastedCompletion, that.forecastedCompletion);
            if (c != 0)
                return c;
            c = Integer.compare(this.hashCode(), that.hashCode());
            if (c != 0)
                return c;
            return Integer.compare(this.future.hashCode(), that.future.hashCode());
        }
    }

    private static final class Batch implements Comparable<Batch>
    {
        final TreeSet<Result> results;
        final long timeout;
        final int executorIndex;

        private Batch(TreeSet<Result> results, long timeout, int executorIndex)
        {
            this.results = results;
            this.timeout = timeout;
            this.executorIndex = executorIndex;
        }

        public int compareTo(Batch that)
        {
            int c = Long.compare(this.timeout, that.timeout);
            if (c != 0)
                return c;
            c = Integer.compare(this.results.size(), that.results.size());
            if (c != 0)
                return c;
            return Integer.compare(this.hashCode(), that.hashCode());
        }
    }

    @Test @Ignore // see CASSANDRA-16497. re-evaluate SEPThreadpools post 4.0
    public void testPromptnessOfExecution() throws InterruptedException, ExecutionException
    {
        testPromptnessOfExecution(TimeUnit.MINUTES.toNanos(2L), 0.5f);
    }

    private void testPromptnessOfExecution(long intervalNanos, float loadIncrement) throws InterruptedException, ExecutionException
    {
        final int executorCount = 4;
        int threadCount = 8;
        int scale = 1024;
        final WeibullDistribution workTime = new WeibullDistribution(3, 200000);
        final long minWorkTime = TimeUnit.MICROSECONDS.toNanos(1);
        final long maxWorkTime = TimeUnit.MILLISECONDS.toNanos(1);

        final int[] threadCounts = new int[executorCount];
        final WeibullDistribution[] workCount = new WeibullDistribution[executorCount];
        final ExecutorService[] executors = new ExecutorService[executorCount];
        for (int i = 0 ; i < executors.length ; i++)
        {
            executors[i] = SharedExecutorPool.SHARED.newExecutor(threadCount, "test" + i, "test" + i);
            threadCounts[i] = threadCount;
            workCount[i] = new WeibullDistribution(2, scale);
            threadCount *= 2;
            scale *= 2;
        }

        long runs = 0;
        long events = 0;
        final TreeSet<Batch> pending = new TreeSet<>();
        final BitSet executorsWithWork = new BitSet(executorCount);
        long until = 0;
        // basic idea is to go through different levels of load on the executor service; initially is all small batches
        // (mostly within max queue size) of very short operations, moving to progressively larger batches
        // (beyond max queued size), and longer operations
        for (float multiplier = 0f ; multiplier < 2.01f ; )
        {
            if (nanoTime() > until)
            {
                System.out.println(String.format("Completed %.0fK batches with %.1fM events", runs * 0.001f, events * 0.000001f));
                events = 0;
                until = nanoTime() + intervalNanos;
                multiplier += loadIncrement;
                System.out.println(String.format("Running for %ds with load multiplier %.1f", TimeUnit.NANOSECONDS.toSeconds(intervalNanos), multiplier));
            }

            // wait a random amount of time so we submit new tasks in various stages of
            long timeout;
            if (pending.isEmpty()) timeout = 0;
            else if (Math.random() > 0.98) timeout = Long.MAX_VALUE;
            else if (pending.size() == executorCount) timeout = pending.first().timeout;
            else timeout = (long) (Math.random() * pending.last().timeout);

            while (!pending.isEmpty() && timeout > nanoTime())
            {
                Batch first = pending.first();
                boolean complete = false;
                try
                {
                    for (Result result : first.results.descendingSet())
                        result.future.get(timeout - nanoTime(), TimeUnit.NANOSECONDS);
                    complete = true;
                }
                catch (TimeoutException e)
                {
                }
                if (!complete && nanoTime() > first.timeout)
                {
                    for (Result result : first.results)
                        if (!result.future.isDone())
                            throw new AssertionError();
                    complete = true;
                }
                if (complete)
                {
                    pending.pollFirst();
                    executorsWithWork.clear(first.executorIndex);
                }
            }

            // if we've emptied the executors, give all our threads an opportunity to spin down
            if (timeout == Long.MAX_VALUE)
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);

            // submit a random batch to the first free executor service
            int executorIndex = executorsWithWork.nextClearBit(0);
            if (executorIndex >= executorCount)
                continue;
            executorsWithWork.set(executorIndex);
            ExecutorService executor = executors[executorIndex];
            TreeSet<Result> results = new TreeSet<>();
            int count = (int) (workCount[executorIndex].sample() * multiplier);
            long targetTotalElapsed = 0;
            long start = nanoTime();
            long baseTime;
            if (Math.random() > 0.5) baseTime = 2 * (long) (workTime.sample() * multiplier);
            else  baseTime = 0;
            for (int j = 0 ; j < count ; j++)
            {
                long time;
                if (baseTime == 0) time = (long) (workTime.sample() * multiplier);
                else time = (long) (baseTime * Math.random());
                if (time < minWorkTime)
                    time = minWorkTime;
                if (time > maxWorkTime)
                    time = maxWorkTime;
                targetTotalElapsed += time;
                Future<?> future = executor.submit(new WaitTask(time));
                results.add(new Result(future, nanoTime() + time));
            }
            long end = start + (long) Math.ceil(targetTotalElapsed / (double) threadCounts[executorIndex])
                       + TimeUnit.MILLISECONDS.toNanos(100L);
            long now = nanoTime();
            if (runs++ > executorCount && now > end)
                throw new AssertionError();
            events += results.size();
            pending.add(new Batch(results, end, executorIndex));
//            System.out.println(String.format("Submitted batch to executor %d with %d items and %d permitted millis", executorIndex, count, TimeUnit.NANOSECONDS.toMillis(end - start)));
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
        // do longer test
        new LongSharedExecutorPoolTest().testPromptnessOfExecution(TimeUnit.MINUTES.toNanos(10L), 0.1f);
    }

}
