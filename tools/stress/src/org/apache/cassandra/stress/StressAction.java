/**
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
package org.apache.cassandra.stress;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.cassandra.stress.operations.OpDistribution;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.apache.cassandra.stress.report.StressMetrics;
import org.apache.cassandra.stress.settings.ConnectionAPI;
import org.apache.cassandra.stress.settings.SettingsCommand;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.transport.SimpleClient;
import org.jctools.queues.SpscArrayQueue;
import org.jctools.queues.SpscUnboundedArrayQueue;

import com.google.common.util.concurrent.Uninterruptibles;

public class StressAction implements Runnable
{

    private final StressSettings settings;
    private final ResultLogger output;
    public StressAction(StressSettings settings, ResultLogger out)
    {
        this.settings = settings;
        output = out;
    }

    public void run()
    {
        // creating keyspace and column families
        settings.maybeCreateKeyspaces();

        if (settings.command.count == 0)
        {
            output.println("N=0: SCHEMA CREATED, NOTHING ELSE DONE.");
            settings.disconnect();
            return;
        }

        output.println("Sleeping 2s...");
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        if (!settings.command.noWarmup)
            warmup(settings.command.getFactory(settings));

        if ((settings.command.truncate == SettingsCommand.TruncateWhen.ONCE) ||
            ((settings.rate.threadCount != -1) && (settings.command.truncate == SettingsCommand.TruncateWhen.ALWAYS)))
            settings.command.truncateTables(settings);

        // Required for creating a graph from the output file
        if (settings.rate.threadCount == -1)
            output.println("Thread count was not specified");

        // TODO : move this to a new queue wrapper that gates progress based on a poisson (or configurable) distribution
        UniformRateLimiter rateLimiter = null;
        if (settings.rate.opsPerSecond > 0)
            rateLimiter = new UniformRateLimiter(settings.rate.opsPerSecond);

        boolean success;
        if (settings.rate.minThreads > 0)
            success = runMulti(settings.rate.auto, rateLimiter);
        else
            success = null != run(settings.command.getFactory(settings), settings.rate.threadCount, settings.command.count,
                                  settings.command.duration, rateLimiter, settings.command.durationUnits, output, false);

        if (success)
            output.println("END");
        else
            output.println("FAILURE");

        settings.disconnect();

        if (!success)
            throw new RuntimeException("Failed to execute stress action");
    }

    // type provided separately to support recursive call for mixed command with each command type it is performing
    @SuppressWarnings("resource") // warmupOutput doesn't need closing
    private void warmup(OpDistributionFactory operations)
    {
        // do 25% of iterations as warmup but no more than 50k (by default hotspot compiles methods after 10k invocations)
        int iterations = (settings.command.count >= 0
                          ? Math.min(50000, (int)(settings.command.count * 0.25))
                          : 50000) * settings.node.nodes.size();
        if (iterations <= 0) return;

        int threads = 100;

        if (settings.rate.maxThreads > 0)
            threads = Math.min(threads, settings.rate.maxThreads);
        if (settings.rate.threadCount > 0)
            threads = Math.min(threads, settings.rate.threadCount);

        for (OpDistributionFactory single : operations.each())
        {
            // we need to warm up all the nodes in the cluster ideally, but we may not be the only stress instance;
            // so warm up all the nodes we're speaking to only.
            output.println(String.format("Warming up %s with %d iterations...", single.desc(), iterations));
            boolean success = null != run(single, threads, iterations, 0, null, null, ResultLogger.NOOP, true);
            if (!success)
                throw new RuntimeException("Failed to execute warmup");
        }

    }

    // TODO : permit varying more than just thread count
    // TODO : vary thread count based on percentage improvement of previous increment, not by fixed amounts
    private boolean runMulti(boolean auto, UniformRateLimiter rateLimiter)
    {
        if (settings.command.targetUncertainty >= 0)
            output.println("WARNING: uncertainty mode (err<) results in uneven workload between thread runs, so should be used for high level analysis only");
        int prevThreadCount = -1;
        int threadCount = settings.rate.minThreads;
        List<StressMetrics> results = new ArrayList<>();
        List<String> runIds = new ArrayList<>();
        do
        {
            output.println("");
            output.println(String.format("Running with %d threadCount", threadCount));

            if (settings.command.truncate == SettingsCommand.TruncateWhen.ALWAYS)
                settings.command.truncateTables(settings);

            StressMetrics result = run(settings.command.getFactory(settings), threadCount, settings.command.count,
                                       settings.command.duration, rateLimiter, settings.command.durationUnits, output, false);
            if (result == null)
                return false;
            results.add(result);

            if (prevThreadCount > 0)
                System.out.println(String.format("Improvement over %d threadCount: %.0f%%",
                        prevThreadCount, 100 * averageImprovement(results, 1)));

            runIds.add(threadCount + " threadCount");
            prevThreadCount = threadCount;
            if (threadCount < 16)
                threadCount *= 2;
            else
                threadCount *= 1.5;

            if (!results.isEmpty() && threadCount > settings.rate.maxThreads)
                break;

            if (settings.command.type.updates)
            {
                // pause an arbitrary period of time to let the commit log flush, etc. shouldn't make much difference
                // as we only increase load, never decrease it
                output.println("Sleeping for 15s");
                try
                {
                    Thread.sleep(15 * 1000);
                } catch (InterruptedException e)
                {
                    return false;
                }
            }
            // run until we have not improved throughput significantly for previous three runs
        } while (!auto || (hasAverageImprovement(results, 3, 0) && hasAverageImprovement(results, 5, settings.command.targetUncertainty)));

        // summarise all results
        StressMetrics.summarise(runIds, results, output);
        return true;
    }

    private boolean hasAverageImprovement(List<StressMetrics> results, int count, double minImprovement)
    {
        return results.size() < count + 1 || averageImprovement(results, count) >= minImprovement;
    }

    private double averageImprovement(List<StressMetrics> results, int count)
    {
        double improvement = 0;
        for (int i = results.size() - count ; i < results.size() ; i++)
        {
            double prev = results.get(i - 1).opRate();
            double cur = results.get(i).opRate();
            improvement += (cur - prev) / prev;
        }
        return improvement / count;
    }

    private StressMetrics run(OpDistributionFactory operations,
                              int threadCount,
                              long opCount,
                              long duration,
                              UniformRateLimiter rateLimiter,
                              TimeUnit durationUnits,
                              ResultLogger output,
                              boolean isWarmup)
    {
        output.println(String.format("Running %s with %d threads %s",
                                     operations.desc(),
                                     threadCount,
                                     durationUnits != null ? duration + " " + durationUnits.toString().toLowerCase()
                                        : opCount > 0      ? "for " + opCount + " iteration"
                                                           : "until stderr of mean < " + settings.command.targetUncertainty));
        final WorkManager workManager;
        if (opCount < 0)
            workManager = new WorkManager.ContinuousWorkManager();
        else
            workManager = new WorkManager.FixedWorkManager(opCount);

        final StressMetrics metrics = new StressMetrics(output, settings.log.intervalMillis, settings);

        final CountDownLatch releaseConsumers = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(threadCount);
        final Consumer[] consumers = new Consumer[threadCount];
        for (int i = 0; i < threadCount; i++)
        {
            consumers[i] = new Consumer(operations, isWarmup,
                                        done, start, releaseConsumers, workManager, metrics, rateLimiter);
        }

        // starting worker threadCount
        for (int i = 0; i < threadCount; i++)
            consumers[i].start();

        // wait for the lot of them to get their pants on
        try
        {
            start.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Unexpected interruption", e);
        }
        // start counting from NOW!
        if(rateLimiter != null)
        {
            rateLimiter.start();
        }
        // release the hounds!!!
        releaseConsumers.countDown();

        metrics.start();

        if (durationUnits != null)
        {
            Uninterruptibles.sleepUninterruptibly(duration, durationUnits);
            workManager.stop();
        }
        else if (opCount <= 0)
        {
            try
            {
                metrics.waitUntilConverges(settings.command.targetUncertainty,
                        settings.command.minimumUncertaintyMeasurements,
                        settings.command.maximumUncertaintyMeasurements);
            } catch (InterruptedException e) { }
            workManager.stop();
        }

        try
        {
            done.await();
            metrics.stop();
        }
        catch (InterruptedException e) {}

        if (metrics.wasCancelled())
            return null;

        metrics.summarise();

        boolean success = true;
        for (Consumer consumer : consumers)
            success &= consumer.success;

        if (!success)
            return null;

        return metrics;
    }

    /**
     * Provides a 'next operation time' for rate limited operation streams. The rate limiter is thread safe and is to be
     * shared by all consumer threads.
     */
    private static class UniformRateLimiter
    {
        long start = Long.MIN_VALUE;
        final long intervalNs;
        final AtomicLong opIndex = new AtomicLong();

        UniformRateLimiter(int opsPerSec)
        {
            intervalNs = 1000000000 / opsPerSec;
        }

        void start()
        {
            start = System.nanoTime();
        }

        /**
         * @param partitionCount
         * @return expect start time in ns for the operation
         */
        long acquire(int partitionCount)
        {
            long currOpIndex = opIndex.getAndAdd(partitionCount);
            return start + currOpIndex * intervalNs;
        }
    }

    /**
     * Provides a blocking stream of operations per consumer.
     */
    private static class StreamOfOperations
    {
        private final OpDistribution operations;
        private final UniformRateLimiter rateLimiter;
        private final WorkManager workManager;

        public StreamOfOperations(OpDistribution operations, UniformRateLimiter rateLimiter, WorkManager workManager)
        {
            this.operations = operations;
            this.rateLimiter = rateLimiter;
            this.workManager = workManager;
        }

        /**
         * This method will block until the next operation becomes available.
         *
         * @return next operation or null if no more ops are coming
         */
        Operation nextOp()
        {
            Operation op = operations.next();
            final int partitionCount = op.ready(workManager);
            if (partitionCount == 0)
                return null;
            if (rateLimiter != null)
            {
                long intendedTime = rateLimiter.acquire(partitionCount);
                op.intendedStartNs(intendedTime);
                long now;
                while ((now = System.nanoTime()) < intendedTime)
                {
                    LockSupport.parkNanos(intendedTime - now);
                }
            }
            return op;
        }

        void abort()
        {
            workManager.stop();
        }
    }
    public static class OpMeasurement
    {
        public String opType;
        public long intended,started,ended,rowCnt,partitionCnt;
        public boolean err;
        @Override
        public String toString()
        {
            return "OpMeasurement [opType=" + opType + ", intended=" + intended + ", started=" + started + ", ended="
                    + ended + ", rowCnt=" + rowCnt + ", partitionCnt=" + partitionCnt + ", err=" + err + "]";
        }
    }
    public interface MeasurementSink
    {
        void record(String opType,long intended, long started, long ended, long rowCnt, long partitionCnt, boolean err);
    }
    public class Consumer extends Thread implements MeasurementSink
    {
        private final StreamOfOperations opStream;
        private final StressMetrics metrics;
        private volatile boolean success = true;
        private final CountDownLatch done;
        private final CountDownLatch start;
        private final CountDownLatch releaseConsumers;
        public final Queue<OpMeasurement> measurementsRecycling;
        public final Queue<OpMeasurement> measurementsReporting;
        public Consumer(OpDistributionFactory operations,
                        boolean isWarmup,
                        CountDownLatch done,
                        CountDownLatch start,
                        CountDownLatch releaseConsumers,
                        WorkManager workManager,
                        StressMetrics metrics,
                        UniformRateLimiter rateLimiter)
        {
            OpDistribution opDistribution = operations.get(isWarmup, this);
            this.done = done;
            this.start = start;
            this.releaseConsumers = releaseConsumers;
            this.metrics = metrics;
            this.opStream = new StreamOfOperations(opDistribution, rateLimiter, workManager);
            this.measurementsRecycling =  new SpscArrayQueue<OpMeasurement>(8*1024);
            this.measurementsReporting =  new SpscUnboundedArrayQueue<OpMeasurement>(2048);
            metrics.add(this);
        }


        public void run()
        {
            try
            {
                SimpleClient sclient = null;
                ThriftClient tclient = null;
                JavaDriverClient jclient = null;
                final ConnectionAPI clientType = settings.mode.api;

                try
                {
                    switch (clientType)
                    {
                        case JAVA_DRIVER_NATIVE:
                            jclient = settings.getJavaDriverClient();
                            break;
                        case SIMPLE_NATIVE:
                            sclient = settings.getSimpleNativeClient();
                            break;
                        case THRIFT:
                        case THRIFT_SMART:
                            tclient = settings.getThriftClient();
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                }
                finally
                {
                    // synchronize the start of all the consumer threads
                    start.countDown();
                }

                releaseConsumers.await();

                while (true)
                {
                    // Assumption: All ops are thread local, operations are never shared across threads.
                    Operation op = opStream.nextOp();
                    if (op == null)
                        break;

                    try
                    {
                        switch (clientType)
                        {
                            case JAVA_DRIVER_NATIVE:
                                op.run(jclient);
                                break;
                            case SIMPLE_NATIVE:
                                op.run(sclient);
                                break;
                            case THRIFT:
                            case THRIFT_SMART:
                            default:
                                op.run(tclient);
                        }
                    }
                    catch (Exception e)
                    {
                        if (output == null)
                            System.err.println(e.getMessage());
                        else
                            output.printException(e);

                        success = false;
                        opStream.abort();
                        metrics.cancel();
                        return;
                    }
                }
            }
            catch (Exception e)
            {
                System.err.println(e.getMessage());
                success = false;
            }
            finally
            {
                done.countDown();
            }
        }

        @Override
        public void record(String opType, long intended, long started, long ended, long rowCnt, long partitionCnt, boolean err)
        {
            OpMeasurement opMeasurement = measurementsRecycling.poll();
            if(opMeasurement == null) {
                opMeasurement = new OpMeasurement();
            }
            opMeasurement.opType = opType;
            opMeasurement.intended = intended;
            opMeasurement.started = started;
            opMeasurement.ended = ended;
            opMeasurement.rowCnt = rowCnt;
            opMeasurement.partitionCnt = partitionCnt;
            opMeasurement.err = err;
            measurementsReporting.offer(opMeasurement);
        }
    }
}
