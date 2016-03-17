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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.stress.operations.OpDistribution;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.apache.cassandra.stress.settings.SettingsCommand;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.transport.SimpleClient;

public class StressAction implements Runnable
{

    private final StressSettings settings;
    private final PrintStream output;

    public StressAction(StressSettings settings, PrintStream out)
    {
        this.settings = settings;
        output = out;
    }

    public void run()
    {
        // creating keyspace and column families
        settings.maybeCreateKeyspaces();

        output.println("Sleeping 2s...");
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        if (!settings.command.noWarmup)
            warmup(settings.command.getFactory(settings));
        if (settings.command.truncate == SettingsCommand.TruncateWhen.ONCE)
            settings.command.truncateTables(settings);

        // TODO : move this to a new queue wrapper that gates progress based on a poisson (or configurable) distribution
        RateLimiter rateLimiter = null;
        if (settings.rate.opRateTargetPerSecond > 0)
            rateLimiter = RateLimiter.create(settings.rate.opRateTargetPerSecond);

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
    private void warmup(OpDistributionFactory operations)
    {
        PrintStream warmupOutput = new PrintStream(new OutputStream() { @Override public void write(int b) throws IOException { } } );
        // do 25% of iterations as warmup but no more than 50k (by default hotspot compiles methods after 10k invocations)
        int iterations = (settings.command.count > 0
                         ? Math.min(50000, (int)(settings.command.count * 0.25))
                         : 50000) * settings.node.nodes.size();
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
            boolean success = null != run(single, threads, iterations, 0, null, null, warmupOutput, true);
            if (!success)
                throw new RuntimeException("Failed to execute warmup");
        }

    }

    // TODO : permit varying more than just thread count
    // TODO : vary thread count based on percentage improvement of previous increment, not by fixed amounts
    private boolean runMulti(boolean auto, RateLimiter rateLimiter)
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
        StressMetrics.summarise(runIds, results, output, settings.samples.historyCount);
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
            double prev = results.get(i - 1).getTiming().getHistory().opRate();
            double cur = results.get(i).getTiming().getHistory().opRate();
            improvement += (cur - prev) / prev;
        }
        return improvement / count;
    }

    private StressMetrics run(OpDistributionFactory operations,
                              int threadCount,
                              long opCount,
                              long duration,
                              RateLimiter rateLimiter,
                              TimeUnit durationUnits,
                              PrintStream output,
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

        final CountDownLatch done = new CountDownLatch(threadCount);
        final Consumer[] consumers = new Consumer[threadCount];
        int sampleCount = settings.samples.liveCount / threadCount;
        for (int i = 0; i < threadCount; i++)
        {

            consumers[i] = new Consumer(operations.get(metrics.getTiming(), sampleCount, isWarmup),
                                        done, workManager, metrics, rateLimiter);
        }

        // starting worker threadCount
        for (int i = 0; i < threadCount; i++)
            consumers[i].start();

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

    private class Consumer extends Thread
    {

        private final OpDistribution operations;
        private final StressMetrics metrics;
        private final RateLimiter rateLimiter;
        private volatile boolean success = true;
        private final WorkManager workManager;
        private final CountDownLatch done;

        public Consumer(OpDistribution operations,
                        CountDownLatch done,
                        WorkManager workManager,
                        StressMetrics metrics,
                        RateLimiter rateLimiter)
        {
            this.done = done;
            this.rateLimiter = rateLimiter;
            this.workManager = workManager;
            this.metrics = metrics;
            this.operations = operations;
        }

        public void run()
        {
            operations.initTimers();

            try
            {
                SimpleClient sclient = null;
                ThriftClient tclient = null;
                JavaDriverClient jclient = null;

                switch (settings.mode.api)
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

                while (true)
                {
                    Operation op = operations.next();
                    if (!op.ready(workManager, rateLimiter))
                        break;

                    try
                    {
                        switch (settings.mode.api)
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
                            e.printStackTrace(output);

                        success = false;
                        workManager.stop();
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
                operations.closeTimers();
            }
        }
    }
}
