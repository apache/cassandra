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

package org.apache.cassandra.harry.stress;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.execution.CompiledStatement;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.execution.LockingDataTracker;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.stress.distribution.Distribution;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Condition;

import javax.annotation.Nullable;

/**
 * At any given point, we will have only N active partitions
 *
 * for every N partitions there should be its own invertible generator that will generate a partition key;
 * however we need to somehow guarantee that later partition keys won't have same descriptors.
 */
public class HarryStress
{
    public static final Logger LOGGER = LoggerFactory.getLogger(HarryStress.class);

    // VisitGenerator is a stateful component that generates operation specs. Operation specs are then used by
    private final VisitGenerator visitGenerator;

    private final ActivePartition.Partitions partitionFactory;
    private final DataTracker.ReplayingDataTracker innerTracker;

    private final Generator<VisitGenerator.VisitType> visitTypeGen;
    private final Distribution visitSizeDistribution;
    private final VisitGenerator.OpKindGenFactory operationKindGen;

    private final List<StressWorker> workers;

    private final EndCondition endCondition;
    private final MetricCollector metrics = new MetricCollector();
    private final int ratePerSecond;
    private final @Nullable PrintStream metricsOut;
    private final int reportIntervalSeconds;

    public HarryStress(SchemaSpec schema,
                       Distribution rowPopulation,
                       VisitGenerator.ColumnPopulation columnPopulation,
                       Generator<VisitGenerator.VisitType> visitTypeGen,
                       Distribution visitSizeDistribution,
                       VisitGenerator.OpKindGenFactory operationKindGen,
                       RotationStrategy rotationStrategy,
                       @Nullable PrintStream metricsOut,
                       int reportIntervalSeconds,
                       Supplier<BiFunction<CompiledStatement, Runnable, Object[][]>> sutFactory,
                       int concurrency,
                       int ratePerSecond,
                       long minPartitionIdx,
                       long maxPartitionIdx,
                       long initialLts)
    {
        this.ratePerSecond = ratePerSecond;
        this.metricsOut = metricsOut;
        this.reportIntervalSeconds = reportIntervalSeconds;
        this.visitTypeGen = visitTypeGen;
        this.visitSizeDistribution = visitSizeDistribution;
        this.operationKindGen = operationKindGen;
        this.partitionFactory = new ActivePartition.Partitions(schema, rowPopulation, columnPopulation, rotationStrategy, minPartitionIdx, maxPartitionIdx, initialLts);
        this.visitGenerator = new VisitGenerator(partitionFactory,
                                                 visitTypeGen,
                                                 visitSizeDistribution,
                                                 operationKindGen,
                                                 initialLts);

        this.innerTracker = new DataTracker.SimpleDataTracker();

        partitionFactory.onRemove(innerTracker::gc);
        partitionFactory.populate();
        DataTracker outerTracker = new ActivePartition.TrackerWrapper(new LockingDataTracker(innerTracker, Integer.MAX_VALUE, 1),
                                                                      partitionFactory);

        this.workers = new ArrayList<>();
        this.endCondition = new EndCondition();

        for (int i = 0; i < concurrency; i++)
            workers.add(new StressWorker(i, innerTracker, outerTracker, partitionFactory, sutFactory, endCondition, this.ratePerSecond / concurrency));

        metrics.partitionCount.set(partitionFactory.activePartitions.size());
        metrics.activePartitionCount.set(partitionFactory.activePartitions.size());
    }

    private static class EndCondition implements Consumer<Throwable>
    {
        final List<Throwable> exceptions = new CopyOnWriteArrayList<>();
        final Condition condition = Condition.newOneTimeCondition();

        @Override
        public void accept(Throwable e)
        {
            exceptions.add(e);
            condition.signal();
        }

        public boolean awaitUntil(long nanoTimeDeadline) throws InterruptedException
        {
            return condition.awaitUntil(nanoTimeDeadline);
        }

        public Throwable maybeReportExceptions()
        {
            if (!exceptions.isEmpty())
            {
                RuntimeException ex = new RuntimeException("Caught exception while running bechmark");
                for (Throwable exception : exceptions)
                    ex.addSuppressed(exception);
                return ex;
            }
            return null;
        }
    }

    /**
     * Replays the history [fromLts, toLts) into the model only (no SUT execution), so that reads over partitions whose
     * data was loaded out-of-band (e.g. offline-generated SSTables produced from the same history) can be validated.
     */
    public void replay(long fromLts, long toLts)
    {
        VisitGenerator seed = new VisitGenerator(partitionFactory, visitTypeGen, visitSizeDistribution, operationKindGen, fromLts);
        for (long lts = fromLts; lts < toLts; lts++)
        {
            Visit visit = seed.get();
            innerTracker.begin(visit);
            innerTracker.end(visit);
            partitionFactory.maybeSwitchPartition(visit.lts, action -> {});
        }
    }

    private Visit nextVisit()
    {
        Visit nextVisit = visitGenerator.get();
        for (long pd : nextVisit.visitedPartitions)
            partitionFactory.forPd(pd).ref();
        return nextVisit;
    }

    // TODO: warm up
    public void start(long maxIterations, long runUntil) throws Throwable
    {
        long now = Clock.Global.nanoTime();
        long nextMetricsCollect = nextReport(now, reportIntervalSeconds);

        List<PrintStream> metricOuts = new ArrayList<>();
        metricOuts.add(System.out);
        if (metricsOut != null)
        {
            metricOuts.add(metricsOut);
            metricsOut.println(HEAD);
        }

        metrics.start(now);
        Visit nextVisit = nextVisit();

        while (true)
        {
            if (now > nextMetricsCollect)
            {
                // TODO (expected): this is potentially lossy, as worker may update metrics after we grab them
                for (StressWorker worker : workers)
                    metrics.merge(worker.resetMetrics());

                metrics.end(now);
                System.out.println(HEAD);
                printRow(metrics, metricOuts);
                metrics.start(now);
                nextMetricsCollect = nextReport(Clock.Global.nanoTime(), reportIntervalSeconds);
            }

            for (StressWorker worker : workers)
            {
                int remaining = worker.getFreeSlots();
                while (remaining-- > 0 && worker.offer(nextVisit))
                {
                    nextVisit = nextVisit();
                    partitionFactory.maybeSwitchPartition(nextVisit.lts, action -> {});
                }
            }

            if (endCondition.awaitUntil(now + TimeUnit.SECONDS.toNanos(1)))
            {
                System.out.println("Exiting early due to condition");
                break; // errored out
            }

            now = Clock.Global.nanoTime();
            if (nextVisit.lts >= maxIterations || now > runUntil)
            {
                break;
            }
        }

        System.out.printf("Completed! %d%n", nextVisit.lts);

        for (StressWorker worker : workers)
            worker.shutdown();
        for (StressWorker worker : workers)
            worker.awaitTermination(1, TimeUnit.MINUTES);

        Throwable t = endCondition.maybeReportExceptions();
        if (t != null)
            throw t;
    }

    private static long nextReport(long nowNanos, int reportIntervalSeconds)
    {
        Calendar calendar = Calendar.getInstance();
        long nowMillis = calendar.getTimeInMillis();
        int addSeconds = reportIntervalSeconds - (calendar.get(Calendar.SECOND) % reportIntervalSeconds);
        calendar.add(Calendar.SECOND, addSeconds);
        return nowNanos + TimeUnit.MILLISECONDS.toNanos(calendar.getTimeInMillis() - nowMillis);
    }

    public static final String HEADFORMAT = "%19s %10s %8s %8s %7s %8s %8s %8s %8s %8s %8s %8s %8s %8s";
    public static final String ROWFORMAT =  "%tF %tT " + // time
                                            "%10d " + // counts
                                            "%8.0f " +
                                            "%8d " +
                                            "%7s " +
                                            "%8d " +
                                            "%8d " +    // count
                                            "%8.0f " +  // rates
                                            "%8.1f " +   // latency
                                            "%8.1f " +
                                            "%8.1f " +
                                            "%8.1f " +
                                            "%8.1f " +
                                            "%8.1f";

    public static final String[] HEADMETRICS = new String[]{ "time", "pcount", "pk/s", "pactive", "type", "count", "errors", "op/s","mean","med",".95",".99",".999","max"};
    public static final String HEAD = String.format(HEADFORMAT, (Object[]) HEADMETRICS);

    public static void main(String[] args)
    {
        System.out.println(HEAD);
        printRow(new MetricCollector(), Collections.singletonList(System.out));
        long waitUntil = nextReport(Clock.Global.nanoTime(), 30);
        while (true)
        {
            long wait = waitUntil - Clock.Global.nanoTime();
            if (wait <= 0)
                break;

            Uninterruptibles.sleepUninterruptibly(wait, TimeUnit.NANOSECONDS);
        }
        System.out.printf("%tT\n", Calendar.getInstance());
    }

    private static void printRow(MetricCollector metrics, List<PrintStream> outs)
    {
        Calendar calendar = Calendar.getInstance();
        String reads = String.format(ROWFORMAT, calendar, calendar,
                metrics.partitionCount.get(),
                metrics.partitionRate(),
                metrics.activePartitionCount.get(),
                "read",
                metrics.readCount(),
                metrics.failedReads.getAndSet(0),
                metrics.readRate(),
                metrics.meanReadLatencyMs(),
                metrics.medianReadLatencyMs(),
                metrics.readLatencyAtPercentileMs(95.0),
                metrics.readLatencyAtPercentileMs(99.0),
                metrics.readLatencyAtPercentileMs(99.9),
                metrics.maxReadLatencyMs());

        String writes = String.format(ROWFORMAT, calendar, calendar,
                metrics.partitionCount.get(),
                metrics.partitionRate(),
                metrics.activePartitionCount.get(),
                "write",
                metrics.writeCount(),
                metrics.failedWrites.getAndSet(0),
                metrics.writeRate(),
                metrics.meanWriteLatencyMs(),
                metrics.medianWriteLatencyMs(),
                metrics.writeLatencyAtPercentileMs(95.0),
                metrics.writeLatencyAtPercentileMs(99.0),
                metrics.writeLatencyAtPercentileMs(99.9),
                metrics.maxWriteLatencyMs());

        for (PrintStream out : outs)
        {
            out.println(reads);
            out.println(writes);
            out.flush();
        }
    }

}
