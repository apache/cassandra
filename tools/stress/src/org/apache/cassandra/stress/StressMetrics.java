package org.apache.cassandra.stress;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

import org.apache.cassandra.stress.util.*;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.stress.settings.StressSettings;

public class StressMetrics
{

    private static final ThreadFactory tf = new NamedThreadFactory("StressMetrics");

    private final PrintStream output;
    private final Thread thread;
    private volatile boolean stop = false;
    private volatile boolean cancelled = false;
    private final Uncertainty rowRateUncertainty = new Uncertainty();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final Timing timing;
    private final Callable<JmxCollector.GcStats> gcStatsCollector;
    private volatile JmxCollector.GcStats totalGcStats;
    private final StressSettings settings;

    public StressMetrics(PrintStream output, final long logIntervalMillis, StressSettings settings)
    {
        this.output = output;
        this.settings = settings;
        Callable<JmxCollector.GcStats> gcStatsCollector;
        totalGcStats = new JmxCollector.GcStats(0);
        try
        {
            gcStatsCollector = new JmxCollector(settings.node.resolveAllPermitted(settings), settings.port.jmxPort);
        }
        catch (Throwable t)
        {
            switch (settings.log.level)
            {
                case VERBOSE:
                    t.printStackTrace();
            }
            System.err.println("Failed to connect over JMX; not collecting these stats");
            gcStatsCollector = new Callable<JmxCollector.GcStats>()
            {
                public JmxCollector.GcStats call() throws Exception
                {
                    return totalGcStats;
                }
            };
        }
        this.gcStatsCollector = gcStatsCollector;
        this.timing = new Timing(settings.samples.historyCount, settings.samples.reportCount);

        printHeader("", output);
        thread = tf.newThread(new Runnable()
        {
            @Override
            public void run()
            {
                timing.start();
                try {

                    while (!stop)
                    {
                        try
                        {
                            long sleepNanos = timing.getHistory().endNanos() - System.nanoTime();
                            long sleep = (sleepNanos / 1000000) + logIntervalMillis;

                            if (sleep < logIntervalMillis >>> 3)
                                // if had a major hiccup, sleep full interval
                                Thread.sleep(logIntervalMillis);
                            else
                                Thread.sleep(sleep);

                            update();
                        } catch (InterruptedException e)
                        {
                            break;
                        }
                    }

                    update();
                }
                catch (InterruptedException e)
                {}
                catch (Exception e)
                {
                    cancel();
                    e.printStackTrace(StressMetrics.this.output);
                }
                finally
                {
                    rowRateUncertainty.wakeAll();
                    stopped.countDown();
                }
            }
        });
    }

    public void start()
    {
        thread.start();
    }

    public void waitUntilConverges(double targetUncertainty, int minMeasurements, int maxMeasurements) throws InterruptedException
    {
        rowRateUncertainty.await(targetUncertainty, minMeasurements, maxMeasurements);
    }

    public void cancel()
    {
        cancelled = true;
        stop = true;
        thread.interrupt();
        rowRateUncertainty.wakeAll();
    }

    public void stop() throws InterruptedException
    {
        stop = true;
        thread.interrupt();
        stopped.await();
    }

    private void update() throws InterruptedException
    {
        Timing.TimingResult<JmxCollector.GcStats> result = timing.snap(gcStatsCollector);
        totalGcStats = JmxCollector.GcStats.aggregate(Arrays.asList(totalGcStats, result.extra));
        TimingInterval current = result.intervals.combine(settings.samples.reportCount);
        TimingInterval history = timing.getHistory().combine(settings.samples.historyCount);
        rowRateUncertainty.update(current.adjustedRowRate());
        if (current.operationCount != 0)
        {
            if (result.intervals.intervals().size() > 1)
            {
                for (Map.Entry<String, TimingInterval> type : result.intervals.intervals().entrySet())
                    printRow("", type.getKey(), type.getValue(), timing.getHistory().get(type.getKey()), result.extra, rowRateUncertainty, output);
            }

            printRow("", "total", current, history, result.extra, rowRateUncertainty, output);
        }
        if (timing.done())
            stop = true;
    }


    // PRINT FORMATTING

    public static final String HEADFORMAT = "%-10s%10s,%8s,%8s,%8s,%8s,%8s,%8s,%8s,%8s,%8s,%7s,%9s,%7s,%7s,%8s,%8s,%8s,%8s";
    public static final String ROWFORMAT =  "%-10s%10d,%8.0f,%8.0f,%8.0f,%8.1f,%8.1f,%8.1f,%8.1f,%8.1f,%8.1f,%7.1f,%9.5f,%7d,%7.0f,%8.0f,%8.0f,%8.0f,%8.0f";
    public static final String[] HEADMETRICS = new String[]{"type", "total ops","op/s","pk/s","row/s","mean","med",".95",".99",".999","max","time","stderr", "errors", "gc: #", "max ms", "sum ms", "sdv ms", "mb"};
    public static final String HEAD = String.format(HEADFORMAT, (Object[]) HEADMETRICS);

    private static void printHeader(String prefix, PrintStream output)
    {
        output.println(prefix + HEAD);
    }

    private static void printRow(String prefix, String type, TimingInterval interval, TimingInterval total, JmxCollector.GcStats gcStats, Uncertainty opRateUncertainty, PrintStream output)
    {
        output.println(prefix + String.format(ROWFORMAT,
                type + ",",
                total.operationCount,
                interval.opRate(),
                interval.partitionRate(),
                interval.rowRate(),
                interval.meanLatency(),
                interval.medianLatency(),
                interval.rankLatency(0.95f),
                interval.rankLatency(0.99f),
                interval.rankLatency(0.999f),
                interval.maxLatency(),
                total.runTime() / 1000f,
                opRateUncertainty.getUncertainty(),
                interval.errorCount,
                gcStats.count,
                gcStats.maxms,
                gcStats.summs,
                gcStats.sdvms,
                gcStats.bytes / (1 << 20)
        ));
    }

    public void summarise()
    {
        output.println("\n");
        output.println("Results:");

        TimingIntervals opHistory = timing.getHistory();
        TimingInterval history = opHistory.combine(settings.samples.historyCount);
        output.println(String.format("op rate                   : %.0f %s", history.opRate(), opHistory.opRates()));
        output.println(String.format("partition rate            : %.0f %s", history.partitionRate(), opHistory.partitionRates()));
        output.println(String.format("row rate                  : %.0f %s", history.rowRate(), opHistory.rowRates()));
        output.println(String.format("latency mean              : %.1f %s", history.meanLatency(), opHistory.meanLatencies()));
        output.println(String.format("latency median            : %.1f %s", history.medianLatency(), opHistory.medianLatencies()));
        output.println(String.format("latency 95th percentile   : %.1f %s", history.rankLatency(.95f), opHistory.rankLatencies(0.95f)));
        output.println(String.format("latency 99th percentile   : %.1f %s", history.rankLatency(0.99f), opHistory.rankLatencies(0.99f)));
        output.println(String.format("latency 99.9th percentile : %.1f %s", history.rankLatency(0.999f), opHistory.rankLatencies(0.999f)));
        output.println(String.format("latency max               : %.1f %s", history.maxLatency(), opHistory.maxLatencies()));
        output.println(String.format("Total partitions          : %d %s",   history.partitionCount, opHistory.partitionCounts()));
        output.println(String.format("Total errors              : %d %s",   history.errorCount, opHistory.errorCounts()));
        output.println(String.format("total gc count            : %.0f", totalGcStats.count));
        output.println(String.format("total gc mb               : %.0f", totalGcStats.bytes / (1 << 20)));
        output.println(String.format("total gc time (s)         : %.0f", totalGcStats.summs / 1000));
        output.println(String.format("avg gc time(ms)           : %.0f", totalGcStats.summs / totalGcStats.count));
        output.println(String.format("stdev gc time(ms)         : %.0f", totalGcStats.sdvms));
        output.println("Total operation time      : " + DurationFormatUtils.formatDuration(
                history.runTime(), "HH:mm:ss", true));
        output.println(""); // Newline is important here to separate the aggregates section from the END or the next stress iteration
    }

    public static void summarise(List<String> ids, List<StressMetrics> summarise, PrintStream out, int historySampleCount)
    {
        int idLen = 0;
        for (String id : ids)
            idLen = Math.max(id.length(), idLen);
        String formatstr = "%" + idLen + "s, ";
        printHeader(String.format(formatstr, "id"), out);
        for (int i = 0 ; i < ids.size() ; i++)
        {
            for (Map.Entry<String, TimingInterval> type : summarise.get(i).timing.getHistory().intervals().entrySet())
            {
                printRow(String.format(formatstr, ids.get(i)),
                         type.getKey(),
                         type.getValue(),
                         type.getValue(),
                         summarise.get(i).totalGcStats,
                         summarise.get(i).rowRateUncertainty,
                         out);
            }
            TimingInterval hist = summarise.get(i).timing.getHistory().combine(historySampleCount);
            printRow(String.format(formatstr, ids.get(i)),
                    "total",
                    hist,
                    hist,
                    summarise.get(i).totalGcStats,
                    summarise.get(i).rowRateUncertainty,
                    out
            );
        }
    }

    public Timing getTiming()
    {
        return timing;
    }

    public boolean wasCancelled()
    {
        return cancelled;
    }

}
