package org.apache.cassandra.stress.report;
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


import java.io.FileNotFoundException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.time.DurationFormatUtils;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.apache.cassandra.stress.StressAction.Consumer;
import org.apache.cassandra.stress.StressAction.MeasurementSink;
import org.apache.cassandra.stress.StressAction.OpMeasurement;
import org.apache.cassandra.stress.settings.SettingsLog.Level;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JmxCollector;
import org.apache.cassandra.stress.util.JmxCollector.GcStats;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.cassandra.stress.util.Uncertainty;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class StressMetrics implements MeasurementSink
{
    private final List<Consumer> consumers = new ArrayList<>();
    private final ResultLogger output;
    private final Thread thread;
    private final Uncertainty rowRateUncertainty = new Uncertainty();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final Callable<JmxCollector.GcStats> gcStatsCollector;
    private final HistogramLogWriter histogramWriter;
    private final long epochNs = nanoTime();
    private final long epochMs = currentTimeMillis();

    private volatile JmxCollector.GcStats totalGcStats = new GcStats(0);

    private volatile boolean stop = false;
    private volatile boolean cancelled = false;


    // collected data for intervals and summary
    private final Map<String, TimingInterval> opTypeToCurrentTimingInterval = new TreeMap<>();
    private final Map<String, TimingInterval> opTypeToSummaryTimingInterval = new TreeMap<>();
    private final Queue<OpMeasurement> leftovers = new ArrayDeque<>();
    private final TimingInterval totalCurrentInterval;
    private final TimingInterval totalSummaryInterval;
    private final int outputFrequencyInSeconds;
    private final int headerFrequencyInSeconds;
    private int outputLines = 0;

    public StressMetrics(ResultLogger output, final long logIntervalMillis, StressSettings settings)
    {
        this.output = output;
        if (settings.log.hdrFile != null)
        {
            try
            {
                histogramWriter = new HistogramLogWriter(settings.log.hdrFile);
                histogramWriter.outputComment("Logging op latencies for Cassandra Stress");
                histogramWriter.outputLogFormatVersion();
                final long roundedEpoch = epochMs - (epochMs%1000);
                histogramWriter.outputBaseTime(roundedEpoch);
                histogramWriter.setBaseTime(roundedEpoch);
                histogramWriter.outputStartTime(roundedEpoch);
                histogramWriter.outputLegend();
            }
            catch (FileNotFoundException e)
            {
                throw new IllegalArgumentException(e);
            }
        }
        else
        {
            histogramWriter = null;
        }
        Callable<JmxCollector.GcStats> gcStatsCollector;
        totalGcStats = new JmxCollector.GcStats(0);
        try
        {
            gcStatsCollector = new JmxCollector(toJmxNodes(settings.node.resolveAllPermitted(settings)),
                                                settings.port.jmxPort,
                                                settings.jmx);
        }
        catch (Throwable t)
        {
            if (settings.log.level == Level.VERBOSE)
            {
                t.printStackTrace();
            }
            System.err.println("Failed to connect over JMX; not collecting these stats");
            gcStatsCollector = () -> totalGcStats;
        }
        this.gcStatsCollector = gcStatsCollector;
        this.totalCurrentInterval = new TimingInterval(settings.rate.isFixed);
        this.totalSummaryInterval = new TimingInterval(settings.rate.isFixed);
        printHeader("", output);
        thread = new Thread(() -> {
            reportingLoop(logIntervalMillis);
        });
        thread.setName("StressMetrics");
        headerFrequencyInSeconds = settings.reporting.headerFrequency;
        outputFrequencyInSeconds = settings.reporting.outputFrequency;
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

    private void reportingLoop(final long logIntervalMillis)
    {
        // align report timing to the nearest second
        final long currentTimeMs = currentTimeMillis();
        final long startTimeMs = currentTimeMs - (currentTimeMs % 1000);
        // reporting interval starts rounded to the second
        long reportingStartNs = (nanoTime() - MILLISECONDS.toNanos(currentTimeMs - startTimeMs));
        final long parkIntervalNs = TimeUnit.MILLISECONDS.toNanos(logIntervalMillis);
        try
        {
            while (!stop)
            {
                final long wakupTarget = reportingStartNs + parkIntervalNs;
                sleepUntil(wakupTarget);
                if (stop)
                {
                    break;
                }
                recordInterval(wakupTarget, parkIntervalNs);
                reportingStartNs += parkIntervalNs;
            }

            final long end = nanoTime();
            recordInterval(end, end - reportingStartNs);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            cancel();
        }
        finally
        {
            rowRateUncertainty.wakeAll();
            stopped.countDown();
        }
    }


    private void sleepUntil(final long until)
    {
        long parkFor;
        while (!stop &&
               (parkFor = until - nanoTime()) > 0)
        {
            LockSupport.parkNanos(parkFor);
        }
    }

    @Override
    public void record(String opType, long intended, long started, long ended, long rowCnt, long partitionCnt, boolean err)
    {
        TimingInterval current = opTypeToCurrentTimingInterval.computeIfAbsent(opType, k -> new TimingInterval(totalCurrentInterval.isFixed));
        record(current, intended, started, ended, rowCnt, partitionCnt, err);
    }

    private void record(TimingInterval t, long intended, long started, long ended, long rowCnt, long partitionCnt, boolean err)
    {
        t.rowCount += rowCnt;
        t.partitionCount += partitionCnt;
        if (err)
            t.errorCount++;
        if (intended != 0) {
            t.responseTime().recordValue(ended-intended);
            t.waitTime().recordValue(started-intended);
        }
        final long sTime = ended-started;
        t.serviceTime().recordValue(sTime);
    }

    private void recordInterval(long intervalEnd, long parkIntervalNs)
    {

        drainConsumerMeasurements(intervalEnd, parkIntervalNs);

        GcStats gcStats = null;
        try
        {
            gcStats = gcStatsCollector.call();
        }
        catch (Exception e)
        {
            gcStats = new GcStats(0);
        }
        totalGcStats = JmxCollector.GcStats.aggregate(Arrays.asList(totalGcStats, gcStats));

        rowRateUncertainty.update(totalCurrentInterval.adjustedRowRate());
        if (totalCurrentInterval.operationCount() != 0)
        {
            // if there's a single operation we only print the total
            final boolean logPerOpSummaryLine = opTypeToCurrentTimingInterval.size() > 1;

            for (Map.Entry<String, TimingInterval> type : opTypeToCurrentTimingInterval.entrySet())
            {
                final String opName = type.getKey();
                final TimingInterval opInterval = type.getValue();
                if (logPerOpSummaryLine)
                {
                    printRow("", opName, opInterval, opTypeToSummaryTimingInterval.get(opName), gcStats, rowRateUncertainty, output);
                }
                logHistograms(opName, opInterval);
                opInterval.reset();
            }

            ++outputLines;
            if (outputFrequencyInSeconds == 0 || outputLines % outputFrequencyInSeconds == 0)
                printRow("", "total", totalCurrentInterval, totalSummaryInterval, gcStats, rowRateUncertainty, output);
            if (headerFrequencyInSeconds != 0 && outputLines % headerFrequencyInSeconds == 0)
                printHeader("\n", output);

            totalCurrentInterval.reset();
        }
    }

    private void drainConsumerMeasurements(long intervalEnd, long parkIntervalNs)
    {
        // record leftover measurements if any
        int leftoversSize = leftovers.size();
        for (int i=0;i<leftoversSize;i++)
        {
            OpMeasurement last = leftovers.poll();
            if (last.ended <= intervalEnd)
            {
                record(last.opType, last.intended, last.started, last.ended, last.rowCnt, last.partitionCnt, last.err);
                // round robin-ish redistribution of leftovers
                consumers.get(i%consumers.size()).measurementsRecycling.offer(last);
            }
            else
            {
                // no record for you! wait one interval!
                leftovers.offer(last);
            }
        }
        // record interval collected measurements
        for (Consumer c: consumers) {
            Queue<OpMeasurement> in = c.measurementsReporting;
            Queue<OpMeasurement> out = c.measurementsRecycling;
            OpMeasurement last;
            while ((last = in.poll()) != null)
            {
                if (last.ended > intervalEnd)
                {
                    // measurements for any given consumer are ordered, we stop when we stop.
                    leftovers.add(last);
                    break;
                }
                record(last.opType, last.intended, last.started, last.ended, last.rowCnt, last.partitionCnt, last.err);
                out.offer(last);
            }
        }
        // set timestamps and summarize
        for (Entry<String, TimingInterval> currPerOp : opTypeToCurrentTimingInterval.entrySet()) {
            currPerOp.getValue().endNanos(intervalEnd);
            currPerOp.getValue().startNanos(intervalEnd-parkIntervalNs);
            TimingInterval summaryPerOp = opTypeToSummaryTimingInterval.computeIfAbsent(currPerOp.getKey(), k -> new TimingInterval(totalCurrentInterval.isFixed));
            summaryPerOp.add(currPerOp.getValue());
            totalCurrentInterval.add(currPerOp.getValue());
        }
        totalCurrentInterval.endNanos(intervalEnd);
        totalCurrentInterval.startNanos(intervalEnd-parkIntervalNs);

        totalSummaryInterval.add(totalCurrentInterval);
    }


    private void logHistograms(String opName, TimingInterval opInterval)
    {
        if (histogramWriter == null)
            return;
        final long startNs = opInterval.startNanos();
        final long endNs = opInterval.endNanos();

        logHistogram(opName + "-st", startNs, endNs, opInterval.serviceTime());
        logHistogram(opName + "-rt", startNs, endNs, opInterval.responseTime());
        logHistogram(opName + "-wt", startNs, endNs, opInterval.waitTime());
    }

    private void logHistogram(String opName, final long startNs, final long endNs, final Histogram histogram)
    {
        if (histogram.getTotalCount() != 0)
        {
            histogram.setTag(opName);
            final long relativeStartNs = startNs - epochNs;
            final long startMs = (long) (1000 *((epochMs + NANOSECONDS.toMillis(relativeStartNs))/1000.0));
            histogram.setStartTimeStamp(startMs);
            final long relativeEndNs = endNs - epochNs;
            final long endMs = (long) (1000 *((epochMs + NANOSECONDS.toMillis(relativeEndNs))/1000.0));
            histogram.setEndTimeStamp(endMs);
            histogramWriter.outputIntervalHistogram(histogram);
        }
    }


    // PRINT FORMATTING
    public static final String HEADFORMAT = "%-50s%10s,%8s,%8s,%8s,%8s,%8s,%8s,%8s,%8s,%8s,%7s,%9s,%7s,%7s,%8s,%8s,%8s,%8s";
    public static final String ROWFORMAT =  "%-50s%10d,%8.0f,%8.0f,%8.0f,%8.1f,%8.1f,%8.1f,%8.1f,%8.1f,%8.1f,%7.1f,%9.5f,%7d,%7.0f,%8.0f,%8.0f,%8.0f,%8.0f";

    public static final String[] HEADMETRICS = new String[]{"type", "total ops","op/s","pk/s","row/s","mean","med",".95",".99",".999","max","time","stderr", "errors", "gc: #", "max ms", "sum ms", "sdv ms", "mb"};
    public static final String HEAD = String.format(HEADFORMAT, (Object[]) HEADMETRICS);

    @VisibleForTesting
    public static Set<String> toJmxNodes(Set<String> nodes)
    {
        return nodes.stream().map(n -> n.split(":")[0]).collect(Collectors.toSet());
    }

    private static void printHeader(String prefix, ResultLogger output)
    {
        output.println(prefix + HEAD);
    }

    private static void printRow(String prefix, String type, TimingInterval interval, TimingInterval total,
                                 JmxCollector.GcStats gcStats, Uncertainty opRateUncertainty, ResultLogger output)
    {
        output.println(prefix + String.format(ROWFORMAT,
                type + ",",
                total.operationCount(),
                interval.opRate(),
                interval.partitionRate(),
                interval.rowRate(),
                interval.meanLatencyMs(),
                interval.medianLatencyMs(),
                interval.latencyAtPercentileMs(95.0),
                interval.latencyAtPercentileMs(99.0),
                interval.latencyAtPercentileMs(99.9),
                interval.maxLatencyMs(),
                total.runTimeMs() / 1000f,
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

        TimingIntervals opHistory = new TimingIntervals(opTypeToSummaryTimingInterval);
        TimingInterval history = this.totalSummaryInterval;
        output.println(String.format("Op rate                   : %,8.0f op/s  %s", history.opRate(), opHistory.opRates()));
        output.println(String.format("Partition rate            : %,8.0f pk/s  %s", history.partitionRate(), opHistory.partitionRates()));
        output.println(String.format("Row rate                  : %,8.0f row/s %s", history.rowRate(), opHistory.rowRates()));
        output.println(String.format("Latency mean              : %6.1f ms %s", history.meanLatencyMs(), opHistory.meanLatencies()));
        output.println(String.format("Latency median            : %6.1f ms %s", history.medianLatencyMs(), opHistory.medianLatencies()));
        output.println(String.format("Latency 95th percentile   : %6.1f ms %s", history.latencyAtPercentileMs(95.0), opHistory.latenciesAtPercentile(95.0)));
        output.println(String.format("Latency 99th percentile   : %6.1f ms %s", history.latencyAtPercentileMs(99.0), opHistory.latenciesAtPercentile(99.0)));
        output.println(String.format("Latency 99.9th percentile : %6.1f ms %s", history.latencyAtPercentileMs(99.9), opHistory.latenciesAtPercentile(99.9)));
        output.println(String.format("Latency max               : %6.1f ms %s", history.maxLatencyMs(), opHistory.maxLatencies()));
        output.println(String.format("Total partitions          : %,10d %s",   history.partitionCount, opHistory.partitionCounts()));
        output.println(String.format("Total errors              : %,10d %s",   history.errorCount, opHistory.errorCounts()));
        output.println(String.format("Total GC count            : %,1.0f", totalGcStats.count));
        output.println(String.format("Total GC memory           : %s", FBUtilities.prettyPrintMemory((long)totalGcStats.bytes, " ")));
        output.println(String.format("Total GC time             : %,6.1f seconds", totalGcStats.summs / 1000));
        output.println(String.format("Avg GC time               : %,6.1f ms", totalGcStats.summs / totalGcStats.count));
        output.println(String.format("StdDev GC time            : %,6.1f ms", totalGcStats.sdvms));
        output.println("Total operation time      : " + DurationFormatUtils.formatDuration(
                history.runTimeMs(), "HH:mm:ss", true));
        output.println(""); // Newline is important here to separate the aggregates section from the END or the next stress iteration
    }

    public static void summarise(List<String> ids, List<StressMetrics> summarise, ResultLogger out)
    {
        int idLen = 0;
        for (String id : ids)
            idLen = Math.max(id.length(), idLen);
        String formatstr = "%" + idLen + "s, ";
        printHeader(String.format(formatstr, "id"), out);
        for (int i = 0 ; i < ids.size() ; i++)
        {
            for (Map.Entry<String, TimingInterval> type : summarise.get(i).opTypeToSummaryTimingInterval.entrySet())
            {
                printRow(String.format(formatstr, ids.get(i)),
                         type.getKey(),
                         type.getValue(),
                         type.getValue(),
                         summarise.get(i).totalGcStats,
                         summarise.get(i).rowRateUncertainty,
                         out);
            }
            TimingInterval hist = summarise.get(i).totalSummaryInterval;
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

    public boolean wasCancelled()
    {
        return cancelled;
    }

    public void add(Consumer consumer)
    {
        consumers.add(consumer);
    }

    public double opRate()
    {
        return totalSummaryInterval.opRate();
    }
}
