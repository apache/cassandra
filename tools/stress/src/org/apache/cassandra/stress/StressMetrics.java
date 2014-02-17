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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.stress.util.Timing;
import org.apache.cassandra.stress.util.TimingInterval;
import org.apache.cassandra.stress.util.Uncertainty;
import org.apache.commons.lang3.time.DurationFormatUtils;

public class StressMetrics
{

    private static final ThreadFactory tf = new NamedThreadFactory("StressMetrics");

    private final PrintStream output;
    private final Thread thread;
    private volatile boolean stop = false;
    private volatile boolean cancelled = false;
    private final Uncertainty opRateUncertainty = new Uncertainty();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final Timing timing = new Timing();

    public StressMetrics(PrintStream output, final long logIntervalMillis)
    {
        this.output = output;
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
                            long sleep = timing.getHistory().endMillis() + logIntervalMillis - System.currentTimeMillis();
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
        opRateUncertainty.await(targetUncertainty, minMeasurements, maxMeasurements);
    }

    public void cancel()
    {
        cancelled = true;
        stop = true;
        thread.interrupt();
        opRateUncertainty.wakeAll();
    }

    public void stop() throws InterruptedException
    {
        stop = true;
        thread.interrupt();
        stopped.await();
    }

    private void update() throws InterruptedException
    {
        TimingInterval interval = timing.snapInterval();
        printRow("", interval, timing.getHistory(), opRateUncertainty, output);
        opRateUncertainty.update(interval.adjustedOpRate());
    }


    // PRINT FORMATTING

    public static final String HEADFORMAT = "%-10s,%8s,%8s,%8s,%8s,%8s,%8s,%8s,%8s,%8s,%7s,%9s";
    public static final String ROWFORMAT =  "%-10d,%8.0f,%8.0f,%8.0f,%8.1f,%8.1f,%8.1f,%8.1f,%8.1f,%8.1f,%7.1f,%9.5f";

    private static void printHeader(String prefix, PrintStream output)
    {
        output.println(prefix + String.format(HEADFORMAT, "ops","op/s", "adj op/s","key/s","mean","med",".95",".99",".999","max","time","stderr"));
    }

    private static void printRow(String prefix, TimingInterval interval, TimingInterval total, Uncertainty opRateUncertainty, PrintStream output)
    {
        output.println(prefix + String.format(ROWFORMAT,
                total.operationCount,
                interval.realOpRate(),
                interval.adjustedOpRate(),
                interval.keyRate(),
                interval.meanLatency(),
                interval.medianLatency(),
                interval.rankLatency(0.95f),
                interval.rankLatency(0.99f),
                interval.rankLatency(0.999f),
                interval.maxLatency(),
                total.runTime() / 1000f,
                opRateUncertainty.getUncertainty()));
    }

    public void summarise()
    {
        output.println("\n");
        output.println("Results:");
        TimingInterval history = timing.getHistory();
        output.println(String.format("real op rate              : %.0f", history.realOpRate()));
        output.println(String.format("adjusted op rate          : %.0f", history.adjustedOpRate()));
        output.println(String.format("adjusted op rate stderr   : %.0f", opRateUncertainty.getUncertainty()));
        output.println(String.format("key rate                  : %.0f", history.keyRate()));
        output.println(String.format("latency mean              : %.1f", history.meanLatency()));
        output.println(String.format("latency median            : %.1f", history.medianLatency()));
        output.println(String.format("latency 95th percentile   : %.1f", history.rankLatency(.95f)));
        output.println(String.format("latency 99th percentile   : %.1f", history.rankLatency(0.99f)));
        output.println(String.format("latency 99.9th percentile : %.1f", history.rankLatency(0.999f)));
        output.println(String.format("latency max               : %.1f", history.maxLatency()));
        output.println("Total operation time      : " + DurationFormatUtils.formatDuration(
                history.runTime(), "HH:mm:ss", true));
    }

    public static final void summarise(List<String> ids, List<StressMetrics> summarise, PrintStream out)
    {
        int idLen = 0;
        for (String id : ids)
            idLen = Math.max(id.length(), idLen);
        String formatstr = "%" + idLen + "s, ";
        printHeader(String.format(formatstr, "id"), out);
        for (int i = 0 ; i < ids.size() ; i++)
            printRow(String.format(formatstr, ids.get(i)),
                    summarise.get(i).timing.getHistory(),
                    summarise.get(i).timing.getHistory(),
                    summarise.get(i).opRateUncertainty,
                    out
            );
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
