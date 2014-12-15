package org.apache.cassandra.utils;
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


import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.StringTokenizer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;

public class BackgroundActivityMonitor
{
    private static final Logger logger = LoggerFactory.getLogger(BackgroundActivityMonitor.class);

    public static final int USER_INDEX = 0;
    public static final int NICE_INDEX = 1;
    public static final int SYS_INDEX = 2;
    public static final int IDLE_INDEX = 3;
    public static final int IOWAIT_INDEX = 4;
    public static final int IRQ_INDEX = 5;
    public static final int SOFTIRQ_INDEX = 6;

    private static final int NUM_CPUS = Runtime.getRuntime().availableProcessors();
    private static final String PROC_STAT_PATH = "/proc/stat";

    private final AtomicDouble compaction_severity = new AtomicDouble();
    private final AtomicDouble manual_severity = new AtomicDouble();
    private final ScheduledExecutorService reportThread = new DebuggableScheduledThreadPoolExecutor("Background_Reporter");

    private RandomAccessFile statsFile;
    private long[] lastReading;

    public BackgroundActivityMonitor()
    {
        try
        {
            statsFile = new RandomAccessFile(PROC_STAT_PATH, "r");
            lastReading = readAndCompute();
        }
        catch (IOException ex)
        {
            if (FBUtilities.hasProcFS())
                logger.warn("Couldn't open /proc/stats");
            statsFile = null;
        }
        reportThread.scheduleAtFixedRate(new BackgroundActivityReporter(), 1, 1, TimeUnit.SECONDS);
    }

    private long[] readAndCompute() throws IOException
    {
        statsFile.seek(0);
        StringTokenizer tokenizer = new StringTokenizer(statsFile.readLine());
        String name = tokenizer.nextToken();
        assert name.equalsIgnoreCase("cpu");
        long[] returned = new long[tokenizer.countTokens()];
        for (int i = 0; i < returned.length; i++)
            returned[i] = Long.parseLong(tokenizer.nextToken());
        return returned;
    }

    private float compareAtIndex(long[] reading1, long[] reading2, int index)
    {
        long total1 = 0, total2 = 0;
        for (int i = 0; i <= SOFTIRQ_INDEX; i++)
        {
            total1 += reading1[i];
            total2 += reading2[i];
        }
        float totalDiff = total2 - total1;

        long intrested1 = reading1[index], intrested2 = reading2[index];
        float diff = intrested2 - intrested1;
        if (diff == 0)
            return 0f;
        return (diff / totalDiff) * 100; // yes it is hard coded to 100 [update
                                         // unit?]
    }

    public void incrCompactionSeverity(double sev)
    {
        compaction_severity.addAndGet(sev);
    }

    public void incrManualSeverity(double sev)
    {
        manual_severity.addAndGet(sev);
    }

    public double getIOWait() throws IOException
    {
        if (statsFile == null)
            return -1d;
        long[] newComp = readAndCompute();
        double value = compareAtIndex(lastReading, newComp, IOWAIT_INDEX);
        lastReading = newComp;
        return value;
    }

    public double getNormalizedLoadAvg()
    {
        double avg = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
        return avg / NUM_CPUS;
    }

    public double getSeverity(InetAddress endpoint)
    {
        VersionedValue event;
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state != null && (event = state.getApplicationState(ApplicationState.SEVERITY)) != null)
            return Double.parseDouble(event.value);
        return 0.0;
    }

    public class BackgroundActivityReporter implements Runnable
    {
        public void run()
        {
            double report = -1;
            try
            {
                report = getIOWait();
            }
            catch (IOException e)
            {
                // ignore;
                if (FBUtilities.hasProcFS())
                    logger.warn("Couldn't read /proc/stats");
            }
            if (report == -1d)
                report = compaction_severity.get();

            if (!Gossiper.instance.isEnabled())
                return;
            report += manual_severity.get(); // add manual severity setting.
            VersionedValue updated = StorageService.instance.valueFactory.severity(report);
            Gossiper.instance.addLocalApplicationState(ApplicationState.SEVERITY, updated);
        }
    }
}
