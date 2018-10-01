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

package org.apache.cassandra.test.microbench;

import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchEMA;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchHistogram;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.net.LatencyMeasurementType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;


@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsPrepend = "-Xmx256M")
public class SnitchMetricsBench
{
    private DynamicEndpointSnitchEMA emaSnitch;
    private DynamicEndpointSnitchHistogram histogramSnitch;
    private Random srandom;

    private static final int NUM_UPDATES = 10000;
    private InetAddressAndPort[] hosts;

    @Setup(Level.Trial)
    public void trialSetup() throws UnknownHostException
    {
        // So the snitch doesn't consult gossip information
        System.setProperty("cassandra.ignore_dynamic_snitch_severity", "true");
        SimpleSnitch ss = new SimpleSnitch();
        emaSnitch = new DynamicEndpointSnitchEMA(ss);
        histogramSnitch = new DynamicEndpointSnitchHistogram(ss);

        hosts = new InetAddressAndPort[100];
        for (int i = 0; i < hosts.length; i ++)
        {
            hosts[i] = InetAddressAndPort.getByName("127.0.0." + (i + 2));
        }

        srandom = new Random(1234);
    }

    @Benchmark
    public void snitchUpdateEMA()
    {
        for (int i = 0; i < NUM_UPDATES; i++)
        {
            emaSnitch.receiveTiming(hosts[srandom.nextInt(hosts.length)],
                                    srandom.nextInt(100),
                                    LatencyMeasurementType.READ);
        }
        emaSnitch.calculateScores();
    }

    @Benchmark
    public void snitchUpdateHistogram()
    {
        for (int i = 0; i < NUM_UPDATES; i++)
        {
            histogramSnitch.receiveTiming(hosts[srandom.nextInt(hosts.length)],
                                          srandom.nextInt(100),
                                          LatencyMeasurementType.READ);
        }
        histogramSnitch.calculateScores();
    }

    @Benchmark
    @Threads(10)
    public void snitchThreadedReceiveTimingAndCalculateEMA()
    {
        ThreadLocalRandom trandom = ThreadLocalRandom.current();

        for (int i = 0; i < NUM_UPDATES; i++)
        {
            emaSnitch.receiveTiming(hosts[trandom.nextInt(hosts.length)], trandom.nextInt(100),
                                    LatencyMeasurementType.READ);
        }
        emaSnitch.calculateScores();
    }

    @Benchmark
    @Threads(10)
    public void snitchThreadedReceiveTimingAndCalculateHistogram()
    {
        ThreadLocalRandom trandom = ThreadLocalRandom.current();

        for (int i = 0; i < NUM_UPDATES; i++)
        {
            histogramSnitch.receiveTiming(hosts[trandom.nextInt(hosts.length)], trandom.nextInt(100),
                                          LatencyMeasurementType.READ);
        }
        histogramSnitch.calculateScores();
    }

    @Benchmark
    @Threads(10)
    public void snitchThreadedReceiveTimingEMA()
    {
        ThreadLocalRandom trandom = ThreadLocalRandom.current();

        for (int i = 0; i < NUM_UPDATES; i++)
        {
            emaSnitch.receiveTiming(hosts[trandom.nextInt(hosts.length)], trandom.nextInt(100),
                                    LatencyMeasurementType.READ);
        }
    }

    @Benchmark
    @Threads(10)
    public void snitchThreadedReceiveTimingHistogram()
    {
        ThreadLocalRandom trandom = ThreadLocalRandom.current();

        for (int i = 0; i < NUM_UPDATES; i++)
        {
            histogramSnitch.receiveTiming(hosts[trandom.nextInt(hosts.length)], trandom.nextInt(100),
                                          LatencyMeasurementType.READ);
        }
    }
}