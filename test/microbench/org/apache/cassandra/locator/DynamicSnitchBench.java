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

package org.apache.cassandra.locator;

import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchHistogram;
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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsPrepend = "-Xmx256M")
public class DynamicSnitchBench
{
    private DynamicEndpointSnitchHistogram histogramSnitch;
    private EndpointsForRange neighbors;
    private Random srandom;

    private static final int NUM_UPDATES = 1000;
    private static final int NUM_NEIGHBORS = 18;
    private static final int NUM_ENDPOINTS = 100;
    private InetAddressAndPort[] hosts;

    @Setup(Level.Trial)
    public void trialSetup() throws UnknownHostException
    {
        // So the snitch doesn't consult gossip information
        System.setProperty("cassandra.ignore_dynamic_snitch_severity", "true");
        SimpleSnitch ss = new SimpleSnitch();
        histogramSnitch = new DynamicEndpointSnitchHistogram(ss);
        histogramSnitch.applyConfigChanges(100,
                                           (int)DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS,
                                           0.2);

        hosts = new InetAddressAndPort[NUM_ENDPOINTS];
        for (int i = 0; i < hosts.length; i ++)
        {
            hosts[i] = InetAddressAndPort.getByName("127.0.0." + (i + 2));
        }

        srandom = new Random(1234);

        EndpointsForRange.Builder rlist = EndpointsForRange.builder(ReplicaUtils.FULL_RANGE, 8);
        for (int i = 1; i < NUM_NEIGHBORS; i ++)
        {
            rlist.add(ReplicaUtils.full(hosts[i]));
        }
        neighbors = rlist.build();
    }

    @Setup(Level.Iteration)
    public void emulatePings()
    {
        for (int i = 0; i < NUM_ENDPOINTS; i++)
        {
            histogramSnitch.receiveTiming(hosts[i],
                                          srandom.nextInt(100),
                                          LatencyMeasurementType.PROBE);
        }
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
    }

    @Benchmark
    public void snitchCalculateScores()
    {
        for (int i = 0; i < NUM_UPDATES; i++)
        {
            histogramSnitch.receiveTiming(hosts[srandom.nextInt(hosts.length)],
                                          srandom.nextInt(100),
                                          LatencyMeasurementType.READ);
        }

        DynamicEndpointSnitch.calculateScores(histogramSnitch.getMeasurementsWithPort());
    }

    @Benchmark
    public void snitchCalculateScoresNeigbors()
    {
        DynamicEndpointSnitch.calculateProbes(histogramSnitch.getMeasurementsWithPort(), DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS);

        for (int i = 0; i < NUM_UPDATES; i++)
        {
            histogramSnitch.receiveTiming(neighbors.get(srandom.nextInt(neighbors.size())).endpoint(),
                                          srandom.nextInt(100),
                                          LatencyMeasurementType.READ);
        }

        DynamicEndpointSnitch.calculateScores(histogramSnitch.getMeasurementsWithPort());
    }

    @Benchmark
    public void snitchTestSortedEndpoints()
    {
        histogramSnitch.sortedByProximity(hosts[0], neighbors);
    }

    @Benchmark
    @Threads(10)
    public void snitchThreadedCalculateScores()
    {
        DynamicEndpointSnitch.calculateScores(histogramSnitch.getMeasurementsWithPort());
    }

    @Benchmark
    @Threads(10)
    public void snitchThreadedTestSortedEndpoints()
    {
        histogramSnitch.sortedByProximity(hosts[0], neighbors);
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