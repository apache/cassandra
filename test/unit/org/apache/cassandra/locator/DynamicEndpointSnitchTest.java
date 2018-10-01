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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchEMA;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchHistogram;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchLegacyHistogram;
import org.apache.cassandra.net.LatencyMeasurementType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class DynamicEndpointSnitchTest
{
    private static InetAddressAndPort[] hosts;
    // Reduce the update interval significantly so that tests run quickly
    private static final long UPDATE_INTERVAL_MS = 10;
    // Intentially 31 and a little bit instead of 30 seconds flat so this doesn't divide evenly into the default
    // MAX_PROBE_INTERVAL_MS. Also pretty high so latency probes don't interfere with the unit tests
    private static final long PING_INTERVAL_MS = 31 * 1003;

    private final DynamicEndpointSnitch dsnitch;

    public DynamicEndpointSnitchTest(DynamicEndpointSnitch dsnitch)
    {
        this.dsnitch = dsnitch;
    }

    @Before
    public void prepareDES()
    {
        for (InetAddressAndPort host : hosts)
        {
            Gossiper.instance.initializeNodeUnsafe(host, UUID.randomUUID(), 1);
            Gossiper.instance.realMarkAlive(host, Gossiper.instance.getEndpointStateForEndpoint(host));
        }
        dsnitch.reset();
    }

    @Parameterized.Parameters(name="{index}: {0}")
    public static Iterable<?> getDESImplementation() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        // do this because SS needs to be initialized before DES can work properly.
        StorageService.instance.unsafeInitialize();

        hosts = new InetAddressAndPort[] {
            FBUtilities.getBroadcastAddressAndPort(),
            InetAddressAndPort.getByName("127.0.0.2"),
            InetAddressAndPort.getByName("127.0.0.3"),
            InetAddressAndPort.getByName("127.0.0.4"),
            InetAddressAndPort.getByName("127.0.0.5"),
        };

        SimpleSnitch ss1 = new SimpleSnitch();
        DynamicEndpointSnitch histogramDES = new DynamicEndpointSnitchHistogram(ss1, String.valueOf(ss1.hashCode()));
        histogramDES.applyConfigChanges((int) UPDATE_INTERVAL_MS, (int) PING_INTERVAL_MS, DatabaseDescriptor.getDynamicBadnessThreshold());

        SimpleSnitch ss2 = new SimpleSnitch();
        DynamicEndpointSnitch emaDES = new DynamicEndpointSnitchEMA(ss2, String.valueOf(ss2.hashCode()));
        emaDES.applyConfigChanges((int) UPDATE_INTERVAL_MS, (int) PING_INTERVAL_MS, DatabaseDescriptor.getDynamicBadnessThreshold());

        SimpleSnitch ss3 = new SimpleSnitch();
        DynamicEndpointSnitch oldDES = new DynamicEndpointSnitchLegacyHistogram(ss3, String.valueOf(ss3.hashCode()));
        oldDES.applyConfigChanges((int) UPDATE_INTERVAL_MS, (int) PING_INTERVAL_MS, DatabaseDescriptor.getDynamicBadnessThreshold());

        return Arrays.asList(histogramDES, emaDES, oldDES);
    }

    @After
    public void resetDES()
    {
        dsnitch.reset();
    }

    private static void setScores(DynamicEndpointSnitch dsnitch, int rounds, List<InetAddressAndPort> hosts, Integer... scores) throws InterruptedException
    {
        for (int round = 0; round < rounds; round++)
        {
            for (int i = 0; i < hosts.size(); i++)
                dsnitch.receiveTiming(hosts.get(i), scores[i], LatencyMeasurementType.READ);
        }
        // Slightly higher than the update interval to allow scores to propagate
        Thread.sleep(UPDATE_INTERVAL_MS + 10);
    }

    private static EndpointsForRange full(InetAddressAndPort... endpoints)
    {
        EndpointsForRange.Builder rlist = EndpointsForRange.builder(ReplicaUtils.FULL_RANGE, endpoints.length);
        for (InetAddressAndPort endpoint: endpoints)
        {
            rlist.add(ReplicaUtils.full(endpoint));
        }
        return rlist.build();
    }

    @Test
    public void testSortedByProximity() throws InterruptedException, IOException, ConfigurationException
    {
        InetAddressAndPort self = hosts[0];
        List<InetAddressAndPort> allHosts = Arrays.asList(hosts[1], hosts[2], hosts[3]);

        // first, make all hosts equal
        setScores(dsnitch, 1, allHosts, 10, 10, 10);
        EndpointsForRange order = full(hosts[1], hosts[2], hosts[3]);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3])));

        // make hosts[1] a little worse
        setScores(dsnitch, 2, allHosts, 20, 10, 10);
        order = full(hosts[2], hosts[3], hosts[1]);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3])));

        // make hosts[2] as bad as hosts[1]
        setScores(dsnitch, 4, allHosts, 15, 20, 10);
        order = full(hosts[3], hosts[1], hosts[2]);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3])));

        // make hosts[3] the worst
        setScores(dsnitch, 10, allHosts, 10, 10, 30);
        order = full(hosts[1], hosts[2], hosts[3]);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3])));

        // make hosts[3] equal to the others
        setScores(dsnitch, 15, allHosts, 10, 10, 10);
        order = full(hosts[1], hosts[2], hosts[3]);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3])));

        /// Tests CASSANDRA-6683 improvements
        // make the scores differ enough from the ideal order that we sort by score; under the old
        // dynamic snitch behavior (where we only compared neighbors), these wouldn't get sorted
        dsnitch.reset();
        setScores(dsnitch, 20, allHosts, 10, 70, 20);
        order = full(hosts[1], hosts[3], hosts[2]);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3])));

        order = full(hosts[4], hosts[1], hosts[3], hosts[2]);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3], hosts[4])));

        setScores(dsnitch, 40, allHosts, 10, 10, 11);
        order = full(hosts[4], hosts[1], hosts[2], hosts[3]);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3], hosts[4])));
    }

    // CASSANDRA-14459 improvements to add latency probes instead of resets
    @Test
    public void testLatencyProbeNeeded()
    {
        InetAddressAndPort self = hosts[0];

        // Three hosts, one is very latent, but all are requested for ranking
        dsnitch.receiveTiming(hosts[1], 20, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[2], 10, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[3], 1000, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[4], 1000, LatencyMeasurementType.READ);
        dsnitch.updateScores();

        EndpointsForRange orderBefore = full(hosts[2], hosts[1], hosts[3], hosts[4]);
        List<InetAddressAndPort> latencyProbeSequence = new ArrayList<>();

        // At this point we haven't ranked any of hosts so we should need no probes
        int probePosition = dsnitch.latencyProbeNeeded(dsnitch.getMeasurementsWithPort(), latencyProbeSequence, 0);
        assertEquals(0, probePosition);

        // Two hosts continue receiving traffic but the last two is always ranked by the snitch
        for (int i = 0; i < 10; i++)
        {
            dsnitch.receiveTiming(hosts[1], 20, LatencyMeasurementType.READ);
            dsnitch.receiveTiming(hosts[2], 10, LatencyMeasurementType.READ);
            Util.assertRCEquals(orderBefore, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3], hosts[4])));
        }
        dsnitch.updateScores();

        // Now we should need a probe against host[3] or host[4]
        probePosition = dsnitch.latencyProbeNeeded(dsnitch.getMeasurementsWithPort(), latencyProbeSequence, probePosition);
        assertTrue(latencyProbeSequence.contains(hosts[3]));
        assertTrue(latencyProbeSequence.contains(hosts[4]));
        assertEquals(2, latencyProbeSequence.size());

        probePosition = dsnitch.latencyProbeNeeded(dsnitch.getMeasurementsWithPort(), latencyProbeSequence, probePosition);
        assertEquals(1, probePosition);

        probePosition = dsnitch.latencyProbeNeeded(dsnitch.getMeasurementsWithPort(), latencyProbeSequence, probePosition);
        assertEquals(2, probePosition);
        assertTrue(probePosition >= latencyProbeSequence.size());

        Util.assertRCEquals(orderBefore, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3], hosts[4])));

        // Now we simulate ~10 minutes worth of probes happening that are very fast, the node should come back
        for (int i = 0; i < 55; i++)
        {
            dsnitch.receiveTiming(hosts[3], 5, LatencyMeasurementType.PROBE);
            dsnitch.receiveTiming(hosts[4], 5, LatencyMeasurementType.PROBE);
        }
        dsnitch.updateScores();
        Util.assertRCEquals(full(hosts[3], hosts[4], hosts[2], hosts[1]),
                            dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3], hosts[4])));
    }

    // Tests CASSANDRA-14459 improvements when hosts are not getting traffic
    @Test
    public void testLatencyProbeNeeded_NOProbe()
    {
        InetAddressAndPort self = hosts[0];
        List<InetAddressAndPort> latencyProbeSequence = new ArrayList<>();

        // Three hosts, one is very latent, but all are requested for ranking
        dsnitch.receiveTiming(hosts[1], 20, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[2], 10, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[3], 1000, LatencyMeasurementType.READ);
        dsnitch.updateScores();

        int probePosition = dsnitch.latencyProbeNeeded(dsnitch.getMeasurementsWithPort(), latencyProbeSequence, 0);
        // At this point we haven't ranked any of hosts so we should not need probes
        assertEquals(0, probePosition);

        // Two hosts continue receiving traffic and the third is never ranked by the snitch
        for (int i = 0; i < 10; i++)
        {
            dsnitch.receiveTiming(hosts[1], 20, LatencyMeasurementType.READ);
            dsnitch.receiveTiming(hosts[2], 10, LatencyMeasurementType.READ);
            Util.assertRCEquals(full(hosts[2], hosts[1]), dsnitch.sortedByProximity(self, full(hosts[1], hosts[2])));
        }
        dsnitch.updateScores();

        // Now we should NOT need a probe against host[3] (as it was not ranked)
        assertEquals(0, dsnitch.latencyProbeNeeded(dsnitch.getMeasurementsWithPort(), latencyProbeSequence, 0));
        Util.assertRCEquals(full(hosts[2], hosts[1], hosts[3]), dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3])));
    }

    @Test
    public void testEvaluateEndpointForProbe()
    {
        // PING_INTERVAL_MS is larger than MIN_PROBE_INTERVAL_MS
        assertTrue(DynamicEndpointSnitch.MIN_PROBE_INTERVAL_MS < PING_INTERVAL_MS);
        assertEquals(DynamicEndpointSnitch.ProbeType.EXP, dsnitch.evaluateEndpointForProbe(1));

        // Test that the exponential phase works as expected
        long probes = 0;
        for (int i = 1; i < DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS / PING_INTERVAL_MS; i++)
        {
            if (i == 1 << probes)
            {
                assertEquals(DynamicEndpointSnitch.ProbeType.EXP, dsnitch.evaluateEndpointForProbe(i));
                probes += 1;
            }
            else
            {
                assertEquals(DynamicEndpointSnitch.ProbeType.NO, dsnitch.evaluateEndpointForProbe(i));
            }
        }

        long firstConstantProbe = (long) Math.ceil((double) DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS / (double) PING_INTERVAL_MS);
        int expectedProbes = (int) Math.ceil(Math.log(firstConstantProbe) / Math.log(2));
        assertEquals(expectedProbes, probes);
        assertTrue(probes > 2);


        long lastProbe = firstConstantProbe;
        assertEquals(DynamicEndpointSnitch.ProbeType.CONSTANT, dsnitch.evaluateEndpointForProbe(firstConstantProbe));

        for(long i = firstConstantProbe + 1; i < firstConstantProbe * 100; i += 1)
        {
            if (dsnitch.evaluateEndpointForProbe(i) != DynamicEndpointSnitch.ProbeType.NO)
            {
                assertTrue(i - lastProbe >= firstConstantProbe - 1);
                lastProbe = i;
                assertEquals(DynamicEndpointSnitch.ProbeType.NO, dsnitch.evaluateEndpointForProbe(i - 1));
                assertEquals(DynamicEndpointSnitch.ProbeType.NO, dsnitch.evaluateEndpointForProbe(i + 1));
            }

            // We should never have gone more than the firstConstantProbe intervals without a probe
            assertTrue(i - lastProbe <= firstConstantProbe);
        }
    }
}
