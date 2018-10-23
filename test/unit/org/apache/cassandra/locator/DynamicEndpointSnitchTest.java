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
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchHistogram;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchLegacyHistogram;
import org.apache.cassandra.net.LatencyMeasurementType;
import org.apache.cassandra.net.async.TestScheduledFuture;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
        DynamicEndpointSnitch probeDES = new DynamicEndpointSnitchHistogram(ss1, String.valueOf(ss1.hashCode()));
        probeDES.applyConfigChanges((int) UPDATE_INTERVAL_MS, (int) PING_INTERVAL_MS, DatabaseDescriptor.getDynamicBadnessThreshold());

        SimpleSnitch ss2 = new SimpleSnitch();
        DynamicEndpointSnitch oldDES = new DynamicEndpointSnitchLegacyHistogram(ss2, String.valueOf(ss2.hashCode()));
        oldDES.applyConfigChanges((int) UPDATE_INTERVAL_MS, (int) PING_INTERVAL_MS, DatabaseDescriptor.getDynamicBadnessThreshold());

        return Arrays.asList(probeDES, oldDES);
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
        dsnitch.updateScores();
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
    public void testSortedByProximity() throws InterruptedException, ConfigurationException
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

        // Four hosts, two are very latent, but all are requested for ranking
        dsnitch.receiveTiming(hosts[1], 20, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[2], 10, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[3], 1000, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[4], 1000, LatencyMeasurementType.READ);
        dsnitch.updateScores();

        EndpointsForRange expectedOrder = full(hosts[2], hosts[1], hosts[3], hosts[4]);

        Map<InetAddressAndPort, DynamicEndpointSnitch.AnnotatedMeasurement> measurements = dsnitch.getMeasurementsWithPort();
        // At this point we haven't ranked any of hosts so we should not need probes
        DynamicEndpointSnitch.calculateProbes(measurements, dsnitch.dynamicSampleUpdateInterval);
        assertFalse(measurements.values().stream().anyMatch(m -> m.nextProbeDelayMillis > 0));
        assertTrue(measurements.values().stream().allMatch(m -> m.probeFuture == null));

        // Two hosts continue receiving traffic but the last two are always ranked by the snitch
        for (int i = 0; i < 10; i++)
        {
            dsnitch.receiveTiming(hosts[1], 20, LatencyMeasurementType.READ);
            dsnitch.receiveTiming(hosts[2], 10, LatencyMeasurementType.READ);
            dsnitch.updateScores();
            Util.assertRCEquals(expectedOrder, dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3], hosts[4])));
        }

        int intervalsBeforeProbesStart = (int) ((DynamicEndpointSnitch.MIN_PROBE_INTERVAL_MS /
                                                 dsnitch.dynamicSampleUpdateInterval));

        for (int i = 0; i < intervalsBeforeProbesStart; i++)
            DynamicEndpointSnitch.calculateProbes(measurements, dsnitch.dynamicSampleUpdateInterval);

        // Probes should start after the MIN_PROBE_INTERVAL_MS has elapsed
        assertEquals(2, measurements.values().stream().filter(m -> m.nextProbeDelayMillis > 0).count());
        assertEquals(0, measurements.values().stream().filter(m -> m.probeFuture != null).count());

        // Both requested but non measured hosts should have single interval timers set
        assertEquals(dsnitch.dynamicSampleUpdateInterval, measurements.get(hosts[3]).nextProbeDelayMillis);
        assertEquals(dsnitch.dynamicSampleUpdateInterval, measurements.get(hosts[4]).nextProbeDelayMillis);

        dsnitch.scheduleProbes(measurements);
        // We should have two scheduled futures now
        assertEquals(2, measurements.values().stream().filter(m -> m.probeFuture != null).count());

        int numProbes = 0;
        for (int i = 1; (1 << i) * dsnitch.dynamicSampleUpdateInterval < DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS; i++)
        {
            DynamicEndpointSnitch.AnnotatedMeasurement m3 = measurements.get(hosts[3]);
            DynamicEndpointSnitch.AnnotatedMeasurement m4 = measurements.get(hosts[4]);
            // Emulate the probe actually running
            m3.probeFuture.cancel(false);
            m4.probeFuture.cancel(false);
            DynamicEndpointSnitch.calculateProbes(measurements, dsnitch.dynamicSampleUpdateInterval);
            dsnitch.scheduleProbes(measurements);
            assertEquals((1 << i) * dsnitch.dynamicSampleUpdateInterval, m3.nextProbeDelayMillis);
            assertEquals((1 << i) * dsnitch.dynamicSampleUpdateInterval, m4.nextProbeDelayMillis);
            numProbes++;
        }

        // If we did exponential backoff right ... we should have the right number of probes
        assertTrue(numProbes > 0);
        assertEquals(numProbes,
                     Math.floor(Math.log((DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS - DynamicEndpointSnitch.MIN_PROBE_INTERVAL_MS) /
                                         dsnitch.dynamicSampleUpdateInterval) /
                                Math.log(2.0)),
                     0.01);

        measurements.get(hosts[3]).probeFuture.cancel(false);
        measurements.get(hosts[4]).probeFuture.cancel(false);

        // Now we should do ten minute probes until we exceed MAX_PROBE_INTERVAL_MS since we last requested ranking
        int interval = dsnitch.dynamicSampleUpdateInterval;
        for (int i = (1 + intervalsBeforeProbesStart + numProbes) * interval; i < DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS; i += interval)
        {
            DynamicEndpointSnitch.calculateProbes(measurements, dsnitch.dynamicSampleUpdateInterval);
            assertEquals(DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS, measurements.get(hosts[3]).nextProbeDelayMillis);
            assertEquals(DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS, measurements.get(hosts[4]).nextProbeDelayMillis);
            dsnitch.scheduleProbes(measurements);
            measurements.get(hosts[3]).probeFuture.cancel(false);
            measurements.get(hosts[4]).probeFuture.cancel(false);
        }

        // Since we haven't requested these two hosts for ranking, we should stop probing after the backoff finishes
        DynamicEndpointSnitch.calculateProbes(measurements, dsnitch.dynamicSampleUpdateInterval);
        assertEquals(0, measurements.get(hosts[3]).nextProbeDelayMillis);
        assertEquals(0, measurements.get(hosts[4]).nextProbeDelayMillis);

        // And now we put one of those non requested hosts down in gossip and ensure it's cleaned out of samples
        assertTrue(measurements.containsKey(hosts[3]));
        Gossiper.instance.markDead(hosts[3], Gossiper.instance.getEndpointStateForEndpoint(hosts[3]));
        dsnitch.scheduleProbes(measurements);
        assertFalse(measurements.containsKey(hosts[3]));
    }

    // Tests CASSANDRA-14459 improvements when hosts are not getting traffic
    @Test
    public void testNoLatencyProbesNeeded()
    {
        InetAddressAndPort self = hosts[0];

        // Three hosts, one is very latent to start with
        dsnitch.receiveTiming(hosts[1], 20, LatencyMeasurementType.PROBE);
        dsnitch.receiveTiming(hosts[2], 10, LatencyMeasurementType.PROBE);
        dsnitch.receiveTiming(hosts[3], 1000, LatencyMeasurementType.PROBE);
        dsnitch.updateScores();

        dsnitch.updateSamples();
        // At this point we haven't ranked any of hosts so we should not need probes
        assertEquals(0, dsnitch.getMeasurementsWithPort().values().stream().filter(m -> m.probeFuture != null).count());

        // Two hosts continue receiving traffic and the third is never ranked by the snitch
        for (int i = 0; i < 10; i++)
        {
            dsnitch.receiveTiming(hosts[1], 20, LatencyMeasurementType.READ);
            dsnitch.receiveTiming(hosts[2], 10, LatencyMeasurementType.READ);
            Util.assertRCEquals(full(hosts[2], hosts[1]), dsnitch.sortedByProximity(self, full(hosts[1], hosts[2])));
        }
        dsnitch.updateScores();

        // Now we should NOT need a probe against host[3] (as it was not ranked)
        dsnitch.updateSamples();
        // At this point we haven't ranked any of hosts so we should not need probes
        assertEquals(0, dsnitch.getMeasurementsWithPort().values().stream().filter(m -> m.probeFuture != null).count());

        Util.assertRCEquals(full(hosts[2], hosts[1], hosts[3]), dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3])));
    }

    @Test
    public void testCalculateProbes()
    {
        Map<InetAddressAndPort, DynamicEndpointSnitch.AnnotatedMeasurement> measurements = new HashMap<>();
        measurements.put(hosts[1], new DynamicEndpointSnitch.AnnotatedMeasurement(dsnitch.measurementImpl(10)));
        DynamicEndpointSnitch.AnnotatedMeasurement m = measurements.get(hosts[1]);

        int interval = 1000;

        DynamicEndpointSnitch.calculateProbes(measurements, interval);
        assertEquals(0, m.nextProbeDelayMillis);

        m.millisSinceLastRequest.set(0);
        DynamicEndpointSnitch.calculateProbes(measurements, interval);
        for (int i = 2; (i + 1) * interval < DynamicEndpointSnitch.MIN_PROBE_INTERVAL_MS; i++)
        {
            DynamicEndpointSnitch.calculateProbes(measurements, interval);
            assertEquals(0, m.nextProbeDelayMillis);
        }

        DynamicEndpointSnitch.calculateProbes(measurements, interval);
        assertEquals(interval, m.nextProbeDelayMillis);

        m.probeFuture = new TestScheduledFuture();
        m.probeFuture.cancel(false);

        DynamicEndpointSnitch.calculateProbes(measurements, interval);
        assertEquals(interval * 2, m.nextProbeDelayMillis);

        for (int i = 1; (1 << i) * interval < DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS; i++)
        {
            m.probeFuture = new TestScheduledFuture();
            DynamicEndpointSnitch.calculateProbes(measurements, interval);
            assertEquals(interval * (1 << i), m.nextProbeDelayMillis);
            m.probeFuture.cancel(false);
            DynamicEndpointSnitch.calculateProbes(measurements, interval);
            if (m.nextProbeDelayMillis >= DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS)
            {
                assertEquals(DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS, m.nextProbeDelayMillis);
                break;
            }
            assertEquals(interval * (1 << (i + 1)), m.nextProbeDelayMillis);
        }

        assertEquals(DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS, m.nextProbeDelayMillis);

        m.probeFuture = new TestScheduledFuture();
        m.probeFuture.cancel(false);

        DynamicEndpointSnitch.calculateProbes(measurements, interval);
        assertEquals(DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS, m.nextProbeDelayMillis);
    }

    @Test
    public void testCalculateScores()
    {
        Map<InetAddressAndPort, Double> scores;
        Map<InetAddressAndPort, DynamicEndpointSnitch.AnnotatedMeasurement> measurements = new HashMap<>();
        DynamicEndpointSnitch.AnnotatedMeasurement m1 = new DynamicEndpointSnitch.AnnotatedMeasurement(dsnitch.measurementImpl(10));
        DynamicEndpointSnitch.AnnotatedMeasurement m2 = new DynamicEndpointSnitch.AnnotatedMeasurement(dsnitch.measurementImpl(20));
        m1.measurement.sample(10);
        m2.measurement.sample(20);
        measurements.put(hosts[1], m1);
        measurements.put(hosts[2], m2);

        scores = DynamicEndpointSnitch.calculateScores(measurements);
        assertEquals(2, scores.size());
        assertTrue(scores.get(hosts[1]) < scores.get(hosts[2]));

        // Hosts that are not ranked or timed with in a long time shouldn't have latencies updated
        double oldM1Value = m1.cachedMeasurement;
        double oldM2Value = m2.cachedMeasurement;

        m1.millisSinceLastMeasure.addAndGet(DynamicEndpointSnitch.MIN_PROBE_INTERVAL_MS);
        m1.millisSinceLastRequest.addAndGet(1);
        // This sample shouldn't matter because we're outside the window of recalculating
        m1.measurement.sample(100);
        // This sample should matter
        m2.measurement.sample(5);
        m2.measurement.sample(5);

        Map<InetAddressAndPort, Double> newScores;
        newScores = DynamicEndpointSnitch.calculateScores(measurements);
        assertEquals(2, newScores.size());

        // m1 should not have changed, but m2 should
        assertEquals(oldM1Value, m1.cachedMeasurement, 0.01);
        assertNotEquals(oldM2Value, m2.cachedMeasurement);

        // Order should have swapped
        assertTrue(newScores.get(hosts[2]) < newScores.get(hosts[1]));
    }
}
