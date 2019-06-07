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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchHistogram;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchLegacyHistogram;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.LatencyMeasurementType;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.PingMessage;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.net.async.TestScheduledFuture;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
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
    // How long to wait for background tasks to complete in these tests
    private static final long TEST_TIMEOUT_MS = 10000;

    private final DynamicEndpointSnitch dsnitch;
    private final Map<InetAddressAndPort, List<Pair<Integer, MessageOut>>> pingMessagesByHost;
    private PingSink pingSink;

    public DynamicEndpointSnitchTest(DynamicEndpointSnitch dsnitch)
    {
       this.dsnitch = dsnitch;
       this.pingMessagesByHost = new HashMap<>();
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

        SimpleSnitch ss2 = new SimpleSnitch();
        DynamicEndpointSnitch oldDES = new DynamicEndpointSnitchLegacyHistogram(ss2, String.valueOf(ss2.hashCode()));

        return Arrays.asList(probeDES, oldDES);
    }

    @Before
    public void prepareDES()
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        VersionedValue.VersionedValueFactory valueFactory;
        valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        for (InetAddressAndPort host : hosts)
        {
            Gossiper.instance.initializeNodeUnsafe(host, UUID.randomUUID(), 1);
            Gossiper.instance.realMarkAlive(host, Gossiper.instance.getEndpointStateForEndpoint(host));
            // As we change the application state later to test how probes handle nodes leaving, and
            // initializeNodeUnsafe is an upsert (not an insert), we have to do this to keep a consistent test setup
            Token token = partitioner.getToken(ByteBufferUtil.bytes("key" + host.toString()));
            Gossiper.instance.injectApplicationState(host, ApplicationState.STATUS_WITH_PORT,
                                                     valueFactory.normal(Collections.singleton(token)));
        }
        dsnitch.reset();
        dsnitch.open(true);
        dsnitch.applyConfigChanges((int) UPDATE_INTERVAL_MS, (int) PING_INTERVAL_MS, DatabaseDescriptor.getDynamicBadnessThreshold());

        pingSink = new PingSink(pingMessagesByHost);
        MessagingService.instance().addMessageSink(pingSink);
    }

    @After
    public void resetDES()
    {
        dsnitch.reset();
        dsnitch.close();
        MessagingService.instance().clearMessageSinks();
        pingMessagesByHost.clear();
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
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(hosts[4], hosts[1], hosts[2], hosts[3])));

        setScores(dsnitch, 40, allHosts, 10, 10, 11);
        order = full(hosts[4], hosts[1], hosts[2], hosts[3]);
        Util.assertRCEquals(order, dsnitch.sortedByProximity(self, full(hosts[4], hosts[1], hosts[2], hosts[3])));
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
        VersionedValue.VersionedValueFactory valueFactory;
        valueFactory = new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());

        assertTrue(measurements.containsKey(hosts[3]));
        // mark the node as removed
        Gossiper.instance.injectApplicationState(hosts[3], ApplicationState.STATUS_WITH_PORT,
                                                 valueFactory.hibernate(true));
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

        // Hosts that are not ranked in a long time shouldn't have latencies updated
        double oldM1Value = m1.cachedMeasurement;
        double oldM2Value = m2.cachedMeasurement;

        m1.millisSinceLastRequest.addAndGet(DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS);
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

    @Test
    public void testCleanShutdown()
    {
        dsnitch.receiveTiming(hosts[1], 1, LatencyMeasurementType.PROBE);
        dsnitch.receiveTiming(hosts[2], 2, LatencyMeasurementType.PROBE);
        dsnitch.receiveTiming(hosts[3], 3, LatencyMeasurementType.PROBE);
        dsnitch.updateScores();
        dsnitch.updateSamples();

        assertEquals(3, dsnitch.samples.size());
        assertTrue(MessagingService.instance().getLatencySubscribers().contains(dsnitch));

        // Now close and we should wipe all the info
        dsnitch.close();

        assertEquals(3, dsnitch.samples.size());
        for (DynamicEndpointSnitch.AnnotatedMeasurement measurement : dsnitch.samples.values())
        {
            assertEquals(0, measurement.nextProbeDelayMillis);
            assertEquals(0, measurement.millisSinceLastRequest.get());
            assertEquals(0, measurement.millisSinceLastMeasure.get());
            assertNull(measurement.probeFuture);
        }

        assertFalse(MessagingService.instance().getLatencySubscribers().contains(dsnitch));
    }

    @Test
    public void testScheduleProbes() throws InterruptedException
    {
        InetAddressAndPort self = hosts[0];

        dsnitch.receiveTiming(hosts[0], 0, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[1], 0, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[2], 0, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[3], 0, LatencyMeasurementType.READ);
        dsnitch.sortedByProximity(self, full(hosts[1], hosts[2], hosts[3], hosts[4]));
        dsnitch.updateSamples();

        Map<InetAddressAndPort, DynamicEndpointSnitch.AnnotatedMeasurement> measurementMap = dsnitch.samples;
        // Simulate some time going by
        DynamicEndpointSnitch.calculateProbes(measurementMap, DynamicEndpointSnitch.MIN_PROBE_INTERVAL_MS + 1);

        // Move the probes up so they actually execute and make the rate limter much faster
        long rateLimitMillis = 5;

        for (DynamicEndpointSnitch.AnnotatedMeasurement measurement: measurementMap.values())
        {
            assertEquals(DynamicEndpointSnitch.MIN_PROBE_INTERVAL_MS + 1, measurement.nextProbeDelayMillis);
            measurement.nextProbeDelayMillis = rateLimitMillis;
        }

        // Set the rate limiter to be faster then usual so probes actually execute
        dsnitch.setProbeRateLimiter((int) rateLimitMillis);
        long timeBefore = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());

        CountDownLatch outCountDown = pingSink.setPingOut(6);
        CountDownLatch inCountDown = pingSink.setPingIn(6);

        // Send the probes! At this point the threadpool inside of the DES should start sending out some probes
        dsnitch.scheduleProbes(measurementMap);
        assertEquals(4, measurementMap.values().stream().filter(m -> m.probeFuture != null).count());

        outCountDown.await(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // Even after all requests are sent, we have to wait for all of the probe futures to be completed before
        // proceeding
        long timeout = TEST_TIMEOUT_MS;
        while (timeout >= 0 && measurementMap.values().stream().anyMatch(m -> !m.probeFuture.isDone()))
        {
            Thread.sleep(rateLimitMillis);
            timeout -= rateLimitMillis;
        }
        assertTrue(timeout > 0);

        long timeAfter = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
        // We should have done a fixed rateLimitMillis delay plus 4 requests with at least 5 milliseconds of delay each
        // due to the rate limiter
        assertTrue((timeAfter - timeBefore) >= rateLimitMillis * 5);

        // Check that sendLatencyProbes were sent and we actually used the maxes
        Map<InetAddressAndPort, Long> maxLatencyInMicrosPerHost = new HashMap<>();
        Map<InetAddressAndPort, Integer> smallMessagesSent = new HashMap<>();
        Map<InetAddressAndPort, Integer> largeMessagesSent = new HashMap<>();
        for (Map.Entry<InetAddressAndPort, List<Pair<Integer, MessageOut>>> entry : pingMessagesByHost.entrySet())
        {
            for (Pair<Integer, MessageOut> msgPair: entry.getValue())
            {
                if (msgPair.right.verb == (MessagingService.Verb.PING))
                {
                    PingMessage ping = (PingMessage) msgPair.right.payload;
                    if (ping.connectionType == OutboundConnectionIdentifier.ConnectionType.LARGE_MESSAGE)
                        largeMessagesSent.compute(entry.getKey(), (k, v) -> v == null ? 1 : v + 1);
                    else if (ping.connectionType == OutboundConnectionIdentifier.ConnectionType.SMALL_MESSAGE)
                        smallMessagesSent.compute(entry.getKey(), (k, v) -> v == null ? 1 : v + 1);

                    long callbackAgeNS = System.nanoTime() - MessagingService.instance().getRegisteredCallbackAge(msgPair.left);
                    long requestLatencyMS = TimeUnit.NANOSECONDS.toMicros(callbackAgeNS);
                    maxLatencyInMicrosPerHost.compute(entry.getKey(), (k, v) -> v == null ? requestLatencyMS : Math.max(v, requestLatencyMS));

                    // Simulate a node responding
                    MessageIn msgIn = MessageIn.create(entry.getKey(), msgPair.right.payload, Collections.emptyMap(),
                                                       MessagingService.Verb.REQUEST_RESPONSE, 1);
                    MessagingService.instance().receive(msgIn, msgPair.left);
                }
            }
        }

        inCountDown.await(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        dsnitch.updateScores();

        assertEquals(3, smallMessagesSent.size());
        assertEquals(3, largeMessagesSent.size());
        assertEquals(3, maxLatencyInMicrosPerHost.size());
        assertTrue(smallMessagesSent.values().stream().allMatch(count -> count == 1));
        assertTrue(largeMessagesSent.values().stream().allMatch(count -> count == 1));

        for (Map.Entry<InetAddressAndPort, Long> entry : maxLatencyInMicrosPerHost.entrySet())
        {
            // Soemtimes it appears to take a few milliseconds for measurements to show up from the callbacks
            List<Double> observedLatencyMicros = Lists.newArrayList(measurementMap.get(entry.getKey()).measurement.measurements());
            timeout = TEST_TIMEOUT_MS;
            while (timeout >= 0 && observedLatencyMicros.size() != 2)
            {
                observedLatencyMicros = Lists.newArrayList(measurementMap.get(entry.getKey()).measurement.measurements());
                Thread.sleep(rateLimitMillis);
                timeout -= rateLimitMillis;
            }
            assertTrue(timeout > 0);

            // Should have a zero from the start of the method, and a max measurement
            assertEquals(2, observedLatencyMicros.size());

            assertTrue(Collections.max(observedLatencyMicros) >= entry.getValue());
        }
    }

    private static class PingSink implements IMessageSink
    {
        final Map<InetAddressAndPort, List<Pair<Integer, MessageOut>>> pingMessagesByHost;
        private CountDownLatch pingOut = null;
        private CountDownLatch pingIn = null;

        PingSink(Map<InetAddressAndPort, List<Pair<Integer, MessageOut>>> pingMessagesByHost)
        {
            this.pingMessagesByHost = pingMessagesByHost;
        }

        public CountDownLatch setPingOut(int count)
        {
            this.pingOut = new CountDownLatch(count);
            return this.pingOut;
        }


        public CountDownLatch setPingIn(int count)
        {
            this.pingIn = new CountDownLatch(count);
            return this.pingIn;
        }

        @Override
        public boolean allowOutgoingMessage(MessageOut message, int id, InetAddressAndPort to)
        {
            synchronized (pingMessagesByHost)
            {
                if (message.verb != MessagingService.Verb.PING)
                    return false;

                if (!pingMessagesByHost.containsKey(to))
                    pingMessagesByHost.put(to, new ArrayList<>());
                pingMessagesByHost.get(to).add(Pair.create(id, message));

                if (pingOut != null)
                    pingOut.countDown();
            }
            return false;
        }

        @Override
        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            synchronized (pingMessagesByHost)
            {
                // This is ... not efficient, but it shouldn't matter
                for (Map.Entry<InetAddressAndPort, List<Pair<Integer, MessageOut>>> entry : pingMessagesByHost.entrySet())
                {
                    for (Pair<Integer, MessageOut> msgPair : entry.getValue())
                    {
                        if (msgPair.right.verb == MessagingService.Verb.PING && msgPair.left == id)
                        {
                            if (pingIn != null)
                                pingIn.countDown();
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }
}
