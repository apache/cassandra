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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Test;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import org.apache.cassandra.CassandraTestBase;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.CassandraTestBase.DisableMBeanRegistration;
import static org.apache.cassandra.CassandraTestBase.PrepareServerNoRegister;
import static org.apache.cassandra.CassandraTestBase.UseMurmur3Partitioner;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

/**
 * We need to be able to convert network topology strategy keyspace to satellite replication strategy
 * keyspaces without any differences relating to replica placement. A replica placement bug will cause dataloss
 *
 * This test checks the replica placement of both strategies against each other and a test model under
 * a variety of configurations to confirm there are no differences between the 2.
 */
@PrepareServerNoRegister
@DisableMBeanRegistration
@UseMurmur3Partitioner
public class SatelliteReplicationStrategyEquivalenceTest extends CassandraTestBase
{
    private static final String KEYSPACE = "test";

    @After
    public void teardown()
    {
        ServerTestUtils.resetCMS();
    }

    static class TestCase
    {
        final List<String> fullDcs;
        final List<String> satelliteDcs;
        final Map<String, Integer> fullDcRf;
        final Map<String, Integer> satelliteRf;
        final String primaryDc;
        final Set<String> disabledDCs;
        final List<Token> nodeTokens;
        final List<Token> queryTokens;

        final List<Integer> nodeCounts;
        final List<Integer> rackCounts;
        final List<Integer> satNodeCounts;
        final List<Integer> satRackCounts;
        final Map<String, String> satelliteToParent;

        TestCase(List<String> fullDcs, List<String> satelliteDcs,
                 Map<String, Integer> fullDcRf, Map<String, Integer> satelliteRf,
                 String primaryDc, Set<String> disabledDCs,
                 List<Token> nodeTokens, List<Token> queryTokens,
                 List<Integer> nodeCounts, List<Integer> rackCounts,
                 List<Integer> satNodeCounts, List<Integer> satRackCounts,
                 Map<String, String> satelliteToParent)
        {
            this.fullDcs = fullDcs;
            this.satelliteDcs = satelliteDcs;
            this.fullDcRf = fullDcRf;
            this.satelliteRf = satelliteRf;
            this.primaryDc = primaryDc;
            this.disabledDCs = disabledDCs;
            this.nodeTokens = nodeTokens;
            this.queryTokens = queryTokens;
            this.nodeCounts = nodeCounts;
            this.rackCounts = rackCounts;
            this.satNodeCounts = satNodeCounts;
            this.satRackCounts = satRackCounts;
            this.satelliteToParent = satelliteToParent;
        }


        static Gen<TestCase> gen()
        {
            return rnd -> {
                int numFullDcs = arbitrary().pick(1, 2, 3).generate(rnd);
                List<String> fullDcs = new ArrayList<>();
                List<Integer> nodeCounts = new ArrayList<>();
                List<Integer> rackCounts = new ArrayList<>();
                Map<String, Integer> fullDcRf = new HashMap<>();

                for (int i = 1; i <= numFullDcs; i++)
                {
                    String dcName = "dc" + i;
                    fullDcs.add(dcName);

                    int nodes = SourceDSL.integers().between(3, 10).generate(rnd);
                    nodeCounts.add(nodes);

                    int racks = SourceDSL.integers().between(1, 3).generate(rnd);
                    rackCounts.add(racks);

                    int rf = SourceDSL.integers().between(1, Math.min(7, nodes)).generate(rnd);
                    fullDcRf.put(dcName, rf);
                }

                int numSatellites = arbitrary().pick(0, 1, 2, 3).generate(rnd);
                numSatellites = Math.min(numSatellites, numFullDcs);

                List<String> satelliteDcs = new ArrayList<>();
                List<Integer> satNodeCounts = new ArrayList<>();
                List<Integer> satRackCounts = new ArrayList<>();
                Map<String, Integer> satelliteRf = new HashMap<>();
                Map<String, String> satelliteToParent = new HashMap<>();

                for (int i = 0; i < numSatellites; i++)
                {
                    String satName = "sat" + (i + 1);
                    satelliteDcs.add(satName);

                    int satNodes = SourceDSL.integers().between(2, 5).generate(rnd);
                    satNodeCounts.add(satNodes);

                    int satRacks = SourceDSL.integers().between(1, Math.min(3, satNodes)).generate(rnd);
                    satRackCounts.add(satRacks);

                    int satRf = SourceDSL.integers().between(1, Math.min(3, satNodes)).generate(rnd);
                    satelliteRf.put(satName, satRf);

                    satelliteToParent.put(satName, fullDcs.get(i));
                }

                String primaryDc = arbitrary().pick(fullDcs).generate(rnd);

                // Randomly disable some non-primary DCs (~25% chance per DC)
                Set<String> disabledDCs = new HashSet<>();
                for (String dc : fullDcs)
                {
                    if (!dc.equals(primaryDc) && arbitrary().pick(true, false, false, false).generate(rnd))
                        disabledDCs.add(dc);
                }

                int totalNodes = nodeCounts.stream().mapToInt(Integer::intValue).sum() +
                                 satNodeCounts.stream().mapToInt(Integer::intValue).sum();

                Set<Long> uniqueTokenValues = new HashSet<>();
                while (uniqueTokenValues.size() < totalNodes)
                {
                    long tokenValue = SourceDSL.longs().between(Long.MIN_VALUE, Long.MAX_VALUE).generate(rnd);
                    uniqueTokenValues.add(tokenValue);
                }

                List<Token> nodeTokens = new ArrayList<>();
                for (long tokenValue : uniqueTokenValues)
                {
                    nodeTokens.add(new LongToken(tokenValue));
                }
                nodeTokens.sort(null);

                List<Token> queryTokens = new ArrayList<>();

                // boundary tokens
                for (Token nodeToken : nodeTokens)
                {
                    long tokenValue = nodeToken.getLongValue();
                    queryTokens.add(nodeToken);
                    queryTokens.add(new LongToken(tokenValue - 1));
                    queryTokens.add(new LongToken(tokenValue + 1));
                }

                // random tokens
                int remainingTokens = Math.max(0, 2000 - queryTokens.size());
                for (int i = 0; i < remainingTokens; i++)
                {
                    long tokenValue = SourceDSL.longs().between(Long.MIN_VALUE, Long.MAX_VALUE).generate(rnd);
                    queryTokens.add(new LongToken(tokenValue));
                }

                return new TestCase(fullDcs, satelliteDcs, fullDcRf, satelliteRf, primaryDc, disabledDCs, nodeTokens, queryTokens,
                                    nodeCounts, rackCounts, satNodeCounts, satRackCounts, satelliteToParent);
            };
        }
    }

    /**
     * reference model for replica placement verification.
     *
     * implements basic rack-aware replica selection by walking the token ring.
     */
    static class ReplicaPlacementModel
    {
        private final Map<String, Integer> fullDcRf;
        private final Map<String, Integer> satelliteRf;

        ReplicaPlacementModel(Map<String, Integer> fullDcRf, Map<String, Integer> satelliteRf)
        {
            this.fullDcRf = fullDcRf;
            this.satelliteRf = satelliteRf;
        }

        EndpointsForRange calculateNaturalReplicas(Token searchToken, ClusterMetadata metadata)
        {
            Range<Token> range = TokenRingUtils.getRange(metadata.tokenMap.tokens(), searchToken);
            List<Replica> allReplicas = new ArrayList<>();

            for (Map.Entry<String, Integer> entry : fullDcRf.entrySet())
            {
                allReplicas.addAll(calculateReplicas(searchToken, range, entry.getKey(), entry.getValue(), metadata, false));
            }

            for (Map.Entry<String, Integer> entry : satelliteRf.entrySet())
            {
                allReplicas.addAll(calculateReplicas(searchToken, range, entry.getKey(), entry.getValue(), metadata, true));
            }

            return EndpointsForRange.copyOf(allReplicas);
        }

        /**
         * Calculate replicas for a datacenter using simple ring walk with rack awareness.
         */
        private static List<Replica> calculateReplicas(Token searchToken,
                                                       Range<Token> range,
                                                       String datacenter,
                                                       int rf,
                                                       ClusterMetadata metadata,
                                                       boolean isTransient)
        {
            List<Replica> replicas = new ArrayList<>();
            Set<String> seenRacks = new HashSet<>();
            Set<InetAddressAndPort> seenEndpoints = new HashSet<>();

            Iterator<Token> ringIter = TokenRingUtils.ringIterator(
                metadata.tokenMap.tokens(), searchToken, false);

            while (replicas.size() < rf && ringIter.hasNext())
            {
                Token token = ringIter.next();
                NodeId owner = metadata.tokenMap.owner(token);
                InetAddressAndPort endpoint = metadata.directory.endpoint(owner);
                Location location = metadata.directory.location(owner);

                if (!location.datacenter.equals(datacenter))
                    continue;
                if (seenEndpoints.contains(endpoint))
                    continue;

                // rack awareness: prefer new racks when available
                boolean newRack = seenRacks.add(location.rack);
                int remainingSlots = rf - replicas.size();

                // count unique racks in this DC
                long totalRacks = metadata.directory.datacenterEndpoints(datacenter).stream()
                    .map(ep -> metadata.directory.location(metadata.directory.peerId(ep)).rack)
                    .distinct()
                    .count();
                int remainingRacks = (int)(totalRacks - seenRacks.size());

                if (newRack || remainingSlots > remainingRacks)
                {
                    Replica replica = isTransient ? Replica.transientReplica(endpoint, range)
                                                : Replica.fullReplica(endpoint, range);
                    replicas.add(replica);
                    seenEndpoints.add(endpoint);
                }
            }

            return replicas;
        }
    }

    private void setupCluster(TestCase testCase) throws UnknownHostException
    {
        ServerTestUtils.resetCMS();

        int tokenIndex = 0;

        // create full DCs
        for (int dcIdx = 0; dcIdx < testCase.fullDcs.size(); dcIdx++)
        {
            String dcName = testCase.fullDcs.get(dcIdx);
            int nodeCount = testCase.nodeCounts.get(dcIdx);
            int rackCount = testCase.rackCounts.get(dcIdx);

            for (int nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++)
            {
                String rackName = "rack" + (nodeIdx % rackCount + 1);
                Location location = new Location(dcName, rackName);
                byte[] address = new byte[]{10, (byte) dcIdx, (byte) (nodeIdx / 256), (byte) (nodeIdx % 256)};

                Token token = testCase.nodeTokens.get(tokenIndex++);

                addEndpoint(token, address, location);
            }
        }

        // create satellite DCs
        for (int satIdx = 0; satIdx < testCase.satelliteDcs.size(); satIdx++)
        {
            String satName = testCase.satelliteDcs.get(satIdx);
            int satNodeCount = testCase.satNodeCounts.get(satIdx);
            int satRackCount = testCase.satRackCounts.get(satIdx);

            for (int nodeIdx = 0; nodeIdx < satNodeCount; nodeIdx++)
            {
                String rackName = "rack" + (nodeIdx % satRackCount + 1);
                Location location = new Location(satName, rackName);
                byte[] address = new byte[]{10, (byte) (100 + satIdx), (byte) (nodeIdx / 256), (byte) (nodeIdx % 256)};

                Token token = testCase.nodeTokens.get(tokenIndex++);

                addEndpoint(token, address, location);
            }
        }
    }

    private void addEndpoint(Token token, byte[] address, Location location) throws UnknownHostException
    {
        InetAddressAndPort addr = InetAddressAndPort.getByAddress(address);
        ClusterMetadataTestHelper.addEndpoint(addr, token, location);
    }

    private Map<String, String> buildSrsOptions(TestCase testCase)
    {
        Map<String, String> options = new HashMap<>();

        for (Map.Entry<String, Integer> entry : testCase.fullDcRf.entrySet())
        {
            options.put(entry.getKey(), String.valueOf(entry.getValue()));
        }

        for (Map.Entry<String, Integer> entry : testCase.satelliteRf.entrySet())
        {
            String satDc = entry.getKey();
            int rf = entry.getValue();
            String parentDc = testCase.satelliteToParent.get(satDc);
            options.put(parentDc + ".satellite." + satDc, rf + "/" + rf);
        }

        for (String dc : testCase.disabledDCs)
        {
            options.put(dc + ".disabled", "true");
        }

        options.put("primary", testCase.primaryDc);
        return options;
    }

    private Map<String, String> buildNtsOptions(TestCase testCase)
    {
        Map<String, String> options = new HashMap<>();

        // full dcs only - NTS can't handle all-witness satellites
        for (Map.Entry<String, Integer> entry : testCase.fullDcRf.entrySet())
        {
            options.put(entry.getKey(), String.valueOf(entry.getValue()));
        }

        return options;
    }

    private Map<String, String> buildNtsOptionsWithSatellites(TestCase testCase)
    {
        Map<String, String> options = new HashMap<>();

        // Full DCs
        for (Map.Entry<String, Integer> entry : testCase.fullDcRf.entrySet())
        {
            options.put(entry.getKey(), String.valueOf(entry.getValue()));
        }

        // Satellites as normal full replicas
        for (Map.Entry<String, Integer> entry : testCase.satelliteRf.entrySet())
        {
            options.put(entry.getKey(), String.valueOf(entry.getValue()));
        }

        return options;
    }

    private EndpointsForRange filterByDatacenters(EndpointsForRange replicas, List<String> datacenters)
    {
        Range<Token> range = replicas.range();
        List<Replica> filtered = new ArrayList<>();

        for (Replica replica : replicas)
        {
            Location location = ClusterMetadata.current()
                .directory
                .location(ClusterMetadata.current().directory.peerId(replica.endpoint()));

            if (datacenters.contains(location.datacenter))
            {
                filtered.add(replica);
            }
        }

        if (filtered.isEmpty())
            return EndpointsForRange.empty(range);

        return EndpointsForRange.copyOf(filtered);
    }

    private static void assertReplicaSetsEqual(ReplicaCollection<?> actual, ReplicaCollection<?> expected, String context)
    {
        if (actual.size() != expected.size())
        {
            throw new AssertionError(String.format("%s: Replica count mismatch - expected %d, got %d\nExpected: %s\nActual: %s",
                context, expected.size(), actual.size(), expected, actual));
        }

        Set<Replica> actualSet = new HashSet<>();
        Set<Replica> expectedSet = new HashSet<>();
        actual.forEach(actualSet::add);
        expected.forEach(expectedSet::add);

        if (!actualSet.equals(expectedSet))
        {
            Set<Replica> missing = new HashSet<>(expectedSet);
            missing.removeAll(actualSet);
            Set<Replica> extra = new HashSet<>(actualSet);
            extra.removeAll(expectedSet);

            throw new AssertionError(String.format("%s: Replica sets don't match\nMissing: %s\nExtra: %s",
                context, missing, extra));
        }
    }

    @Test
    public void testSrsEquivalenceToNts() throws Exception
    {
        qt().withShrinkCycles(0)
            .withExamples(100)
            .forAll(TestCase.gen())
            .checkAssert(testCase -> {
                try
                {
                    setupCluster(testCase);

                    // create strategies
                    Map<String, String> srsOptions = buildSrsOptions(testCase);
                    SatelliteReplicationStrategy srs = new SatelliteReplicationStrategy(KEYSPACE,
                                                                                        srsOptions,
                                                                                        ReplicationType.tracked);

                    // NTS without satellite options to confirm that SRS full datacenter replica selection (even with satellites) matches
                    // the NTS replica selection without satellites. This is the migration use case where the satellites are added to the
                    // replication strategy
                    Map<String, String> ntsOptions = buildNtsOptions(testCase);
                    NetworkTopologyStrategy nts = new NetworkTopologyStrategy(KEYSPACE,
                                                                              ntsOptions,
                                                                              ReplicationType.untracked);

                    // Create NTS with satellites as full replicas to verify that the replica selection is the same as NTS
                    Map<String, String> ntsWithSatellitesOptions = buildNtsOptionsWithSatellites(testCase);
                    NetworkTopologyStrategy ntsWithSatellites = new NetworkTopologyStrategy(KEYSPACE,
                                                                                            ntsWithSatellitesOptions,
                                                                                            ReplicationType.untracked);

                    ReplicaPlacementModel model = new ReplicaPlacementModel(testCase.fullDcRf, testCase.satelliteRf);

                    for (Token queryToken : testCase.queryTokens)
                    {
                        EndpointsForRange srsReplicas = srs.calculateNaturalReplicas(queryToken, ClusterMetadata.current());
                        EndpointsForRange ntsReplicas = nts.calculateNaturalReplicas(queryToken, ClusterMetadata.current());
                        EndpointsForRange ntsWithSatellitesReplicas = ntsWithSatellites.calculateNaturalReplicas(queryToken, ClusterMetadata.current());
                        EndpointsForRange modelReplicas = model.calculateNaturalReplicas(queryToken, ClusterMetadata.current());

                        // SRS full DC replicas == NTS replicas
                        EndpointsForRange srsFullDcReplicas = filterByDatacenters(srsReplicas, testCase.fullDcs);
                        assertReplicaSetsEqual(srsFullDcReplicas, ntsReplicas, "SRS full DC vs NTS");

                        // Model full DC replicas == NTS replicas
                        EndpointsForRange modelFullDcReplicas = filterByDatacenters(modelReplicas, testCase.fullDcs);
                        assertReplicaSetsEqual(modelFullDcReplicas, ntsReplicas, "Model full DC vs NTS");

                        // SRS replicas == Model replicas
                        assertReplicaSetsEqual(srsReplicas, modelReplicas, "SRS vs Model");

                        // SRS satellite node selection == NTS satellite node selection
                        if (!testCase.satelliteDcs.isEmpty())
                        {
                            EndpointsForRange srsSatelliteReplicas = filterByDatacenters(srsReplicas, testCase.satelliteDcs);
                            EndpointsForRange ntsSatelliteReplicas = filterByDatacenters(ntsWithSatellitesReplicas, testCase.satelliteDcs);

                            // endpoints only - isTransient will be different
                            Set<InetAddressAndPort> srsEndpoints = new HashSet<>();
                            Set<InetAddressAndPort> ntsEndpoints = new HashSet<>();
                            srsSatelliteReplicas.forEach(r -> srsEndpoints.add(r.endpoint()));
                            ntsSatelliteReplicas.forEach(r -> ntsEndpoints.add(r.endpoint()));

                            if (!srsEndpoints.equals(ntsEndpoints))
                            {
                                throw new AssertionError(String.format(
                                    "Satellite node selection differs between SRS and NTS\nSRS endpoints: %s\nNTS endpoints: %s",
                                    srsEndpoints, ntsEndpoints));
                            }
                        }

                        // check all satellite replicas are transient
                        for (Replica replica : srsReplicas)
                        {
                            Location location = ClusterMetadata.current()
                                .directory
                                .location(ClusterMetadata.current().directory.peerId(replica.endpoint()));

                            if (testCase.satelliteDcs.contains(location.datacenter))
                            {
                                if (!replica.isTransient())
                                {
                                    throw new AssertionError("Satellite replica must be transient: " + replica);
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Test failed", e);
                }
            });
    }
}
