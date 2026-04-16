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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.quicktheories.core.Gen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.BaseProximity;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NodeProximity;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;

public abstract class ResponseHandlerPropertyTestBase
{
    protected static final Logger logger = LoggerFactory.getLogger(ResponseHandlerPropertyTestBase.class);
    protected static final AtomicInteger keyspaceCounter = new AtomicInteger(0);
    protected static final Map<String, Keyspace> topologyCache = new HashMap<>();
    protected static final Set<InetAddressAndPort> registeredNodes = new HashSet<>();

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        // Set partitioner system property BEFORE DatabaseDescriptor initialization
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());

        SchemaLoader.loadSchema();

        // Configure node proximity
        NodeProximity sorter = new BaseProximity()
        {
            public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C replicas)
            {
                return replicas;
            }

            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }

            public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
            {
                return false;
            }
        };
        DatabaseDescriptor.setNodeProximity(sorter);
        // Set broadcast address to match first replica of datacenter1 (replicaIdx=0 → 127.1.0.255)
        // This ensures InOurDc.endpoints() correctly identifies local DC replicas
        InetAddress broadcastAddr = InetAddress.getByName("127.1.0.255");
        DatabaseDescriptor.setBroadcastAddress(broadcastAddr);
        // Register broadcast address with datacenter1 so locator knows our DC
        InetAddressAndPort broadcastEndpoint = InetAddressAndPort.getByAddress(broadcastAddr);
        ClusterMetadataTestHelper.register(broadcastEndpoint, "datacenter1", "rack1");
        registeredNodes.add(broadcastEndpoint);
    }

    // ========================================
    // Data Structures
    // ========================================

    /**
     * Describes a cluster topology configuration.
     */
    public static class TopologyConfig
    {
        public final int numDatacenters;
        public final Map<String, Integer> replicationFactors; // DC name -> RF
        public final Map<String, Integer> pendingReplicas; // DC name -> pending count
        public final int totalReplicas;
        public final int totalPending;

        TopologyConfig(int numDatacenters, Map<String, Integer> replicationFactors, Map<String, Integer> pendingReplicas)
        {
            this.numDatacenters = numDatacenters;
            this.replicationFactors = replicationFactors;
            this.pendingReplicas = pendingReplicas;
            this.totalReplicas = replicationFactors.values().stream().mapToInt(Integer::intValue).sum();
            this.totalPending = pendingReplicas.values().stream().mapToInt(Integer::intValue).sum();
        }

        String signature()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("dcs=").append(numDatacenters);
            replicationFactors.forEach((dc, rf) -> sb.append(",").append(dc).append(":").append(rf));
            if (totalPending > 0)
            {
                sb.append(",pending:");
                pendingReplicas.forEach((dc, count) -> {
                    if (count > 0)
                        sb.append(dc).append("=").append(count).append(";");
                });
            }
            return sb.toString();
        }

        @Override
        public String toString()
        {
            return signature();
        }
    }

    /**
     * A single response message (success or failure).
     */
    public static class ResponseMessage
    {
        public final int replicaIdx;
        public final RequestFailureReason failureReason; // null = success

        public ResponseMessage(int replicaIdx, RequestFailureReason failureReason)
        {
            this.replicaIdx = replicaIdx;
            this.failureReason = failureReason;
        }

        public boolean isSuccess()
        {
            return failureReason == null;
        }

        @Override
        public String toString()
        {
            return String.format("replica=%d, %s", replicaIdx, isSuccess() ? "SUCCESS" : "FAILURE(" + failureReason + ")");
        }
    }

    /**
     * A test scenario for a specific consistency level with pre-generated responses.
     */
    public static class CLScenario
    {
        public final ConsistencyLevel cl;
        public final List<ResponseMessage> responses;

        public CLScenario(ConsistencyLevel cl, List<ResponseMessage> responses)
        {
            this.cl = cl;
            this.responses = responses;
        }

        @Override
        public String toString()
        {
            return String.format("CL=%s, responses=%d", cl, responses.size());
        }
    }

    /**
     * A complete test case with topology and multiple CL scenarios.
     */
    public static class TestCase
    {
        public final TopologyConfig topology;
        public final List<CLScenario> scenarios;

        public TestCase(TopologyConfig topology, List<CLScenario> scenarios)
        {
            this.topology = topology;
            this.scenarios = scenarios;
        }

        @Override
        public String toString()
        {
            return String.format("topology=%s, scenarios=%d", topology, scenarios.size());
        }
    }

    /**
     * Holds both full and pending replica sets for a topology.
     */
    public static class ReplicaSets
    {
        public final EndpointsForToken fullReplicas;
        public final EndpointsForToken pendingReplicas;

        public ReplicaSets(EndpointsForToken fullReplicas, EndpointsForToken pendingReplicas)
        {
            this.fullReplicas = fullReplicas;
            this.pendingReplicas = pendingReplicas;
        }
    }

    /**
     * Expected outcome of a response sequence
     */
    public enum ExpectedOutcome
    {
        SUCCESS,   // Handler should complete successfully
        FAILURE    // Handler should fail
    }

    // ========================================
    // Generators
    // ========================================

    /**
     * Generates varied datacenter topologies (1-7 DCs with varying RF and pending replicas).
     */
    protected Gen<TopologyConfig> topologyGen()
    {
        return integers().between(1, 7).flatMap(numDcs -> {
            return lists().of(integers().between(1, 5)).ofSize(numDcs).flatMap(rfList -> {
                // Generate 0-2 pending replicas per DC
                return lists().of(integers().between(0, 2)).ofSize(numDcs).map(pendingList -> {
                    Map<String, Integer> replicationFactors = new LinkedHashMap<>();
                    Map<String, Integer> pendingReplicas = new LinkedHashMap<>();
                    for (int i = 0; i < numDcs; i++)
                    {
                        String dcName = "datacenter" + (i + 1);
                        replicationFactors.put(dcName, rfList.get(i));
                        pendingReplicas.put(dcName, pendingList.get(i));
                    }
                    return new TopologyConfig(numDcs, replicationFactors, pendingReplicas);
                });
            });
        });
    }


    // ========================================
    // Topology Setup
    // ========================================

    /**
     * Gets or creates a keyspace for the given topology.
     */
    public static Keyspace getOrCreateKeyspace(TopologyConfig topology) throws Exception
    {
        String signature = topology.signature();
        if (topologyCache.containsKey(signature))
            return topologyCache.get(signature);

        String keyspaceName = "PropTest" + keyspaceCounter.incrementAndGet();

        // Register full replica nodes
        int replicaIdx = 0;
        for (Map.Entry<String, Integer> entry : topology.replicationFactors.entrySet())
        {
            String dcName = entry.getKey();
            int rf = entry.getValue();
            int dcNum = Integer.parseInt(dcName.substring("datacenter".length()));

            for (int i = 0; i < rf; i++)
            {
                String ip = String.format("127.%d.0.%d", dcNum, 255 - replicaIdx);
                InetAddressAndPort endpoint = InetAddressAndPort.getByName(ip);

                if (!registeredNodes.contains(endpoint))
                {
                    ClusterMetadataTestHelper.register(endpoint, dcName, "rack1");
                    registeredNodes.add(endpoint);
                }
                replicaIdx++;
            }
        }

        // Pending replica nodes don't need ClusterMetadata registration
        // We pass them directly to ReplicaPlan in createHandler()
        // Just register them with basic info so InOurDc can identify their datacenter
        for (Map.Entry<String, Integer> entry : topology.pendingReplicas.entrySet())
        {
            String dcName = entry.getKey();
            int pending = entry.getValue();
            int dcNum = Integer.parseInt(dcName.substring("datacenter".length()));

            for (int i = 0; i < pending; i++)
            {
                String ip = String.format("127.%d.0.%d", dcNum, 255 - replicaIdx);
                InetAddressAndPort endpoint = InetAddressAndPort.getByName(ip);

                if (!registeredNodes.contains(endpoint))
                {
                    // Just register for DC/rack identification - no join process needed
                    ClusterMetadataTestHelper.register(endpoint, dcName, "rack1");
                    registeredNodes.add(endpoint);
                }
                replicaIdx++;
            }
        }

        // Create keyspace
        Object[] dcRfPairs = topology.replicationFactors.entrySet().stream()
                                                        .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
                                                        .toArray();
        SchemaLoader.createKeyspace(keyspaceName, KeyspaceParams.nts(dcRfPairs),
                                    SchemaLoader.standardCFMD(keyspaceName, "Standard"));

        Keyspace ks = Keyspace.open(keyspaceName);
        topologyCache.put(signature, ks);
        return ks;
    }

    /**
     * Creates full and pending replica sets for the given topology.
     */
    public static ReplicaSets createReplicaSets(TopologyConfig topology) throws Exception
    {
        List<Replica> fullReplicas = new ArrayList<>();
        List<Replica> pendingReplicas = new ArrayList<>();
        int replicaIdx = 0;

        // Create full replicas for all DCs first (must match getOrCreateKeyspace ordering)
        for (Map.Entry<String, Integer> entry : topology.replicationFactors.entrySet())
        {
            int rf = entry.getValue();
            int dcNum = Integer.parseInt(entry.getKey().substring("datacenter".length()));

            for (int i = 0; i < rf; i++)
            {
                String ip = String.format("127.%d.0.%d", dcNum, 255 - replicaIdx);
                fullReplicas.add(ReplicaUtils.full(InetAddressAndPort.getByName(ip)));
                replicaIdx++;
            }
        }

        // Then create pending replicas for all DCs
        for (Map.Entry<String, Integer> entry : topology.pendingReplicas.entrySet())
        {
            int pending = entry.getValue();
            int dcNum = Integer.parseInt(entry.getKey().substring("datacenter".length()));

            for (int i = 0; i < pending; i++)
            {
                String ip = String.format("127.%d.0.%d", dcNum, 255 - replicaIdx);
                pendingReplicas.add(ReplicaUtils.full(InetAddressAndPort.getByName(ip)));
                replicaIdx++;
            }
        }

        var token = Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(0));
        return new ReplicaSets(
            EndpointsForToken.of(token, fullReplicas.toArray(new Replica[0])),
            EndpointsForToken.of(token, pendingReplicas.toArray(new Replica[0]))
        );
    }

    // ========================================
    // Response Generation
    // ========================================

    /**
     * Maps replica index to its datacenter.
     * Indices 0..(totalReplicas-1) are full replicas, totalReplicas..(totalReplicas+totalPending-1) are pending.
     */
    public static String getReplicaDatacenter(int replicaIdx, TopologyConfig topology)
    {
        int idx = 0;

        // First check full replicas
        for (Map.Entry<String, Integer> entry : topology.replicationFactors.entrySet())
        {
            int rf = entry.getValue();
            if (replicaIdx < idx + rf)
                return entry.getKey();
            idx += rf;
        }

        // Then check pending replicas (indices continue from where full replicas left off)
        int pendingStartIdx = topology.totalReplicas;
        idx = pendingStartIdx;
        for (Map.Entry<String, Integer> entry : topology.pendingReplicas.entrySet())
        {
            int pending = entry.getValue();
            if (replicaIdx < idx + pending)
                return entry.getKey();
            idx += pending;
        }

        throw new IllegalArgumentException("Invalid replica index: " + replicaIdx);
    }

    protected abstract List<ConsistencyLevel> consistencyLevels();

    /**
     * Generates write-applicable consistency levels.
     */
    protected Gen<ConsistencyLevel> consistencyLevelGen()
    {
        return arbitrary().pick(consistencyLevels());
    }

    /**
     * Generates failure reasons for failed responses.
     */
    protected Gen<RequestFailureReason> failureReasonGen()
    {
        return arbitrary().pick(
        RequestFailureReason.TIMEOUT,
        RequestFailureReason.UNKNOWN,
        RequestFailureReason.INCOMPATIBLE_SCHEMA,
        RequestFailureReason.COORDINATOR_BEHIND
        );
    }

    /**
     * Generates a random permutation of integers [0, n-1] using Fisher-Yates shuffle.
     * This ensures deterministic reproducibility from QuickTheories' seed.
     */
    protected static Gen<List<Integer>> permutationGen(int n)
    {
        if (n == 0) return lists().of(integers().all()).ofSize(0);
        if (n == 1) return lists().of(integers().all()).ofSize(1).map(list -> {
            List<Integer> result = new ArrayList<>();
            result.add(0);
            return result;
        });

        // Generate n-1 random swap indices for Fisher-Yates shuffle
        return lists().of(integers().between(0, n - 1)).ofSize(n - 1).map(swaps -> {
            List<Integer> result = new ArrayList<>();
            for (int i = 0; i < n; i++)
                result.add(i);

            // Fisher-Yates shuffle using generated swap indices
            for (int i = 0; i < n - 1; i++)
            {
                int maxSwap = n - i - 1;
                int j = i + Math.min(swaps.get(i), maxSwap);
                Collections.swap(result, i, j);
            }

            return result;
        });
    }

    /**
     * Generates a response sequence of the given size where each element gets a random type
     * from the provided enum values, in a random order.
     *
     * @param size number of responses to generate
     * @param values possible response types
     * @param factory creates a response message from (index, type)
     */
    protected static <T extends Enum<T>, R> Gen<List<R>> typedResponseSequenceGen(
        int size, T[] values, BiFunction<Integer, T, R> factory)
    {
        return lists().of(arbitrary().pick(values)).ofSize(size).flatMap(types ->
            permutationGen(size).map(ordering -> {
                List<R> responses = new ArrayList<>();
                for (int idx : ordering)
                    responses.add(factory.apply(idx, types.get(idx)));
                return responses;
            })
        );
    }

    /**
     * Creates a hardcoded sequence where all responses have the same type.
     */
    protected static <T extends Enum<T>, R> List<R> allSameTypeSequence(
        int size, T type, BiFunction<Integer, T, R> factory)
    {
        List<R> responses = new ArrayList<>();
        for (int i = 0; i < size; i++)
            responses.add(factory.apply(i, type));
        return responses;
    }

    /**
     * Generates a response sequence for a given topology.
     * Each replica (full + pending) gets a success/failure outcome, and responses are in random order.
     */
    private Gen<List<ResponseMessage>> responseSequenceGen(TopologyConfig topology)
    {
        int totalEndpoints = topology.totalReplicas + topology.totalPending;

        // Generate success/failure for each endpoint (full + pending)
        return lists().of(booleans().all()).ofSize(totalEndpoints).flatMap(successFlags -> {
            // Generate failure reasons for each endpoint (used only if failure)
            return lists().of(failureReasonGen()).ofSize(totalEndpoints).flatMap(failureReasons -> {
                // Generate random ordering of responses
                return permutationGen(totalEndpoints).map(ordering -> {
                    List<ResponseMessage> responses = new ArrayList<>();
                    for (int idx : ordering)
                    {
                        RequestFailureReason reason = successFlags.get(idx) ? null : failureReasons.get(idx);
                        responses.add(new ResponseMessage(idx, reason));
                    }
                    return responses;
                });
            });
        });
    }

    /**
     * Generates hardcoded all-success response sequence (full + pending replicas).
     */
    private static List<ResponseMessage> allSuccessSequence(TopologyConfig topology)
    {
        List<ResponseMessage> responses = new ArrayList<>();
        int totalEndpoints = topology.totalReplicas + topology.totalPending;
        for (int i = 0; i < totalEndpoints; i++)
            responses.add(new ResponseMessage(i, null));
        return responses;
    }

    /**
     * Generates hardcoded all-failure response sequence (full + pending replicas).
     */
    private static List<ResponseMessage> allFailureSequence(TopologyConfig topology)
    {
        List<ResponseMessage> responses = new ArrayList<>();
        int totalEndpoints = topology.totalReplicas + topology.totalPending;
        for (int i = 0; i < totalEndpoints; i++)
            responses.add(new ResponseMessage(i, RequestFailureReason.TIMEOUT));
        return responses;
    }

    /**
     * Generates a CL scenario (consistency level + response sequence).
     */
    private Gen<CLScenario> scenarioGen(TopologyConfig topology)
    {
        return consistencyLevelGen().flatMap(cl ->
                                             responseSequenceGen(topology).map(responses ->
                                                                               new CLScenario(cl, responses)
                                             )
        );
    }

    /**
     * Generates a complete test case with topology and multiple CL scenarios.
     * Includes 3 random scenarios plus 2 hardcoded scenarios (all-success, all-failure).
     */
    protected Gen<TestCase> testCaseGen()
    {
        return random -> {
            TopologyConfig topology = topologyGen().generate(random);

            List<CLScenario> scenarios = new ArrayList<>();
            for (int i=0; i<50; i++)
                scenarios.add(scenarioGen(topology).generate(random));

            // Add hardcoded all-success and failure scenarios for each CL
            for (ConsistencyLevel cl: consistencyLevels())
            {
                scenarios.add(new CLScenario(cl, allSuccessSequence(topology)));
                scenarios.add(new CLScenario(cl, allFailureSequence(topology)));
            }

            return new TestCase(topology, scenarios);
        };
    }

}
