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
package org.apache.cassandra.dht;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class BootStrapperTest
{
    static IPartitioner oldPartitioner;
    static Predicate<Replica> originalAlivePredicate = RangeStreamer.ALIVE_PREDICATE;

    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        SchemaLoader.startGossiper();
        SchemaLoader.schemaDefinition("BootStrapperTest");
        RangeStreamer.ALIVE_PREDICATE = Predicates.alwaysTrue();
        ServerTestUtils.markCMS();
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setPartitionerUnsafe(oldPartitioner);
        RangeStreamer.ALIVE_PREDICATE = originalAlivePredicate;
    }

    @Test
    public void testSourceTargetComputation() throws UnknownHostException
    {
        final int[] clusterSizes = new int[] { 1, 3, 5, 10, 100 };
        for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces().names())
        {
            if (keyspaceName.equals(SchemaConstants.METADATA_KEYSPACE_NAME))
                continue;
            int replicationFactor = Keyspace.open(keyspaceName).getReplicationStrategy().getReplicationFactor().allReplicas;
            for (int clusterSize : clusterSizes)
                if (clusterSize >= replicationFactor)
                    testSourceTargetComputation(keyspaceName, clusterSize, replicationFactor);
        }
    }

    private RangeStreamer testSourceTargetComputation(String keyspaceName, int numOldNodes, int replicationFactor) throws UnknownHostException
    {
        ServerTestUtils.resetCMS();
        generateFakeEndpoints(numOldNodes);
        ClusterMetadata metadata = ClusterMetadata.current();

        assertEquals(numOldNodes, metadata.tokenMap.tokens().size());
        IFailureDetector mockFailureDetector = new IFailureDetector()
        {
            public boolean isAlive(InetAddressAndPort ep)
            {
                return true;
            }

            public void interpret(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void report(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void remove(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void forceConviction(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
        };

        Token myToken = metadata.partitioner.getRandomToken();
        InetAddressAndPort myEndpoint = InetAddressAndPort.getByName("127.0.0.1");
        NodeId newNode = ClusterMetadataTestHelper.register(myEndpoint);
        ClusterMetadataTestHelper.JoinProcess join = ClusterMetadataTestHelper.lazyJoin(myEndpoint, myToken);
        join.prepareJoin();
        metadata = ClusterMetadata.current();
        BootstrapAndJoin joiningPlan = (BootstrapAndJoin) metadata.inProgressSequences.get(newNode);
        Pair<MovementMap, MovementMap> movements = joiningPlan.getMovementMaps(metadata);
        RangeStreamer s = new RangeStreamer(metadata,
                                            StreamOperation.BOOTSTRAP,
                                            true,
                                            DatabaseDescriptor.getEndpointSnitch(),
                                            new StreamStateStore(),
                                            mockFailureDetector,
                                            false,
                                            1,
                                            movements.left,
                                            movements.right);

        assertNotNull(Keyspace.open(keyspaceName));
        s.addKeyspaceToFetch(keyspaceName);
        Multimap<InetAddressAndPort, RangeStreamer.FetchReplica> toFetch = s.toFetch().get(keyspaceName);

        // Pre-TCM this test would always run with RangeStreamer::useStrictSourcesForRanges returning false because
        // the RangeStreamer instance was constructed with a null Collection<Token>, relying on pending ranges being
        // calculated in the test code, and then cloning TokenMetadata with pending tokens added to pass to
        // calculateRangesToFetchWithPreferredEndpoints. Post-TCM, we calculate both relaxed and strict movements
        // together and TokenMetadata is no more, so the equivalent operation now ends up using strict movements. For
        // that reason, when RF includes transient replicas, toFetch will include both a transient and full source,
        // hence we dedupe the ranges here.
        Set<Range<Token>> fetchRanges = toFetch.values()
                                               .stream()
                                               .map(fr -> fr.remote.range())
                                               .collect(Collectors.toSet());
        // Post CEP-21 wrapping ranges are also unwrapped at this point, so account for that when setting expectation
        int expectedRangeCount = replicationFactor + (includesWraparound(fetchRanges) ? 1 : 0);
        assertEquals(expectedRangeCount, fetchRanges.size());

        // there isn't any point in testing the size of these collections for any specific size.  When a random partitioner
        // is used, they will vary.
        assert toFetch.values().size() > 0;
        assert toFetch.keys().stream().noneMatch(myEndpoint::equals);
        return s;
    }

    private boolean includesWraparound(Collection<Range<Token>> toFetch)
    {
        long minTokenCount = toFetch.stream()
                                    .filter(r -> r.left.isMinimum() || r.right.isMinimum())
                                    .count();
        assertTrue("Ranges to fetch should either include both or neither parts of normalised wrapping range",
                   minTokenCount % 2 == 0);
        return minTokenCount > 0;
    }

    private void generateFakeEndpoints(int numOldNodes) throws UnknownHostException
    {
        generateFakeEndpoints(numOldNodes, 1);
    }

    private void generateFakeEndpoints(int numOldNodes, int numVNodes) throws UnknownHostException
    {
        generateFakeEndpoints(numOldNodes, numVNodes, "0", "0");
    }

    Random rand = new Random(1);

    private void generateFakeEndpoints(int numOldNodes, int numVNodes, String dc, String rack) throws UnknownHostException
    {
        IPartitioner p = ClusterMetadata.current().partitioner;
        for (int i = 1; i <= numOldNodes; i++)
        {
            // leave .1 for myEndpoint
            InetAddressAndPort addr = InetAddressAndPort.getByName("127." + dc + "." + rack + "." + (i + 1));
            List<Token> tokens = Lists.newArrayListWithCapacity(numVNodes);
            for (int j = 0; j < numVNodes; ++j)
                tokens.add(p.getRandomToken(rand));

            ClusterMetadataTestHelper.addEndpoint(addr, tokens);
        }
    }

}
