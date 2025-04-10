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
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.RangeStreamer.FetchReplica;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamOperation;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(BMUnitRunner.class)
public class BootStrapperTest
{
    static IPartitioner oldPartitioner;
    static Predicate<Replica> originalAlivePredicate = RangeStreamer.ALIVE_PREDICATE;
    public static AtomicBoolean nonOptimizationHit = new AtomicBoolean(false);
    public static AtomicBoolean optimizationHit = new AtomicBoolean(false);
    private static final IFailureDetector mockFailureDetector = new IFailureDetector()
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

    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.startGossiper();
        SchemaLoader.prepareServer();
        SchemaLoader.schemaDefinition("BootStrapperTest");
        RangeStreamer.ALIVE_PREDICATE = Predicates.alwaysTrue();
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
        final int[] clusterSizes = new int[] { 1, 3, 5, 10, 100};
        for (String keyspaceName : Schema.instance.distributedKeyspaces().names())
        {
            int replicationFactor = Keyspace.open(keyspaceName).getReplicationStrategy().getReplicationFactor().allReplicas;
            for (int clusterSize : clusterSizes)
                if (clusterSize >= replicationFactor)
                    testSourceTargetComputation(keyspaceName, clusterSize, replicationFactor);
        }
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Make sure the non-optimized path is picked up for some operations",
                               targetClass = "org.apache.cassandra.dht.RangeStreamer",
                               targetMethod = "convertPreferredEndpointsToWorkMap(EndpointsByReplica)",
                               action = "org.apache.cassandra.dht.BootStrapperTest.nonOptimizationHit.set(true)"),
                       @BMRule(name = "Make sure the optimized path is picked up for some operations",
                               targetClass = "org.apache.cassandra.dht.RangeStreamer",
                               targetMethod = "getOptimizedWorkMap(EndpointsByReplica,Collection,String)",
                               action = "org.apache.cassandra.dht.BootStrapperTest.optimizationHit.set(true)") })
    public void testStreamingCandidatesOptmizationSkip() throws UnknownHostException
    {
        testSkipStreamingCandidatesOptmizationFeatureFlag(true, true, false);
        testSkipStreamingCandidatesOptmizationFeatureFlag(false, true, true);
    }

    private void testSkipStreamingCandidatesOptmizationFeatureFlag(boolean disableOptimization, boolean nonOptimizedPathHit, boolean optimizedPathHit) throws UnknownHostException
    {
        try
        {
            nonOptimizationHit.set(false);
            optimizationHit.set(false);
            CassandraRelevantProperties.SKIP_OPTIMAL_STREAMING_CANDIDATES_CALCULATION.setBoolean(disableOptimization);

            for (String keyspaceName : Schema.instance.getUserKeyspaces())
            {
                StorageService ss = StorageService.instance;
                TokenMetadata tmd = ss.getTokenMetadata();

                generateFakeEndpoints(10);
                Token myToken = tmd.partitioner.getRandomToken();
                InetAddressAndPort myEndpoint = InetAddressAndPort.getByName("127.0.0.1");

                assertEquals(10, tmd.sortedTokens().size());
                RangeStreamer s = new RangeStreamer(tmd, null, myEndpoint, StreamOperation.BOOTSTRAP, true, DatabaseDescriptor.getEndpointSnitch(), new StreamStateStore(), mockFailureDetector, false, 1);
                s.addRanges(keyspaceName, Keyspace.open(keyspaceName).getReplicationStrategy().getPendingAddressRanges(tmd, myToken, myEndpoint));
            }

            assertEquals(nonOptimizedPathHit, nonOptimizationHit.get());
            assertEquals(optimizedPathHit, optimizationHit.get());
        }
        finally
        {
            CassandraRelevantProperties.SKIP_OPTIMAL_STREAMING_CANDIDATES_CALCULATION.reset();
        }
    }

    private RangeStreamer testSourceTargetComputation(String keyspaceName, int numOldNodes, int replicationFactor) throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();

        generateFakeEndpoints(numOldNodes);
        Token myToken = tmd.partitioner.getRandomToken();
        InetAddressAndPort myEndpoint = InetAddressAndPort.getByName("127.0.0.1");

        assertEquals(numOldNodes, tmd.sortedTokens().size());
        RangeStreamer s = new RangeStreamer(tmd, null, myEndpoint, StreamOperation.BOOTSTRAP, true, DatabaseDescriptor.getEndpointSnitch(), new StreamStateStore(), mockFailureDetector, false, 1);
        assertNotNull(Keyspace.open(keyspaceName));
        s.addRanges(keyspaceName, Keyspace.open(keyspaceName).getReplicationStrategy().getPendingAddressRanges(tmd, myToken, myEndpoint));


        Multimap<InetAddressAndPort, FetchReplica> toFetch = s.toFetch().get(keyspaceName);

        // Check we get get RF new ranges in total
        assertEquals(replicationFactor, toFetch.size());

        // there isn't any point in testing the size of these collections for any specific size.  When a random partitioner
        // is used, they will vary.
        assert toFetch.values().size() > 0;
        assert toFetch.keys().stream().noneMatch(myEndpoint::equals);
        return s;
    }

    private void generateFakeEndpoints(int numOldNodes) throws UnknownHostException
    {
        generateFakeEndpoints(StorageService.instance.getTokenMetadata(), numOldNodes, 1);
    }

    private void generateFakeEndpoints(TokenMetadata tmd, int numOldNodes, int numVNodes) throws UnknownHostException
    {
        tmd.clearUnsafe();
        generateFakeEndpoints(tmd, numOldNodes, numVNodes, "0", "0");
    }

    Random rand = new Random(1);

    private void generateFakeEndpoints(TokenMetadata tmd, int numOldNodes, int numVNodes, String dc, String rack) throws UnknownHostException
    {
        IPartitioner p = tmd.partitioner;

        for (int i = 1; i <= numOldNodes; i++)
        {
            // leave .1 for myEndpoint
            InetAddressAndPort addr = InetAddressAndPort.getByName("127." + dc + "." + rack + "." + (i + 1));
            List<Token> tokens = Lists.newArrayListWithCapacity(numVNodes);
            for (int j = 0; j < numVNodes; ++j)
                tokens.add(p.getRandomToken(rand));

            tmd.updateNormalTokens(tokens, addr);
        }
    }

}
