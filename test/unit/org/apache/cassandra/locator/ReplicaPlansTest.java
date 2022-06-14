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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.NeverSpeculativeRetryPolicy;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import static org.apache.cassandra.db.ConsistencyLevel.ANY;
import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.ReplicaUtils.*;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class ReplicaPlansTest
{
    private static final String DC1 = "datacenter1";
    private static final String KEYSPACE1 = "ks1";
    private static final String KEYSPACE2 = "ks2";

    private static final Map<InetAddressAndPort, String> ipToKeyspaceAffinity = new HashMap<>();

    private IEndpointSnitch savedEndpointSnitch;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        savedEndpointSnitch = DatabaseDescriptor.getEndpointSnitch();
    }

    @After
    public void cleanup()
    {
        DatabaseDescriptor.setEndpointSnitch(savedEndpointSnitch);
    }

    static class FilterByKeyspaceAffinitySnitch extends AbstractNetworkTopologySnitch
    {
        public String getRack(InetAddressAndPort endpoint)
        {
            byte[] address = endpoint.addressBytes;
            return "rack" + address[1];
        }

        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return DC1;
        }

        public Predicate<Replica> filterByAffinity(String keyspace)
        {
            return replica -> {
                // Filter replicas by keyspace affinity
                return ipToKeyspaceAffinity.get(replica.endpoint()).equals(keyspace);
            };
        }
    }

    @Test
    @BMRule(name = "FailureDetector sees all nodes as live",
    targetClass = "FailureDetector",
    targetMethod = "isAlive",
    action = "return true;")
    public void testFilterByAffinity() throws UnknownHostException
    {
        IEndpointSnitch filterByKeyspaceAffinitySnitch = new FilterByKeyspaceAffinitySnitch();
        DatabaseDescriptor.setEndpointSnitch(filterByKeyspaceAffinitySnitch);

        setupReplicas();

        Token token = new Murmur3Partitioner.LongToken(0);

        Keyspace keyspace1 = keyspaceWithSnitch(KEYSPACE1, filterByKeyspaceAffinitySnitch);
        ReplicaPlan.ForTokenRead plan1 = ReplicaPlans.forRead(keyspace1, token, null, ANY, NeverSpeculativeRetryPolicy.INSTANCE);
        assertEndpointsMatchKeyspaceAffinity(KEYSPACE1, plan1.contacts());

        Keyspace keyspace2 = keyspaceWithSnitch(KEYSPACE2, filterByKeyspaceAffinitySnitch);
        ReplicaPlan.ForTokenRead plan2 = ReplicaPlans.forRead(keyspace2, token, null, ANY, NeverSpeculativeRetryPolicy.INSTANCE);
        assertEndpointsMatchKeyspaceAffinity(KEYSPACE2, plan2.contacts());
    }

    private void setupReplicas() throws UnknownHostException
    {
        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
        tokenMetadata.clearUnsafe();

        // List of replicas
        List<InetAddressAndPort> replicas = ImmutableList.of(
        InetAddressAndPort.getByName("127.1.0.255"), InetAddressAndPort.getByName("127.1.0.254"), InetAddressAndPort.getByName("127.1.0.253"),
        InetAddressAndPort.getByName("127.2.0.255"), InetAddressAndPort.getByName("127.2.0.254"), InetAddressAndPort.getByName("127.2.0.253"),
        InetAddressAndPort.getByName("127.3.0.255"), InetAddressAndPort.getByName("127.3.0.254"), InetAddressAndPort.getByName("127.3.0.253")
        );

        // Update token metadata and keyspace affinity for each replica
        for (int i = 0; i < replicas.size(); i++)
        {
            InetAddressAndPort ip = replicas.get(i);
            tokenMetadata.updateHostId(UUID.randomUUID(), ip);
            tokenMetadata.updateNormalToken(new Murmur3Partitioner.LongToken(i), ip);

            // Alternate keyspace affinity across replicas
            ipToKeyspaceAffinity.put(ip, i % 2 == 0 ? KEYSPACE1 : KEYSPACE2);
        }
    }

    private Keyspace keyspaceWithSnitch(String keyspaceName, IEndpointSnitch snitch)
    {
        // Create keyspace metadata
        KeyspaceParams keyspaceParams = KeyspaceParams.nts(DC1, 3);
        Tables keyspaceTables = Tables.of(SchemaLoader.standardCFMD(keyspaceName, "Bar").build());
        KeyspaceMetadata keyspaceMetadata = KeyspaceMetadata.create(keyspaceName, keyspaceParams, keyspaceTables);

        Keyspace keyspace = Keyspace.mockKS(keyspaceMetadata);

        // Associate keyspace to the given snitch
        keyspace.getReplicationStrategy().snitch = snitch;

        return keyspace;
    }

    private void assertEndpointsMatchKeyspaceAffinity(String keyspaceName, EndpointsForToken endpoints)
    {
        assertTrue(endpoints.stream()
                            .allMatch(replica -> ipToKeyspaceAffinity.get(replica.endpoint()).equals(keyspaceName)));
    }

    static class Snitch extends AbstractNetworkTopologySnitch
    {
        final Set<InetAddressAndPort> dc1;

        Snitch(Set<InetAddressAndPort> dc1)
        {
            this.dc1 = dc1;
        }

        @Override
        public String getRack(InetAddressAndPort endpoint)
        {
            return dc1.contains(endpoint) ? "R1" : "R2";
        }

        @Override
        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return dc1.contains(endpoint) ? "DC1" : "DC2";
        }
    }

    private static Keyspace ks(Set<InetAddressAndPort> dc1, Map<String, String> replication)
    {
        replication = ImmutableMap.<String, String>builder().putAll(replication).put("class", "NetworkTopologyStrategy").build();
        Keyspace keyspace = Keyspace.mockKS(KeyspaceMetadata.create("blah", KeyspaceParams.create(false, replication)));
        Snitch snitch = new Snitch(dc1);
        DatabaseDescriptor.setEndpointSnitch(snitch);
        keyspace.getReplicationStrategy().snitch = snitch;
        return keyspace;
    }

    private static Replica full(InetAddressAndPort ep)
    {
        return fullReplica(ep, R1);
    }


    @Test
    public void testWriteEachQuorum()
    {
        final Token token = tk(1L);
        {
            // all full natural
            Keyspace ks = ks(ImmutableSet.of(EP1, EP2, EP3), ImmutableMap.of("DC1", "3", "DC2", "3"));
            EndpointsForToken natural = EndpointsForToken.of(token, full(EP1), full(EP2), full(EP3), full(EP4), full(EP5), full(EP6));
            EndpointsForToken pending = EndpointsForToken.empty(token);
            ReplicaPlan.ForTokenWrite plan = ReplicaPlans.forWrite(ks, ConsistencyLevel.EACH_QUORUM, natural, pending, Predicates.alwaysTrue(), ReplicaPlans.writeNormal);
            assertEquals(natural, plan.liveAndDown);
            assertEquals(natural, plan.live);
            assertEquals(natural, plan.contacts());
        }
        {
            // all natural and up, one transient in each DC
            Keyspace ks = ks(ImmutableSet.of(EP1, EP2, EP3), ImmutableMap.of("DC1", "3", "DC2", "3"));
            EndpointsForToken natural = EndpointsForToken.of(token, full(EP1), full(EP2), trans(EP3), full(EP4), full(EP5), trans(EP6));
            EndpointsForToken pending = EndpointsForToken.empty(token);
            ReplicaPlan.ForTokenWrite plan = ReplicaPlans.forWrite(ks, ConsistencyLevel.EACH_QUORUM, natural, pending, Predicates.alwaysTrue(), ReplicaPlans.writeNormal);
            assertEquals(natural, plan.liveAndDown);
            assertEquals(natural, plan.live);
            EndpointsForToken expectContacts = EndpointsForToken.of(token, full(EP1), full(EP2), full(EP4), full(EP5));
            assertEquals(expectContacts, plan.contacts());
        }
    }
}
