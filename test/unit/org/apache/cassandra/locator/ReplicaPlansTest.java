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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.ReplicaUtils.*;

public class ReplicaPlansTest
{

    static
    {
        DatabaseDescriptor.daemonInitialization();
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

    private static Replica full(InetAddressAndPort ep) { return fullReplica(ep, R1); }



    @Test
    public void testWriteEachQuorum()
    {
        IEndpointSnitch stash = DatabaseDescriptor.getEndpointSnitch();
        final Token token = tk(1L);
        try
        {
            {
                // all full natural
                Keyspace ks = ks(ImmutableSet.of(EP1, EP2, EP3), ImmutableMap.of("DC1", "3", "DC2", "3"));
                EndpointsForToken natural = EndpointsForToken.of(token, full(EP1), full(EP2), full(EP3), full(EP4), full(EP5), full(EP6));
                EndpointsForToken pending = EndpointsForToken.empty(token);
                ReplicaPlan.ForWrite plan = ReplicaPlans.forWrite(ks, ConsistencyLevel.EACH_QUORUM, natural, pending, Predicates.alwaysTrue(), ReplicaPlans.writeNormal);
                assertEquals(natural, plan.liveAndDown);
                assertEquals(natural, plan.live);
                assertEquals(natural, plan.contacts());
            }
            {
                // all natural and up, one transient in each DC
                Keyspace ks = ks(ImmutableSet.of(EP1, EP2, EP3), ImmutableMap.of("DC1", "3", "DC2", "3"));
                EndpointsForToken natural = EndpointsForToken.of(token, full(EP1), full(EP2), trans(EP3), full(EP4), full(EP5), trans(EP6));
                EndpointsForToken pending = EndpointsForToken.empty(token);
                ReplicaPlan.ForWrite plan = ReplicaPlans.forWrite(ks, ConsistencyLevel.EACH_QUORUM, natural, pending, Predicates.alwaysTrue(), ReplicaPlans.writeNormal);
                assertEquals(natural, plan.liveAndDown);
                assertEquals(natural, plan.live);
                EndpointsForToken expectContacts = EndpointsForToken.of(token, full(EP1), full(EP2), full(EP4), full(EP5));
                assertEquals(expectContacts, plan.contacts());
            }
        }
        finally
        {
            DatabaseDescriptor.setEndpointSnitch(stash);
        }

        {
            // test simple

        }
    }

}
