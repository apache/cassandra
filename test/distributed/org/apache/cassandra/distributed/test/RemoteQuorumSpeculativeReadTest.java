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

package org.apache.cassandra.distributed.test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.service.reads.AlwaysSpeculativeRetryPolicy;

import static org.apache.cassandra.locator.ReplicaUtils.FULL_BOUNDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RemoteQuorumSpeculativeReadTest extends TestBaseImpl
{
    @Test
    public void tokenReadRemoteQuorumSpeculatesWhenAllowed() throws Exception
    {
        try (Cluster cluster = Cluster.build()
                                      .withRacks(2, 3, 1) // 2 DCs, 1 rack per DC, 3 nodes per DC
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks_rq WITH replication = {'class':'NetworkTopologyStrategy','datacenter1':3,'datacenter2':3}");
            cluster.schemaChange("CREATE TABLE ks_rq.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH speculative_retry='ALWAYS'");

            IInvokableInstance dc1 = cluster.get(1);
            dc1.runOnInstance(() -> {
                String local = DatabaseDescriptor.getLocalDataCenter();
                DatabaseDescriptor.setRemoteQuorumTargetDcs(Collections.singletonMap(local, "datacenter2"));

                Keyspace ks = Keyspace.open("ks_rq");
                Token token = new Murmur3Partitioner.LongToken(0L);
                ReplicaPlan.ForTokenRead plan = ReplicaPlans.forRead(ks, token, ConsistencyLevel.REMOTE_QUORUM, AlwaysSpeculativeRetryPolicy.INSTANCE);

                int expected = ConsistencyLevel.REMOTE_QUORUM.blockFor(ks.getReplicationStrategy()) + 1;
                assertEquals(expected, plan.contacts().size());

                IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
                plan.contacts().forEach(replica -> assertEquals("datacenter2", snitch.getDatacenter(replica)));

                // Additionally verify the actual endpoints belong to the remote DC by IP/port
                Set<InetAddressAndPort> remoteAddrs = new HashSet<>();
                for (InetAddressAndPort ep : Gossiper.instance.getLiveMembers())
                    if ("datacenter2".equals(snitch.getDatacenter(ep)))
                        remoteAddrs.add(ep);
                plan.contacts().forEach(replica -> assertTrue(remoteAddrs.contains(replica.endpoint())));
            });
        }
    }

    @Test
    public void rangeReadRemoteQuorumDoesNotSpeculate() throws Exception
    {
        try (Cluster cluster = Cluster.build()
                                      .withRacks(2, 3, 1)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks_rq2 WITH replication = {'class':'NetworkTopologyStrategy','datacenter1':3,'datacenter2':3}");
            cluster.schemaChange("CREATE TABLE ks_rq2.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH speculative_retry='ALWAYS'");

            IInvokableInstance dc1 = cluster.get(1);
            dc1.runOnInstance(() -> {
                String local = DatabaseDescriptor.getLocalDataCenter();
                DatabaseDescriptor.setRemoteQuorumTargetDcs(Collections.singletonMap(local, "datacenter2"));

                Keyspace ks = Keyspace.open("ks_rq2");
                ReplicaPlan.ForRangeRead plan = ReplicaPlans.forRangeRead(ks, ConsistencyLevel.REMOTE_QUORUM, FULL_BOUNDS, 1);

                int expected = ConsistencyLevel.REMOTE_QUORUM.blockFor(ks.getReplicationStrategy());
                assertEquals(expected, plan.contacts().size());

                IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
                plan.contacts().forEach(replica -> assertEquals("datacenter2", snitch.getDatacenter(replica)));

                // Additionally verify the actual endpoints belong to the remote DC by IP/port
                Set<InetAddressAndPort> remoteAddrs = new HashSet<>();
                for (InetAddressAndPort ep : Gossiper.instance.getLiveMembers())
                    if ("datacenter2".equals(snitch.getDatacenter(ep)))
                        remoteAddrs.add(ep);
                plan.contacts().forEach(replica -> assertTrue(remoteAddrs.contains(replica.endpoint())));
            });
        }
    }
}
