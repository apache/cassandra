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
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.CassandraTestBase;
import org.apache.cassandra.CassandraTestBase.UseMurmur3Partitioner;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.AbstractReplicaCollection.ReplicaList;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.SatellitePaxosParticipants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

import static org.apache.cassandra.CassandraTestBase.DisableMBeanRegistration;
import static org.apache.cassandra.CassandraTestBase.PrepareServerNoRegister;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@PrepareServerNoRegister
@DisableMBeanRegistration
@UseMurmur3Partitioner
public class SatelliteCommitPlanTest extends CassandraTestBase
{
    private static final String KEYSPACE = "scp_test";
    private static final LongToken TOKEN = new LongToken(150);

    @After
    public void teardown()
    {
        ServerTestUtils.resetCMS();
    }

    private void addToken(long token, String address, Location location) throws UnknownHostException
    {
        InetAddressAndPort addr = InetAddressAndPort.getByName(address);
        ClusterMetadataTestHelper.addEndpoint(addr, new LongToken(token), location);
    }

    private void setupTopology() throws UnknownHostException
    {
        DatabaseDescriptor.setPaxosVariant(Config.PaxosVariant.v2);

        Location dc1 = new Location("dc1", "rack1");
        Location dc2 = new Location("dc2", "rack1");
        Location sat1 = new Location("sat1", "rack1");
        Location sat2 = new Location("sat2", "rack1");

        addToken(100, "10.0.0.10", dc1);
        addToken(200, "10.0.0.11", dc1);
        addToken(300, "10.0.0.12", dc1);

        addToken(400, "10.1.0.10", dc2);
        addToken(500, "10.1.0.11", dc2);
        addToken(600, "10.1.0.12", dc2);

        addToken(700, "10.2.0.10", sat1);
        addToken(800, "10.2.0.11", sat1);
        addToken(1100, "10.2.0.12", sat1);

        addToken(900, "10.3.0.10", sat2);
        addToken(1000, "10.3.0.11", sat2);
        addToken(1200, "10.3.0.12", sat2);
    }

    private void createDualDCKeyspace() throws Exception
    {
        String cql = "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '3/3', " +
                     "'dc2': '3', " +
                     "'dc2.satellite.sat2': '3/3', " +
                     "'primary': 'dc1'" +
                     "} AND replication_type = 'tracked'";
        ClusterMetadataTestHelper.createKeyspace(cql);
    }

    private SatelliteReplicationStrategy getSRS()
    {
        KeyspaceMetadata ksm = ClusterMetadata.current().schema.getKeyspaces().getNullable(KEYSPACE);
        return (SatelliteReplicationStrategy) ksm.replicationStrategy;
    }

    private SatelliteReplicationStrategy.SatelliteCommitPlan createPlan() throws Exception
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        SatelliteReplicationStrategy srs = getSRS();
        Keyspace keyspace = Keyspace.mockKS(metadata.schema.getKeyspaces().getNullable(KEYSPACE));
        return srs.createSatelliteCommitPlan(metadata, keyspace, TOKEN);
    }

    private Set<String> collectDCs(AbstractReplicaCollection<?> endpoints)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        Set<String> dcs = new HashSet<>();
        ReplicaList epList = endpoints.list;
        for (int i = 0; i < endpoints.size(); i++)
            dcs.add(metadata.locator.location(epList.get(i).endpoint()).datacenter);
        return dcs;
    }

    private void assertExpectedDCs(Set<String> dcs)
    {
        assertTrue("Should include sat1 (primary's satellite)", dcs.contains("sat1"));
        assertTrue("Should include dc2 (other full DC)", dcs.contains("dc2"));
        assertFalse("Should NOT include sat2 (dc2's satellite, not primary's)", dcs.contains("sat2"));
        assertFalse("Should NOT include dc1 (primary, handled by paxos)", dcs.contains("dc1"));
    }

    /**
     * Both createSatelliteCommitPlan and paxosParticipants should include only
     * the primary DC's satellite (sat1) and other full DCs (dc2), excluding
     * the primary DC itself (dc1) and non-primary satellites (sat2).
     */
    @Test
    public void testEndpointDCSelection() throws Exception
    {
        setupTopology();
        createDualDCKeyspace();

        // check commit plan endpoint selection
        SatelliteReplicationStrategy.SatelliteCommitPlan plan = createPlan();
        assertExpectedDCs(collectDCs(plan.liveEndpoints));

        // check paxos participant endpoint selection
        ClusterMetadata metadata = ClusterMetadata.current();
        SatelliteReplicationStrategy srs = getSRS();
        TableMetadata table = TableMetadata.builder(KEYSPACE, "test_table")
                                           .addPartitionKeyColumn("key", AsciiType.instance)
                                           .build();

        Paxos.Participants participants = srs.paxosParticipants(metadata, table,
                                                                 TOKEN,
                                                                 ConsistencyLevel.SERIAL,
                                                                 r -> true);

        assertTrue(participants instanceof SatellitePaxosParticipants);
        SatellitePaxosParticipants spp = (SatellitePaxosParticipants) participants;
        assertExpectedDCs(collectDCs(spp.getAdditionalSummaryEndpoints()));
    }

    /**
     * The tracker should not be complete with only the pre-completed primary DC,
     * but should complete once a quorum of groups has responded (dc1 pre-completed + sat1).
     */
    @Test
    public void testTrackerCompletesWithQuorumOfGroups() throws Exception
    {
        setupTopology();
        createDualDCKeyspace();

        SatelliteReplicationStrategy.SatelliteCommitPlan plan = createPlan();
        ClusterMetadata metadata = ClusterMetadata.current();

        assertFalse("Tracker should not be complete with only primary DC pre-completed", plan.tracker.isComplete());

        // meet quorum in sat1
        for (int i = 0; i < plan.liveEndpoints.size(); i++)
        {
            InetAddressAndPort ep = plan.liveEndpoints.endpoint(i);
            if (metadata.locator.location(ep).datacenter.equals("sat1"))
                plan.tracker.onResponse(ep);
        }

        assertTrue("Should be complete with quorum of groups", plan.tracker.isComplete());
        assertTrue("Should be successful", plan.tracker.isSuccessful());
    }
}
