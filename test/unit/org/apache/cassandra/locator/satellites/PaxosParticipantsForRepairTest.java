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
package org.apache.cassandra.locator.satellites;

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SatelliteReplicationStrategy;
import org.apache.cassandra.locator.SatelliteReplicationStrategyTestBase;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.transformations.AlterSchema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link SatelliteReplicationStrategy#paxosParticipantsForRepair}.
 */
public class PaxosParticipantsForRepairTest extends SatelliteReplicationStrategyTestBase
{
    private static final Token TOKEN = new LongToken(150);

    @Test
    public void testDelegatesWhenNotInTransitionAck() throws Exception
    {
        createDualDCKeyspace("dc1");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();
        TableMetadata table = tableMetadata(DUAL_DC_KEYSPACE);

        // In NORMAL state, paxosParticipantsForRepair should delegate to paxosParticipants
        Paxos.Participants participants = strategy.paxosParticipantsForRepair(
            metadata, table, TOKEN, ConsistencyLevel.LOCAL_SERIAL, FailureDetector.isReplicaAlive);

        assertNotNull(participants);
        // Electorate should include dc1 nodes (current primary)
        Set<InetAddressAndPort> dc1Endpoints = replicasInDC(strategy.calculateNaturalReplicas(TOKEN, metadata), "dc1", metadata);
        for (InetAddressAndPort ep : dc1Endpoints)
            assertTrue("dc1 endpoint should be in electorate", containsEndpoint(participants, ep));
    }

    @Test
    public void testReturnsOldPrimaryDuringTransitionAck() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();
        TableMetadata table = tableMetadata(DUAL_DC_KEYSPACE);

        // Verify we're in TRANSITION_ACK
        SatelliteFailover.Info info = strategy.getFailoverInfo(metadata);
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, info.stateForToken(TOKEN));
        assertEquals("dc1", info.getFromDC());

        // paxosParticipantsForRepair should return dc1 (old primary) electorate, not throw
        Paxos.Participants participants = strategy.paxosParticipantsForRepair(
            metadata, table, TOKEN, ConsistencyLevel.LOCAL_SERIAL, FailureDetector.isReplicaAlive);

        assertNotNull(participants);

        // Electorate should be dc1 nodes only (old primary), not dc2 (new primary)
        Set<InetAddressAndPort> dc1Endpoints = replicasInDC(strategy.calculateNaturalReplicas(TOKEN, metadata), "dc1", metadata);
        Set<InetAddressAndPort> dc2Endpoints = replicasInDC(strategy.calculateNaturalReplicas(TOKEN, metadata), "dc2", metadata);

        for (InetAddressAndPort ep : dc1Endpoints)
            assertTrue("dc1 endpoint should be in repair electorate", containsEndpoint(participants, ep));

        for (InetAddressAndPort ep : dc2Endpoints)
            assertFalse("dc2 endpoint should NOT be in repair electorate", containsEndpoint(participants, ep));
    }

    @Test
    public void testSatelliteExcludedFromRepairElectorate() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();
        TableMetadata table = tableMetadata(DUAL_DC_KEYSPACE);

        Paxos.Participants participants = strategy.paxosParticipantsForRepair(
            metadata, table, TOKEN, ConsistencyLevel.LOCAL_SERIAL, FailureDetector.isReplicaAlive);

        // Satellite endpoints (sat1, sat2) should not be in the electorate
        Set<InetAddressAndPort> sat1Endpoints = replicasInDC(strategy.calculateNaturalReplicas(TOKEN, metadata), "sat1", metadata);
        if (sat1Endpoints != null)
        {
            for (InetAddressAndPort ep : sat1Endpoints)
                assertFalse("sat1 endpoint should NOT be in repair electorate", containsEndpoint(participants, ep));
        }
    }

    @Test
    public void testNormalPaxosParticipantsThrowsDuringTransitionAck() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();
        TableMetadata table = tableMetadata(DUAL_DC_KEYSPACE);

        // Normal paxosParticipants should throw during TRANSITION_ACK
        try
        {
            strategy.paxosParticipants(metadata, table, TOKEN, ConsistencyLevel.LOCAL_SERIAL, FailureDetector.isReplicaAlive);
            fail("Expected UnavailableException during TRANSITION_ACK");
        }
        catch (UnavailableException e)
        {
            // expected
        }

        // paxosParticipantsForRepair should NOT throw
        Paxos.Participants participants = strategy.paxosParticipantsForRepair(
            metadata, table, TOKEN, ConsistencyLevel.LOCAL_SERIAL, FailureDetector.isReplicaAlive);
        assertNotNull(participants);
    }

    private boolean containsEndpoint(Paxos.Participants participants, InetAddressAndPort endpoint)
    {
        for (Replica r : participants.readCandidates())
        {
            if (r.endpoint().equals(endpoint))
                return true;
        }
        return false;
    }

    private TableMetadata tableMetadata(String keyspace)
    {
        return TableMetadata.builder(keyspace, "test_table")
                            .addPartitionKeyColumn("key", org.apache.cassandra.db.marshal.AsciiType.instance)
                            .build();
    }

    private void alterKeyspacePrimary(String keyspace, String newPrimary) throws Exception
    {
        String cql = "ALTER KEYSPACE " + keyspace + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '3/3', " +
                     "'dc2': '3', " +
                     "'dc2.satellite.sat2': '3/3', " +
                     "'primary': '" + newPrimary + "'" +
                     "} AND replication_type = 'tracked'";
        AlterSchemaStatement stmt = (AlterSchemaStatement) QueryProcessor.parseStatement(cql)
            .prepare(ClientState.forInternalCalls());
        ClusterMetadataTestHelper.commit(new AlterSchema(stmt));
    }
}
