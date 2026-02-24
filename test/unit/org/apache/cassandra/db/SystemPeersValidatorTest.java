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
package org.apache.cassandra.db;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.test.log.CMSTestBase;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.LEGACY_PEERS;
import static org.apache.cassandra.db.SystemKeyspace.PEERS_V2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for SystemPeersValidator to verify it correctly validates and repairs
 * inconsistencies between system.peers/peers_v2 tables and ClusterMetadata.
 */
public class SystemPeersValidatorTest
{
    private CMSTestBase.CMSSut sut;

    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
    }

    @Before
    public void before() throws Exception
    {
        ClusterMetadataService.unsetInstance();
        sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, new TokenPlacementModel.SimpleReplicationFactor(3));
        cleanupPeersTables();
    }

    @After
    public void after()
    {
        if (sut != null)
            sut.close();
    }

    @Test
    public void testValidationWithConsistentTables()
    {
        ClusterMetadataTestHelper.register(1);
        ClusterMetadataTestHelper.join(1, 1);
        ClusterMetadataTestHelper.register(2);
        ClusterMetadataTestHelper.join(2, 2);
        ClusterMetadataTestHelper.register(3);
        ClusterMetadataTestHelper.join(3, 3);

        ClusterMetadata metadata = ClusterMetadata.current();
        SystemPeersValidator.validateAndRepair(metadata);

        int peersV2CountBefore = countPeersV2Entries();
        int legacyPeersCountBefore = countLegacyPeersEntries();

        SystemPeersValidator.validateAndRepair(metadata);

        int peersV2CountAfter = countPeersV2Entries();
        int legacyPeersCountAfter = countLegacyPeersEntries();

        assertEquals("peers_v2 count should not change", peersV2CountBefore, peersV2CountAfter);
        assertEquals("legacy peers count should not change", legacyPeersCountBefore, legacyPeersCountAfter);
    }

    @Test
    public void testValidationRemovesExtraEntries() throws UnknownHostException
    {
        ClusterMetadataTestHelper.register(1);
        ClusterMetadataTestHelper.join(1, 1);
        ClusterMetadataTestHelper.register(2);
        ClusterMetadataTestHelper.join(2, 2);

        InetAddressAndPort staleEndpoint1 = InetAddressAndPort.getByName("127.0.0.97");
        InetAddressAndPort staleEndpoint2 = InetAddressAndPort.getByName("127.0.0.98");
        InetAddressAndPort staleEndpoint3 = InetAddressAndPort.getByName("127.0.0.99");
        addStalePeerEntry(staleEndpoint1);
        addStalePeerEntry(staleEndpoint2);
        addStalePeerEntry(staleEndpoint3);

        assertTrue("Stale entry 1 should exist in peers_v2", entryExistsInPeersV2(staleEndpoint1));
        assertTrue("Stale entry 1 should exist in legacy peers", entryExistsInLegacyPeers(staleEndpoint1.getAddress()));
        assertTrue("Stale entry 2 should exist in peers_v2", entryExistsInPeersV2(staleEndpoint2));
        assertTrue("Stale entry 2 should exist in legacy peers", entryExistsInLegacyPeers(staleEndpoint2.getAddress()));
        assertTrue("Stale entry 3 should exist in peers_v2", entryExistsInPeersV2(staleEndpoint3));
        assertTrue("Stale entry 3 should exist in legacy peers", entryExistsInLegacyPeers(staleEndpoint3.getAddress()));

        ClusterMetadata metadata = ClusterMetadata.current();

        SystemPeersValidator.validateAndRepair(metadata);

        assertFalse("Stale entry 1 should be removed from peers_v2", entryExistsInPeersV2(staleEndpoint1));
        assertFalse("Stale entry 1 should be removed from legacy peers", entryExistsInLegacyPeers(staleEndpoint1.getAddress()));
        assertFalse("Stale entry 2 should be removed from peers_v2", entryExistsInPeersV2(staleEndpoint2));
        assertFalse("Stale entry 2 should be removed from legacy peers", entryExistsInLegacyPeers(staleEndpoint2.getAddress()));
        assertFalse("Stale entry 3 should be removed from peers_v2", entryExistsInPeersV2(staleEndpoint3));
        assertFalse("Stale entry 3 should be removed from legacy peers", entryExistsInLegacyPeers(staleEndpoint3.getAddress()));
    }

    @Test
    public void testValidationAddsMissingEntries()
    {
        ClusterMetadataTestHelper.register(1);
        ClusterMetadataTestHelper.join(1, 1);
        ClusterMetadataTestHelper.register(2);
        ClusterMetadataTestHelper.join(2, 2);

        ClusterMetadata metadata = ClusterMetadata.current();

        InetAddressAndPort peerEndpoint = metadata.directory.endpoint(ClusterMetadataTestHelper.nodeId(2));
        removePeerEntry(peerEndpoint);

        assertFalse("Entry should be missing from peers_v2", entryExistsInPeersV2(peerEndpoint));
        assertFalse("Entry should be missing from legacy peers", entryExistsInLegacyPeers(peerEndpoint.getAddress()));

        SystemPeersValidator.validateAndRepair(metadata);

        assertTrue("Entry should be added back to peers_v2", entryExistsInPeersV2(peerEndpoint));
        assertTrue("Entry should be added back to legacy peers", entryExistsInLegacyPeers(peerEndpoint.getAddress()));
    }

    @Test
    public void testValidationRepairsMissingFromPeersV2Only()
    {
        ClusterMetadataTestHelper.register(1);
        ClusterMetadataTestHelper.join(1, 1);
        ClusterMetadataTestHelper.register(2);
        ClusterMetadataTestHelper.join(2, 2);

        ClusterMetadata metadata = ClusterMetadata.current();

        InetAddressAndPort peerEndpoint = metadata.directory.endpoint(ClusterMetadataTestHelper.nodeId(2));

        // Populate both tables via the validator, then remove only the peers_v2 entry
        SystemPeersValidator.validateAndRepair(metadata);
        removePeersV2Entry(peerEndpoint);

        assertTrue("Entry should still exist in legacy peers", entryExistsInLegacyPeers(peerEndpoint.getAddress()));
        assertFalse("Entry should be missing from peers_v2", entryExistsInPeersV2(peerEndpoint));

        SystemPeersValidator.validateAndRepair(metadata);

        assertTrue("Entry should be restored in peers_v2", entryExistsInPeersV2(peerEndpoint));
        assertTrue("Entry should still exist in legacy peers", entryExistsInLegacyPeers(peerEndpoint.getAddress()));
    }

    @Test
    public void testValidationRepairsMissingFromLegacyPeersOnly()
    {
        ClusterMetadataTestHelper.register(1);
        ClusterMetadataTestHelper.join(1, 1);
        ClusterMetadataTestHelper.register(2);
        ClusterMetadataTestHelper.join(2, 2);

        ClusterMetadata metadata = ClusterMetadata.current();

        InetAddressAndPort peerEndpoint = metadata.directory.endpoint(ClusterMetadataTestHelper.nodeId(2));

        // Populate both tables via the validator, then remove only the legacy peers entry
        SystemPeersValidator.validateAndRepair(metadata);
        removeLegacyPeersEntry(peerEndpoint);

        assertTrue("Entry should still exist in peers_v2", entryExistsInPeersV2(peerEndpoint));
        assertFalse("Entry should be missing from legacy peers", entryExistsInLegacyPeers(peerEndpoint.getAddress()));

        SystemPeersValidator.validateAndRepair(metadata);

        assertTrue("Entry should be restored in legacy peers", entryExistsInLegacyPeers(peerEndpoint.getAddress()));
        assertTrue("Entry should still exist in peers_v2", entryExistsInPeersV2(peerEndpoint));
    }

    @Test
    public void testValidationWithNullMetadata()
    {
        SystemPeersValidator.validateAndRepair(null);
    }

    @Test
    public void testValidationWithSingleNode()
    {
        ClusterMetadataTestHelper.register(1);
        ClusterMetadataTestHelper.join(1, 1);

        ClusterMetadata metadata = ClusterMetadata.current();

        SystemPeersValidator.validateAndRepair(metadata);

        assertEquals("peers_v2 should be empty for single node", 0, countPeersV2Entries());
        assertEquals("legacy peers should be empty for single node", 0, countLegacyPeersEntries());
    }

    @Test
    public void testJmxEntryPoint()
    {
        ClusterMetadataTestHelper.register(1);
        ClusterMetadataTestHelper.join(1, 1);
        ClusterMetadataTestHelper.register(2);
        ClusterMetadataTestHelper.join(2, 2);
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());
        ClusterMetadata metadata = ClusterMetadata.current();
        InetAddressAndPort peer = metadata.directory.endpoint(ClusterMetadataTestHelper.nodeId(2));
        removePeerEntry(peer);

        StorageService.instance.validateAndRepairPeersMetadata();

        assertTrue(entryExistsInPeersV2(peer));
    }

    private void cleanupPeersTables()
    {
        executeInternal(String.format("TRUNCATE %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2));
        executeInternal(String.format("TRUNCATE %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS));
    }

    private int countPeersV2Entries()
    {
        String query = String.format("SELECT COUNT(*) FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2);
        UntypedResultSet result = executeInternal(query);
        return (int) result.one().getLong("count");
    }

    private int countLegacyPeersEntries()
    {
        String query = String.format("SELECT COUNT(*) FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS);
        UntypedResultSet result = executeInternal(query);
        return (int) result.one().getLong("count");
    }

    private boolean entryExistsInPeersV2(InetAddressAndPort endpoint)
    {
        String query = String.format("SELECT peer FROM %s.%s WHERE peer = ? AND peer_port = ?",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2);
        UntypedResultSet result = executeInternal(query, endpoint.getAddress(), endpoint.getPort());
        return !result.isEmpty();
    }

    private boolean entryExistsInLegacyPeers(InetAddress address)
    {
        String query = String.format("SELECT peer FROM %s.%s WHERE peer = ?",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS);
        UntypedResultSet result = executeInternal(query, address);
        return !result.isEmpty();
    }

    private void addStalePeerEntry(InetAddressAndPort endpoint)
    {
        String queryV2 = String.format("INSERT INTO %s.%s (peer, peer_port, data_center, host_id, rack, release_version, tokens) " +
                                       "VALUES (?, ?, ?, ?, ?, ?, ?)",
                                       SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2);
        executeInternal(queryV2,
                        endpoint.getAddress(),
                        endpoint.getPort(),
                        "dc1",
                        java.util.UUID.randomUUID(),
                        "rack1",
                        "5.0.0",
                        new HashSet<String>());

        String queryLegacy = String.format("INSERT INTO %s.%s (peer, data_center, host_id, rack, release_version, tokens) " +
                                           "VALUES (?, ?, ?, ?, ?, ?)",
                                           SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS);
        executeInternal(queryLegacy,
                        endpoint.getAddress(),
                        "dc1",
                        java.util.UUID.randomUUID(),
                        "rack1",
                        "5.0.0",
                        new HashSet<String>());
    }

    private void removePeerEntry(InetAddressAndPort endpoint)
    {
        removePeersV2Entry(endpoint);
        removeLegacyPeersEntry(endpoint);
    }

    private void removePeersV2Entry(InetAddressAndPort endpoint)
    {
        String query = String.format("DELETE FROM %s.%s WHERE peer = ? AND peer_port = ?",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2);
        executeInternal(query, endpoint.getAddress(), endpoint.getPort());
    }

    private void removeLegacyPeersEntry(InetAddressAndPort endpoint)
    {
        String query = String.format("DELETE FROM %s.%s WHERE peer = ?",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS);
        executeInternal(query, endpoint.getAddress());
    }
}