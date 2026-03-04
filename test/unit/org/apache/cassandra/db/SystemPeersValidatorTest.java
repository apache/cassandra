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
import java.util.UUID;

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

public class SystemPeersValidatorTest
{
    private CMSTestBase.CMSSut sut;
    private InetAddressAndPort peerEndpoint;

    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
    }

    @Before
    public void before() throws Exception
    {
        ClusterMetadataService.unsetInstance();
        sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false,
                                     new TokenPlacementModel.SimpleReplicationFactor(3));
        ClusterMetadataTestHelper.register(2);
        ClusterMetadataTestHelper.join(2, 2);
        peerEndpoint = ClusterMetadata.current().directory.endpoint(ClusterMetadataTestHelper.nodeId(2));
        cleanupPeersTables();
    }

    @After
    public void after()
    {
        if (sut != null)
            sut.close();
    }

    @Test
    public void testNoRepairWhenTablesAreConsistent()
    {
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertEquals("peers_v2 should have 1 peer", 1, countEntries(PEERS_V2));
        assertEquals("legacy peers should have 1 peer", 1, countEntries(LEGACY_PEERS));

        // Second call should be a no-op
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertEquals("peers_v2 count should not change on second call", 1, countEntries(PEERS_V2));
        assertEquals("legacy peers count should not change on second call", 1, countEntries(LEGACY_PEERS));
    }

    @Test
    public void testStaleEntriesAreRemovedFromBothTables() throws UnknownHostException
    {
        InetAddressAndPort staleEndpoint = InetAddressAndPort.getByName("127.0.0.99");
        insertStalePeerEntry(staleEndpoint, PEERS_V2);
        insertStalePeerEntry(staleEndpoint, LEGACY_PEERS);

        assertTrue(entryExistsInPeersV2(staleEndpoint));
        assertTrue(entryExistsInLegacyPeers(staleEndpoint.getAddress()));

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertFalse("Stale entry should be removed from peers_v2", entryExistsInPeersV2(staleEndpoint));
        assertFalse("Stale entry should be removed from legacy peers", entryExistsInLegacyPeers(staleEndpoint.getAddress()));
    }

    @Test
    public void testStaleEntryOnlyInPeersV2IsRemoved() throws UnknownHostException
    {
        InetAddressAndPort staleEndpoint = InetAddressAndPort.getByName("127.0.0.99");
        insertStalePeerEntry(staleEndpoint, PEERS_V2);

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertFalse("Stale entry should be removed from peers_v2", entryExistsInPeersV2(staleEndpoint));
    }

    @Test
    public void testStaleEntryOnlyInLegacyPeersIsRemoved() throws UnknownHostException
    {
        InetAddressAndPort staleEndpoint = InetAddressAndPort.getByName("127.0.0.99");
        insertStalePeerEntry(staleEndpoint, LEGACY_PEERS);

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertFalse("Stale entry should be removed from legacy peers", entryExistsInLegacyPeers(staleEndpoint.getAddress()));
    }

    @Test
    public void testMissingPeerIsAddedToBothTables()
    {
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());
        removePeerEntry(peerEndpoint);

        assertFalse(entryExistsInPeersV2(peerEndpoint));
        assertFalse(entryExistsInLegacyPeers(peerEndpoint.getAddress()));

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertTrue("Missing peer should be added to peers_v2", entryExistsInPeersV2(peerEndpoint));
        assertTrue("Missing peer should be added to legacy peers", entryExistsInLegacyPeers(peerEndpoint.getAddress()));
    }

    @Test
    public void testStaleFieldInPeersV2IsRepaired()
    {
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        executeInternal(String.format("UPDATE %s.%s SET data_center = 'stale-dc' WHERE peer = ? AND peer_port = ?",
                                      SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2),
                        peerEndpoint.getAddress(), peerEndpoint.getPort());

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        String expectedDc = ClusterMetadata.current().directory.location(ClusterMetadataTestHelper.nodeId(2)).datacenter;
        assertEquals("data_center in peers_v2 should match ClusterMetadata",
                     expectedDc, getDataCenter(peerEndpoint, PEERS_V2));
    }

    @Test
    public void testStaleFieldInLegacyPeersIsRepaired()
    {
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        executeInternal(String.format("UPDATE %s.%s SET data_center = 'stale-dc' WHERE peer = ?",
                                      SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS),
                        peerEndpoint.getAddress());

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        String expectedDc = ClusterMetadata.current().directory.location(ClusterMetadataTestHelper.nodeId(2)).datacenter;
        assertEquals("data_center in legacy peers should match ClusterMetadata",
                     expectedDc, getDataCenter(peerEndpoint, LEGACY_PEERS));
    }

    @Test
    public void testJmxEntryPointRepairsMissingPeer()
    {
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());
        removePeerEntry(peerEndpoint);

        StorageService.instance.validateAndRepairPeersMetadata();

        assertTrue("JMX repair should restore peer in peers_v2", entryExistsInPeersV2(peerEndpoint));
        assertTrue("JMX repair should restore peer in legacy peers", entryExistsInLegacyPeers(peerEndpoint.getAddress()));
    }

    private void cleanupPeersTables()
    {
        executeInternal(String.format("TRUNCATE %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2));
        executeInternal(String.format("TRUNCATE %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS));
    }

    private int countEntries(String table)
    {
        UntypedResultSet result = executeInternal(String.format("SELECT COUNT(*) FROM %s.%s",
                                                                SchemaConstants.SYSTEM_KEYSPACE_NAME, table));
        return (int) result.one().getLong("count");
    }

    private boolean entryExistsInPeersV2(InetAddressAndPort endpoint)
    {
        UntypedResultSet result = executeInternal(
            String.format("SELECT peer FROM %s.%s WHERE peer = ? AND peer_port = ?",
                          SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2),
            endpoint.getAddress(), endpoint.getPort());
        return !result.isEmpty();
    }

    private boolean entryExistsInLegacyPeers(InetAddress address)
    {
        UntypedResultSet result = executeInternal(
            String.format("SELECT peer FROM %s.%s WHERE peer = ?",
                          SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS),
            address);
        return !result.isEmpty();
    }

    private String getDataCenter(InetAddressAndPort endpoint, String table)
    {
        UntypedResultSet result;
        if (table.equals(PEERS_V2))
            result = executeInternal(
                String.format("SELECT data_center FROM %s.%s WHERE peer = ? AND peer_port = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME, table),
                endpoint.getAddress(), endpoint.getPort());
        else
            result = executeInternal(
                String.format("SELECT data_center FROM %s.%s WHERE peer = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME, table),
                endpoint.getAddress());
        return result.one().getString("data_center");
    }

    private void insertStalePeerEntry(InetAddressAndPort endpoint, String table)
    {
        if (table.equals(PEERS_V2))
            executeInternal(
                String.format("INSERT INTO %s.%s (peer, peer_port, data_center, host_id, rack, release_version, tokens) " +
                              "VALUES (?, ?, ?, ?, ?, ?, ?)",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME, table),
                endpoint.getAddress(), endpoint.getPort(), "dc1", UUID.randomUUID(), "rack1", "5.0.0", new HashSet<String>());
        else
            executeInternal(
                String.format("INSERT INTO %s.%s (peer, data_center, host_id, rack, release_version, tokens) " +
                              "VALUES (?, ?, ?, ?, ?, ?)",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME, table),
                endpoint.getAddress(), "dc1", UUID.randomUUID(), "rack1", "5.0.0", new HashSet<String>());
    }

    private void removePeerEntry(InetAddressAndPort endpoint)
    {
        executeInternal(
            String.format("DELETE FROM %s.%s WHERE peer = ? AND peer_port = ?",
                      SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2),
            endpoint.getAddress(), endpoint.getPort());

        executeInternal(
            String.format("DELETE FROM %s.%s WHERE peer = ?",
                      SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS), endpoint.getAddress());
    }
}