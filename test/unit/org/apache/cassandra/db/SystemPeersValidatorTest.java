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
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.distributed.test.log.CMSTestBase;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.LEGACY_PEERS;
import static org.apache.cassandra.db.SystemKeyspace.PEERS_V2;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
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
        assertEquals("peers should have 1 peer", 1, countEntries(LEGACY_PEERS));

        // Second call should be a no-op
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertEquals("peers_v2 count should not change on second call", 1, countEntries(PEERS_V2));
        assertEquals("peers count should not change on second call", 1, countEntries(LEGACY_PEERS));
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
    public void testStaleEntryOnlyInPeersIsRemoved() throws UnknownHostException
    {
        InetAddressAndPort staleEndpoint = InetAddressAndPort.getByName("127.0.0.99");
        insertStalePeerEntry(staleEndpoint, LEGACY_PEERS);

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertFalse("Stale entry should be removed from peers", entryExistsInPeers(staleEndpoint.getAddress()));
    }

    @Test
    public void testMissingPeerIsAddedToBothTables()
    {
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());
        removePeerEntry(peerEndpoint);

        assertFalse(entryExistsInPeersV2(peerEndpoint));
        assertFalse(entryExistsInPeers(peerEndpoint.getAddress()));

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertPeersV2RowMatchesMetadata(peerEndpoint);
        assertPeersRowMatchesMetadata(peerEndpoint);
    }

    @Test
    public void testStaleFieldInPeersV2IsRepaired()
    {
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        executeInternal(String.format("UPDATE %s.%s SET data_center = 'stale-dc' WHERE peer = ? AND peer_port = ?",
                                      SYSTEM_KEYSPACE_NAME, PEERS_V2),
                        peerEndpoint.getAddress(), peerEndpoint.getPort());

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertPeersV2RowMatchesMetadata(peerEndpoint);
    }

    @Test
    public void testStaleFieldInPeersIsRepaired()
    {
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        executeInternal(String.format("UPDATE %s.%s SET data_center = 'stale-dc' WHERE peer = ?",
                                      SYSTEM_KEYSPACE_NAME, LEGACY_PEERS),
                        peerEndpoint.getAddress());

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        assertPeersRowMatchesMetadata(peerEndpoint);
    }

    @Test
    public void testNullColumnInPeersV2IsRepaired()
    {
        String[] columns = { "data_center", "host_id", "preferred_ip", "preferred_port",
                             "rack", "release_version", "native_address", "native_port",
                             "schema_version", "tokens" };

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        for (String column : columns)
        {
            executeInternal(String.format("DELETE %s FROM %s.%s WHERE peer = ? AND peer_port = ?",
                                          column, SYSTEM_KEYSPACE_NAME, PEERS_V2),
                            peerEndpoint.getAddress(), peerEndpoint.getPort());

            SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

            assertPeersV2RowMatchesMetadata(peerEndpoint);
        }
    }

    @Test
    public void testNullColumnInPeersIsRepaired()
    {
        String[] columns = { "data_center", "host_id", "preferred_ip",
                             "rack", "release_version", "rpc_address",
                             "schema_version", "tokens" };

        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

        for (String column : columns)
        {
            executeInternal(String.format("DELETE %s FROM %s.%s WHERE peer = ?",
                                          column, SYSTEM_KEYSPACE_NAME, LEGACY_PEERS),
                            peerEndpoint.getAddress());

            SystemPeersValidator.validateAndRepair(ClusterMetadata.current());

            assertPeersRowMatchesMetadata(peerEndpoint);
        }
    }

    @Test
    public void testJmxEntryPointRepairsMissingPeer()
    {
        SystemPeersValidator.validateAndRepair(ClusterMetadata.current());
        removePeerEntry(peerEndpoint);

        StorageService.instance.validateAndRepairPeersMetadata();

        assertTrue("JMX repair should restore peer in peers_v2", entryExistsInPeersV2(peerEndpoint));
        assertTrue("JMX repair should restore peer in peers", entryExistsInPeers(peerEndpoint.getAddress()));
    }

    private void cleanupPeersTables()
    {
        executeInternal(String.format("TRUNCATE %s.%s", SYSTEM_KEYSPACE_NAME, PEERS_V2));
        executeInternal(String.format("TRUNCATE %s.%s", SYSTEM_KEYSPACE_NAME, LEGACY_PEERS));
    }

    private int countEntries(String table)
    {
        UntypedResultSet result = executeInternal(String.format("SELECT COUNT(*) FROM %s.%s",
                                                                SYSTEM_KEYSPACE_NAME, table));
        return (int) result.one().getLong("count");
    }

    private boolean entryExistsInPeersV2(InetAddressAndPort endpoint)
    {
        UntypedResultSet result = executeInternal(
            String.format("SELECT peer FROM %s.%s WHERE peer = ? AND peer_port = ?",
                          SYSTEM_KEYSPACE_NAME, PEERS_V2),
            endpoint.getAddress(), endpoint.getPort());
        return !result.isEmpty();
    }

    private boolean entryExistsInPeers(InetAddress address)
    {
        UntypedResultSet result = executeInternal(
            String.format("SELECT peer FROM %s.%s WHERE peer = ?",
                          SYSTEM_KEYSPACE_NAME, LEGACY_PEERS),
            address);
        return !result.isEmpty();
    }

    private void assertPeersV2RowMatchesMetadata(InetAddressAndPort endpoint)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId nodeId = ClusterMetadataTestHelper.nodeId(2);
        NodeAddresses addresses = metadata.directory.getNodeAddresses(nodeId);
        Location location = metadata.directory.location(nodeId);

        UntypedResultSet result = executeInternal(
            String.format("SELECT * FROM %s.%s WHERE peer = ? AND peer_port = ?",
                          SYSTEM_KEYSPACE_NAME, PEERS_V2),
            endpoint.getAddress(), endpoint.getPort());
        UntypedResultSet.Row row = result.one();

        assertEquals(addresses.broadcastAddress.getAddress(), row.getInetAddress("preferred_ip"));
        assertEquals(addresses.broadcastAddress.getPort(), row.getInt("preferred_port"));
        assertEquals(addresses.nativeAddress.getAddress(), row.getInetAddress("native_address"));
        assertEquals(addresses.nativeAddress.getPort(), row.getInt("native_port"));
        assertEquals(location.datacenter, row.getString("data_center"));
        assertEquals(location.rack, row.getString("rack"));
        assertEquals(nodeId.toUUID(), row.getUUID("host_id"));
        assertEquals(metadata.directory.version(nodeId).cassandraVersion.toString(), row.getString("release_version"));
        assertEquals(metadata.schema.getVersion(), row.getUUID("schema_version"));
        assertEquals(SystemKeyspace.tokensAsSet(metadata.tokenMap.tokens(nodeId)), row.getSet("tokens", UTF8Type.instance));
    }

    private void assertPeersRowMatchesMetadata(InetAddressAndPort endpoint)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId nodeId = ClusterMetadataTestHelper.nodeId(2);
        NodeAddresses addresses = metadata.directory.getNodeAddresses(nodeId);
        Location location = metadata.directory.location(nodeId);

        UntypedResultSet result = executeInternal(
            String.format("SELECT * FROM %s.%s WHERE peer = ?",
                          SYSTEM_KEYSPACE_NAME, LEGACY_PEERS),
            endpoint.getAddress());
        UntypedResultSet.Row row = result.one();

        assertEquals(addresses.broadcastAddress.getAddress(), row.getInetAddress("preferred_ip"));
        assertEquals(addresses.nativeAddress.getAddress(), row.getInetAddress("rpc_address"));
        assertEquals(location.datacenter, row.getString("data_center"));
        assertEquals(location.rack, row.getString("rack"));
        assertEquals(nodeId.toUUID(), row.getUUID("host_id"));
        assertEquals(metadata.directory.version(nodeId).cassandraVersion.toString(), row.getString("release_version"));
        assertEquals(metadata.schema.getVersion(), row.getUUID("schema_version"));
        assertEquals(SystemKeyspace.tokensAsSet(metadata.tokenMap.tokens(nodeId)), row.getSet("tokens", UTF8Type.instance));
    }

    private void insertStalePeerEntry(InetAddressAndPort endpoint, String table)
    {
        if (table.equals(PEERS_V2))
            executeInternal(
                String.format("INSERT INTO %s.%s (peer, peer_port, data_center, host_id, rack, release_version, tokens) " +
                              "VALUES (?, ?, ?, ?, ?, ?, ?)",
                              SYSTEM_KEYSPACE_NAME, table),
                endpoint.getAddress(), endpoint.getPort(), "dc1", UUID.randomUUID(), "rack1", "5.0.0", new HashSet<String>());
        else
            executeInternal(
                String.format("INSERT INTO %s.%s (peer, data_center, host_id, rack, release_version, tokens) " +
                              "VALUES (?, ?, ?, ?, ?, ?)",
                              SYSTEM_KEYSPACE_NAME, table),
                endpoint.getAddress(), "dc1", UUID.randomUUID(), "rack1", "5.0.0", new HashSet<String>());
    }

    private void removePeerEntry(InetAddressAndPort endpoint)
    {
        executeInternal(
            String.format("DELETE FROM %s.%s WHERE peer = ? AND peer_port = ?", SYSTEM_KEYSPACE_NAME, PEERS_V2),
            endpoint.getAddress(), endpoint.getPort());

        executeInternal(
            String.format("DELETE FROM %s.%s WHERE peer = ?", SYSTEM_KEYSPACE_NAME, LEGACY_PEERS),
                endpoint.getAddress());
    }
}