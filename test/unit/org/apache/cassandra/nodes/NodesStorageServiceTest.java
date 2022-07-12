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
package org.apache.cassandra.nodes;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.SystemViewsKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualSchemaKeyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.virtual.NodeConstants;
import org.apache.cassandra.nodes.virtual.NodesSystemViews;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.nodes.Nodes.local;
import static org.apache.cassandra.nodes.Nodes.nodes;
import static org.apache.cassandra.nodes.Nodes.peers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NodesStorageServiceTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        VirtualKeyspaceRegistry.instance.register(VirtualSchemaKeyspace.instance);
        VirtualKeyspaceRegistry.instance.register(SystemViewsKeyspace.instance);
        CommitLog.instance.stopUnsafe(true);
        CommitLog.instance.start();
        SchemaKeyspace.saveSystemKeyspacesSchema();
    }

    @AfterClass
    public static void teardown()
    {
        nodes().resetUnsafe();
    }

    @Before
    public void reset()
    {
        nodes().resetUnsafe();
    }

    @Test
    public void testGetPreferred() throws UnknownHostException
    {
        final InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.2");
        final InetAddressAndPort preferred = InetAddressAndPort.getByName("127.0.0.3");

        assertFalse(peers().hasPreferred(peer));
        assertEquals(peer, peers().getPreferred(peer));

        peers().update(peer, p -> p.setPreferred(preferred));
        assertTrue(peers().hasPreferred(peer));
        assertEquals(preferred, peers().getPreferred(peer));
    }

    @Test
    public void testLocalTokens()
    {
        // Remove all existing tokens
        Collection<Token> current = peers().getTokens().get(FBUtilities.getLocalAddressAndPort());
        if (current != null && !current.isEmpty())
            StorageService.instance.updateTokenMetadata(FBUtilities.getLocalAddressAndPort(), current);

        List<Token> tokens = new ArrayList<Token>()
        {{
            for (int i = 0; i < 9; i++)
                add(DatabaseDescriptor.getPartitioner().getRandomToken());
        }};

        local().updateTokens(tokens);
        int count = 0;

        for (Token tok : local().getSavedTokens())
            assertEquals(tok, tokens.get(count++));
    }

    @Test
    public void testNonLocalToken() throws UnknownHostException
    {
        Murmur3Partitioner.LongToken token = new Murmur3Partitioner.LongToken(3);
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.2");

        nodes().update(address, Collections.singletonList(token), NodeInfo::setTokens);
        assert peers().getTokens().get(address).contains(token);

        StorageService.instance.removeEndpoint(address);
        assert !peers().getTokens().containsValue(token);
    }

    @Test
    public void testLocalHostID()
    {
        UUID firstId = Nodes.local().get().getHostId();
        UUID secondId = StorageService.instance.getLocalHostUUID();
        assertEquals(firstId, secondId);

        UUID anotherId = UUID.randomUUID();
        local().update(anotherId, NodeInfo::setHostId, true);
        assertEquals(anotherId, StorageService.instance.getLocalHostUUID());
    }

    @Test
    public void testUpdateDcAndRacks() throws UnknownHostException
    {
        final InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.2");

        final List<Pair<String, String>> dcRacks = new ArrayList<>(2);
        dcRacks.add(Pair.create("dc1", "rack1"));
        dcRacks.add(Pair.create("dc2", "rack2"));

        assertEquals(0, peers().getDcRackInfo().size());

        for (Pair<String, String> pair : dcRacks)
        {
            peers().update(peer, p -> {
                p.setRack(pair.right);
                p.setDataCenter(pair.left);
            });

            Map<InetAddressAndPort, Map<String, String>> ret = peers().getDcRackInfo();
            assertEquals(1, ret.size());

            assertEquals(pair.left, ret.get(peer).get("data_center"));
            assertEquals(pair.right, ret.get(peer).get("rack"));
        }

        StorageService.instance.removeEndpoint(peer);
        assertEquals(0, peers().getDcRackInfo().size());
    }

    @Test
    public void testUpdateSchemaVersion()
    {
        testUpdateLocalInfo("schema_version", UUID.randomUUID(), NodeInfo::setSchemaVersion);
    }

    @Test
    public void testLocalBootstrapState()
    {
        testUpdateLocalInfo("bootstrapped", BootstrapState.IN_PROGRESS, LocalInfo::setBootstrapState);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testLocalTruncationRecords()
    {
        testUpdateLocalInfo("truncated_at", Collections.singletonMap(UUID.randomUUID(), null), LocalInfo::setTruncationRecords);
    }

    @Test
    public void testRemoteSchemaVersion() throws UnknownHostException
    {
        final InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.2");
        testUpdatePeerInfo(peer, "schema_version", UUID.randomUUID(), NodeInfo::setSchemaVersion);
    }

    private void fakeLocal(LocalInfo l)
    {
        l.setClusterName("NodesTest");
        l.setDataCenter("dataCenter");
        l.setRack("rack");
        l.setNativeProtocolVersion("66");
        l.setBootstrapState(BootstrapState.COMPLETED);
        l.setHostId(UUID.randomUUID());
        l.setTokens(Arrays.asList(DatabaseDescriptor.getPartitioner().getRandomToken(),
                                  DatabaseDescriptor.getPartitioner().getRandomToken(),
                                  DatabaseDescriptor.getPartitioner().getRandomToken(),
                                  DatabaseDescriptor.getPartitioner().getRandomToken()));
        Map<UUID, LocalInfo.TruncationRecord> truncationRecords = new HashMap<>();
        truncationRecords.put(UUID.randomUUID(), new LocalInfo.TruncationRecord(new CommitLogPosition(1234, 123), 12345));
        truncationRecords.put(UUID.randomUUID(), new LocalInfo.TruncationRecord(new CommitLogPosition(5678, 567), 56789));
        truncationRecords.put(UUID.randomUUID(), new LocalInfo.TruncationRecord(new CommitLogPosition(1, 2), 3));
        l.setBroadcastAddressAndPort(InetAddressAndPort.getByAddress(InetAddress.getLoopbackAddress()));
    }

    private <T> void testUpdateLocalInfo(String columnName, T value, BiConsumer<LocalInfo, T> updater)
    {
        UntypedResultSet ret = loadLocalInfo(columnName);
        assertNotNull(ret);
        assertEquals(1, ret.size());
        if ("bootstrapped".equals(columnName))
        {
            // LocalInfo.bootstrapState is initialized to NEEDS_BOOTSTRAP
            assertTrue(ret.one().has(columnName));
        }
        else
        {
            assertFalse(ret.one().has(columnName));
        }

        local().update(value, updater, false);

        ret = loadLocalInfo(columnName);
        assertRowValue(columnName, value, ret);
    }

    private <T> void testUpdatePeerInfo(InetAddressAndPort peer, String columnName, T value, BiConsumer<PeerInfo, T> updater)
    {
        UntypedResultSet ret = loadPeerInfo(peer, columnName);
        assertNotNull(ret);
        assertEquals(0, ret.size());

        peers().update(peer, value, updater);

        ret = loadPeerInfo(peer, columnName);
        assertRowValue(columnName, value, ret);

        peers().remove(peer);

        ret = loadPeerInfo(peer, columnName);
        assertNotNull(ret);
        assertEquals(0, ret.size());
    }

    private <T> void assertRowValue(String columnName, T value, UntypedResultSet ret)
    {
        assertNotNull(ret);
        assertEquals(1, ret.size());

        UntypedResultSet.Row row = ret.one();
        assertTrue(row.has(columnName));

        if (value instanceof String)
            assertEquals(value, row.getString(columnName));
        else if (value instanceof UUID)
            assertEquals(value, row.getUUID(columnName));
        else if (value instanceof Set)
            assertEquals(value, row.getSet(columnName, UTF8Type.instance));
        else if (value instanceof BootstrapState)
            assertEquals(value, BootstrapState.valueOf(row.getString(columnName)));
        else
            fail("Unsupported value type " + value.getClass().getName() + " / " + value);
    }

    private static UntypedResultSet loadLocalInfo(String columnName)
    {
        String req = "SELECT %s FROM %s.%s";
        return executeInternal(format(req, columnName, SchemaConstants.SYSTEM_KEYSPACE_NAME, NodeConstants.LOCAL));
    }

    private static UntypedResultSet loadPeerInfo(InetAddressAndPort ep, String columnName)
    {
        String req = "SELECT %s FROM %s.%s WHERE peer = '%s'";
        return executeInternal(format(req, columnName, SchemaConstants.SYSTEM_KEYSPACE_NAME, NodeConstants.PEERS_V2, ep.address.getHostAddress()));
    }
}
