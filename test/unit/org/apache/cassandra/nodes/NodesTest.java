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

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NodesTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static UUID hostId = UUID.randomUUID();

    private static Collection<Token> tokens;

    private static Map<UUID, LocalInfo.TruncationRecord> truncationRecords;

    private File dir;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.clientInitialization(false);
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        tokens = Arrays.asList(DatabaseDescriptor.getPartitioner().getRandomToken(),
                               DatabaseDescriptor.getPartitioner().getRandomToken(),
                               DatabaseDescriptor.getPartitioner().getRandomToken(),
                               DatabaseDescriptor.getPartitioner().getRandomToken());

        truncationRecords = new HashMap<>();

        truncationRecords.put(UUID.randomUUID(), new LocalInfo.TruncationRecord(new CommitLogPosition(1234, 123), 12345));
        truncationRecords.put(UUID.randomUUID(), new LocalInfo.TruncationRecord(new CommitLogPosition(5678, 567), 56789));
        truncationRecords.put(UUID.randomUUID(), new LocalInfo.TruncationRecord(new CommitLogPosition(1, 2), 3));
    }

    @Before
    public void initializeNodes() throws Throwable
    {
        dir = folder.newFolder();
        Nodes.Instance.unsafeSetup(dir.toPath());
    }

    @After
    public void shutdownNodes() throws Throwable
    {
        Nodes.nodes().shutdown();
    }

    @Test
    public void mapAndUpdateFunctionsLocalTest() throws Exception
    {
        UUID schemaVersion = UUID.randomUUID();

        assertNull(Nodes.nodes().map(FBUtilities.getBroadcastAddressAndPort(), NodeInfo::getSchemaVersion));

        assertEquals(hostId, Nodes.nodes().map(FBUtilities.getBroadcastAddressAndPort(), NodeInfo::getSchemaVersion, () -> hostId));

        Nodes.local().update(hostId, NodeInfo::setHostId, true);
        assertEquals(hostId, Nodes.local().get().getHostId());
        assertEquals(hostId, Nodes.nodes().map(FBUtilities.getBroadcastAddressAndPort(), NodeInfo::getHostId));
        assertEquals(hostId, Nodes.nodes().map(FBUtilities.getBroadcastAddressAndPort(), NodeInfo::getHostId, UUID::randomUUID));

        assertNull(Nodes.local().get().getSchemaVersion());
        Nodes.local().update(l -> l.setSchemaVersion(schemaVersion), false);
        assertEquals(schemaVersion, Nodes.local().get().getSchemaVersion());
        assertEquals(schemaVersion, Nodes.nodes().map(FBUtilities.getBroadcastAddressAndPort(), NodeInfo::getSchemaVersion));
        assertEquals(schemaVersion, Nodes.nodes().map(FBUtilities.getBroadcastAddressAndPort(), NodeInfo::getSchemaVersion, UUID::randomUUID));
    }

    @Test
    public void mapAndUpdateFunctionsPeerTest() throws Exception
    {
        UUID schemaVersion = UUID.randomUUID();

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.99.99.99");

        assertNull(Nodes.peers().get(peer));

        Nodes.peers().update(peer, p -> {});

        assertNotNull(Nodes.peers().get(peer));

        assertNull(Nodes.peers().get(peer).getHostId());

        assertNull(Nodes.nodes().map(peer, NodeInfo::getSchemaVersion));

        assertEquals(hostId, Nodes.nodes().map(peer, NodeInfo::getSchemaVersion, () -> hostId));

        Nodes.peers().update(peer, hostId, NodeInfo::setHostId);
        assertEquals(hostId, Nodes.peers().get(peer).getHostId());
        assertEquals(hostId, Nodes.nodes().map(peer, NodeInfo::getHostId));
        assertEquals(hostId, Nodes.nodes().map(peer, NodeInfo::getHostId, UUID::randomUUID));

        assertNull(Nodes.peers().get(peer).getSchemaVersion());
        Nodes.peers().update(peer, l -> l.setSchemaVersion(schemaVersion));
        assertEquals(schemaVersion, Nodes.peers().get(peer).getSchemaVersion());
        assertEquals(schemaVersion, Nodes.nodes().map(peer, NodeInfo::getSchemaVersion));
        assertEquals(schemaVersion, Nodes.nodes().map(peer, NodeInfo::getSchemaVersion, UUID::randomUUID));
        assertEquals(schemaVersion, Nodes.peers().map(peer, NodeInfo::getSchemaVersion, UUID::randomUUID));
    }

    @Test
    public void mapAndUpdateFunctionsNodeTest() throws Exception
    {
        UUID schemaVersion = UUID.randomUUID();

        InetAddressAndPort node = InetAddressAndPort.getByName("127.88.88.88");

        assertNull(Nodes.nodes().get(node));

        Nodes.nodes().update(node, p -> {});

        assertNotNull(Nodes.nodes().get(node));

        assertNull(Nodes.nodes().map(node, NodeInfo::getSchemaVersion));

        assertEquals(hostId, Nodes.nodes().map(node, NodeInfo::getSchemaVersion, () -> hostId));

        Nodes.nodes().update(node, hostId, NodeInfo::setHostId);
        assertEquals(hostId, Nodes.peers().get(node).getHostId());
        assertEquals(hostId, Nodes.nodes().map(node, NodeInfo::getHostId));
        assertEquals(hostId, Nodes.peers().map(node, NodeInfo::getHostId, UUID::randomUUID));

        assertNull(Nodes.peers().get(node).getSchemaVersion());
        Nodes.peers().update(node, l -> l.setSchemaVersion(schemaVersion));
        assertEquals(schemaVersion, Nodes.peers().get(node).getSchemaVersion());
        assertEquals(schemaVersion, Nodes.nodes().map(node, NodeInfo::getSchemaVersion));
        assertEquals(schemaVersion, Nodes.nodes().map(node, NodeInfo::getSchemaVersion, UUID::randomUUID));
        assertEquals(schemaVersion, Nodes.peers().map(node, NodeInfo::getSchemaVersion, UUID::randomUUID));
    }

    @Test
    public void testPeersSerialization() throws Exception
    {
        Nodes.local().update(this::fakeLocal, true);
        LocalInfo local = Nodes.local().get();
        validateLocalInfo(local);
        Nodes.peers().update(InetAddressAndPort.getByName("127.0.0.2"), NodesTest::fakePeer);
        Nodes.peers().update(InetAddressAndPort.getByName("127.0.0.3"), NodesTest::fakePeer);
        Nodes.peers().update(InetAddressAndPort.getByName("127.0.0.4"), NodesTest::fakePeer);
        assertNull(Nodes.peers().get(InetAddressAndPort.getByName("127.42.42.42")));
        Nodes.nodes().syncToDisk();
        Nodes.nodes().shutdown();

        // Reopen the nodes to load the saved local and peer info
        Nodes.Instance.unsafeSetup(dir.toPath());

        validateLocalInfo(Nodes.local().get());
        validatePeer(Nodes.peers().get(InetAddressAndPort.getByName("127.0.0.2")));
        validatePeer(Nodes.peers().get(InetAddressAndPort.getByName("127.0.0.3")));
        validatePeer(Nodes.peers().get(InetAddressAndPort.getByName("127.0.0.4")));

        Nodes.peers().remove(InetAddressAndPort.getByName("127.0.0.3"));
        Nodes.nodes().syncToDisk();
        Nodes.nodes().shutdown();

        Nodes.Instance.unsafeSetup(dir.toPath());

        validateLocalInfo(Nodes.local().get());
        validatePeer(Nodes.peers().get(InetAddressAndPort.getByName("127.0.0.2")));
        assertNull(Nodes.peers().get(InetAddressAndPort.getByName("127.0.0.3")));
        validatePeer(Nodes.peers().get(InetAddressAndPort.getByName("127.0.0.4")));
    }

    @Test
    public void snapshotTest() throws Throwable
    {
        File snapshotsDir = new File(dir, "snapshots");

        Nodes.nodes().snapshot("EMPTY_SNAPSHOT");
        File expectedDir = new File(snapshotsDir, "EMPTY_SNAPSHOT");
        assertTrue(expectedDir.isDirectory());
        // The hostId is set during initialization so we will always have a
        // local file
        assertTrue(new File(expectedDir, "local").exists());
        assertFalse(new File(expectedDir, "nodes").exists());

        Nodes.local().update(this::fakeLocal, true);
        Nodes.peers().update(InetAddressAndPort.getByName("127.0.0.2"), NodesTest::fakePeer);
        Nodes.peers().update(InetAddressAndPort.getByName("127.0.0.3"), NodesTest::fakePeer);
        Nodes.peers().update(InetAddressAndPort.getByName("127.0.0.4"), NodesTest::fakePeer);

        Nodes.nodes().snapshot("THAT_SNAPSHOT");
        File expectedDir2 = new File(snapshotsDir, "THAT_SNAPSHOT");
        assertTrue(expectedDir2.isDirectory());
        assertTrue(new File(expectedDir2, "local").isFile());
        assertTrue(new File(expectedDir2, "peers").isFile());

        Nodes.nodes().clearSnapshot("EMPTY_SNAPSHOT");
        assertFalse(expectedDir.isDirectory());
        assertTrue(expectedDir2.isDirectory());
        Nodes.nodes().clearSnapshot(null);
        assertFalse(snapshotsDir.isDirectory());
    }

    @Test
    public void testLocalInfoUnknownFieldsAreIgnoredDuringDeserialization() throws IOException
    {
        String clusterName = "clusterName_" + RandomStringUtils.randomAlphabetic(8).toLowerCase();
        NonCompatibleLocalInfo existingLocalInfo = new NonCompatibleLocalInfo();
        existingLocalInfo.setClusterName(clusterName);
        existingLocalInfo.setUnsupportedField("unsupported");

        File local = dir.toPath().resolve("local").toFile();
        ObjectMapper objectMapper = Nodes.createObjectMapper();
        objectMapper.writerFor(NonCompatibleLocalInfo.class).writeValue(local, existingLocalInfo);

        Nodes.Instance.unsafeSetup(dir.toPath());

        LocalInfo loadedLocalInfo = Nodes.local().get();
        assertEquals(clusterName, loadedLocalInfo.getClusterName());
    }

    static void fakePeer(PeerInfo p)
    {
        int nodeId = p.getPeer().address.getAddress()[3];
        p.setPreferred(preferredIp(p));
        p.setDataCenter("DC" + nodeId);
        p.setRack("RAC" + nodeId);
        p.setHostId(UUID.randomUUID());
    }

    private void validatePeer(PeerInfo p)
    {
        int nodeId = p.getPeer().address.getAddress()[3];
        assertEquals(preferredIp(p), p.getPreferred());
        assertEquals("DC" + nodeId, p.getDataCenter());
        assertEquals("RAC" + nodeId, p.getRack());
    }

    private static InetAddressAndPort preferredIp(PeerInfo p)
    {
        try
        {
            return InetAddressAndPort.getByName("127.123.123." + p.getPeer().address.getAddress()[3]);
        }
        catch (UnknownHostException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    private void fakeLocal(LocalInfo l)
    {
        l.setClusterName("NodesTest");
        l.setDataCenter("dataCenter");
        l.setRack("rack");
        l.setNativeProtocolVersion("66");
        l.setBootstrapState(BootstrapState.COMPLETED);
        l.setHostId(hostId);
        l.setTokens(tokens);
        l.setTruncationRecords(truncationRecords);
        l.setBroadcastAddressAndPort(FBUtilities.getBroadcastAddressAndPort());
    }

    private void validateLocalInfo(LocalInfo local)
    {
        assertEquals("NodesTest", local.getClusterName());
        assertEquals("dataCenter", local.getDataCenter());
        assertEquals("rack", local.getRack());
        assertEquals("66", local.getNativeProtocolVersion());
        assertEquals(hostId, local.getHostId());
        assertEquals(tokens, local.getTokens());
        assertEquals(truncationRecords, local.getTruncationRecords());
        assertEquals(FBUtilities.getBroadcastAddressAndPort(), local.getBroadcastAddressAndPort());
        assertNotNull(local.getGossipGeneration());
        assertNull(local.getCqlVersion());
        assertNull(local.getReleaseVersion());
        assertNull(local.getPartitioner());
        assertNull(local.getListenAddressAndPort());
        assertNull(local.getNativeTransportAddressAndPort());
        assertNull(local.getSchemaVersion());
    }

    private static class NonCompatibleLocalInfo extends NodeInfo
    {
        private String clusterName;
        private String unsupportedField;

        @JsonProperty("cluster_name")
        public String getClusterName()
        {
            return clusterName;
        }

        public void setClusterName(String clusterName)
        {
            this.clusterName = clusterName;
        }

        @Override
        public NodeInfo copy()
        {
            throw new UnsupportedOperationException("copy is not meant to be used in the test");
        }

        @JsonProperty("unsupported_field")
        public String getUnsupportedField()
        {
            return unsupportedField;
        }

        public void setUnsupportedField(String unsupportedField)
        {
            this.unsupportedField = unsupportedField;
        }
    }
}
