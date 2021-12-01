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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.CassandraVersion;

import static org.assertj.core.api.Assertions.assertThat;

public class NodesPersistenceTest extends CQLTester
{
    private final NodesPersistence np = new NodesPersistence();


    private void fillNodeInfoSampleData(NodeInfo info) throws UnknownHostException
    {
        info.setHostId(UUID.randomUUID());
        info.setReleaseVersion(new CassandraVersion("4.1.2"));
        info.setRack("rack1");
        info.setDataCenter("dc1");
        info.setSchemaVersion(UUID.randomUUID());
        info.setNativeTransportAddressAndPort(InetAddressAndPort.getByNameOverrideDefaults("127.1.2.3", 111));
        Token.TokenFactory tf = StorageService.instance.getTokenFactory();
        info.setTokens(Arrays.asList(tf.fromString("1"), tf.fromString("2"), tf.fromString("3")));
    }

    private void fillLocalInfoSampleData(LocalInfo info) throws UnknownHostException
    {
        fillNodeInfoSampleData(info);
        info.setListenAddressAndPort(InetAddressAndPort.getByNameOverrideDefaults("127.2.3.4", 222));
        info.setClusterName("cluster1");
        info.setCqlVersion(new CassandraVersion("2.3.4"));
        info.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
        info.setBroadcastAddressAndPort(InetAddressAndPort.getByNameOverrideDefaults("127.3.4.5", 333));
        info.setNativeProtocolVersion(ProtocolVersion.V5);
        info.setPartitionerClass(Murmur3Partitioner.class);
        info.setTruncationRecords(ImmutableMap.of(UUID.randomUUID(), new TruncationRecord(new CommitLogPosition(22, 33), 44),
                                                  UUID.randomUUID(), new TruncationRecord(new CommitLogPosition(55, 66), 77)));
    }

    private void fillPeerInfoSampleData(PeerInfo info) throws UnknownHostException
    {
        fillNodeInfoSampleData(info);
        info.setPeerAddressAndPort(InetAddressAndPort.getByNameOverrideDefaults("127.4.5.6", 444));
        info.setPreferredAddressAndPort(InetAddressAndPort.getByNameOverrideDefaults("127.5.6.7", 555));
    }

    @Test
    public void testLocal() throws Exception
    {
        LocalInfo info = new LocalInfo();
        fillLocalInfoSampleData(info);
        np.saveLocal(info.duplicate());
        LocalInfo loaded = np.loadLocal();
        assertThat(loaded).isEqualTo(info);
    }

    @Test
    public void testPeer() throws Exception
    {
        PeerInfo info = new PeerInfo();
        fillPeerInfoSampleData(info);
        np.savePeer(info.duplicate());
        List<PeerInfo> loaded = np.loadPeers().collect(Collectors.toList());
        assertThat(loaded).containsExactlyInAnyOrder(info);
        np.deletePeer(info.getPeerAddressAndPort());
        List<PeerInfo> loaded2 = np.loadPeers().collect(Collectors.toList());
        assertThat(loaded2).isEmpty();
    }

    @Test
    public void testUpdateTokens() throws Exception
    {
        LocalInfo info = new LocalInfo();
        fillLocalInfoSampleData(info);
        np.saveLocal(info);
        LocalInfo loaded = np.loadLocal();
        assertThat(loaded.getTokens()).hasSize(3);
        ArrayList<Token> tokens = Lists.newArrayList(loaded.getTokens());
        tokens.remove(0);
        np.saveLocal(loaded.duplicate().setTokens(tokens));
        loaded = np.loadLocal();
        assertThat(loaded.getTokens()).hasSize(2);
    }
}