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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.accord.AccordFastPath;
import org.apache.cassandra.service.accord.AccordStaleReplicas;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.utils.CassandraVersion;

import static org.assertj.core.api.Assertions.assertThat;


public class CMSOfflineToolTest extends OfflineToolUtils
{

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testDefaultCmd()
    {

        ToolRunner.ToolResult tool = ToolRunner.invokeClass(CMSOfflineTool.class);

        tool.assertOnCleanExit();
        assertThat(tool.getExitCode()).isZero();
        assertThat(tool.getStdout()).contains("usage");
        assertThat(tool.getStderr()).isEmpty();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testRunCommandThatDoesNotExist()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(CMSOfflineTool.class, "cmddoesnotexist");
        assertThat(tool.getExitCode()).isOne();
    }

    @Test
    public void testRunCommandWithMetadataFileThatDoesnotExist()
    {
        String metadataFile = temporaryFolder.getRoot().getAbsolutePath() + "/file-does-not-exists.dump";
        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "printdirectory",
                                                              "-f",
                                                              metadataFile);

        assertThat(result.getExitCode()).isEqualTo(2);
    }

    @Test
    public void testaddToCMS() throws IOException
    {
        assertAddToCMS("127.0.0.1:7000");
    }

    @Test
    public void testaddToCMSIpAddressAlone() throws IOException
    {
        assertAddToCMS("127.0.0.1");
    }

    @Test
    public void testaddToCMSNodeThatDoesNotExistEarlier() throws IOException
    {
        // Node that doesn't exist in Cluster Metadata can be added as CMS member
        assertAddToCMS("127.0.0.55:7000");
    }

    private void assertAddToCMS(String ipAddress) throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "addtocms",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              ipAddress,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata).isNotNull();

        // Check given ip address is added to CMS members
        InetAddressAndPort candidate = InetAddressAndPort.getByNameUnchecked(ipAddress);
        assertThat(outMetadata.isCMSMember(candidate)).isTrue();

        // Check existing nodes retained as CMS members
        ClusterMetadata inMetadata = deserializeMetadata(metadataFile);
        for (InetAddressAndPort existingMember : inMetadata.fullCMSMembers())
        {
            assertThat(inMetadata.isCMSMember(existingMember)).isTrue();
        }

        assertCorrectEnvPostTest();
    }

    @Test
    public void testaddToCMSInvalidIpAddress() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String invalidIpAddress = "/127.0.0.";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "addtocms",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              invalidIpAddress,
                                                              "-o",
                                                              outputFile);

        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getExitCode()).isEqualTo(2);
        assertThat(result.getStderr()).contains("java.net.UnknownHostException");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testaddToCMSInvalidSerializationVersion() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String invalidIpAddress = "127.0.0.3";
        String serializationVersion = "-1";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "addtocms",
                                                              "-f",
                                                              metadataFile,
                                                              "-sv",
                                                              serializationVersion,
                                                              "-ip",
                                                              invalidIpAddress,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(1);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("cmsofflinetool: serializationVersion: can not convert \"" +
                                                serializationVersion + "\" to a Version");
    }

    @Test
    public void testAssignTokens() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String newNodeIpWithPort = "127.0.0.4:7000";
        InetAddressAndPort newNodeInetAddress = InetAddressAndPort.getByNameUnchecked(newNodeIpWithPort);
        Directory newDirectory = metadata.directory.with(new NodeAddresses(newNodeInetAddress),
                                                         new Location("datacenter1", "rack4"),
                                                         NodeVersion.CURRENT);
        metadata = updateMetadata(metadata, newDirectory);

        NodeId newNodeId = metadata.directory.peerId(newNodeInetAddress);

        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        Token token1ToAssign = metadata.partitioner.getRandomToken();
        Token token2ToAssign = metadata.partitioner.getRandomToken();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assigntokens",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              newNodeIpWithPort,
                                                              "-t",
                                                              String.valueOf(token1ToAssign.getTokenValue()),
                                                              "--token",
                                                              String.valueOf(token2ToAssign.getTokenValue()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata).isNotNull();
        assertThat(outMetadata.directory.peerId(newNodeInetAddress)).isNotNull();
        assertThat(outMetadata.tokenMap.tokens(newNodeId)).contains(token1ToAssign)
                                                          .contains(token2ToAssign);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAssignTokensForNonExistingNode() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String unknownNodeIpWithPort = "127.0.0.5:7000";
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        Token tokenToAssign = metadata.partitioner.getRandomToken();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assigntokens",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              unknownNodeIpWithPort,
                                                              "-t",
                                                              String.valueOf(tokenToAssign.getTokenValue()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(result.getStderr()).contains(" Cassandra node with address " + unknownNodeIpWithPort
                                                + " does not exist.");
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAssignTokensInvalidIpAddress() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String invalidIp = "127.0.0.";
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        Token tokenToAssign = metadata.partitioner.getRandomToken();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assigntokens",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              invalidIp,
                                                              "-t",
                                                              String.valueOf(tokenToAssign.getTokenValue()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(result.getStderr()).contains("java.net.UnknownHostException");
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertCorrectEnvPostTest();
    }

    @Test
    public void testAssignTokensInvalidToken() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String newNodeIpWithPort = "127.0.0.1:7000";
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String invalidToken = "somegibberishinvalidtoken";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assigntokens",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              newNodeIpWithPort,
                                                              "-t",
                                                              invalidToken,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoin() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String newNodeIpWithPort = "127.0.0.4:7000";
        InetAddressAndPort newNodeInetAddress = InetAddressAndPort.getByNameUnchecked(newNodeIpWithPort);
        Directory newDirectory = metadata.directory.with(new NodeAddresses(newNodeInetAddress),
                                                         new Location("datacenter1", "rack4"),
                                                         NodeVersion.CURRENT);
        metadata = updateMetadata(metadata, newDirectory);

        NodeId newNodeId = metadata.directory.peerId(newNodeInetAddress);
        assertThat(metadata.directory.states.get(newNodeId)).isNotEqualTo(NodeState.JOINED);

        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forcejoin",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(newNodeId.id()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata).isNotNull();
        assertThat(outMetadata.directory.peerId(newNodeInetAddress)).isNotNull();
        assertThat(outMetadata.directory.states.get(newNodeId)).isEqualTo(NodeState.JOINED);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoinNodeThatDoesNotExist() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String nodeId = "-1";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forcejoin",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              nodeId,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).isNotEmpty();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForgetNode() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();

        String joinedNodeIpWithPort = "127.0.0.4:7000";
        InetAddressAndPort joiningNodeInetAddress = InetAddressAndPort.getByNameUnchecked(joinedNodeIpWithPort);
        NodeId joinedNodeId = NodeId.fromUUID(new UUID(0, 4L));

        Directory newDirectory = metadata.directory
                                 .with(joinedNodeId,
                                       new NodeAddresses(joiningNodeInetAddress),
                                       new Location("datacenter1", "rack4"),
                                       NodeVersion.CURRENT)
                                 .withNodeState(joinedNodeId, NodeState.JOINED);

        TokenMap tokenMap = new TokenMap(metadata.partitioner)
                            .assignTokens(joinedNodeId, Collections.singleton(metadata.partitioner.getRandomToken()));

        metadata = updateMetadata(metadata, newDirectory);
        metadata = updateMetadata(metadata, tokenMap);

        assertThat(metadata.tokenMap.tokens(joinedNodeId)).isNotEmpty();


        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forgetnode",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(joinedNodeId.id()),
                                                              "-o",
                                                              outputFile);


        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata).isNotNull();
        assertThat(outMetadata.directory.peerIds()).doesNotContain(joinedNodeId);
        assertThat(outMetadata.tokenMap.tokens(joinedNodeId)).isEmpty();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForgetNodeInvalidNodeId() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String nodeId = String.valueOf(-1);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forgetnode",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              nodeId,
                                                              "-o",
                                                              outputFile);


        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Node with id " + nodeId + " does not exist.");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testPrintDataPlacements() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String keyspaceName = "ks1";
        KeyspaceParams ksParams = KeyspaceParams.create(true,
                                                        Map.of("class", "NetworkTopologyStrategy",
                                                               "datacenter1", "3"));
        Keyspaces keyspaces = Keyspaces.none().with(KeyspaceMetadata.create(keyspaceName, ksParams));
        DistributedSchema newSchema = new DistributedSchema(keyspaces);
        metadata = updateMetadata(metadata, newSchema);

        InetAddressAndPort inetAddressAndPort = InetAddressAndPort.getByNameUnchecked("127.0.0.1:7000");
        InetAddressAndPort i2 = InetAddressAndPort.getByNameUnchecked("127.0.0.2:7000");
        InetAddressAndPort i3 = InetAddressAndPort.getByNameUnchecked("127.0.0.3:7000");

        DatabaseDescriptor.toolInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(metadata.partitioner);

        DataPlacement dataPlacement = DataPlacement.builder()
                                                   .withWriteReplica(Epoch.FIRST, Replica.fullReplica(inetAddressAndPort, new Range<>(metadata.partitioner.getMinimumToken(), metadata.partitioner.getTokenFactory().fromString("0"))))
                                                   .withWriteReplica(Epoch.FIRST, Replica.fullReplica(i2, new Range<>(metadata.partitioner.getMinimumToken(), metadata.partitioner.getTokenFactory().fromString("0"))))
                                                   .withWriteReplica(Epoch.FIRST, Replica.fullReplica(i3, new Range<>(metadata.partitioner.getMinimumToken(), metadata.partitioner.getTokenFactory().fromString("0"))))
                                                   .withReadReplica(Epoch.FIRST, Replica.fullReplica(inetAddressAndPort, new Range<>(metadata.partitioner.getMinimumToken(), metadata.partitioner.getTokenFactory().fromString("0"))))
                                                   .withReadReplica(Epoch.FIRST, Replica.fullReplica(i2, new Range<>(metadata.partitioner.getMinimumToken(), metadata.partitioner.getTokenFactory().fromString("0"))))
                                                   .withReadReplica(Epoch.FIRST, Replica.fullReplica(i3, new Range<>(metadata.partitioner.getMinimumToken(), metadata.partitioner.getTokenFactory().fromString("0"))))
                                                   .build();

        DataPlacements dataPlacements = DataPlacements.builder(1)
                                                      .with(ReplicationParams.fromMap(Map.of("class", "NetworkTopologyStrategy",
                                                                                             "datacenter1", "3")),
                                                            dataPlacement)
                                                      .build();
        metadata = updateMetadata(metadata, dataPlacements);
        String metadataFile = dumpMetadata(metadata);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "printdataplacements",
                                                              "-f",
                                                              metadataFile,
                                                              "-ks",
                                                              keyspaceName);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(result.getStdout()).isNotEmpty();
        assertThat(result.getStderr()).isNullOrEmpty();

        String stdout = result.getStdout();
        assertThat(stdout).isEqualTo(
        "Token Range                Type    Endpoints\n" +
        "(-9223372036854775808,0]   read    /127.0.0.1:7000, /127.0.0.2:7000, /127.0.0.3:7000\n" +
        "(-9223372036854775808,0]   write   /127.0.0.1:7000, /127.0.0.2:7000, /127.0.0.3:7000\n");
    }

    @Test
    public void testPrintDataPlacementsKeyspaceDoesNotExist() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String keyspaceName = "ks1";
        String metadataFile = dumpMetadata(metadata);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "printdataplacements",
                                                              "-f",
                                                              metadataFile,
                                                              "-ks",
                                                              keyspaceName);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(result.getStderr()).isNotEmpty();
        assertThat(result.getStderr()).contains("Keyspace " + keyspaceName + " not found in cluster metadata");
    }

    @Test
    public void testPrintDirectory() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "printdirectory",
                                                              "-f",
                                                              metadataFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(result.getStdout()).isNotEmpty();
        assertThat(result.getStderr()).isNullOrEmpty();

        String stdout = result.getStdout();
        assertThat(stdout).isEqualTo(
        "NodeId: 1\n" +
        "  rack                  rack1\n" +
        "  local_port            7000\n" +
        "  broadcast_port        7000\n" +
        "  host_id               6d194555-f6eb-41d0-c000-000000000001\n" +
        "  broadcast_address     /127.0.0.1\n" +
        "  native_address        /127.0.0.1\n" +
        "  native_port           7000\n" +
        "  local_address         /127.0.0.1\n" +
        "  state                 REGISTERED\n" +
        "  serialization_version V7\n" +
        "  cassandra_version     5.1.0\n" +
        "  dc                    datacenter1\n" +
        "  is_cms_member         false\n" +
        "NodeId: 2\n" +
        "  rack                  rack2\n" +
        "  local_port            7000\n" +
        "  broadcast_port        7000\n" +
        "  host_id               6d194555-f6eb-41d0-c000-000000000002\n" +
        "  broadcast_address     /127.0.0.2\n" +
        "  native_address        /127.0.0.2\n" +
        "  native_port           7000\n" +
        "  local_address         /127.0.0.2\n" +
        "  state                 REGISTERED\n" +
        "  serialization_version V7\n" +
        "  cassandra_version     5.1.0\n" +
        "  dc                    datacenter1\n" +
        "  is_cms_member         false\n" +
        "NodeId: 3\n" +
        "  rack                  rack3\n" +
        "  local_port            7000\n" +
        "  broadcast_port        7000\n" +
        "  host_id               6d194555-f6eb-41d0-c000-000000000003\n" +
        "  broadcast_address     /127.0.0.3\n" +
        "  native_address        /127.0.0.3\n" +
        "  native_port           7000\n" +
        "  local_address         /127.0.0.3\n" +
        "  state                 REGISTERED\n" +
        "  serialization_version V7\n" +
        "  cassandra_version     5.1.0\n" +
        "  dc                    datacenter1\n" +
        "  is_cms_member         false\n"
        );
    }

    private String dumpMetadata(ClusterMetadata metadata) throws IOException
    {
        String tempFile = temporaryFolder.newFile().getAbsolutePath();
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(tempFile))
        {
            VerboseMetadataSerializer.serialize(ClusterMetadata.serializer,
                                                metadata,
                                                out,
                                                NodeVersion.CURRENT.serializationVersion());
        }
        return tempFile;
    }

    private ClusterMetadata getThreeNodeClusterMetadata()
    {
        KeyspaceParams ksParams = KeyspaceParams.create(true,
                                                        Map.of("class", "MetaStrategy", "datacenter1", "1"));
        Keyspaces keyspaces = Keyspaces.none().with(KeyspaceMetadata.create(SchemaConstants.METADATA_KEYSPACE_NAME,
                                                                            ksParams));
        DistributedSchema distributedSchema = new DistributedSchema(keyspaces);
        Directory directory = new Directory()
                              .with(new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.1:7000")),
                                    new Location("datacenter1", "rack1"),
                                    NodeVersion.fromCassandraVersion(new CassandraVersion("5.1")))
                              .with(new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.2:7000")),
                                    new Location("datacenter1", "rack2"),
                                    NodeVersion.fromCassandraVersion(new CassandraVersion("5.1")))
                              .with(new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.3:7000")),
                                    new Location("datacenter1", "rack3"),
                                    NodeVersion.fromCassandraVersion(new CassandraVersion("5.1")));

        IPartitioner partitioner = Murmur3Partitioner.instance;

        return new ClusterMetadata(Epoch.EMPTY, partitioner,
                                   distributedSchema,
                                   directory,
                                   new TokenMap(partitioner),
                                   DataPlacements.empty(),
                                   AccordFastPath.EMPTY,
                                   LockedRanges.EMPTY,
                                   InProgressSequences.EMPTY,
                                   ConsensusMigrationState.EMPTY,
                                   ImmutableMap.of(),
                                   AccordStaleReplicas.EMPTY);
    }

    private ClusterMetadata updateMetadata(ClusterMetadata metadata, Directory directory)
    {
        return metadata.transformer().with(directory).build().metadata;
    }

    private ClusterMetadata updateMetadata(ClusterMetadata metadata, DataPlacements dataPlacements)
    {
        return metadata.transformer().with(dataPlacements).build().metadata;
    }

    private ClusterMetadata updateMetadata(ClusterMetadata metadata, DistributedSchema newSchema)
    {
        return metadata.transformer().with(newSchema).build().metadata;
    }

    private ClusterMetadata updateMetadata(ClusterMetadata metadata, TokenMap tokenMap)
    {
        return new ClusterMetadata(metadata.epoch,
                                   metadata.partitioner,
                                   metadata.schema,
                                   metadata.directory,
                                   tokenMap,
                                   metadata.placements,
                                   metadata.accordFastPath,
                                   metadata.lockedRanges,
                                   metadata.inProgressSequences,
                                   metadata.consensusMigrationState,
                                   metadata.extensions,
                                   metadata.accordStaleReplicas);
    }


    ClusterMetadata deserializeMetadata(String medataDumpFile) throws IOException
    {
        return ClusterMetadataService.deserializeClusterMetadata(medataDumpFile);
    }
}
