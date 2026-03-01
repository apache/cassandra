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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileInputStreamPlus;
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
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.tcm.transformations.Register;

import static org.apache.cassandra.config.DatabaseDescriptor.getStoragePort;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.prepareJoin;
import static org.assertj.core.api.Assertions.assertThat;


public class CMSOfflineToolTest extends OfflineToolUtils
{

    public static final String DC = "datacenter1";
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static ClusterMetadata getClusterMetadata(Keyspaces keyspaces, IPartitioner partitioner, Directory directory)
    {
        DistributedSchema distributedSchema = new DistributedSchema(keyspaces);

        return new ClusterMetadata(Epoch.EMPTY,
                                   partitioner,
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

    @Before
    public void setup()
    {
        DatabaseDescriptor.toolInitialization();
        ClusterMetadataService.initializeForTools(true);
    }

    @Test
    public void testDefaultCmd()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(CMSOfflineTool.class);

        tool.assertOnCleanExit();
        assertThat(tool.getExitCode()).isZero();
        assertThat(tool.getStderr()).isEmpty();
        assertThat(tool.getStdout()).withFailMessage(tool.getStderr()).contains("Usage");
        assertCorrectEnvPostTest();
    }

    @Test
    public void testRunCommandThatDoesNotExist()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(CMSOfflineTool.class, "cmddoesnotexist");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertCorrectEnvPostTest();
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
        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortbootstrapJoiningNode() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        int nodeToMove = 4;
        metadata = startJoining(nodeToMove, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String nodeId = String.valueOf(nodeToMove);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortbootstrap",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              nodeId,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.directory.peerIds()).doesNotContain(new NodeId(nodeToMove));

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortbootstrapReplacingNode() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        NodeId nodeToReplaceId = new NodeId(3);
        NodeId newNodeId = new NodeId(4);
        metadata = startReplacing(nodeToReplaceId, newNodeId, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        String newNodeAddr = metadata.directory.getNodeAddresses(newNodeId).nativeAddress.getHostAddressAndPort();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortbootstrap",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              newNodeAddr,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.directory.peerIds()).doesNotContain(newNodeId);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortbootstrapNodeGettingReplaced() throws IOException
    {
        // Assuming that operator unintentionally invokes abortbootstrap on node getting replaced
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        NodeId nodeToReplaceId = new NodeId(3);
        NodeId newNodeId = new NodeId(4);
        metadata = startReplacing(nodeToReplaceId, newNodeId, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        String nodeGettingReplaced = metadata.directory.getNodeAddresses(nodeToReplaceId).nativeAddress.getHostAddressAndPort();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortbootstrap",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              nodeGettingReplaced,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Did not find any sequences in progress for node " + nodeGettingReplaced);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortbootstrapWhenLeavingSequenceIsInProgress() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        NodeId nodeId = new NodeId(4);
        metadata = startLeaving(nodeId, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortbootstrap",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId.id()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("abortbootstrap is not a valid operation when sequence of kind LEAVE");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortbootstrapJoinedNode() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        int nodeId = 4;

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortbootstrap",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortMoveNoInProgressSequence() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        int nodeId = 4;

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortmove",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("No transformation sequence is in progress for");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortMoveNonMovingSequence() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        NodeId nodeId = new NodeId(4);
        metadata = startLeaving(nodeId, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortmove",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId.id()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Multi step operation of kind")
                                      .contains("LEAVE")
                                      .contains("Cannot proceed with abort move");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortMoveMovingNode() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        NodeId nodeId = new NodeId(4);
        metadata = startMoving(nodeId, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortmove",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId.id()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.inProgressSequences.contains(nodeId)).isFalse();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortDecommissionNoInProgressSequence() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        int nodeId = 4;

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortdecommission",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("No transformation sequence is in progress for");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortDecommissionNonLeavingSequence() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        NodeId nodeId = new NodeId(4);
        metadata = startMoving(nodeId, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortdecommission",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId.id()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Multi step operation of kind")
                                      .contains("MOVE")
                                      .contains("Cannot proceed with abort decommission");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAbortDecommissionLeavingNode() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        NodeId nodeId = new NodeId(4);
        metadata = startLeaving(nodeId, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "abortdecommission",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId.id()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.inProgressSequences.contains(nodeId)).isFalse();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAssassinateCMSMember() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        // Verify that we have CMS members
        assertThat(metadata.fullCMSMembers()).isNotEmpty();

        // Pick the first CMS member to assassinate
        NodeId cmsNodeId = metadata.directory.peerIds().stream().findFirst().orElseThrow();
        assertThat(metadata.isCMSMember(metadata.directory.endpoint(cmsNodeId))).isTrue();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assassinate",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(cmsNodeId.id()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.directory.peerState(cmsNodeId)).isEqualTo(NodeState.LEFT);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAssassinateNodeNonCMSMember() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        // Verify that we have CMS members
        assertThat(metadata.fullCMSMembers()).isNotEmpty();

        // Select non-CMS node
        NodeId nonCMSNodeId = metadata.directory.peerIds().stream()
                                             .filter(id -> !metadata.isCMSMember(metadata.directory.endpoint(id)))
                                             .findFirst().orElseThrow();
        assertThat(metadata.isCMSMember(metadata.directory.endpoint(nonCMSNodeId))).isFalse();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assassinate",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nonCMSNodeId.id()),
                                                              "-o",
                                                              outputFile);

        assertThat(Files.exists(Paths.get(outputFile))).isTrue();
        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.directory.peerState(nonCMSNodeId)).isEqualTo(NodeState.LEFT);
        assertCorrectEnvPostTest();
    }

    @Test
    public void testAssassinateNodeInvalidNodeId() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String nodeId = String.valueOf(-1);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assassinate",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              nodeId,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("No node present with id " + nodeId +
                                                " in the given cluster metadata");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAssassinateMovingNode() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        int nodeToMove = 4;
        metadata = startMoving(new NodeId(nodeToMove), metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String nodeId = String.valueOf(nodeToMove);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assassinate",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              nodeId,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();
        assertThat(result.getStdout()).contains("Cancelling in-progress sequence");

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.directory.peerState(new NodeId(nodeToMove))).isEqualTo(NodeState.LEFT);
        assertCorrectEnvPostTest();
    }

    @Test
    public void testAssassinateJoiningNode() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        int nodeToMove = 4;
        metadata = startJoining(nodeToMove, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String nodeId = String.valueOf(nodeToMove);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assassinate",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              nodeId,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();

        // It should suggest to use abortbootstrap for node in joining state
        assertThat(result.getStderr()).contains("abortbootstrap");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAssassinateNodeGettingReplaced() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        NodeId nodeToReplaceId = new NodeId(4);
        NodeId newNodeId = new NodeId(5);
        metadata = startReplacing(nodeToReplaceId, newNodeId, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        String nodeGettingReplaced = metadata.directory.getNodeAddresses(nodeToReplaceId)
                                     .nativeAddress.getHostAddressAndPort();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assassinate",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              nodeGettingReplaced,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("INVALID: Rejecting this plan as it interacts with a range locked");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testAssassinateLeavingNode() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        NodeId nodeId = new NodeId(4);
        metadata = startLeaving(nodeId, metadata);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String ip = metadata.directory.endpoint(nodeId).getHostAddressAndPort();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "assassinate",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              ip,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();
        assertThat(result.getStdout()).contains("Cancelling in-progress sequence");

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.directory.peerState(nodeId)).isEqualTo(NodeState.LEFT);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testDescribe() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "describe",
                                                              "-f",
                                                              metadataFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);

        int storagePort = getStoragePort();

        String expectedOutput =
        "Cluster Metadata Service:\n" +
        "Members: /127.0.0.1:" + storagePort + ",/127.0.0.2:" + storagePort + ",/127.0.0.3:" + storagePort + '\n' +
        "Needs reconfiguration: false\n" +
        "Service State: LOCAL\n" +
        "Epoch: 2\n" +
        "Replication factor: ReplicationParams{class=org.apache.cassandra.locator.MetaStrategy, datacenter1=3}\n";
        assertThat(result.getStdout()).isEqualTo(expectedOutput);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testDescribeRawString() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "print",
                                                              "-f",
                                                              metadataFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(result.getStdout()).isNotEmpty().startsWith("ClusterMetadata{");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testResetCMSUsingNodeId() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out" + new Random().nextLong() + ".dump";
        NodeId nodeId = new NodeId(1);

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "resetcms",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId.id()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(result.getStderr()).isEmpty();
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        InetAddressAndPort nodeAddress = metadata.directory.getNodeAddresses(nodeId).broadcastAddress;
        assertThat(outMetadata.isCMSMember(nodeAddress)).isTrue();
        assertThat(outMetadata.fullCMSMembers().size()).isEqualTo(1);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testResetCMS() throws IOException
    {
        assertResetCMS("127.0.0.1:" + getStoragePort());
    }

    @Test
    public void testResetCMSUsingIp() throws IOException
    {
        assertResetCMS("127.0.0.1");
    }

    private void assertResetCMS(String ipAddress) throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out" + new Random().nextLong() + ".dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "resetcms",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              ipAddress,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(result.getStderr()).isEmpty();
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata).isNotNull();

        // Check given ip address is added to CMS members
        InetAddressAndPort candidate = InetAddressAndPort.getByNameUnchecked(ipAddress);
        assertThat(outMetadata.isCMSMember(candidate)).isTrue();
        assertThat(outMetadata.fullCMSMembers().size()).isEqualTo(1);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testResetCMSUsingIpAddressThatDoesNotExist() throws IOException
    {
        // Node that doesn't exist in Cluster Metadata can be added as CMS member
        String ipAddress = "127.0.0.55:" + getStoragePort();
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "resetcms",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              ipAddress,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isNotEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("No node present").contains(ipAddress);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testResetCMSInvalidIpAddress() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String invalidIpAddress = "/127.0.0.";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "resetcms",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              invalidIpAddress,
                                                              "-o",
                                                              outputFile);

        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(result.getStderr()).contains("java.net.UnknownHostException");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testResetCMSInvalidSerializationVersion() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String invalidIpAddress = "127.0.0.3";
        String serializationVersion = "-1";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "resetcms",
                                                              "-f",
                                                              metadataFile,
                                                              "-sv",
                                                              serializationVersion,
                                                              "-ip",
                                                              invalidIpAddress,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Invalid value for option '--serialization-version'");
        assertCorrectEnvPostTest();
    }

    @Test
    public void testMoveToken() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata(16);
        String nodeToMove = "127.0.0.4:" + getStoragePort();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "move",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              nodeToMove,
                                                              "-t",
                                                              String.valueOf(metadata.partitioner.getRandomToken()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("This node has more than one token and cannot be moved thusly.");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testMoveTokenVnodes() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata(4);
        metadata = addNewNode(metadata, 4, Set.of(metadata.partitioner.getRandomToken()));
        String nodeToMove = "127.0.0.4:" + getStoragePort();
        InetAddressAndPort newNodeInetAddress = InetAddressAndPort.getByNameUnchecked(nodeToMove);
        NodeId nodeId = metadata.directory.peerId(InetAddressAndPort.getByName(nodeToMove));
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        Token desiredToken = metadata.partitioner.getRandomToken();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "move",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              nodeToMove,
                                                              "-t",
                                                              String.valueOf(desiredToken.getTokenValue()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata).isNotNull();
        assertThat(outMetadata.directory.peerId(newNodeInetAddress)).isNotNull();
        assertThat(outMetadata.tokenMap.tokens(nodeId)).contains(desiredToken);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testMoveTokenWithoutToken() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        String nodeToMove = "127.0.0.4:" + getStoragePort();
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "move",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              nodeToMove,
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Token required").contains("MOVE");
        assertCorrectEnvPostTest();
    }

    @Test
    public void testMoveTokenForNonExistingNode() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String unknownNodeIpWithPort = "127.0.0.5:" + getStoragePort();
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        Token tokenToAssign = metadata.partitioner.getRandomToken();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "move",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              unknownNodeIpWithPort,
                                                              "-t",
                                                              String.valueOf(tokenToAssign.getTokenValue()),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(result.getStderr()).contains("No node present with ip address")
                                      .contains(unknownNodeIpWithPort);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testMoveInvalidIpAddress() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String invalidIp = "127.0.0.";
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        Token tokenToAssign = metadata.partitioner.getRandomToken();

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "move",
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
    public void testMoveNodeToInvalidToken() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String newNodeIpWithPort = "127.0.0.1:" + getStoragePort();
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String invalidToken = "somegibberishinvalidtoken";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "move",
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
    public void testMoveNodeToSomeOtherNodeToken() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String metadataFile = dumpMetadata(metadata);
        String newNodeIpWithPort = "127.0.0.1:" + getStoragePort();
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ImmutableList<Token> secondNodeTokenList = metadata.tokenMap.tokens(new NodeId(2));

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "move",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              newNodeIpWithPort,
                                                              "-t",
                                                              secondNodeTokenList.get(0).toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testMoveNodeWhenMoveInProgressToAnotherToken() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        NodeId nodeId = new NodeId(2);
        Token randomToken = metadata.partitioner.getRandomToken();
        Set<Token> toTokenSet = Set.of(randomToken);
        metadata = startMoving(nodeId, metadata, toTokenSet);
        String metadataFile = dumpMetadata(metadata);
        String newNodeIpWithPort = "127.0.0." + nodeId.id() + ':' + getStoragePort();

        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "move",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              newNodeIpWithPort,
                                                              "-t",
                                                              randomToken.toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.tokenMap.tokens(nodeId)).isEqualTo(ImmutableList.of(randomToken));

        assertCorrectEnvPostTest();
    }

    @Test
    public void testMoveNodeFinishInProgress() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        NodeId nodeId = new NodeId(2);
        metadata = startMoving(nodeId, metadata);
        String metadataFile = dumpMetadata(metadata);
        String newNodeIpWithPort = "127.0.0." + nodeId.id() + ':' + getStoragePort();

        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "move",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              newNodeIpWithPort,
                                                              "-t",
                                                              metadata.partitioner.getRandomToken().toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Move in progress for another token(s)");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoin() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String newNodeIpWithPort = "127.0.0.5:" + getStoragePort();
        InetAddressAndPort newNodeInetAddress = InetAddressAndPort.getByNameUnchecked(newNodeIpWithPort);
        Directory newDirectory = metadata.directory.with(new NodeAddresses(newNodeInetAddress),
                                                         new Location("datacenter1", "rack4"),
                                                         NodeVersion.CURRENT);
        metadata = updateMetadata(metadata, newDirectory);

        NodeId newNodeId = metadata.directory.peerId(newNodeInetAddress);
        assertThat(metadata.directory.states.get(newNodeId)).isEqualTo(NodeState.REGISTERED);

        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forcejoin",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(newNodeId.id()),
                                                              "-t",
                                                              metadata.partitioner.getRandomToken().toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata).isNotNull();
        assertThat(outMetadata.directory.peerId(newNodeInetAddress)).isNotNull();
        assertThat(outMetadata.directory.states.get(newNodeId)).isEqualTo(NodeState.JOINED);
        assertThat(outMetadata.tokenMap.tokens(newNodeId)).isNotEmpty();

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoinNewNodeWithoutTokens() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String newNodeIpWithPort = "127.0.0.5:" + getStoragePort();
        InetAddressAndPort newNodeInetAddress = InetAddressAndPort.getByNameUnchecked(newNodeIpWithPort);
        Directory newDirectory = metadata.directory.with(new NodeAddresses(newNodeInetAddress),
                                                         new Location("datacenter1", "rack4"),
                                                         NodeVersion.CURRENT);
        metadata = updateMetadata(metadata, newDirectory);

        NodeId newNodeId = metadata.directory.peerId(newNodeInetAddress);
        assertThat(metadata.directory.states.get(newNodeId)).isEqualTo(NodeState.REGISTERED);

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

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Tokens must be provided to force join a node.");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoinUnknownNode() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String newNodeIpWithPort = "127.0.0.5:" + getStoragePort();
        // New node is not registered, it should fail
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forcejoin",
                                                              "-f",
                                                              metadataFile,
                                                              "-ip",
                                                              newNodeIpWithPort,
                                                              "-t",
                                                              metadata.partitioner.getRandomToken().toString(),
                                                              "-t",
                                                              metadata.partitioner.getRandomToken().toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isNotEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains(newNodeIpWithPort)
                                      .contains("No node present with ip address");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoinMovingNode() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        NodeId nodeId = new NodeId(4);
        metadata = startMoving(nodeId, metadata);
        assertThat(metadata.directory.peerState(nodeId)).isEqualTo(NodeState.MOVING);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forcejoin",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId.id()),
                                                              "-t",
                                                              metadata.partitioner.getRandomToken().toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Another sequence of kind MOVE is in progress for node " + nodeId.id() +
                                                ". Cannot proceed with force join.");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoinAlreadyJoinedNode() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        NodeId nodeId = new NodeId(4);
        assertThat(metadata.directory.peerState(nodeId)).isEqualTo(NodeState.JOINED);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forcejoin",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId.id()),
                                                              "-t",
                                                              metadata.partitioner.getRandomToken().toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Node " + nodeId.id() + " is already in JOINED state.");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoinLeftNode() throws IOException
    {
        ClusterMetadata metadata = getFourNodeMetadata();
        int id = 4;
        NodeId nodeId = new NodeId(id);
        metadata = startLeaving(nodeId, metadata);
        UnbootstrapAndLeave unbootstrapAndLeave = (UnbootstrapAndLeave) metadata.inProgressSequences.get(nodeId);
        metadata = unbootstrapAndLeave.applyTo(metadata).success().metadata;
        assertThat(metadata.directory.peerState(nodeId)).isEqualTo(NodeState.LEFT);

        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forcejoin",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId.id()),
                                                              "-t",
                                                              metadata.partitioner.getRandomToken().toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.directory.peerState(nodeId)).isEqualTo(NodeState.JOINED);
        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoinJoiningNodeTokenMismatch() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        int nodeNum = 4;
        metadata = startJoining(nodeNum, metadata);
        NodeId nodeId = new NodeId(nodeNum);
        assertThat(metadata.directory.peerState(nodeId)).isEqualTo(NodeState.BOOTSTRAPPING);

        BootstrapAndJoin bootstrapAndJoin = (BootstrapAndJoin) metadata.inProgressSequences.get(nodeId);
        Set<Token> sequenceTokens = bootstrapAndJoin.finishJoin.tokens;

        // Pick a token that is different from the in-progress sequence tokens
        Token differentToken = metadata.partitioner.getRandomToken();
        while (sequenceTokens.contains(differentToken))
            differentToken = metadata.partitioner.getRandomToken();

        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forcejoin",
                                                              "-f",
                                                              metadataFile,
                                                              "-id",
                                                              String.valueOf(nodeId.id()),
                                                              "-t",
                                                              differentToken.toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("do not match with")
                                      .contains("in progress BootstrapAndJoin sequence tokens");

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoinJoiningNodeTokenMatch() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        int nodeNum = 4;
        metadata = startJoining(nodeNum, metadata);
        NodeId nodeId = new NodeId(nodeNum);
        assertThat(metadata.directory.peerState(nodeId)).isEqualTo(NodeState.BOOTSTRAPPING);

        BootstrapAndJoin bootstrapAndJoin = (BootstrapAndJoin) metadata.inProgressSequences.get(nodeId);
        Set<Token> sequenceTokens = bootstrapAndJoin.finishJoin.tokens;

        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        // Build args: one -t per token in the sequence
        List<String> args = new ArrayList<>(List.of("forcejoin", "-f", metadataFile,
                                                    "-id", String.valueOf(nodeId.id()),
                                                    "-o", outputFile));
        for (Token t : sequenceTokens)
        {
            args.add("-t");
            args.add(t.toString());
        }

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              args.toArray(new String[0]));

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.directory.peerState(nodeId)).isEqualTo(NodeState.JOINED);
        assertThat(outMetadata.tokenMap.tokens(nodeId)).containsAll(sequenceTokens);

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
    public void testForceJoinWithSerializationVersion() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String newNodeIpWithPort = "127.0.0.5:" + getStoragePort();
        InetAddressAndPort newNodeInetAddress = InetAddressAndPort.getByNameUnchecked(newNodeIpWithPort);
        Directory newDirectory = metadata.directory.with(new NodeAddresses(newNodeInetAddress),
                                                         new Location("datacenter1", "rack4"),
                                                         NodeVersion.CURRENT);
        metadata = updateMetadata(metadata, newDirectory);

        NodeId newNodeId = metadata.directory.peerId(newNodeInetAddress);
        assertThat(metadata.directory.states.get(newNodeId)).isEqualTo(NodeState.REGISTERED);

        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";

        // Use V7 — a known valid version older than the current (V8)
        Version targetVersion = Version.V7;

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forcejoin",
                                                              "-f",
                                                              metadataFile,
                                                              "-sv",
                                                              targetVersion.toString(),
                                                              "-id",
                                                              String.valueOf(newNodeId.id()),
                                                              "-t",
                                                              metadata.partitioner.getRandomToken().toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStderr()).isEqualTo(0);
        assertThat(Files.exists(Paths.get(outputFile))).isTrue();

        // Verify that the output file was serialized using the requested version
        try (FileInputStreamPlus fisp = new FileInputStreamPlus(outputFile))
        {
            int versionInt = fisp.readUnsignedVInt32();
            assertThat(Version.fromInt(versionInt)).isEqualTo(targetVersion);
        }

        // Verify the node was force-joined in the output metadata
        ClusterMetadata outMetadata = deserializeMetadata(outputFile);
        assertThat(outMetadata.directory.peerState(newNodeId)).isEqualTo(NodeState.JOINED);

        assertCorrectEnvPostTest();
    }

    @Test
    public void testForceJoinInvalidSerializationVersion() throws IOException
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        String newNodeIpWithPort = "127.0.0.5:" + getStoragePort();
        InetAddressAndPort newNodeInetAddress = InetAddressAndPort.getByNameUnchecked(newNodeIpWithPort);
        Directory newDirectory = metadata.directory.with(new NodeAddresses(newNodeInetAddress),
                                                         new Location("datacenter1", "rack4"),
                                                         NodeVersion.CURRENT);
        metadata = updateMetadata(metadata, newDirectory);

        NodeId newNodeId = metadata.directory.peerId(newNodeInetAddress);
        String metadataFile = dumpMetadata(metadata);
        String outputFile = temporaryFolder.getRoot() + "/metadata-out.dump";
        String invalidSerializationVersion = "-1";

        ToolRunner.ToolResult result = ToolRunner.invokeClass(CMSOfflineTool.class,
                                                              "forcejoin",
                                                              "-f",
                                                              metadataFile,
                                                              "-sv",
                                                              invalidSerializationVersion,
                                                              "-id",
                                                              String.valueOf(newNodeId.id()),
                                                              "-t",
                                                              metadata.partitioner.getRandomToken().toString(),
                                                              "-o",
                                                              outputFile);

        assertThat(result.getExitCode()).withFailMessage(result.getStdout()).isEqualTo(2);
        assertThat(Files.exists(Paths.get(outputFile))).isFalse();
        assertThat(result.getStderr()).contains("Invalid value for option '--serialization-version'");

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

        InetAddressAndPort inetAddressAndPort = InetAddressAndPort.getByNameUnchecked("127.0.0.1:" + getStoragePort());
        InetAddressAndPort i2 = InetAddressAndPort.getByNameUnchecked("127.0.0.2:" + getStoragePort());
        InetAddressAndPort i3 = InetAddressAndPort.getByNameUnchecked("127.0.0.3:" + getStoragePort());

        Range<Token> tokenRange = new Range<>(metadata.partitioner.getMinimumToken(), metadata.partitioner.getTokenFactory().fromString("0"));

        DataPlacement dataPlacement = DataPlacement.builder()
                                                   .withWriteReplica(Epoch.FIRST, Replica.fullReplica(inetAddressAndPort, tokenRange))
                                                   .withWriteReplica(Epoch.FIRST, Replica.fullReplica(i2, tokenRange))
                                                   .withWriteReplica(Epoch.FIRST, Replica.fullReplica(i3, tokenRange))
                                                   .withReadReplica(Epoch.FIRST, Replica.fullReplica(inetAddressAndPort, tokenRange))
                                                   .withReadReplica(Epoch.FIRST, Replica.fullReplica(i2, tokenRange))
                                                   .withReadReplica(Epoch.FIRST, Replica.fullReplica(i3, tokenRange))
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
        "(-9223372036854775808,0]   read    /127.0.0.1:" + getStoragePort() + ", /127.0.0.2:" + getStoragePort() + ", /127.0.0.3:" + getStoragePort() + '\n' +
        "(-9223372036854775808,0]   write   /127.0.0.1:" + getStoragePort() + ", /127.0.0.2:" + getStoragePort() + ", /127.0.0.3:" + getStoragePort() + '\n');

        assertCorrectEnvPostTest();
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

        assertCorrectEnvPostTest();
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

        String expectedSerializationVersion = NodeVersion.CURRENT.serializationVersion().toString();
        assertThat(stdout).isEqualTo(
        "NodeId: 1\n" +
        "  rack                  rack1\n" +
        "  local_port            " + getStoragePort() + '\n' +
        "  broadcast_port        " + getStoragePort() + '\n' +
        "  host_id               6d194555-f6eb-41d0-c000-000000000001\n" +
        "  broadcast_address     /127.0.0.1\n" +
        "  native_address        /127.0.0.1\n" +
        "  native_port           " + getStoragePort() + '\n' +
        "  local_address         /127.0.0.1\n" +
        "  state                 JOINED\n" +
        "  serialization_version " + expectedSerializationVersion + '\n' +
        "  cassandra_version     " + metadata.directory.version(new NodeId(1)).cassandraVersion + '\n' +
        "  dc                    datacenter1\n" +
        "  is_cms_member         true\n" +
        "NodeId: 2\n" +
        "  rack                  rack2\n" +
        "  local_port            " + getStoragePort() + '\n' +
        "  broadcast_port        " + getStoragePort() + '\n' +
        "  host_id               6d194555-f6eb-41d0-c000-000000000002\n" +
        "  broadcast_address     /127.0.0.2\n" +
        "  native_address        /127.0.0.2\n" +
        "  native_port           " + getStoragePort() + '\n' +
        "  local_address         /127.0.0.2\n" +
        "  state                 JOINED\n" +
        "  serialization_version " + expectedSerializationVersion + '\n' +
        "  cassandra_version     " + metadata.directory.version(new NodeId(2)).cassandraVersion + '\n' +
        "  dc                    datacenter1\n" +
        "  is_cms_member         true\n" +
        "NodeId: 3\n" +
        "  rack                  rack3\n" +
        "  local_port            " + getStoragePort() + '\n' +
        "  broadcast_port        " + getStoragePort() + '\n' +
        "  host_id               6d194555-f6eb-41d0-c000-000000000003\n" +
        "  broadcast_address     /127.0.0.3\n" +
        "  native_address        /127.0.0.3\n" +
        "  native_port           " + getStoragePort() + '\n' +
        "  local_address         /127.0.0.3\n" +
        "  state                 JOINED\n" +
        "  serialization_version " + expectedSerializationVersion + '\n' +
        "  cassandra_version     " + metadata.directory.version(new NodeId(3)).cassandraVersion + '\n' +
        "  dc                    datacenter1\n" +
        "  is_cms_member         true\n"
        );

        assertCorrectEnvPostTest();
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

    /**
     * Creates a three-node cluster metadata for testing.
     */

    private ClusterMetadata getThreeNodeClusterMetadata()
    {
        return getThreeNodeClusterMetadata(1);
    }

    private ClusterMetadata getThreeNodeClusterMetadata(int tokenSize)
    {
        IPartitioner partitioner = Murmur3Partitioner.instance;
        NodeId nodeId1 = new NodeId(1);
        NodeId nodeId2 = new NodeId(2);
        NodeId nodeId3 = new NodeId(3);

        InetAddressAndPort addr1 = InetAddressAndPort.getByNameUnchecked("127.0.0.1:" + getStoragePort());
        InetAddressAndPort addr2 = InetAddressAndPort.getByNameUnchecked("127.0.0.2:" + getStoragePort());
        InetAddressAndPort addr3 = InetAddressAndPort.getByNameUnchecked("127.0.0.3:" + getStoragePort());

        NodeVersion nodeVersion = NodeVersion.CURRENT;

        Directory directory =
        new Directory()
        .unsafeWithNodeForTesting(nodeId1, new NodeAddresses(addr1), new Location(DC, "rack1"), nodeVersion)
        .unsafeWithNodeForTesting(nodeId2, new NodeAddresses(addr2), new Location(DC, "rack2"), nodeVersion)
        .unsafeWithNodeForTesting(nodeId3, new NodeAddresses(addr3), new Location(DC, "rack3"), nodeVersion)
        .withRackAndDC(nodeId1)
        .withRackAndDC(nodeId2)
        .withRackAndDC(nodeId3);

        KeyspaceParams metaKsParams = KeyspaceParams.create(true,
                                                            Map.of("class", "MetaStrategy", "datacenter1", "3"));
        KeyspaceMetadata metaKeyspace = KeyspaceMetadata.create(SchemaConstants.METADATA_KEYSPACE_NAME, metaKsParams);
        KeyspaceMetadata normalKeyspace = KeyspaceMetadata.create("ks", KeyspaceParams.simple(3));
        Keyspaces keyspaces = Keyspaces.none().with(metaKeyspace).with(normalKeyspace);

        ClusterMetadata clusterMetadata = getClusterMetadata(keyspaces, partitioner, directory);


        ClusterMetadata metadata = clusterMetadata
                                   .transformer()
                                   .with(directory)
                                   .join(nodeId1)
                                   .proposeToken(nodeId1, getRandomTokens(partitioner, tokenSize))
                                   .join(nodeId2)
                                   .proposeToken(nodeId2, getRandomTokens(partitioner, tokenSize))
                                   .join(nodeId3)
                                   .proposeToken(nodeId3, getRandomTokens(partitioner, tokenSize))
                                   .build().metadata;

        // Create replicas for the metadata keyspace on all three nodes
        ReplicationParams metaParams = ReplicationParams.ntsMeta(Collections.singletonMap(DC, 3));
        DataPlacements placements = DataPlacements.empty().unbuild()
                                                  .with(metaParams, getCMSMemberPlacement(metadata, List.of(addr1, addr2, addr3)))
                                                  .with(ReplicationParams.simple(3), getKeyspacePlacement(metadata, normalKeyspace))
                                                  .build();

        return updateMetadata(metadata, placements);
    }

    List<Token> getRandomTokens(IPartitioner partitioner, int size)
    {
        List<Token> tokens = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            tokens.add(partitioner.getRandomToken());
        }

        return tokens;
    }

    DataPlacement getCMSMemberPlacement(ClusterMetadata clusterMetadata, List<InetAddressAndPort> inetAddressAndPorts)
    {
        IPartitioner partitioner = clusterMetadata.partitioner;
        // Create replicas for the metadata keyspace on all three nodes
        Range<Token> fullRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        DataPlacement.Builder placementBuilder = DataPlacement.builder();
        for (InetAddressAndPort addr : inetAddressAndPorts)
        {
            Replica replica = Replica.fullReplica(addr, fullRange);
            placementBuilder.withReadReplica(Epoch.EMPTY, replica)
                            .withWriteReplica(Epoch.EMPTY, replica);
        }

        return placementBuilder.build();
    }

    DataPlacement getKeyspacePlacement(ClusterMetadata metadata, KeyspaceMetadata keyspaceMetadata)
    {
        UniformRangePlacement uniformRangePlacement = new UniformRangePlacement();
        List<Range<Token>> tokenRanges = uniformRangePlacement.calculateRanges(metadata.tokenMap);

        return keyspaceMetadata.replicationStrategy.calculateDataPlacement(metadata.epoch, tokenRanges, metadata);
    }

    ClusterMetadata getFourNodeMetadata()
    {
        return getFourNodeMetadata(1);
    }

    ClusterMetadata getFourNodeMetadata(int tokenSize)
    {
        ClusterMetadata metadata = getThreeNodeClusterMetadata();
        return addNewNode(metadata, 4, new HashSet<>(getRandomTokens(metadata.partitioner, tokenSize)));
    }

    @SuppressWarnings("SameParameterValue")
    ClusterMetadata addNewNode(ClusterMetadata prev, int newNodeId, Set<Token> tokens)
    {
        NodeId nodeId = new NodeId(newNodeId);
        Directory directory = prev.directory
                              .unsafeWithNodeForTesting(nodeId, getNodeAddresses(newNodeId),
                                                        new Location(DC, "rack" + newNodeId),
                                                        NodeVersion.CURRENT);

        ClusterMetadata metadata = prev.transformer().with(directory).join(nodeId).proposeToken(nodeId, tokens).build().metadata;
        DataPlacements dataPlacements = new UniformRangePlacement()
                                        .calculatePlacements(metadata.epoch.nextEpoch(),
                                                             metadata,
                                                             metadata.schema.getKeyspaces());
        return updateMetadata(metadata, dataPlacements);
    }

    NodeAddresses getNodeAddresses(int num)
    {
        String address = "127.0.0." + num;
        InetAddressAndPort addressAndPort = InetAddressAndPort.getByNameUnchecked(address + ':' + getStoragePort());
        return new NodeAddresses(addressAndPort);
    }

    ClusterMetadata startMoving(NodeId nodeId, ClusterMetadata metadata)
    {
        return startMoving(nodeId, metadata, Set.of(metadata.partitioner.getRandomToken()));
    }

    ClusterMetadata startMoving(NodeId nodeId, ClusterMetadata metadata, Set<Token> tokens)
    {
        PrepareMove prepareMove = new PrepareMove(nodeId, tokens, new UniformRangePlacement(), false);
        metadata = prepareMove.execute(metadata).success().metadata;

        Move moveSequence = (Move) metadata.inProgressSequences.get(nodeId);
        return moveSequence.startMove.execute(metadata).success().metadata;
    }

    ClusterMetadata startJoining(int nodeNum, ClusterMetadata metadata)
    {
        NodeId nodeId = new NodeId(nodeNum);

        if (!metadata.directory.peerIds().contains(nodeId))
        {
            InetAddressAndPort addr = InetAddressAndPort.getByNameUnchecked("127.0.0." + nodeNum + ':' + getStoragePort());

            Location location = new Location(DC, "rack" + nodeNum);
            Register register = new Register(new NodeAddresses(addr), location, NodeVersion.CURRENT);
            metadata = register.execute(metadata).success().metadata;
        }

        PrepareJoin prepareJoin = prepareJoin(nodeId);

        ClusterMetadata updatedMetadata = prepareJoin.execute(metadata).success().metadata;
        BootstrapAndJoin bootstrapAndJoin = (BootstrapAndJoin) updatedMetadata.inProgressSequences.get(nodeId);
        return bootstrapAndJoin.startJoin.execute(updatedMetadata).success().metadata;
    }

    ClusterMetadata startReplacing(NodeId oldNodeId, NodeId newNodeId, ClusterMetadata clusterMetadata)
    {
        Register register = new Register(getNodeAddresses(newNodeId.id()),
                                         new Location(DC, "rack" + newNodeId.id()),
                                         NodeVersion.CURRENT);
        clusterMetadata = register.execute(clusterMetadata).success().metadata;
        PrepareReplace prepareReplace = new PrepareReplace(oldNodeId, newNodeId, new UniformRangePlacement(),
                                                           true, false);
        ClusterMetadata updatedMetadata = prepareReplace.execute(clusterMetadata).success().metadata;
        BootstrapAndReplace replaceSequence = (BootstrapAndReplace) updatedMetadata.inProgressSequences.get(newNodeId);

        updatedMetadata = replaceSequence.startReplace.execute(updatedMetadata).success().metadata;
        return updatedMetadata;
    }

    ClusterMetadata startLeaving(NodeId nodeId, ClusterMetadata metadata)
    {
        PrepareLeave prepareLeave = new PrepareLeave(nodeId, true, new UniformRangePlacement(), LeaveStreams.Kind.UNBOOTSTRAP);
        ClusterMetadata updatedMetadata = prepareLeave.execute(metadata).success().metadata;
        UnbootstrapAndLeave unbootstrapAndLeave = (UnbootstrapAndLeave) updatedMetadata.inProgressSequences.get(nodeId);
        return unbootstrapAndLeave.startLeave.execute(updatedMetadata).success().metadata;
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

    ClusterMetadata deserializeMetadata(String metadataDumpFile) throws IOException
    {
        return ClusterMetadataService.deserializeClusterMetadata(metadataDumpFile);
    }
}
