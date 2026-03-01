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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.Assassinate;
import org.apache.cassandra.tcm.transformations.CancelInProgressSequence;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
import org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration;
import org.apache.cassandra.utils.FBUtilities;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration.needsReconfiguration;

/**
 * Offline tool to print or update cluster metadata stored in a dump file.
 * <p>
 * The tool operates entirely offline: it reads a metadata dump file produced by
 * {@code nodetool cms dump} (or equivalent), applies the requested transformation,
 * and writes the result to a new file. The original dump file is never modified.
 * <p>
 * Run without a subcommand to print usage information.
 */
@SuppressWarnings({ "unused", "DefaultAnnotationParam", "UseOfSystemOutOrSystemErr" })
@Command(name = "cmsofflinetool",
mixinStandardHelpOptions = true,
description = "Offline tool to print or update cluster metadata dump.",
subcommands = { CMSOfflineTool.AbortBootstrap.class,
                CMSOfflineTool.AbortDecommission.class,
                CMSOfflineTool.AbortMove.class,
                CMSOfflineTool.AssassinateNode.class,
                CMSOfflineTool.Describe.class,
                CMSOfflineTool.ForceJoin.class,
                CommandLine.HelpCommand.class,
                CMSOfflineTool.MoveToken.class,
                CMSOfflineTool.Print.class,
                CMSOfflineTool.PrintDataPlacements.class,
                CMSOfflineTool.PrintDirectoryCmd.class,
                CMSOfflineTool.ResetCMS.class })
public class CMSOfflineTool implements Runnable
{
    private final Output output;

    public CMSOfflineTool(Output output)
    {
        this.output = output;
    }

    public static void main(String[] args) throws IOException
    {
        CMSOfflineTool tool = new CMSOfflineTool(new Output(System.out, System.err));
        CommandLine cli = new CommandLine(tool)
                          .setColorScheme(CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.OFF))
                          .setExecutionExceptionHandler((ex, cmd, parseResult) -> {
                              cmd.getErr().println("Error: " + ex.getMessage());
                              cmd.getErr().println("-- StackTrace --");
                              cmd.getErr().println(getStackTraceAsString(ex));
                              return 2;
                          });
        int status = cli.execute(args);
        System.exit(status);
    }

    @Override
    public void run()
    {
        CommandLine.usage(this, output.out, CommandLine.Help.Ansi.OFF);
    }

    public static abstract class ClusterMetadataToolCmd implements Runnable
    {
        @Option(names = { "-f", "--file" }, description = "Cluster metadata dump file path.", required = true)
        protected String metadataDumpFile;

        @Option(names = { "-sv", "--serialization-version" }, description = "Serialization version to use.")
        private Version serializationVersion;

        @CommandLine.ParentCommand
        private CMSOfflineTool parent;

        @Override
        public void run()
        {
            try
            {
                execute(parent.output);
                parent.output.out.flush();
                parent.output.err.flush();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        protected abstract void execute(Output output) throws IOException;

        public ClusterMetadata parseClusterMetadata() throws IOException
        {
            File file = new File(metadataDumpFile);
            if (!file.exists())
            {
                throw new IllegalArgumentException("Cluster metadata dump file " + metadataDumpFile + " does not exist.");
            }

            // Make sure the partitioner we use to manipulate the metadata is the same one used to generate it
            IPartitioner partitioner;
            try (FileInputStreamPlus fisp = new FileInputStreamPlus(metadataDumpFile))
            {
                int x = fisp.readUnsignedVInt32();
                Version version = Version.fromInt(x);
                partitioner = ClusterMetadata.Serializer.getPartitioner(fisp, version);
            }
            DatabaseDescriptor.toolInitialization();
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            ClusterMetadataService.initializeForTools(false);

            return ClusterMetadataService.deserializeClusterMetadata(metadataDumpFile);
        }

        public void writeMetadata(Output output, ClusterMetadata metadata, String outputFilePath) throws IOException
        {
            Path p = outputFilePath != null ?
                     Files.createFile(Path.of(outputFilePath)) :
                     Files.createTempFile("clustermetadata", "dump");

            try (FileOutputStreamPlus out = new FileOutputStreamPlus(p))
            {
                VerboseMetadataSerializer.serialize(ClusterMetadata.serializer,
                                                    metadata,
                                                    out,
                                                    getSerializationVersion(metadata));
                output.out.println("Updated cluster metadata written to file " + p.toAbsolutePath());
            }
        }

        Version getSerializationVersion(ClusterMetadata metadata)
        {
            Version currentVersion = NodeVersion.CURRENT.serializationVersion();
            if (serializationVersion != null)
            {
                // Not a good idea to write metadata using lower serialization version. Ref: CASSANDRA-21174
                if (currentVersion.isBefore(serializationVersion))
                {
                    throw new IllegalArgumentException("Given serialization version " + serializationVersion +
                                                       " is newer compared to current version " + currentVersion + '.');
                }

                return serializationVersion;
            }

            Version metadataVersion = metadata.directory.commonSerializationVersion;
            if (!currentVersion.isAtLeast(metadataVersion))
            {
                throw new IllegalArgumentException("Current version " + currentVersion +
                                                   " is older than version in cluster metadata (" +
                                                   metadataVersion + "). Cannot proceed further. " +
                                                   "Try modifying cluster metadata using binaries that support " +
                                                   "minimum serialization version: " + metadataVersion + '.');
            }

            return metadataVersion;
        }
    }

    /**
     * Cancels a JOIN or REPLACE bootstrap sequence for the given node and unregisters it
     * from the cluster. Use this when a node is stuck in bootstrapping or replacement.
     * Fails if no in-progress sequence exists, or if the sequence is not of kind JOIN or REPLACE.
     */
    @Command(name = "abortbootstrap",
    description = "Aborts bootstrap for given node if in progress.")
    public static class AbortBootstrap extends ClusterMetadataToolCmd
    {
        @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
        NodeIdentifierOption nodeIdentifierOption;

        @Option(names = { "-o", "--output-file" },
        description = "Output file path for storing the updated cluster metadata.")
        private String outputFilePath;

        @Override
        protected void execute(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            NodeId nodeId = nodeIdentifierOption.getNodeId(metadata);
            if (!metadata.inProgressSequences.contains(nodeId))
            {
                throw new IllegalArgumentException("Did not find any sequences in progress for node " +
                                                   nodeIdentifierOption.getNodeIpOrId() + '.');
            }

            MultiStepOperation<?> multiStepOperation = metadata.inProgressSequences.get(nodeId);
            MultiStepOperation.Kind sequenceKind = multiStepOperation.kind();
            if (sequenceKind != MultiStepOperation.Kind.JOIN
                && sequenceKind != MultiStepOperation.Kind.REPLACE)
            {
                throw new IllegalArgumentException("abortbootstrap is not a valid operation when sequence of kind " +
                                                   sequenceKind + " is in progress.");
            }

            CancelInProgressSequence cancelSequence = new CancelInProgressSequence(nodeId);
            ClusterMetadata updatedMetadata = cancelSequence.execute(metadata).success().metadata;

            // Cancelling the sequence is not enough, but we need to unregister as well
            Unregister unregister = new Unregister(nodeId, EnumSet.of(NodeState.REGISTERED),
                                                   ClusterMetadataService.instance().placementProvider());

            updatedMetadata = unregister.execute(updatedMetadata).success().metadata;

            writeMetadata(output, updatedMetadata, outputFilePath);
        }
    }

    /**
     * Cancels an in-progress MOVE sequence for the given node, returning it to its
     * pre-move token assignment. Fails if no in-progress sequence exists, or if the
     * sequence is not of kind MOVE.
     */
    @Command(name = "abortmove", description = "Aborts in progress move sequence for given node.")
    static class AbortMove extends ClusterMetadataToolCmd
    {
        @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
        NodeIdentifierOption nodeIdentifierOption;

        @Option(names = { "-o", "--output-file" },
        description = "Output file path for storing the updated Cluster Metadata.")
        private String outputFilePath;

        @Override
        protected void execute(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            NodeId nodeId = nodeIdentifierOption.getNodeId(metadata);

            MultiStepOperation<?> multiStepOperation = metadata.inProgressSequences.get(nodeId);
            if (multiStepOperation == null)
            {
                throw new IllegalArgumentException("No transformation sequence is in progress for " +
                                                   nodeIdentifierOption.getNodeIpOrId() + '.');
            }

            if (multiStepOperation.kind() != MultiStepOperation.Kind.MOVE)
            {
                throw new IllegalArgumentException("Multi step operation of kind " + multiStepOperation.kind() +
                                                   " is in progress for node " + nodeIdentifierOption.getNodeIpOrId() +
                                                   ". Cannot proceed with abort move.");
            }

            CancelInProgressSequence cancelSequence = new CancelInProgressSequence(nodeId);
            ClusterMetadata updatedMetadata = cancelSequence.execute(metadata).success().metadata;
            writeMetadata(output, updatedMetadata, outputFilePath);
        }
    }

    /**
     * Cancels an in-progress LEAVE (decommission) sequence for the given node, keeping
     * it as an active member of the ring. Fails if no in-progress sequence exists, or if
     * the sequence is not of kind LEAVE.
     */
    @Command(name = "abortdecommission", description = "Aborts in progress decommission sequence for given node.")
    static class AbortDecommission extends ClusterMetadataToolCmd
    {
        @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
        NodeIdentifierOption nodeIdentifierOption;

        @Option(names = { "-o", "--output-file" },
        description = "Output file path for storing the updated Cluster Metadata.")
        private String outputFilePath;

        @Override
        protected void execute(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            NodeId nodeId = nodeIdentifierOption.getNodeId(metadata);

            MultiStepOperation<?> multiStepOperation = metadata.inProgressSequences.get(nodeId);
            if (multiStepOperation == null)
            {
                throw new IllegalArgumentException("No transformation sequence is in progress for " +
                                                   nodeIdentifierOption.getNodeIpOrId() + '.');
            }

            if (multiStepOperation.kind() != MultiStepOperation.Kind.LEAVE)
            {
                throw new IllegalArgumentException("Multi step operation of kind " + multiStepOperation.kind() +
                                                   " is in progress for node " + nodeIdentifierOption.getNodeIpOrId() +
                                                   ". Cannot proceed with abort decommission.");
            }

            CancelInProgressSequence cancelSequence = new CancelInProgressSequence(nodeId);
            ClusterMetadata updatedMetadata = cancelSequence.execute(metadata).success().metadata;
            writeMetadata(output, updatedMetadata, outputFilePath);
        }
    }

    /**
     * Removes a node from cluster metadata by applying the {@link Assassinate} transformation.
     * If a MOVE or LEAVE sequence is in progress for the node, it is cancelled first.
     * If the node is a CMS member, it is removed from CMS before assassination.
     * Fails if the node is in a JOIN or REPLACE sequence; use {@code abortbootstrap} instead.
     */
    @Command(name = "assassinate", description = "Assassinates given node from Cluster metadata.")
    static class AssassinateNode extends ClusterMetadataToolCmd
    {
        private final EnumSet<MultiStepOperation.Kind> supportedCancelSequences =
        EnumSet.of(MultiStepOperation.Kind.MOVE, MultiStepOperation.Kind.LEAVE);
        @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
        NodeIdentifierOption nodeIdentifierOption;
        @Option(names = { "-o", "--output-file" },
        description = "Output file path for storing the updated Cluster Metadata.")
        private String outputFilePath;

        @Override
        protected void execute(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            NodeId nodeId = nodeIdentifierOption.getNodeId(metadata);

            // Check if there are any in-progress sequences for given node
            // If any, then cancel the sequence and then assassinate it
            if (metadata.inProgressSequences.contains(nodeId))
            {
                MultiStepOperation<?> multiStepOperation = metadata.inProgressSequences.get(nodeId);
                MultiStepOperation.Kind sequenceKind = multiStepOperation.kind();
                if (!supportedCancelSequences.contains(sequenceKind))
                {
                    if (sequenceKind == MultiStepOperation.Kind.JOIN || sequenceKind == MultiStepOperation.Kind.REPLACE)
                    {
                        throw new IllegalArgumentException("Cannot assassinate the node when sequence of kind " +
                                                           sequenceKind + " is in progress. " +
                                                           "Run abortbootstrap instead.");
                    }
                    else
                    {
                        throw new IllegalArgumentException("Cannot assassinate the node when sequence of kind " +
                                                           sequenceKind + " is in progress.");
                    }
                }

                output.out.printf("Cancelling in-progress sequence of kind %s before assassinating node.\n",
                                  metadata.inProgressSequences.get(nodeId).kind());
                metadata = new CancelInProgressSequence(nodeId).execute(metadata).success().metadata;
            }

            metadata = maybeReconfigureCMS(metadata, nodeId);
            Assassinate transformation = new Assassinate(nodeId, ClusterMetadataService.instance().placementProvider());
            ClusterMetadata updatedMetadata = transformation.execute(metadata).success().metadata;

            writeMetadata(output, updatedMetadata, outputFilePath);
        }

        ClusterMetadata maybeReconfigureCMS(ClusterMetadata metadata, NodeId nodeId)
        {
            InetAddressAndPort addressAndPort = metadata.directory.endpoint(nodeId);
            if (!metadata.isCMSMember(addressAndPort))
            {
                return metadata;
            }

            // Ref: org.apache.cassandra.tcm.sequences.ReconfigureCMS.maybeReconfigureCMS
            PrepareCMSReconfiguration.Simple transformation = new PrepareCMSReconfiguration.Simple(nodeId, Set.of());
            ClusterMetadata updatedMetadata = transformation.execute(metadata).success().metadata;
            updatedMetadata = updatedMetadata.inProgressSequences.get(ReconfigureCMS.SequenceKey.instance)
                                                                 .applyTo(updatedMetadata).success().metadata;
            if (updatedMetadata.isCMSMember(addressAndPort))
            {
                throw new IllegalStateException("Could not remove node " + nodeIdentifierOption.getNodeIpOrId() +
                                                " from CMS.");
            }
            return updatedMetadata;
        }
    }

    /**
     * Replaces the entire CMS membership with a single node. All existing CMS replicas are
     * removed and the specified node becomes the sole CMS member. Useful for recovering from
     * a state where the CMS is unreachable.
     */
    @Command(name = "resetcms",
    description = "Replaces all CMS members with the specified node. WARNING: all existing CMS replicas are removed.")
    static class ResetCMS extends ClusterMetadataToolCmd
    {
        @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
        NodeIdentifierOption nodeIdentifierOption;

        @Option(names = { "-o", "--output-file" },
        description = "Output file path for storing the updated Cluster Metadata.")
        private String outputFilePath;

        @Override
        public void execute(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            NodeId nodeId = nodeIdentifierOption.getNodeId(metadata);
            InetAddressAndPort nodeAddress = metadata.directory.getNodeAddresses(nodeId).broadcastAddress;
            metadata = resetCMS(metadata, nodeAddress);
            writeMetadata(output, metadata, outputFilePath);
        }

        ClusterMetadata resetCMS(ClusterMetadata metadata, InetAddressAndPort endpoint)
        {
            ReplicationParams metaParams = ReplicationParams.meta(metadata);
            Iterable<Replica> currentReplicas = metadata.placements.get(metaParams).writes.byEndpoint().flattenValues();
            DataPlacement.Builder placementBuilder = metadata.placements.get(metaParams).unbuild();
            for (Replica replica : currentReplicas)
            {
                placementBuilder.withoutReadReplica(metadata.epoch, replica)
                                .withoutWriteReplica(metadata.epoch, replica);
            }

            Replica newCMS = MetaStrategy.replica(endpoint);
            placementBuilder.withReadReplica(metadata.epoch, newCMS)
                            .withWriteReplica(metadata.epoch, newCMS);

            return metadata.transformer()
                           .with(metadata.placements.unbuild()
                                                    .with(metaParams, placementBuilder.build())
                                                    .build())
                           .build().metadata;
        }
    }

    /**
     * Moves a node to a new token. Only supports single-token nodes.
     * If a MOVE sequence is already in progress for the node, it is completed rather than started fresh;
     * in that case the provided token must match the token of the in-progress sequence.
     */
    @Command(name = "move",
    description = "Moves node to given token. Works only for cluster having nodes with single token.")
    static class MoveToken extends ClusterMetadataToolCmd
    {
        @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
        NodeIdentifierOption nodeIdentifierOption;

        @Option(names = { "-t", "--token" },
        description = "Token to assign.")
        private String token;

        @Option(names = { "-o", "--output-file" },
        description = "Output file path for storing the updated Cluster Metadata.")
        private String outputFilePath;

        @Override
        protected void execute(Output output) throws IOException
        {
            // Took the reference from org.apache.cassandra.tcm.sequences.SingleNodeSequences.move
            ClusterMetadata metadata = parseClusterMetadata();
            NodeId nodeId = nodeIdentifierOption.getNodeId(metadata);

            if (metadata.inProgressSequences.contains(nodeId))
            {
                ClusterMetadata updatedMetadata = finishInProgressSequence(nodeId, token, metadata);
                writeMetadata(output, updatedMetadata, outputFilePath);
                return;
            }

            if (null == token)
            {
                throw new IllegalArgumentException("Token required when no MOVE sequence is in progress.");
            }
            metadata.partitioner.getTokenFactory().validate(token);
            Token toToken = metadata.partitioner.getTokenFactory().fromString(token);
            if (metadata.tokenMap.tokens().contains(toToken))
            {
                throw new IllegalArgumentException("Target token " + toToken + " is already owned by another node.");
            }
            if (metadata.tokenMap.tokens(nodeId).size() > 1)
            {
                throw new UnsupportedOperationException("This node has more than one token and cannot be moved thusly.");
            }

            PrepareMove prepareMove = new PrepareMove(nodeId, Set.of(toToken), new UniformRangePlacement(), false);
            ClusterMetadata updatedMetadata = prepareMove.execute(metadata).success().metadata;
            updatedMetadata = updatedMetadata.inProgressSequences.get(nodeId).applyTo(updatedMetadata).success().metadata;
            writeMetadata(output, updatedMetadata, outputFilePath);
        }

        ClusterMetadata finishInProgressSequence(NodeId nodeId, String token, ClusterMetadata metadata)
        {
            MultiStepOperation<?> multiStepOperation = metadata.inProgressSequences.get(nodeId);
            if (multiStepOperation.kind() != MultiStepOperation.Kind.MOVE)
            {
                throw new IllegalArgumentException("Another sequence of kind " + multiStepOperation.kind() +
                                                   " is in progress for node " + nodeIdentifierOption.getNodeIpOrId() +
                                                   ". Cannot proceed with move.");
            }

            Move moveInProgress = (Move) multiStepOperation;
            Collection<Token> inProgressTokens = moveInProgress.tokens;
            Collection<Token> givenTokenSet = token == null
                                              ? Set.of()
                                              : Set.of(metadata.partitioner.getTokenFactory().fromString(token));
            if (givenTokenSet.isEmpty() || new HashSet<>(moveInProgress.tokens).equals(givenTokenSet))
            {
                return moveInProgress.applyTo(metadata).success().metadata;
            }

            throw new IllegalArgumentException("Move in progress for another token(s) " + inProgressTokens + '.');
        }
    }

    /**
     * Identifies a target node using either its IP address or its integer/UUID node ID.
     * Exactly one of {@code -ip} or {@code -id} must be provided.
     */
    static class NodeIdentifierOption
    {
        @Option(names = { "-ip", "--ip-address" }, required = true,
        description = "IP address of the target endpoint. Port can be optionally specified " +
                      "using a colon after the IP address (e.g., 127.0.0.1:9042).")
        private String ip;

        @Option(names = { "-id", "--node-id" }, required = true,
        description = "Node ID. It can be integer ID assigned to node or the node uuid.")
        private String id;

        String getNodeIpOrId()
        {
            return ip != null ? ip : id;
        }

        NodeId getNodeId(ClusterMetadata metadata)
        {
            if (id != null)
            {
                NodeId nodeId = NodeId.fromString(id);
                if (!metadata.directory.peerIds().contains(nodeId))
                {
                    throw new IllegalArgumentException("No node present with id " + id +
                                                       " in the given cluster metadata.");
                }
                return nodeId;
            }
            else if (ip != null)
            {
                InetAddressAndPort nodeAddress = InetAddressAndPort.getByNameUnchecked(ip);
                NodeId nodeId = metadata.directory.peerId(nodeAddress);

                if (null == nodeId)
                {
                    throw new IllegalArgumentException("No node present with ip address " + ip +
                                                       " in given cluster metadata.");
                }
                return nodeId;
            }

            throw new IllegalArgumentException("Neither node id nor ip address specified to fetch NodeId from metadata.");
        }
    }


    /**
     * Prints a high-level summary of the CMS state: members, epoch, replication factor,
     * service state, and whether reconfiguration is needed.
     */
    @Command(name = "describe", description = "Describes the cluster metadata.")
    static class Describe extends ClusterMetadataToolCmd
    {
        @Override
        public void execute(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            String members = metadata.fullCMSMembers()
                                     .stream()
                                     .sorted()
                                     .map(Object::toString)
                                     .collect(Collectors.joining(","));

            output.out.printf("Cluster Metadata Service:%n");
            output.out.printf("Members: %s%n", members);
            output.out.printf("Needs reconfiguration: %s%n", needsReconfiguration(metadata));
            output.out.printf("Service State: %s%n", ClusterMetadataService.state(metadata));
            output.out.printf("Epoch: %s%n", metadata.epoch.getEpoch());
            output.out.printf("Replication factor: %s%n", ReplicationParams.meta(metadata).toString());
        }
    }

    /**
     * Forces a node directly to JOINED state, bypassing the normal bootstrap sequence.
     * <ul>
     *   <li>If the node has an in-progress JOIN sequence, it is completed; provided tokens
     *       must match the sequence tokens (or be omitted to use the sequence tokens).</li>
     *   <li>If no sequence is in progress, tokens must be explicitly provided and
     *       an {@link UnsafeJoin} is applied.</li>
     * </ul>
     * Fails if the node is already JOINED, or if a non-JOIN sequence is in progress.
     */
    @Command(name = "forcejoin", description = "Forces a node to move to JOINED state.")
    static class ForceJoin extends ClusterMetadataToolCmd
    {
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        @Option(names = { "-t", "--token" },
        description = "Token to assign. Pass it multiple times to assign multiple tokens to node.")
        private final List<String> tokens = new ArrayList<>();
        @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
        NodeIdentifierOption nodeIdentifierOption;
        @Option(names = { "-o", "--output-file" },
        description = "Output file path for storing the updated Cluster Metadata.")
        private String outputFilePath;

        @Override
        public void execute(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            NodeId nodeId = nodeIdentifierOption.getNodeId(metadata);
            Set<Token> tokenSet = new HashSet<>(tokens.size());
            Token.TokenFactory tokenFactory = metadata.partitioner.getTokenFactory();
            tokens.forEach(t -> tokenSet.add(tokenFactory.fromString(t)));

            NodeState nodeState = metadata.directory.peerState(nodeId);
            if (nodeState == NodeState.JOINED)
            {
                throw new IllegalArgumentException("Node " + nodeIdentifierOption.getNodeIpOrId() +
                                                   " is already in JOINED state.");
            }

            ClusterMetadata updatedMetadata;
            if (metadata.inProgressSequences.get(nodeId) != null)
            {
                MultiStepOperation<?> multiStepOperation = metadata.inProgressSequences.get(nodeId);
                if (multiStepOperation.kind() != MultiStepOperation.Kind.JOIN)
                {
                    throw new IllegalArgumentException("Another sequence of kind " + multiStepOperation.kind() +
                                                       " is in progress for node " + nodeIdentifierOption.getNodeIpOrId() +
                                                       ". Cannot proceed with force join.");
                }
                BootstrapAndJoin bootstrapAndJoin = (BootstrapAndJoin) multiStepOperation;
                Set<Token> sequenceTokens = bootstrapAndJoin.finishJoin.tokens;
                if (tokens.isEmpty()
                    || (tokenSet.size() == sequenceTokens.size() && sequenceTokens.containsAll(tokenSet)))
                {
                    updatedMetadata = bootstrapAndJoin.applyTo(metadata).success().metadata;
                }
                else
                {
                    // If tokens are provided, then it should match with the in-progress sequence tokens
                    throw new IllegalArgumentException("The tokens provided " + tokens + " do not match with " +
                                                       " in progress BootstrapAndJoin sequence tokens " +
                                                       sequenceTokens + ". Cannot proceed further.");
                }
            }
            else
            {
                // There are no in-progress sequences, force join by using UnsafeJoin transformation
                if (tokenSet.isEmpty())
                {
                    throw new IllegalArgumentException("Tokens must be provided to force join a node.");
                }
                UnsafeJoin unsafeJoin = new UnsafeJoin(nodeId, tokenSet, new UniformRangePlacement());
                updatedMetadata = unsafeJoin.execute(metadata).success().metadata;
            }

            writeMetadata(output, updatedMetadata, outputFilePath);
        }
    }

    /**
     * Prints the full {@code toString()} representation of the cluster metadata to stdout.
     * Useful for a quick human-readable overview of the entire metadata state.
     * <p>
     * <b>Note:</b> The output format is not stable and may change between versions.
     * Do not rely on it for programmatic parsing.
     */
    @Command(name = "print", description = "Prints string output of the cluster metadata. " +
                                           "Output format is subject to change and should not be relied on for parsing.")
    static class Print extends ClusterMetadataToolCmd
    {
        @Override
        protected void execute(Output output) throws IOException
        {
            // It supports only toString output of the ClusterMetadata for now
            ClusterMetadata metadata = parseClusterMetadata();
            output.out.println(metadata);
        }
    }

    /**
     * Prints per-node directory information: addresses, ports, rack, DC, state,
     * serialization version, and CMS membership status.
     */
    @Command(name = "printdirectory", description = "Prints directory information in cluster metadata file.")
    static class PrintDirectoryCmd extends ClusterMetadataToolCmd
    {
        @Override
        public void execute(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            Directory directory = metadata.directory;
            Set<NodeId> nodeIdList = directory.peerIds();
            for (NodeId nodeId : nodeIdList)
            {
                NodeAddresses nodeAddresses = directory.getNodeAddresses(nodeId);
                Location location = directory.location(nodeId);
                output.out.println("NodeId: " + nodeId.id());
                String format = "  %-22s%s\n";
                output.out.printf(format, "rack", location.rack);
                output.out.printf(format, "local_port", nodeAddresses.localAddress.getPort());
                output.out.printf(format, "broadcast_port", nodeAddresses.broadcastAddress.getPort());
                output.out.printf(format, "host_id", nodeId.toUUID());
                output.out.printf(format, "broadcast_address", nodeAddresses.broadcastAddress.getAddress().toString());
                output.out.printf(format, "native_address", nodeAddresses.nativeAddress.getAddress().toString());
                output.out.printf(format, "native_port", nodeAddresses.nativeAddress.getPort());
                output.out.printf(format, "local_address", nodeAddresses.localAddress.getAddress().toString());
                output.out.printf(format, "state", directory.peerState(nodeId));
                output.out.printf(format, "serialization_version", directory.version(nodeId).serializationVersion());
                output.out.printf(format, "cassandra_version", directory.version(nodeId).cassandraVersion);
                output.out.printf(format, "dc", location.datacenter);
                output.out.printf(format, "is_cms_member", metadata.isCMSMember(nodeAddresses.broadcastAddress));
            }
        }
    }

    /**
     * Prints read and write replica placements for a specific keyspace, sorted by token range.
     * Requires the {@code -ks} option to specify the target keyspace.
     */
    @Command(name = "printdataplacements", description = "Prints data placements in cluster metadata file.")
    static class PrintDataPlacements extends ClusterMetadataToolCmd
    {
        @Option(names = { "-ks", "--keyspace" }, required = true,
        description = "Keyspace to use for printing data placements.")
        private String keyspace;

        @Override
        public void execute(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();

            KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaces().getNullable(keyspace);
            if (keyspaceMetadata == null)
            {
                throw new IllegalArgumentException("Keyspace " + keyspace + " not found in cluster metadata.");
            }

            DataPlacement placement = metadata.placements.get(keyspaceMetadata.params.replication);
            List<Object[]> rows = new ArrayList<>();
            rows.addAll(replicaGroupsToRows(placement.reads, "read"));
            rows.addAll(replicaGroupsToRows(placement.writes, "write"));

            rows.sort((o1, o2) -> {
                Range<Token> left = (Range<Token>) o1[0];
                Range<Token> right = (Range<Token>) o2[0];
                return left.compareTo(right);
            });

            int rangeMaxLength = 0;
            for (Object[] objects : rows)
            {
                rangeMaxLength = Math.max(rangeMaxLength, objects[0].toString().length());
            }

            String rowFormat = String.format("%%-%ds %%-7s %%s\n", (rangeMaxLength + 2));
            output.out.printf(rowFormat, "Token Range", "Type", "Endpoints");

            rows.forEach(row -> output.out.printf(rowFormat, row));
        }

        List<Object[]> replicaGroupsToRows(ReplicaGroups replicaGroups, String replicaGroupType)
        {
            List<Object[]> rows = new ArrayList<>();
            replicaGroups.forEach(((tokenRange, forRange) -> {
                Range<Token> range = forRange.get().range();
                List<String> endpoints = new ArrayList<>(forRange.get().size());
                forRange.get().forEach(replica -> endpoints.add(replica.endpoint().toString()));
                String addresses = String.join(", ", endpoints);
                rows.add(new Object[]{ range, replicaGroupType, addresses });
            }));
            return rows;
        }
    }

    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }
}
