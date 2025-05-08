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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.airline.ParseException;
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
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration.needsReconfiguration;

/**
 * Offline tool to print or update cluster metadata dump.
 */
public class CMSOfflineTool
{

    private static final String TOOL_NAME = "cmsofflinetool";
    private final Output output;

    public CMSOfflineTool(Output output)
    {
        this.output = output;
    }

    public static void main(String[] args) throws IOException
    {
        //noinspection UseOfSystemOutOrSystemErr
        System.exit(new CMSOfflineTool(new Output(System.out, System.err)).execute(args));
    }

    public int execute(String... args)
    {

        Cli.CliBuilder<ClusterMetadataToolRunnable> builder = Cli.builder(TOOL_NAME);

        List<Class<? extends ClusterMetadataToolRunnable>> commands = new ArrayList<>()
        {{
            add(ClusterMetadataToolHelp.class);
            add(AddToCMS.class);
            add(AssignTokens.class);
            add(Describe.class);
            add(ForceJoin.class);
            add(ForgetNode.class);
            add(PrintDataPlacements.class);
            add(PrintDirectoryCmd.class);
        }};

        builder.withDescription("Offline tool to print or update cluster metadata dump")
               .withDefaultCommand(ClusterMetadataToolHelp.class)
               .withCommands(commands);

        Cli<ClusterMetadataToolRunnable> parser = builder.build();
        int status = 0;
        try
        {
            ClusterMetadataToolRunnable parse = parser.parse(args);
            parse.run(output);
        }
        catch (ParseException pe)
        {
            status = 1;
            badUse(pe);
        }
        catch (Exception e)
        {
            status = 2;
            err(e);
        }
        return status;
    }


    private void badUse(Exception e)
    {
        output.err.println(TOOL_NAME + ": " + e.getMessage());
        output.err.printf("See '%s help' or '%s help <command>'.%n", TOOL_NAME, TOOL_NAME);
    }

    private void err(Exception e)
    {
        output.err.println("error: " + e.getMessage());
        output.err.println("-- StackTrace --");
        output.err.println(getStackTraceAsString(e));
    }


    interface ClusterMetadataToolRunnable
    {
        void run(Output output) throws IOException;
    }

    public static abstract class ClusterMetadataToolCmd implements ClusterMetadataToolRunnable
    {
        @Option(type = OptionType.COMMAND, name = { "-f", "--file" }, description = "Cluster metadata dump file path", required = true)
        protected String metadataDumpPath;

        @Option(type = OptionType.COMMAND, name = { "-sv", "--serialization-version" }, description = "Serialization version to use")
        private Version serializationVersion;


        public ClusterMetadata parseClusterMetadata() throws IOException
        {
            File file = new File(metadataDumpPath);
            if (!file.exists())
            {
                throw new IllegalArgumentException("Cluster metadata dump file " + metadataDumpPath + " does not exist");
            }

            Version serializationVersion = NodeVersion.CURRENT.serializationVersion();
            // Make sure the partitioner we use to manipulate the metadata is the same one used to generate it
            IPartitioner partitioner;
            try (FileInputStreamPlus fisp = new FileInputStreamPlus(metadataDumpPath))
            {
                // skip over the prefix specifying the metadata version
                fisp.readUnsignedVInt32();
                partitioner = ClusterMetadata.Serializer.getPartitioner(fisp, serializationVersion);
            }
            DatabaseDescriptor.toolInitialization();
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            ClusterMetadataService.initializeForTools(false);

            return ClusterMetadataService.deserializeClusterMetadata(metadataDumpPath);
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
                                                    getSerializationVersion());
                output.out.println("Updated cluster metadata written to file " + p.toAbsolutePath());
            }
        }

        Version getSerializationVersion()
        {
            return serializationVersion != null ? serializationVersion : NodeVersion.CURRENT.serializationVersion();
        }
    }

    public static class ClusterMetadataToolHelp extends Help implements ClusterMetadataToolRunnable
    {

        @Override
        public void run(Output output)
        {
            run();
        }
    }

    @Command(name = "addtocms", description = "Makes a node as CMS member")
    public static class AddToCMS extends ClusterMetadataToolCmd
    {
        @Option(name = { "-ip", "--ip-address" }, description = "IP address of node to make CMS", required = true)
        private String ipAddress;

        @Option(type = OptionType.COMMAND, name = { "-o", "--output-file" }, description = "Ouput file path for storing the updated Cluster Metadata")
        private String outputFilePath;

        @Override
        public void run(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            InetAddressAndPort nodeAddress = InetAddressAndPort.getByNameUnchecked(ipAddress);
            metadata = makeCMS(metadata, nodeAddress);
            writeMetadata(output, metadata, outputFilePath);
        }

        ClusterMetadata makeCMS(ClusterMetadata metadata, InetAddressAndPort endpoint)
        {
            ReplicationParams metaParams = ReplicationParams.meta(metadata);
            DataPlacement.Builder builder = metadata.placements.get(metaParams).unbuild();

            Replica newCMS = MetaStrategy.replica(endpoint);
            builder.withReadReplica(metadata.epoch, newCMS)
                   .withWriteReplica(metadata.epoch, newCMS);
            return metadata.transformer().with(metadata.placements.unbuild().with(metaParams,
                                                                                  builder.build())
                                                                  .build())
                           .build().metadata;
        }
    }

    @Command(name = "assigntokens", description = "Assigns a token for given instance")
    public static class AssignTokens extends ClusterMetadataToolCmd
    {
        @Option(name = { "-ip", "--ip-address" }, description = "IP address of endpoint. Port can be specified as well.", required = true)
        private String ip;

        @Option(name = { "-t", "--token" }, description = "Token to assign. Pass it multiple times to assign multiple tokens to node.", required = true)
        private List<String> tokenList = new ArrayList<>();

        @Option(type = OptionType.COMMAND, name = { "-o", "--output-file" }, description = "Ouput file path for storing the updated Cluster Metadata")
        private String outputFilePath;


        @Override
        public void run(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();

            InetAddressAndPort nodeAddress = InetAddressAndPort.getByNameUnchecked(ip);
            NodeId nodeId = metadata.directory.peerId(nodeAddress);
            if (nodeId == null)
            {
                throw new IllegalArgumentException("Cassandra node with address " + ip + " does not exist.");
            }

            Token.TokenFactory tokenFactory = metadata.partitioner.getTokenFactory();
            List<Token> tokens = tokenList.stream().map(tokenFactory::fromString).collect(Collectors.toList());
            ClusterMetadata updateMetadata = metadata.transformer().proposeToken(nodeId, tokens).build().metadata;
            writeMetadata(output, updateMetadata, outputFilePath);
        }
    }

    @Command(name = "describe", description = "Describes the cluster metadata")
    public static class Describe extends ClusterMetadataToolCmd
    {
        @Override
        public void run(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            String members = metadata.fullCMSMembers().stream().sorted().map(Object::toString).collect(Collectors.joining(","));
            output.out.printf("Cluster Metadata Service:%n");
            output.out.printf("Members: %s%n", members);
            output.out.printf("Needs reconfiguration: %s%n", needsReconfiguration(metadata));
            output.out.printf("Service State: %s%n", ClusterMetadataService.state(metadata));
            output.out.printf("Epoch: %s%n", metadata.epoch.getEpoch());
            output.out.printf("Replication factor: %s%n", ReplicationParams.meta(metadata).toString());
        }
    }

    @Command(name = "forcejoin", description = "Forces a node to move to JOINED stated")
    public static class ForceJoin extends ClusterMetadataToolCmd
    {
        @Option(name = { "-id", "--node-id" }, description = "Node ID. It can be integer ID assigned to node or the node uuid", required = true)
        private String id;

        @Option(type = OptionType.COMMAND, name = { "-o", "--output-file" }, description = "Ouput file path for storing the updated Cluster Metadata")
        private String outputFilePath;

        @Override
        public void run(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            NodeId nodeId = NodeId.fromString(id);

            if (!metadata.directory.peerIds().contains(nodeId))
            {
                throw new IllegalArgumentException("Node with id " + id + " does not exist.");
            }

            ClusterMetadata updatedMetadata = metadata.transformer().join(nodeId).build().metadata;
            writeMetadata(output, updatedMetadata, outputFilePath);
        }
    }

    @Command(name = "forgetnode", description = "Removes a nodes from given cluster metadata")
    public static class ForgetNode extends ClusterMetadataToolCmd
    {
        @Option(name = { "-id", "--node-id" }, description = "Node ID to forget. It can be UUID of node as well.", required = true)
        private String id;

        @Option(type = OptionType.COMMAND, name = { "-o", "--output-file" }, description = "Ouput file path for storing the updated Cluster Metadata")
        private String outputFilePath;

        @Override
        public void run(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();
            NodeId nodeId = NodeId.fromString(id);

            if (!metadata.directory.peerIds().contains(nodeId))
            {
                throw new IllegalArgumentException("Node with id " + id + " does not exist.");
            }

            ClusterMetadata updatedMetadata = metadata.transformer().unregister(nodeId).build().metadata;
            output.out.println("Successfully forgot node having id " + id);
            writeMetadata(output, updatedMetadata, outputFilePath);
        }
    }

    @Command(name = "printdirectory", description = "Prints directory information in cluster metadata file")
    public static class PrintDirectoryCmd extends ClusterMetadataToolCmd
    {

        @Override
        public void run(Output output) throws IOException
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

    @Command(name = "printdataplacements", description = "Prints data placements in cluster medata file")
    public static class PrintDataPlacements extends ClusterMetadataToolCmd
    {
        @Option(name = { "-ks", "--keyspace" }, description = "Keyspace to use for printing data placements", required = true)
        private String keyspace;

        @Override
        public void run(Output output) throws IOException
        {
            ClusterMetadata metadata = parseClusterMetadata();

            KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaces().getNullable(keyspace);
            if (keyspaceMetadata == null)
            {
                throw new IllegalArgumentException("Keyspace " + keyspace + " not found in cluster metadata");
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
