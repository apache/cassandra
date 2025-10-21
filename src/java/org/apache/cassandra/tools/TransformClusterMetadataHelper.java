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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class TransformClusterMetadataHelper
{
    public static void main(String ... args) throws IOException
    {
        if (args.length < 2)
        {
            System.err.println("Usage: addtocmstool <path to dumped metadata> <ip of host to make CMS> [<serialization version>]");
            System.exit(1);
        }
        String sourceFile = args[0];
        Version serializationVersion = NodeVersion.CURRENT.serializationVersion();
        if (args.length > 2)
            serializationVersion = Version.valueOf(args[2]);

        // Make sure the partitioner we use to manipulate the metadata is the same one used to generate it
        IPartitioner partitioner = null;
        try (FileInputStreamPlus fisp = new FileInputStreamPlus(sourceFile))
        {
            // skip over the prefix specifying the metadata version
            fisp.readUnsignedVInt32();
            partitioner = ClusterMetadata.Serializer.getPartitioner(fisp, serializationVersion);
        }
        DatabaseDescriptor.toolInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(partitioner);
        ClusterMetadataService.initializeForTools(false);
        ClusterMetadata metadata = ClusterMetadataService.deserializeClusterMetadata(sourceFile);
        System.out.println("Old CMS: " + metadata.placements.get(ReplicationParams.meta(metadata)));
        metadata = makeCMS(metadata, InetAddressAndPort.getByNameUnchecked(args[1]));
        System.out.println("New CMS: " + metadata.placements.get(ReplicationParams.meta(metadata)));
        Path p = Files.createTempFile("clustermetadata", "dump");
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(p))
        {
            VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, metadata, out, serializationVersion);
        }
        System.out.println(p.toString());
    }

    public static ClusterMetadata makeCMS(ClusterMetadata metadata, InetAddressAndPort endpoint)
    {
        ReplicationParams metaParams = ReplicationParams.meta(metadata);
        Iterable<Replica> currentReplicas = metadata.placements.get(metaParams).writes.byEndpoint().flattenValues();
        DataPlacement.Builder builder = metadata.placements.get(metaParams).unbuild();
        for (Replica replica : currentReplicas)
        {
            builder.withoutReadReplica(metadata.epoch, replica)
                   .withoutWriteReplica(metadata.epoch, replica);
        }
        Replica newCMS = MetaStrategy.replica(endpoint);
        builder.withReadReplica(metadata.epoch, newCMS)
               .withWriteReplica(metadata.epoch, newCMS);
        return metadata.transformer().with(metadata.placements.unbuild().with(metaParams,
                                                                              builder.build())
                                                              .build())
                       .build().metadata;
    }
}
