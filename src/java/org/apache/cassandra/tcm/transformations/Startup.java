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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.FBUtilities;

public class Startup implements Transformation
{
    public static final Serializer serializer = new Serializer();
    private final NodeId nodeId;
    private final NodeVersion nodeVersion;
    private final NodeAddresses addresses;

    public Startup(NodeId nodeId,
                   NodeVersion nodeVersion,
                   NodeAddresses addresses)
    {
        this.nodeId = nodeId;
        this.nodeVersion = nodeVersion;
        this.addresses = addresses;
    }
    @Override
    public Kind kind()
    {
        return Kind.STARTUP;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        for (Map.Entry<NodeId, NodeAddresses> entry : prev.directory.addresses.entrySet())
        {
            NodeAddresses existingAddresses = entry.getValue();
            NodeId existingNodeId = entry.getKey();
            if (!nodeId.equals(existingNodeId) && addresses.conflictsWith(existingAddresses))
                return new Rejected(String.format("New addresses %s conflicts with existing node %s with addresses %s", addresses, entry.getKey(), existingAddresses));
        }

        return success(prev.transformer().withNodeInformation(nodeId, nodeVersion, addresses),
                       LockedRanges.AffectedRanges.EMPTY);
    }

    @Override
    public String toString()
    {
        return "Startup{" +
               "nodeId=" + nodeId +
               ", nodeVersion=" + nodeVersion +
               ", addresses=" + addresses +
               '}';
    }

    public static void maybeExecuteStartupTransformation()
    {
        Directory dir = ClusterMetadata.current().directory;
        NodeId localNodeId = dir.peerId(FBUtilities.getBroadcastAddressAndPort());
        if (!Objects.equals(dir.addresses.get(localNodeId), NodeAddresses.current()) ||
            !Objects.equals(dir.versions.get(localNodeId), NodeVersion.CURRENT))
        {
            ClusterMetadataService.instance().commit(new Startup(localNodeId, NodeVersion.CURRENT, NodeAddresses.current()),
                                                     (metadata) -> {
                                                         // Retry on version mismatch
                                                         if (!NodeVersion.CURRENT.equals(metadata.directory.version(localNodeId)))
                                                             return true;

                                                         // Retry on address mismatch
                                                         if (!NodeAddresses.current().equals(metadata.directory.getNodeAddresses(localNodeId)))
                                                             return true;

                                                         return false;
                                                     } ,
                                                     (metadata) -> null,
                                                     (metadata, reason) -> {
                                                         throw new IllegalStateException(String.format("Startup transformations should be executed unconditionally, but this one got rejected with \"%s\"", reason));
                                                     });
        }
    }

    static class Serializer implements MetadataSerializer<Transformation>
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            Startup startup = (Startup)t;
            NodeId.serializer.serialize(startup.nodeId, out, version);
            NodeVersion.serializer.serialize(startup.nodeVersion, out, version);
            NodeAddresses.serializer.serialize(startup.addresses, out, version);
        }

        @Override
        public Transformation deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId nodeId = NodeId.serializer.deserialize(in, version);
            NodeVersion nodeVersion = NodeVersion.serializer.deserialize(in, version);
            NodeAddresses addresses = NodeAddresses.serializer.deserialize(in, version);
            return new Startup(nodeId, nodeVersion, addresses);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            Startup startup = (Startup)t;
            return NodeId.serializer.serializedSize(startup.nodeId, version) +
                   NodeVersion.serializer.serializedSize(startup.nodeVersion, version) +
                   NodeAddresses.serializer.serializedSize(startup.addresses, version);
        }
    }

}
