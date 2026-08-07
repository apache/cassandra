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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

public class Startup implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(Startup.class);
    public static final Serializer serializer = new Serializer();
    private final NodeId nodeId;
    private final NodeVersion nodeVersion;
    private final NodeAddresses addresses;

    public Startup(NodeId nodeId,
                   NodeAddresses addresses,
                   NodeVersion nodeVersion)
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
        // Prevent downgrade to a version that cannot read cluster metadata.
        // This protects against restarting a node with an older binary.
        Version clusterVersion = prev.directory.commonSerializationVersion;
        Version newNodeVersion = nodeVersion.serializationVersion();
        if (newNodeVersion.isBefore(clusterVersion))
        {
            return new Rejected(INVALID,
                                String.format("Cannot start node: this node's metadata serialization version %s " +
                                              "is lower than the cluster's minimum required version %s. " +
                                              "Node would not be able to read cluster metadata. " +
                                              "Please upgrade the node to a Cassandra version that supports " +
                                              "metadata serialization version %s or higher before restarting.",
                                              newNodeVersion, clusterVersion, clusterVersion));
        }

        ClusterMetadata.Transformer next = prev.transformer();
        NodeAddresses oldAddresses = prev.directory.addresses.get(nodeId);
        if (!oldAddresses.equals(addresses))
        {
            if (!prev.inProgressSequences.isEmpty() && prev.directory.commonSerializationVersion.isBefore(Version.V9))
                return new Rejected(INVALID, "Cannot update address of the node while there are in-progress sequences until the whole cluster is running metadata serialization version V9");
            for (Map.Entry<NodeId, NodeAddresses> entry : prev.directory.addresses.entrySet())
            {
                NodeAddresses existingAddresses = entry.getValue();
                NodeId existingNodeId = entry.getKey();
                if (!nodeId.equals(existingNodeId) && addresses.conflictsWith(existingAddresses))
                    return new Rejected(INVALID, String.format("New addresses %s conflicts with existing node %s with addresses %s", addresses, entry.getKey(), existingAddresses));
            }
            next = next.withNewAddresses(nodeId, addresses);
            DataPlacements newPlacement = next.build().metadata.placements().changeIp(oldAddresses.broadcastAddress, addresses.broadcastAddress);
            next = next.with(newPlacement);
        }

        if (!prev.directory.versions.get(nodeId).equals(nodeVersion))
            next = next.withVersion(nodeId, nodeVersion);

        return Transformation.success(next, LockedRanges.AffectedRanges.EMPTY);
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

    public static void maybeExecuteStartupTransformation(NodeId localNodeId)
    {
        Directory directory = ClusterMetadata.current().directory;

        if (!Objects.equals(directory.addresses.get(localNodeId), NodeAddresses.current()) ||
            !Objects.equals(directory.versions.get(localNodeId), NodeVersion.CURRENT))
        {
            logger.info("Detected change in node addresses or version, committing updates to Cluster Metadata Service");
            ClusterMetadataService.instance()
                                  .commit(new Startup(localNodeId, NodeAddresses.current(), NodeVersion.CURRENT));
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
            return new Startup(nodeId, addresses, nodeVersion);
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
