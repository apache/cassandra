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
import org.apache.cassandra.schema.Keyspaces;
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
    public Result execute(ClusterMetadata prev, long timestampMicros)
    {
        ClusterMetadata.Transformer next = prev.transformer();
        if (!prev.directory.addresses.get(nodeId).equals(addresses))
        {
            if (!prev.inProgressSequences.isEmpty())
                return new Rejected(INVALID, "Cannot update address of the node while there are in-progress sequences");

            for (Map.Entry<NodeId, NodeAddresses> entry : prev.directory.addresses.entrySet())
            {
                NodeAddresses existingAddresses = entry.getValue();
                NodeId existingNodeId = entry.getKey();
                if (!nodeId.equals(existingNodeId) && addresses.conflictsWith(existingAddresses))
                    return new Rejected(INVALID, String.format("New addresses %s conflicts with existing node %s with addresses %s", addresses, entry.getKey(), existingAddresses));
            }

            next = next.withNewAddresses(nodeId, addresses);
            Keyspaces allKeyspaces = prev.schema.getKeyspaces().withAddedOrReplaced(prev.schema.getKeyspaces());

            DataPlacements newPlacement = ClusterMetadataService.instance()
                                                                .placementProvider()
                                                                .calculatePlacements(prev.nextEpoch(),
                                                                                     prev.tokenMap.toRanges(),
                                                                                     next.build().metadata,
                                                                                     allKeyspaces);

            next = next.with(newPlacement);
        }

        if (!prev.directory.versions.get(nodeId).equals(nodeVersion))
            next = next.withVersion(nodeId, nodeVersion);

        return success(next, LockedRanges.AffectedRanges.EMPTY);
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

    @Override
    public boolean allowDuringUpgrades()
    {
        return true;
    }

    public static void maybeExecuteStartupTransformation(NodeId localNodeId)
    {
        Directory directory = ClusterMetadata.current().directory;

        if (!Objects.equals(directory.addresses.get(localNodeId), NodeAddresses.current()) ||
            !Objects.equals(directory.versions.get(localNodeId), NodeVersion.CURRENT))
        {
            ClusterMetadataService.instance().commit(new Startup(localNodeId, NodeAddresses.current(), NodeVersion.CURRENT),
                                                     (metadata) -> null,
                                                     (metadata, code, reason) -> {
                                                         throw new IllegalStateException(String.format("Startup transformations should be executed unconditionally, " +
                                                                                                       "but this one got rejected with [%s]: \"%s\"", code, reason));
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
