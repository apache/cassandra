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
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

public class AlterTopology implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(AlterTopology.class);
    public static final Serializer serializer = new Serializer();

    private final Map<NodeId, Location> updates;
    private final PlacementProvider placementProvider;

    public AlterTopology(Map<NodeId, Location> updates, PlacementProvider placementProvider)
    {
        this.updates = updates;
        this.placementProvider = placementProvider;
    }

    public static Map<NodeId, Location> parseArgs(String args, Directory directory)
    {
        Map<NodeId, Location> asMap = new HashMap<>();
        for (String change : args.split(","))
        {
            String[] parts = change.trim().split("=");
            if (parts.length != 2)
                throw new IllegalArgumentException("Invalid specification: " + change);

            if (parts[0].isEmpty() || parts[1].isEmpty())
                throw new IllegalArgumentException("Invalid specification: " + change);

            NodeId id = getNodeIdFromString(parts[0].trim(), directory);
            if (asMap.containsKey(id))
                throw new IllegalArgumentException("Multiple updates for node " + id + " (" + parts[0].trim() + " )");
            asMap.put(getNodeIdFromString(parts[0].trim(), directory), Location.fromString(parts[1].trim()));
        }
        return asMap;
    }

    private static NodeId getNodeIdFromString(String s, Directory directory)
    {
        // first try to parse the id as a node id, either in UUID or int form
        try
        {
            return NodeId.fromString(s);
        }
        catch (Exception e)
        {
            // fall back to trying the supplied id as an endpoint
            try
            {
                InetAddressAndPort endpoint = InetAddressAndPort.getByName(s);
                return directory.peerId(endpoint);
            }
            catch (UnknownHostException u)
            {
                throw new IllegalArgumentException("Invalid node identifier supplied: " + s);
            }

        }
    }

    @Override
    public Kind kind()
    {
        return Kind.ALTER_TOPOLOGY;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        // Check no inflight range movements
        if (!prev.lockedRanges.locked.isEmpty())
            return new Rejected(INVALID, "The requested topology changes cannot be executed while there are ongoing range movements.");

        Directory dir = prev.directory;
        // Check all node ids are present
        Set<NodeId> missing = updates.keySet()
                                     .stream()
                                     .filter(location -> (null == dir.location(location)))
                                     .collect(Collectors.toSet());
        if (!missing.isEmpty())
            return new Rejected(INVALID, String.format("Some updates specify an unregistered node: %s", missing));

        // Validate there will be no change to placements
        Directory updated = prev.directory;
        for (Map.Entry<NodeId, Location> update : updates.entrySet())
            updated = updated.withUpdatedRackAndDc(update.getKey(), update.getValue());
        ClusterMetadata proposed = prev.transformer().with(updated).build().metadata;
        DataPlacements proposedPlacements = placementProvider.calculatePlacements(prev.placements.lastModified(),
                                                                                  proposed.tokenMap.toRanges(),
                                                                                  proposed,
                                                                                  proposed.schema.getKeyspaces());
        if (!proposedPlacements.equivalentTo(prev.placements))
        {
            logger.info("Rejecting topology modifications which would materially change data placements: {}", updates);
            return new Rejected(INVALID, "Proposed updates modify data placements, violating consistency guarantees");
        }

        ClusterMetadata.Transformer next = prev.transformer().with(updated);
        return Transformation.success(next, LockedRanges.AffectedRanges.EMPTY);
    }


    @Override
    public String toString()
    {
        return "AlterTopology{" +
               "updates=" + updates +
               '}';
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, AlterTopology>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof AlterTopology;
            AlterTopology alterTopology = (AlterTopology)t;
            int size = alterTopology.updates.size();
            out.writeInt(size);
            for (Map.Entry<NodeId, Location> entry : alterTopology.updates.entrySet())
            {
                NodeId.serializer.serialize(entry.getKey(), out, version);
                Location.serializer.serialize(entry.getValue(), out, version);
            }
        }

        public AlterTopology deserialize(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readInt();
            Map<NodeId, Location> updates = new HashMap<>(size);
            for (int i = 0; i < size; i++)
                updates.put(NodeId.serializer.deserialize(in, version), Location.serializer.deserialize(in, version));
            return new AlterTopology(updates, ClusterMetadataService.instance().placementProvider());
        }

        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof AlterTopology;
            AlterTopology alterTopology = (AlterTopology) t;
            long size = TypeSizes.sizeof(alterTopology.updates.size());
            for (Map.Entry<NodeId, Location> entry : alterTopology.updates.entrySet())
            {
                size += NodeId.serializer.serializedSize(entry.getKey(), version);
                size += Location.serializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    }
}