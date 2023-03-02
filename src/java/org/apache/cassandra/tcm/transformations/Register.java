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
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.FBUtilities;

public class Register implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(Register.class);
    public static final Serializer serializer = new Serializer();

    private final NodeAddresses addresses;
    private final Location location;
    private final NodeVersion version;

    public Register(NodeAddresses addresses, Location location, NodeVersion version)
    {
        this.location = location;
        this.version = version;
        this.addresses = addresses;
    }

    @Override
    public Kind kind()
    {
        return Kind.REGISTER;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        for (Map.Entry<NodeId, NodeAddresses> entry : prev.directory.addresses.entrySet())
        {
            NodeAddresses existingAddresses = entry.getValue();
            if (addresses.conflictsWith(existingAddresses))
                return new Rejected(String.format("New addresses %s conflicts with existing node %s with addresses %s", addresses, entry.getKey(), existingAddresses));
        }

        ClusterMetadata.Transformer next = prev.transformer()
                                               .register(addresses, location, version);
        return success(next, LockedRanges.AffectedRanges.EMPTY);
    }

    public static NodeId maybeRegister()
    {
        return register(false);
    }

    @VisibleForTesting
    public static NodeId forceRegister()
    {
        return register(true);
    }

    @VisibleForTesting
    public static NodeId register(NodeAddresses nodeAddresses)
    {
        return register(nodeAddresses, NodeVersion.CURRENT);
    }

    @VisibleForTesting
    public static NodeId register(NodeAddresses nodeAddresses, NodeVersion nodeVersion)
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        Location location = new Location(snitch.getLocalDatacenter(), snitch.getLocalRack());

        NodeId nodeId = ClusterMetadata.current().directory.peerId(nodeAddresses.broadcastAddress);
        if (nodeId == null)
        {
            nodeId = ClusterMetadataService.instance()
                                           .commit(new Register(nodeAddresses, location, nodeVersion),
                                                   (metadata) -> !metadata.directory.isRegistered(nodeAddresses.broadcastAddress),
                                                   (metadata) -> metadata.directory.peerId(nodeAddresses.broadcastAddress),
                                                   (metadata, reason) -> {
                                                       throw new IllegalStateException("Can't register node: " + reason);
                                                   });
        }

        logger.info("Registering with endpoint {}", nodeAddresses.broadcastAddress);
        return nodeId;
    }

    private static NodeId register(boolean force)
    {
        // Try to recover node ID from the system keyspace
        UUID localHostId = SystemKeyspace.getLocalHostId();
        if (force || localHostId == null)
        {
            NodeId nodeId = register(NodeAddresses.current());
            localHostId = nodeId.uuid;
            SystemKeyspace.setLocalHostId(localHostId);
            logger.info("New node ID obtained {}, (Note: This should happen exactly once per node)", nodeId.uuid);
            return nodeId;
        }
        else
        {
            Directory dir = ClusterMetadata.current().directory;
            NodeId nodeId = dir.peerId(FBUtilities.getBroadcastAddressAndPort());
            NodeVersion dirVersion = dir.version(nodeId);

            // If this is a node in the process of upgrading, update the host id in the system.local table
            // TODO: when constructing the initial cluster metadata for upgrade, we include a mapping from
            //      NodeId to the old HostId. We will need to use this lookup to map between the two for
            //      hint delivery immediately following an upgrade.
            if (dirVersion == null || !dirVersion.isUpgraded())
            {
                if (dir.hostId(nodeId).equals(localHostId))
                {
                    SystemKeyspace.setLocalHostId(nodeId.uuid);
                    logger.info("Updated local HostId from pre-upgrade version {} to the one which was pre-registered " +
                                "during initial cluster metadata conversion {}", localHostId, nodeId.uuid);
                }
                else
                {
                    throw new RuntimeException("HostId read from local system table does not match the one recorded " +
                                               "for this endpoint during initial cluster metadata conversion. " +
                                               String.format("Endpoint: %s, NodeId: %s, Recorded: %s, Local: %s",
                                                             FBUtilities.getBroadcastAddressAndPort(),
                                                             nodeId,
                                                             dir.hostId(nodeId),
                                                             localHostId));
                }
            }
            else
            {
                logger.info("Local id was already registered, retaining: {}", localHostId);
            }
            return nodeId;
        }
    }

    public String toString()
    {
        return "Register{" +
               ", addresses=" + addresses +
               ", location=" + location +
               ", version=" + version +
               '}';
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, Register>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof Register;
            Register register = (Register)t;
            NodeAddresses.serializer.serialize(register.addresses, out, version);
            Location.serializer.serialize(register.location, out, version);
            NodeVersion.serializer.serialize(register.version, out, version);
        }

        public Register deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeAddresses addresses = NodeAddresses.serializer.deserialize(in, version);
            Location location = Location.serializer.deserialize(in, version);
            NodeVersion nodeVersion = NodeVersion.serializer.deserialize(in, version);
            return new Register(addresses, location, nodeVersion);
        }

        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof Register;
            Register register = (Register) t;
            return NodeAddresses.serializer.serializedSize(register.addresses, version) +
                   Location.serializer.serializedSize(register.location, version) +
                   NodeVersion.serializer.serializedSize(register.version, version);
        }
    }
}