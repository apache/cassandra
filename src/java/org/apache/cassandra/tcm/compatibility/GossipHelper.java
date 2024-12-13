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

package org.apache.cassandra.tcm.compatibility;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.TokenSerializer;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.gms.ApplicationState.DC;
import static org.apache.cassandra.gms.ApplicationState.HOST_ID;
import static org.apache.cassandra.gms.ApplicationState.INTERNAL_ADDRESS_AND_PORT;
import static org.apache.cassandra.gms.ApplicationState.INTERNAL_IP;
import static org.apache.cassandra.gms.ApplicationState.NATIVE_ADDRESS_AND_PORT;
import static org.apache.cassandra.gms.ApplicationState.RACK;
import static org.apache.cassandra.gms.ApplicationState.RELEASE_VERSION;
import static org.apache.cassandra.gms.ApplicationState.RPC_ADDRESS;
import static org.apache.cassandra.gms.ApplicationState.STATUS_WITH_PORT;
import static org.apache.cassandra.gms.ApplicationState.TOKENS;
import static org.apache.cassandra.gms.Gossiper.isShutdown;
import static org.apache.cassandra.locator.InetAddressAndPort.getByName;
import static org.apache.cassandra.locator.InetAddressAndPort.getByNameOverrideDefaults;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

public class GossipHelper
{
    private static final Logger logger = LoggerFactory.getLogger(GossipHelper.class);

    public static void removeFromGossip(InetAddressAndPort addr)
    {
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.removeEndpoint(addr));
    }

    public static void evictFromMembership(InetAddressAndPort endpoint)
    {
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.evictFromMembership(endpoint));
    }

    public static VersionedValue nodeStateToStatus(NodeId nodeId,
                                                    ClusterMetadata metadata,
                                                    Collection<Token> tokens,
                                                    VersionedValue.VersionedValueFactory valueFactory,
                                                    VersionedValue oldValue)
    {
        NodeState nodeState =  metadata.directory.peerState(nodeId);
        if ((tokens == null || tokens.isEmpty()) && !NodeState.isBootstrap(nodeState))
            return null;

        MultiStepOperation<?> sequence;
        VersionedValue status = null;
        switch (nodeState)
        {
            case JOINED:
                if (isShutdown(oldValue))
                    status = valueFactory.shutdown(true);
                else
                    status = valueFactory.normal(tokens);
                break;
            case LEFT:
                status = valueFactory.left(tokens, Gossiper.computeExpireTime());
                break;
            case BOOTSTRAPPING:
                sequence = metadata.inProgressSequences.get(nodeId);
                if (!(sequence instanceof BootstrapAndJoin))
                {
                    logger.error(String.format("Cannot construct gossip state. Node is in %s state, but the sequence is %s", NodeState.BOOTSTRAPPING, sequence));
                    return null;
                }
                Collection<Token> bootstrapTokens = getTokensFromOperation(sequence);
                status = valueFactory.bootstrapping(bootstrapTokens);
                break;
            case BOOT_REPLACING:
                sequence = metadata.inProgressSequences.get(nodeId);
                if (!(sequence instanceof BootstrapAndReplace))
                {
                    logger.error(String.format("Cannot construct gossip state. Node is in %s state, but the sequence is %s", NodeState.BOOT_REPLACING, sequence));
                    return null;
                }

                NodeId replaced = ((BootstrapAndReplace)sequence).startReplace.replaced();
                if (metadata.directory.versions.values().stream().allMatch(NodeVersion::isUpgraded))
                    status = valueFactory.bootReplacingWithPort(metadata.directory.endpoint(replaced));
                else
                    status = valueFactory.bootReplacing(metadata.directory.endpoint(replaced).getAddress());
                break;
            case LEAVING:
                status = valueFactory.leaving(tokens);
                break;
            case MOVING:
                sequence = metadata.inProgressSequences.get(nodeId);
                if (!(sequence instanceof Move))
                {
                    logger.error(String.format("Cannot construct gossip state. Node is in %s state, but sequence the is %s", NodeState.MOVING, sequence));
                    return null;
                }
                Collection<Token> moveTokens = getTokensFromOperation(sequence);
                if (!moveTokens.isEmpty())
                {
                    Token token = ((Move) sequence).tokens.iterator().next();
                    status = valueFactory.moving(token);
                }
                break;
            case REGISTERED:
                break;
            default:
                throw new RuntimeException("Bad NodeState " + nodeState);
        }
        return status;
    }

    public static Collection<Token> getTokensFromOperation(NodeId nodeId, ClusterMetadata metadata)
    {
        return getTokensFromOperation(metadata.inProgressSequences.get(nodeId));
    }

    public static Collection<Token> getTokensFromOperation(MultiStepOperation<?> sequence)
    {
        if (null == sequence)
            return Collections.emptySet();

        if (sequence.kind() == MultiStepOperation.Kind.JOIN)
            return new HashSet<>(((BootstrapAndJoin)sequence).finishJoin.tokens);
        else if (sequence.kind() == MultiStepOperation.Kind.REPLACE)
            return new HashSet<>(((BootstrapAndReplace)sequence).bootstrapTokens);
        else if (sequence.kind() == MultiStepOperation.Kind.MOVE)
            return new HashSet<>(((Move)sequence).tokens);

        throw new IllegalArgumentException(String.format("Extracting tokens from %s sequence is neither necessary nor supported here",
                                                         sequence.kind()));
    }

    private static Collection<Token> getTokensIn(IPartitioner partitioner, EndpointState epState)
    {
        try
        {
            if (epState == null)
                return Collections.emptyList();

            VersionedValue versionedValue = epState.getApplicationState(TOKENS);
            if (versionedValue == null)
                return Collections.emptyList();

            return TokenSerializer.deserialize(partitioner, new DataInputStream(new ByteArrayInputStream(versionedValue.toBytes())));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static NodeState toNodeState(InetAddressAndPort endpoint, EndpointState epState)
    {
        assert epState != null;

        String status = epState.getStatus();
        if (status.equals(VersionedValue.STATUS_NORMAL) ||
            status.equals(VersionedValue.SHUTDOWN))
            return NodeState.JOINED;
        if (status.equals(VersionedValue.STATUS_LEFT))
            return NodeState.LEFT;
        throw new IllegalStateException("Can't upgrade the first node when STATUS = " + status + " for node " + endpoint);
    }

    public static NodeAddresses getAddressesFromEndpointState(InetAddressAndPort endpoint, EndpointState epState)
    {
        if (endpoint.equals(getBroadcastAddressAndPort()))
            return NodeAddresses.current();
        try
        {
            InetAddressAndPort local = getEitherState(endpoint, epState, INTERNAL_ADDRESS_AND_PORT, INTERNAL_IP, DatabaseDescriptor.getStoragePort());
            InetAddressAndPort nativeAddress = getEitherState(endpoint, epState, NATIVE_ADDRESS_AND_PORT, RPC_ADDRESS, DatabaseDescriptor.getNativeTransportPort());
            return new NodeAddresses(UUID.randomUUID(), endpoint, local, nativeAddress);
        }
        catch (UnknownHostException e)
        {
            throw new ConfigurationException("Unknown host in epState for " + endpoint + " : " + epState, e);
        }
    }

    private static InetAddressAndPort getEitherState(InetAddressAndPort endpoint,
                                                     EndpointState epState,
                                                     ApplicationState primaryState,
                                                     ApplicationState deprecatedState,
                                                     int defaultPortForDeprecatedState) throws UnknownHostException
    {
        if (epState.getApplicationState(primaryState) != null)
        {
            return getByName(epState.getApplicationState(primaryState).value);
        }
        else if (epState.getApplicationState(deprecatedState) != null)
        {
            return getByNameOverrideDefaults(epState.getApplicationState(deprecatedState).value, defaultPortForDeprecatedState);
        }
        else
        {
            return endpoint.withPort(defaultPortForDeprecatedState);
        }
    }

    private static NodeVersion getVersionFromEndpointState(InetAddressAndPort endpoint, EndpointState epState)
    {
        if (endpoint.equals(getBroadcastAddressAndPort()))
            return NodeVersion.CURRENT;
        CassandraVersion cassandraVersion = epState.getReleaseVersion();
        return NodeVersion.fromCassandraVersion(cassandraVersion);
    }

    public static ClusterMetadata emptyWithSchemaFromSystemTables(Set<String> allKnownDatacenters)
    {
        // If this instance was previously upgraded then subsequently downgraded, the metadata keyspace may have been
        // added to system_schema tables. If so, don't include it in the initial schema as this will cause it to be
        // incorrectly configured with the global partitioner. It will be created afresh from
        // DistributedMetadataLogKeyspace.initialMetadata.
        Keyspaces keyspaces = SchemaKeyspace.fetchNonSystemKeyspaces()
                                            .filter(k -> !k.name.equals(SchemaConstants.METADATA_KEYSPACE_NAME));
        return new ClusterMetadata(Epoch.UPGRADE_STARTUP,
                                   DatabaseDescriptor.getPartitioner(),
                                   DistributedSchema.fromSystemTables(keyspaces, allKnownDatacenters),
                                   Directory.EMPTY,
                                   new TokenMap(DatabaseDescriptor.getPartitioner()),
                                   DataPlacements.empty(),
                                   LockedRanges.EMPTY,
                                   InProgressSequences.EMPTY,
                                   Collections.emptyMap());
    }

    public static ClusterMetadata fromEndpointStates(DistributedSchema schema, Map<InetAddressAndPort, EndpointState> epStates)
    {
        return fromEndpointStates(epStates, DatabaseDescriptor.getPartitioner(), schema);
    }

    /**
     * reads state for the local host from system keyspaces and creates an EndpointState, only to be used
     * if we can't contact any peers during upgrade
     */
    public static Map<InetAddressAndPort, EndpointState> storedEpstate()
    {
        EndpointState epstate = new EndpointState(new HeartBeatState(SystemKeyspace.incrementAndGetGeneration(), 0));
        VersionedValue.VersionedValueFactory vf = StorageService.instance.valueFactory;
        epstate.addApplicationState(DC, vf.datacenter(SystemKeyspace.getDatacenter()));
        epstate.addApplicationState(RACK, vf.rack(SystemKeyspace.getRack()));
        UUID hostId = SystemKeyspace.getLocalHostId();
        if (null != hostId)
        {
            epstate.addApplicationState(ApplicationState.HOST_ID,
                                        StorageService.instance.valueFactory.hostId(hostId));
        }
        Collection<Token> tokens = SystemKeyspace.getSavedTokens();
        epstate.addApplicationState(STATUS_WITH_PORT, vf.normal(tokens));
        epstate.addApplicationState(TOKENS, vf.tokens(tokens));
        epstate.addApplicationState(INTERNAL_ADDRESS_AND_PORT, vf.internalAddressAndPort(SystemKeyspace.getPreferredIP(FBUtilities.getLocalAddressAndPort())));
        epstate.addApplicationState(NATIVE_ADDRESS_AND_PORT, vf.nativeaddressAndPort(FBUtilities.getBroadcastNativeAddressAndPort()));

        Map<InetAddressAndPort, EndpointState> epstates = new HashMap<>();
        epstates.put(FBUtilities.getBroadcastAddressAndPort(), epstate);
        epstates.putAll(SystemKeyspace.peerEndpointStates());
        return epstates;
    }

    @VisibleForTesting
    public static ClusterMetadata fromEndpointStates(Map<InetAddressAndPort, EndpointState> epStates, IPartitioner partitioner, DistributedSchema schema)
    {
        Directory directory = new Directory().withLastModified(Epoch.UPGRADE_GOSSIP);
        TokenMap tokenMap = new TokenMap(partitioner).withLastModified(Epoch.UPGRADE_GOSSIP);
        List<InetAddressAndPort> sortedEps = Lists.newArrayList(epStates.keySet());
        Collections.sort(sortedEps);
        Map<ExtensionKey<?, ?>, ExtensionValue<?>> extensions = new HashMap<>();
        for (InetAddressAndPort endpoint : sortedEps)
        {
            EndpointState epState = epStates.get(endpoint);
            String dc = epState.getApplicationState(DC).value;
            String rack = epState.getApplicationState(RACK).value;
            String hostIdString = epState.getApplicationState(HOST_ID).value;
            NodeAddresses nodeAddresses = getAddressesFromEndpointState(endpoint, epState);
            NodeVersion nodeVersion = getVersionFromEndpointState(endpoint, epState);
            assert hostIdString != null;
            directory = directory.withNonUpgradedNode(nodeAddresses,
                                                      new Location(dc, rack),
                                                      nodeVersion,
                                                      toNodeState(endpoint, epState),
                                                      UUID.fromString(hostIdString));
            NodeId nodeId = directory.peerId(endpoint);
            tokenMap = tokenMap.assignTokens(nodeId, getTokensIn(partitioner, epState));
        }

        ClusterMetadata forPlacementCalculation = new ClusterMetadata(Epoch.UPGRADE_GOSSIP,
                                                                      partitioner,
                                                                      schema,
                                                                      directory,
                                                                      tokenMap,
                                                                      DataPlacements.empty(),
                                                                      LockedRanges.EMPTY,
                                                                      InProgressSequences.EMPTY,
                                                                      extensions);
        DataPlacements placements = new UniformRangePlacement().calculatePlacements(Epoch.UPGRADE_GOSSIP,
                                                                                    forPlacementCalculation,
                                                                                    schema.getKeyspaces());
        return new ClusterMetadata(Epoch.UPGRADE_GOSSIP,
                                   partitioner,
                                   schema,
                                   directory,
                                   tokenMap,
                                   placements,
                                   LockedRanges.EMPTY,
                                   InProgressSequences.EMPTY,
                                   extensions);
    }

    public static boolean isValidForClusterMetadata(Map<InetAddressAndPort, EndpointState> epstates)
    {
        if (epstates.isEmpty())
            return false;
        EnumSet<ApplicationState> requiredStates = EnumSet.of(DC, RACK, HOST_ID, TOKENS, RELEASE_VERSION);
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : epstates.entrySet())
        {
            EndpointState epstate = entry.getValue();
            for (ApplicationState state : requiredStates)
                if (epstate.getApplicationState(state) == null)
                {
                    logger.warn("Invalid endpoint state for {}; {} - {}", entry.getKey(), state, epstates);
                    return false;
                }
        }
        return true;
    }
}
