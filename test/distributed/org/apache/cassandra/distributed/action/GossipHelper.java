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

package org.apache.cassandra.distributed.action;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.shared.ThrowingRunnable;
import org.apache.cassandra.distributed.shared.VersionedApplicationState;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.impl.DistributedTestSnitch.toCassandraInetAddressAndPort;

public class GossipHelper
{
    public static InstanceAction statusToBlank(IInvokableInstance newNode)
    {
        return (instance) -> changeGossipState(instance, newNode,Collections.emptyList());
    }

    public static InstanceAction statusToNormal(IInvokableInstance peer)
    {
        return (target) ->
        {
            changeGossipState(target,
                              peer,
                              Arrays.asList(tokens(peer),
                                            statusNormal(peer),
                                            releaseVersion(peer),
                                            netVersion(peer),
                                            statusWithPortNormal(peer)));
        };
    }

    /**
     * This method is unsafe and should be used _only_ when gossip is not used or available: it creates versioned values on the
     * target instance, which means Gossip versioning gets out of sync. Use a safe couterpart at all times when performing _any_
     * ring movement operations _or_ if Gossip is used.
     */
    public static void unsafeStatusToNormal(IInvokableInstance target, IInstance peer)
    {
        final int messagingVersion = getOrDefaultMessagingVersion(target, peer);
        changeGossipState(target,
                          peer,
                          Arrays.asList(unsafeVersionedValue(target,
                                                             ApplicationState.TOKENS,
                                                             (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).tokens(tokens),
                                                             peer.config().getString("partitioner"),
                                                             peer.config().getString("initial_token")),
                                        unsafeVersionedValue(target,
                                                             ApplicationState.STATUS,
                                                             (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).normal(tokens),
                                                             peer.config().getString("partitioner"),
                                                             peer.config().getString("initial_token")),
                                        unsafeVersionedValue(target,
                                                             ApplicationState.STATUS_WITH_PORT,
                                                             (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).normal(tokens),
                                                             peer.config().getString("partitioner"),
                                                             peer.config().getString("initial_token")),
                                        unsafeVersionedValue(target,
                                                             ApplicationState.NET_VERSION,
                                                             (partitioner) -> new VersionedValue.VersionedValueFactory(partitioner).networkVersion(messagingVersion),
                                                             peer.config().getString("partitioner")),
                                        unsafeReleaseVersion(target,
                                                             peer.config().getString("partitioner"),
                                                             peer.getReleaseVersionString())));
    }

    private static VersionedApplicationState tokens(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.TOKENS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).tokens(tokens));
    }

    private static VersionedApplicationState netVersion(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.NET_VERSION, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).networkVersion());
    }

    private static VersionedApplicationState unsafeReleaseVersion(IInvokableInstance instance, String partitionerStr, String releaseVersionStr)
    {
        return unsafeVersionedValue(instance, ApplicationState.RELEASE_VERSION, (partitioner) -> new VersionedValue.VersionedValueFactory(partitioner).releaseVersion(releaseVersionStr), partitionerStr);
    }

    private static VersionedApplicationState releaseVersion(IInvokableInstance instance)
    {
        return unsafeReleaseVersion(instance, instance.config().getString("partitioner"), instance.getReleaseVersionString());
    }

    private static VersionedApplicationState statusNormal(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).normal(tokens));
    }

    private static VersionedApplicationState statusWithPortNormal(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS_WITH_PORT, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).normal(tokens));
    }

    private static VersionedValue toVersionedValue(VersionedApplicationState vv)
    {
        return VersionedValue.unsafeMakeVersionedValue(vv.value, vv.version);
    }

    private static ApplicationState toApplicationState(VersionedApplicationState vv)
    {
        return ApplicationState.values()[vv.applicationState];
    }

    private static VersionedApplicationState unsafeVersionedValue(IInvokableInstance instance,
                                                                 ApplicationState applicationState,
                                                                 IIsolatedExecutor.SerializableBiFunction<IPartitioner, Collection<Token>, VersionedValue> supplier,
                                                                 String partitionerStr, String initialTokenStr)
    {
        return instance.appliesOnInstance((String partitionerString, String tokenString) -> {
            IPartitioner partitioner = FBUtilities.newPartitioner(partitionerString);
            Collection<Token> tokens = tokenString.contains(",")
                                       ? Stream.of(tokenString.split(",")).map(partitioner.getTokenFactory()::fromString).collect(Collectors.toList())
                                       : Collections.singleton(partitioner.getTokenFactory().fromString(tokenString));

            VersionedValue versionedValue = supplier.apply(partitioner, tokens);
            return new VersionedApplicationState(applicationState.ordinal(), versionedValue.value, versionedValue.version);
        }).apply(partitionerStr, initialTokenStr);
    }

    private static VersionedApplicationState unsafeVersionedValue(IInvokableInstance instance,
                                                                 ApplicationState applicationState,
                                                                 IIsolatedExecutor.SerializableFunction<IPartitioner, VersionedValue> supplier,
                                                                 String partitionerStr)
    {
        return instance.appliesOnInstance((String partitionerString) -> {
            IPartitioner partitioner = FBUtilities.newPartitioner(partitionerString);
            VersionedValue versionedValue = supplier.apply(partitioner);
            return new VersionedApplicationState(applicationState.ordinal(), versionedValue.value, versionedValue.version);
        }).apply(partitionerStr);
    }

    private static VersionedApplicationState versionedToken(IInvokableInstance instance, ApplicationState applicationState, IIsolatedExecutor.SerializableBiFunction<IPartitioner, Collection<Token>, VersionedValue> supplier)
    {
        return unsafeVersionedValue(instance, applicationState, supplier, instance.config().getString("partitioner"), instance.config().getString("initial_token"));
    }

    /**
     * Changes gossip state of the `peer` on `target`
     */
    private static void changeGossipState(IInvokableInstance target, IInstance peer, List<VersionedApplicationState> newState)
    {
        InetSocketAddress addr = peer.broadcastAddress();
        UUID hostId = peer.config().hostId();
        final int netVersion = getOrDefaultMessagingVersion(target, peer);
        target.runOnInstance(() -> {
            InetAddressAndPort endpoint = toCassandraInetAddressAndPort(addr);
            StorageService storageService = StorageService.instance;

            Gossiper.runInGossipStageBlocking(() -> {
                EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                if (state == null)
                {
                    Gossiper.instance.initializeNodeUnsafe(endpoint, hostId, netVersion, 1);
                    state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                    if (state.isAlive() && !Gossiper.instance.isDeadState(state))
                        Gossiper.instance.realMarkAlive(endpoint, state);
                }

                for (VersionedApplicationState value : newState)
                {
                    ApplicationState as = toApplicationState(value);
                    VersionedValue vv = toVersionedValue(value);
                    state.addApplicationState(as, vv);
                    storageService.onChange(endpoint, as, vv);
                }
            });
        });
    }

    private static int getOrDefaultMessagingVersion(IInvokableInstance target, IInstance peer)
    {
        int peerVersion = peer.getMessagingVersion();
        final int netVersion = peerVersion == 0 ? target.getMessagingVersion() : peerVersion;
        assert netVersion != 0 : "Unable to determine messaging version for peer {}" + peer.config().num();
        return netVersion;
    }

    public static void withProperty(CassandraRelevantProperties prop, boolean value, ThrowingRunnable r)
    {
        withProperty(prop, Boolean.toString(value), r);
    }

    public static void withProperty(CassandraRelevantProperties prop, String value, ThrowingRunnable r)
    {
        String prev = prop.setString(value);
        try
        {
            r.run();
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (prev == null)
                prop.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
            else
                prop.setString(prev);
        }
    }
}
