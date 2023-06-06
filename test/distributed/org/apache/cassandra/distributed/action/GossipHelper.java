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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.shared.VersionedApplicationState;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.impl.DistributedTestSnitch.toCassandraInetAddressAndPort;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertTrue;

public class GossipHelper
{
    public static InstanceAction statusToBlank(IInvokableInstance newNode)
    {
        return (instance) -> changeGossipState(instance, newNode,Collections.emptyList());
    }

    public static InstanceAction statusToBootstrap(IInvokableInstance newNode)
    {
        return (instance) ->
        {
            changeGossipState(instance,
                              newNode,
                              Arrays.asList(tokens(newNode),
                                            statusBootstrapping(newNode),
                                            statusWithPortBootstrapping(newNode)));
        };
    }

    public static InstanceAction statusToDecommission(IInvokableInstance newNode)
    {
        return (instance) ->
        {
            changeGossipState(instance,
                              newNode,
                              Arrays.asList(tokens(newNode),
                                            statusLeaving(newNode),
                                            statusWithPortLeaving(newNode)));
        };
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

    public static InstanceAction statusToLeaving(IInvokableInstance newNode)
    {
        return (instance) -> {
            changeGossipState(instance,
                              newNode,
                              Arrays.asList(tokens(newNode),
                                            statusLeaving(newNode),
                                            statusWithPortLeaving(newNode)));
        };
    }

    public static InstanceAction bootstrap()
    {
        return new BootstrapAction();
    }

    public static InstanceAction bootstrap(boolean joinRing, Duration waitForBootstrap, Duration waitForSchema)
    {
        return new BootstrapAction(joinRing, waitForBootstrap, waitForSchema);
    }

    public static InstanceAction disseminateGossipState(IInvokableInstance newNode)
    {
        return new DisseminateGossipState(newNode);
    }

    public static InstanceAction pullSchemaFrom(IInvokableInstance pullFrom)
    {
        return new PullSchemaFrom(pullFrom);
    }

    private static InstanceAction disableBinary()
    {
        return (instance) -> instance.nodetoolResult("disablebinary").asserts().success();
    }

    private static class DisseminateGossipState implements InstanceAction
    {
        final Map<InetSocketAddress, byte[]> gossipState;

        public DisseminateGossipState(IInvokableInstance... from)
        {
            gossipState = new HashMap<>();
            for (IInvokableInstance node : from)
            {
                byte[] epBytes = node.callsOnInstance(() -> {
                    EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddressAndPort());
                    return toBytes(epState);
                }).call();
                gossipState.put(node.broadcastAddress(), epBytes);
            }
        }

        public void accept(IInvokableInstance instance)
        {
            instance.appliesOnInstance((IIsolatedExecutor.SerializableFunction<Map<InetSocketAddress, byte[]>, Void>)
                                       (map) -> {
                                           Map<InetAddressAndPort, EndpointState> newState = new HashMap<>();
                                           for (Map.Entry<InetSocketAddress, byte[]> e : map.entrySet())
                                               newState.put(toCassandraInetAddressAndPort(e.getKey()), fromBytes(e.getValue()));

                                           Gossiper.runInGossipStageBlocking(() -> {
                                               Gossiper.instance.applyStateLocally(newState);
                                           });
                                           return null;
                                       }).apply(gossipState);
        }
    }

    private static byte[] toBytes(EndpointState epState)
    {
        try (DataOutputBuffer out = new DataOutputBuffer(1024))
        {
            EndpointState.serializer.serialize(epState, out, MessagingService.current_version);
            return out.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static EndpointState fromBytes(byte[] bytes)
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            return EndpointState.serializer.deserialize(in, MessagingService.current_version);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    private static class PullSchemaFrom implements InstanceAction
    {
        final InetSocketAddress pullFrom;

        public PullSchemaFrom(IInvokableInstance pullFrom)
        {
            this.pullFrom = pullFrom.broadcastAddress();;
        }

        public void accept(IInvokableInstance pullTo)
        {
            pullTo.acceptsOnInstance((InetSocketAddress pullFrom) -> {
                InetAddressAndPort endpoint = toCassandraInetAddressAndPort(pullFrom);
                EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                Gossiper.instance.doOnChangeNotifications(endpoint, ApplicationState.SCHEMA, state.getApplicationState(ApplicationState.SCHEMA));
                assertTrue("schema is ready", Schema.instance.waitUntilReady(Duration.ofSeconds(10)));
            }).accept(pullFrom);
        }
    }

    private static class BootstrapAction implements InstanceAction, Serializable
    {
        private final boolean joinRing;
        private final Duration waitForBootstrap;
        private final Duration waitForSchema;

        public BootstrapAction()
        {
            this(true, Duration.ofMinutes(10), Duration.ofSeconds(10));
        }

        public BootstrapAction(boolean joinRing, Duration waitForBootstrap, Duration waitForSchema)
        {
            this.joinRing = joinRing;
            this.waitForBootstrap = waitForBootstrap;
            this.waitForSchema = waitForSchema;
        }

        public void accept(IInvokableInstance instance)
        {
            instance.appliesOnInstance((String partitionerString, String tokenString) -> {
                IPartitioner partitioner = FBUtilities.newPartitioner(partitionerString);
                List<Token> tokens = Collections.singletonList(partitioner.getTokenFactory().fromString(tokenString));
                try
                {
                    Collection<InetAddressAndPort> collisions = StorageService.instance.prepareForBootstrap(waitForSchema.toMillis(), 0);
                    assert collisions.size() == 0 : String.format("Didn't expect any replacements but got %s", collisions);
                    boolean isBootstrapSuccessful = StorageService.instance.bootstrap(tokens, waitForBootstrap.toMillis());
                    assert isBootstrapSuccessful : "Bootstrap did not complete successfully";
                    StorageService.instance.setUpDistributedSystemKeyspaces();
                    if (joinRing)
                        StorageService.instance.finishJoiningRing(true, tokens);
                }
                catch (Throwable t)
                {
                    throw new RuntimeException(t);
                }

                return null;
            }).apply(instance.config().getString("partitioner"), instance.config().getString("initial_token"));
        }
    }

    public static InstanceAction decommission()
    {
        return decommission(false);
    }

    public static InstanceAction decommission(boolean force)
    {
        return force ? (target) -> target.nodetoolResult("decommission",  "--force").asserts().success()
                     : (target) -> target.nodetoolResult("decommission").asserts().success();
    }

    public static VersionedApplicationState tokens(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.TOKENS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).tokens(tokens));
    }

    public static VersionedApplicationState netVersion(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.NET_VERSION, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).networkVersion());
    }

    private static VersionedApplicationState unsafeReleaseVersion(IInvokableInstance instance, String partitionerStr, String releaseVersionStr)
    {
        return unsafeVersionedValue(instance, ApplicationState.RELEASE_VERSION, (partitioner) -> new VersionedValue.VersionedValueFactory(partitioner).releaseVersion(releaseVersionStr), partitionerStr);
    }

    public static VersionedApplicationState releaseVersion(IInvokableInstance instance)
    {
        return unsafeReleaseVersion(instance, instance.config().getString("partitioner"), instance.getReleaseVersionString());
    }

    public static VersionedApplicationState statusNormal(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).normal(tokens));
    }

    public static VersionedApplicationState statusWithPortNormal(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS_WITH_PORT, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).normal(tokens));
    }

    public static VersionedApplicationState statusBootstrapping(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).bootstrapping(tokens));
    }

    public static VersionedApplicationState statusWithPortBootstrapping(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS_WITH_PORT, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).bootstrapping(tokens));
    }

    public static VersionedApplicationState statusLeaving(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).leaving(tokens));
    }

    public static VersionedApplicationState statusLeft(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS, (partitioner, tokens) -> {
            return new VersionedValue.VersionedValueFactory(partitioner).left(tokens, currentTimeMillis() + Gossiper.aVeryLongTime);
        });
    }

    public static VersionedApplicationState statusWithPortLeft(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS_WITH_PORT, (partitioner, tokens) -> {
            return new VersionedValue.VersionedValueFactory(partitioner).left(tokens, currentTimeMillis() + Gossiper.aVeryLongTime);

        });
    }

    public static VersionedApplicationState statusWithPortLeaving(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS_WITH_PORT, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).leaving(tokens));
    }

    public static VersionedValue toVersionedValue(VersionedApplicationState vv)
    {
        return VersionedValue.unsafeMakeVersionedValue(vv.value, vv.version);
    }

    public static ApplicationState toApplicationState(VersionedApplicationState vv)
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

    public static VersionedApplicationState versionedToken(IInvokableInstance instance, ApplicationState applicationState, IIsolatedExecutor.SerializableBiFunction<IPartitioner, Collection<Token>, VersionedValue> supplier)
    {
        return unsafeVersionedValue(instance, applicationState, supplier, instance.config().getString("partitioner"), instance.config().getString("initial_token"));
    }

    public static InstanceAction removeFromRing(IInvokableInstance peer)
    {
        return (target) -> {
            InetAddressAndPort endpoint = toCassandraInetAddressAndPort(peer.broadcastAddress());
            VersionedApplicationState newState = statusLeft(peer);

            target.runOnInstance(() -> {
                // state to 'left'
                EndpointState currentState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                ApplicationState as = toApplicationState(newState);
                VersionedValue vv = toVersionedValue(newState);
                currentState.addApplicationState(as, vv);
                StorageService.instance.onChange(endpoint, as, vv);
            });
        };
    }

    /**
     * Changes gossip state of the `peer` on `target`
     */
    public static void changeGossipState(IInvokableInstance target, IInstance peer, List<VersionedApplicationState> newState)
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

    public static void withProperty(CassandraRelevantProperties prop, boolean value, Runnable r)
    {
        withProperty(prop, Boolean.toString(value), r);
    }

    public static void withProperty(CassandraRelevantProperties prop, String value, Runnable r)
    {
        String prev = prop.setString(value);
        try
        {
            r.run();
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
