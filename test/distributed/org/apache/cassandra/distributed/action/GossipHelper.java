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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.shared.VersionedApplicationState;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class GossipHelper
{
    public static InstanceAction statusToBootstrap(IInvokableInstance newNode)
    {
        return (instance) ->
        {
            changeGossipState(instance,
                              newNode,
                              Arrays.asList(tokens(newNode),
                                            statusBootstrapping(newNode)));
        };
    }

    public static InstanceAction statusToDecommission(IInvokableInstance newNode)
    {
        return (instance) ->
        {
            changeGossipState(instance,
                              newNode,
                              Arrays.asList(tokens(newNode),
                                            statusLeaving(newNode)));
        };
    }

    public static VersionedApplicationState statusLeaving(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).leaving(tokens));
    }

    public static void withProperty(String prop, String value, Runnable r)
    {
        String before = System.getProperty(prop);
        try
        {
            System.setProperty(prop, value);
            r.run();
        }
        finally
        {
            if (before == null)
                System.clearProperty(prop);
            else
                System.setProperty(prop, before);
        }
    }

    private static VersionedApplicationState unsafeVersionedValue(IInvokableInstance instance,
                                                                  ApplicationState applicationState,
                                                                  IIsolatedExecutor.SerializableBiFunction<IPartitioner, Collection<Token>, VersionedValue> supplier,
                                                                  String partitionerStr, String initialTokenStr)
    {
        return instance.appliesOnInstance((String partitionerString, String tokenString) -> {
            IPartitioner partitioner = FBUtilities.newPartitioner(partitionerString);
            Token token = partitioner.getTokenFactory().fromString(tokenString);

            VersionedValue versionedValue = supplier.apply(partitioner, Collections.singleton(token));
            return new VersionedApplicationState(applicationState.ordinal(), versionedValue.value, versionedValue.version);
        }).apply(partitionerStr, initialTokenStr);
    }

    /**
     * Changes gossip state of the `peer` on `target`
     */
    public static void changeGossipState(IInvokableInstance target, IInstance peer, List<VersionedApplicationState> newState)
    {
        InetSocketAddress addr = peer.broadcastAddress();
        UUID hostId = peer.config().hostId();
        target.runOnInstance(() -> {
            InetAddress endpoint = addr.getAddress();
            StorageService storageService = StorageService.instance;

            Gossiper.runInGossipStageBlocking(() -> {
                EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                if (state == null)
                {
                    Gossiper.instance.initializeNodeUnsafe(endpoint, hostId, 1);
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

    public static VersionedValue toVersionedValue(VersionedApplicationState vv)
    {
        return VersionedValue.unsafeMakeVersionedValue(vv.value, vv.version);
    }

    public static VersionedApplicationState tokens(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.TOKENS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).tokens(tokens));
    }

    public static ApplicationState toApplicationState(VersionedApplicationState vv)
    {
        return ApplicationState.values()[vv.applicationState];
    }

    public static VersionedApplicationState versionedToken(IInvokableInstance instance, ApplicationState applicationState, IIsolatedExecutor.SerializableBiFunction<IPartitioner, Collection<Token>, VersionedValue> supplier)
    {
        return unsafeVersionedValue(instance, applicationState, supplier, instance.config().getString("partitioner"), instance.config().getString("initial_token"));
    }

    public static VersionedApplicationState statusBootstrapping(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).bootstrapping(tokens));
    }
}