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

package org.apache.cassandra.distributed.test.log;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.UnknownHostException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.SealPeriod;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CMS_ALLOW_TRANSFORMATIONS_DURING_UPGRADES;

public class RegisterTest extends TestBaseImpl
{
    @Test
    public void testRegistrationIdempotence() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(5))
                                        .withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(5, "dc0", "rack0"))
                                        .createWithoutStarting())
        {
            // Make sure 2 and 3 do not race for ID
            for (int i : new int[]{ 1,3,2 })
                cluster.get(i).startup();

            for (int i : new int[]{ 3, 2 })
            {
                cluster.get(i).runOnInstance(() -> {
                    ClusterMetadataService.instance().commit(new PrepareLeave(ClusterMetadata.current().myNodeId(),
                                                                              true,
                                                                              ClusterMetadataService.instance().placementProvider(),
                                                                              LeaveStreams.Kind.UNBOOTSTRAP));
                    UnbootstrapAndLeave unbootstrapAndLeave = (UnbootstrapAndLeave) ClusterMetadata.current().inProgressSequences.get(ClusterMetadata.current().myNodeId());
                    ClusterMetadataService.instance().commit(unbootstrapAndLeave.startLeave);
                    ClusterMetadataService.instance().commit(unbootstrapAndLeave.midLeave);
                    ClusterMetadataService.instance().commit(unbootstrapAndLeave.finishLeave);
                    ClusterMetadataService.instance().commit(new Unregister(ClusterMetadata.current().myNodeId()));
                });

                cluster.get(1).runOnInstance(() -> {
                    ClusterMetadataService.instance().commit(SealPeriod.instance);
                });

                IInstanceConfig config = cluster.newInstanceConfig();
                IInvokableInstance newInstance = cluster.bootstrap(config);
                newInstance.startup();
            }
        }
    }

    @Test
    public void serializationVersionDisagreementTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .createWithoutStarting();
             WithProperties prop = new WithProperties().set(CMS_ALLOW_TRANSFORMATIONS_DURING_UPGRADES, "true"))
        {
            cluster.get(1).startup();
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    // Register a ghost node with V0 to fake-force V0 serialization. In a real world cluster we will always be upgrading from a smaller version.
                    ClusterMetadataService.instance().commit(new Register(new NodeAddresses(InetAddressAndPort.getByName("127.0.0.10")),
                                                                          ClusterMetadata.current().directory.location(ClusterMetadata.current().myNodeId()),
                                                                          new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V0)));
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
                ClusterMetadataService.instance().commit(SealPeriod.instance);
            });

            cluster.get(2).runOnInstance(() -> {
                try
                {
                    Field field = NodeVersion.class.getDeclaredField("CURRENT");
                    Field modifiers = Field.class.getDeclaredField("modifiers");

                    field.setAccessible(true);
                    modifiers.setAccessible(true);

                    int newModifiers = field.getModifiers() & ~Modifier.FINAL;
                    modifiers.setInt(field, newModifiers);
                    field.set(null, new NodeVersion(new CassandraVersion(FBUtilities.getReleaseVersionString()), NodeVersion.CURRENT_METADATA_VERSION));

                }
                catch (NoSuchFieldException | IllegalAccessException e)
                {
                    throw new RuntimeException(e);
                }
            });

            cluster.get(2).startup();
        }
    }

    @Test
    public void replayLocallyFromV0Snapshot() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .createWithoutStarting())
        {
            cluster.get(1).startup();
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    // Register a ghost node with V0 to fake-force V0 serialization. In a real world cluster we will always be upgrading from a smaller version.
                    ClusterMetadataService.instance().commit(new Register(new NodeAddresses(InetAddressAndPort.getByName("127.0.0.10")),
                                                                          ClusterMetadata.current().directory.location(ClusterMetadata.current().myNodeId()),
                                                                          new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V0)));
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
                ClusterMetadataService.instance().commit(SealPeriod.instance);

                ClusterMetadata cm = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots().getSnapshot(ClusterMetadata.current().epoch);
                cm.equals(ClusterMetadata.current());
            });


        }
    }

}
