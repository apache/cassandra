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

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.TriggerSnapshot;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.utils.CassandraVersion;

import static org.junit.Assert.assertEquals;

public class RegisterTest extends TestBaseImpl
{
    private static final Location TEST_LOCATION = new Location("datacenter1", "rack1");

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
                    PlacementProvider pp = ClusterMetadataService.instance().placementProvider();
                    ClusterMetadataService.instance().commit(new PrepareLeave(ClusterMetadata.current().myNodeId(),
                                                                              true,
                                                                              pp,
                                                                              LeaveStreams.Kind.UNBOOTSTRAP));
                    UnbootstrapAndLeave unbootstrapAndLeave = (UnbootstrapAndLeave) ClusterMetadata.current().inProgressSequences.get(ClusterMetadata.current().myNodeId());
                    ClusterMetadataService.instance().commit(unbootstrapAndLeave.startLeave);
                    ClusterMetadataService.instance().commit(unbootstrapAndLeave.midLeave);
                    ClusterMetadataService.instance().commit(unbootstrapAndLeave.finishLeave);
                    ClusterMetadataService.instance().commit(new Unregister(ClusterMetadata.current().myNodeId(), EnumSet.of(NodeState.LEFT), pp));
                });

                cluster.get(1).runOnInstance(() -> {
                    ClusterMetadataService.instance().commit(TriggerSnapshot.instance);
                });

                IInstanceConfig config = cluster.newInstanceConfig();
                IInvokableInstance newInstance = cluster.bootstrap(config);
                newInstance.startup();
            }
        }
    }

    @Test
    public void serializationVersionCeilingTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .createWithoutStarting())
        {
            cluster.get(1).startup();
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    // Unregister to make directory empty
                    ClusterMetadataService.instance().commit(new Unregister(ClusterMetadata.current().myNodeId(),
                                                                            EnumSet.allOf(NodeState.class),
                                                                            ClusterMetadataService.instance().placementProvider()));

                    // Register a ghost node with V0 (bypasses version check because directory is now empty).
                    // In a real world cluster we will always be upgrading from a smaller version.
                    ClusterMetadataService.instance().commit(new Register(new NodeAddresses(InetAddressAndPort.getByName("127.0.0.100")),
                                                                          TEST_LOCATION,
                                                                          new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V0)));
                    NodeId oldNode = ClusterMetadata.current().directory.peerId(InetAddressAndPort.getByName("127.0.0.100"));

                    // Register a node with upgraded version
                    CassandraVersion currentVersion = NodeVersion.CURRENT.cassandraVersion;
                    NodeVersion upgraded = new NodeVersion(new CassandraVersion(String.format("%d.%d.%d", currentVersion.major + 1, 0, 0)),
                                                            NodeVersion.CURRENT_METADATA_VERSION);
                    ClusterMetadataService.instance().commit(new Register(new NodeAddresses(InetAddressAndPort.getByName("127.0.0.200")), TEST_LOCATION, upgraded));

                    // Doesn't matter which specific Transformation we use here, we're testing that the serializer uses
                    // the correct lower bound
                    Transformation t = new Register(NodeAddresses.current(), new Location("DC", "RACK"), NodeVersion.CURRENT);
                    try
                    {
                        assertEquals(ClusterMetadata.current().directory.clusterMinVersion.serializationVersion,
                                     Version.V0.asInt());
                        ByteBuffer bytes = t.kind().toVersionedBytes(t);
                        try (DataInputBuffer buf = new DataInputBuffer(bytes, true))
                        {
                            // Because ClusterMetadata.current().directory still contains oldNode we must serialize at
                            // the version it supports
                            assertEquals(Version.V0, Version.fromInt(buf.readUnsignedVInt32()));
                        }

                        // If we unregister oldNode, then the ceiling for serialization version will rise
                        ClusterMetadataService.instance().commit(new Unregister(oldNode, EnumSet.allOf(NodeState.class), ClusterMetadataService.instance().placementProvider()));
                        assertEquals(ClusterMetadata.current().directory.clusterMinVersion.serializationVersion,
                                     NodeVersion.CURRENT_METADATA_VERSION.asInt());
                        bytes = t.kind().toVersionedBytes(t);
                        try (DataInputBuffer buf = new DataInputBuffer(bytes, true))
                        {
                            assertEquals(NodeVersion.CURRENT_METADATA_VERSION, Version.fromInt(buf.readUnsignedVInt32()));
                        }
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            });
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
                    // Unregister to make directory empty
                    ClusterMetadataService.instance().commit(new Unregister(ClusterMetadata.current().myNodeId(),
                                                                            EnumSet.allOf(NodeState.class),
                                                                            ClusterMetadataService.instance().placementProvider()));

                    // Register a ghost node with V0 (bypasses version check because directory is now empty).
                    // In a real world cluster we will always be upgrading from a smaller version.
                    ClusterMetadataService.instance().commit(new Register(new NodeAddresses(InetAddressAndPort.getByName("127.0.0.100")),
                                                                          TEST_LOCATION,
                                                                          new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V0)));
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
                ClusterMetadataService.instance().commit(TriggerSnapshot.instance);

                ClusterMetadata cm = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots().getSnapshot(ClusterMetadata.current().epoch);
                cm.equals(ClusterMetadata.current());
            });
        }
    }
}
