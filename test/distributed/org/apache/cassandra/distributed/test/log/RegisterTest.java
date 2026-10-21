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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.junit.Test;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.TriggerSnapshot;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.addr;
import static org.apache.cassandra.tcm.membership.NodeVersion.CURRENT_METADATA_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

                // Run a custom transformation to inject a fake node into the directory with a known id and an
                // artificially lowered max supported serialization version
                CustomTransformation.registerExtension(RegisterNodeWithOldVersion.NAME, RegisterNodeWithOldVersion.serializer);
                CustomTransformation injectOldNode = new CustomTransformation(RegisterNodeWithOldVersion.NAME,
                                                                              new RegisterNodeWithOldVersion());
                ClusterMetadataService.instance().commit(injectOldNode);

                // Doesn't matter which specific Transformation we use here, we're testing that the serializer uses
                // the correct lower bound
                Transformation t = new Register(NodeAddresses.current(), TEST_LOCATION, NodeVersion.CURRENT);
                try
                {
                    assertEquals(ClusterMetadata.current().directory.commonSerializationVersion, RegisterNodeWithOldVersion.METADATA_VERSION);
                    ByteBuffer bytes = t.kind().toVersionedBytes(t);
                    try (DataInputBuffer buf = new DataInputBuffer(bytes, true))
                    {
                        // Because ClusterMetadata.current().directory still contains the fake old node we must
                        // serialize at the version _it_ supports
                        assertEquals(RegisterNodeWithOldVersion.METADATA_VERSION, Version.fromInt(buf.readUnsignedVInt32()));
                    }

                    // If we unregister the fake node, then the ceiling for serialization version will rise
                    Unregister unregisterOldNode = new Unregister(RegisterNodeWithOldVersion.NODE_ID,
                                                                  EnumSet.allOf(NodeState.class),
                                                                  ClusterMetadataService.instance().placementProvider());
                    ClusterMetadataService.instance().commit(unregisterOldNode);

                    assertEquals(ClusterMetadata.current().directory.commonSerializationVersion, CURRENT_METADATA_VERSION);
                    bytes = t.kind().toVersionedBytes(t);
                    try (DataInputBuffer buf = new DataInputBuffer(bytes, true))
                    {
                        assertEquals(CURRENT_METADATA_VERSION, Version.fromInt(buf.readUnsignedVInt32()));
                    }
                }
                catch (IOException e)
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
                // Run a custom transformation to inject a fake node into the directory with a known id and an
                // artificially lowered max supported serialization version
                CustomTransformation.registerExtension(RegisterNodeWithOldVersion.NAME, RegisterNodeWithOldVersion.serializer);
                CustomTransformation injectOldNode = new CustomTransformation(RegisterNodeWithOldVersion.NAME,
                                                                              new RegisterNodeWithOldVersion());
                ClusterMetadataService.instance().commit(injectOldNode);
                // Now trigger a snapshot which must be written to the snapshot using the old serialization version
                Epoch epoch = ClusterMetadataService.instance().commit(TriggerSnapshot.instance).epoch;
                // fetch the raw bytes of the snapshot we just serialized
                ByteBuffer bytes = SystemKeyspace.getSnapshot(epoch);
                assertNotNull(bytes);
                // assert the prepended version matches
                Version writtenVersion = null;
                try
                {
                    writtenVersion = Version.fromInt(VIntCoding.readUnsignedVInt32(new DataInputBuffer(bytes, false)));
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
                assertEquals(RegisterNodeWithOldVersion.METADATA_VERSION, writtenVersion);
                // load the snapshot using the standard mechanism and assert it matches current cluster metadata
                ClusterMetadata cm = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots().getSnapshot(ClusterMetadata.current().epoch);
                cm.equals(ClusterMetadata.current());
            });
        }
    }

    // Custom transforms to lock/unlock an arbitrary set of ranges to
    // avoid having to actually initiate some range movement
    public static class RegisterNodeWithOldVersion implements Transformation, Serializable
    {
        public static final AsymmetricMetadataSerializer<Transformation, RegisterNodeWithOldVersion> serializer = new AsymmetricMetadataSerializer<Transformation, RegisterNodeWithOldVersion>()
        {
            @Override
            public void serialize(Transformation t, DataOutputPlus out, Version version){}
            @Override
            public RegisterNodeWithOldVersion deserialize(DataInputPlus in, Version version) {return new RegisterNodeWithOldVersion();}
            @Override
            public long serializedSize(Transformation t, Version version) {return 0;}
        };

        public static final String NAME = "TestRegisterNodeWithOldVersion";
        public static final NodeId NODE_ID = new NodeId(99);
        public static final Version METADATA_VERSION = Version.V0;

        @Override
        public Kind kind()
        {
            return Kind.CUSTOM;
        }

        @Override
        public Result execute(ClusterMetadata metadata)
        {
            ClusterMetadata.Transformer transformer = metadata.transformer()
                                                              .unsafeRegisterForTesting(NODE_ID,
                                                                                        new NodeAddresses(addr(99)),
                                                                                        TEST_LOCATION,
                                                                                        new NodeVersion(NodeVersion.CURRENT.cassandraVersion,
                                                                                                        METADATA_VERSION));
            return Transformation.success(transformer, LockedRanges.AffectedRanges.EMPTY);
        }
    }

}
