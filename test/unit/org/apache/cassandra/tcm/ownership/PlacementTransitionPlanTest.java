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

package org.apache.cassandra.tcm.ownership;

import com.google.common.collect.ImmutableMultimap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;

public class PlacementTransitionPlanTest
{
    private static final ReplicationParams params = ReplicationParams.simple(2);
    private static NodeId node;
    private static ClusterMetadata metadata;
    @BeforeClass
    public static void setup()
    {
        ServerTestUtils.prepareServerNoRegister();
        InetAddressAndPort ep = InetAddressAndPort.getByNameUnchecked("127.0.0.2");
        node = ClusterMetadataTestHelper.register(ep);
        ClusterMetadataTestHelper.join(ep, ClusterMetadataTestHelper.bytesToken(0));
        metadata = ClusterMetadata.current();
    }

    @Test(expected = Transformation.RejectedTransformationException.class)
    public void testEmptyWriteReplica()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> newReads = rbe(r(0, 20));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(newReads)).build();
        assertPreExistingWriteReplica(startPlacements, addRead);
    }

    @Test
    public void testHasWriteReplica()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> newReplica = rbe(r(0, 20));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(newReplica)).build();

        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(newReplica)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test
    public void testHasSplitWriteReplica()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> writeReplicas = rbe(r(0, 20), r(20, 40));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas)).build();
        ImmutableMultimap<NodeId, ReplicaNode> readReplicas = rbe(r(0, 40));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(readReplicas)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }
    @Test
    public void testAddSplitReadReplica()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> writeReplicas = rbe(r(0, 40));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas)).build();
        ImmutableMultimap<NodeId, ReplicaNode> readReplicas = rbe(r(0, 20), r(20, 40));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(readReplicas)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test
    public void testAddSplitReadReplicaGap()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> writeReplicas = rbe(r(0, 40));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas)).build();
        ImmutableMultimap<NodeId, ReplicaNode> readReplicas = rbe(r(0, 20), r(25, 40)); // this won't happen, but all read replicas are "covered" by the write replica above
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(readReplicas)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test(expected = Transformation.RejectedTransformationException.class)
    public void testHasSplitWriteReplicaWithGaps()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> writeReplicas = rbe(r(0, 20), r(21, 40)); // token 21 missing
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas)).build();
        ImmutableMultimap<NodeId, ReplicaNode> readReplicas = rbe(r(0, 40));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(readReplicas)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test
    public void testPlacementsAreUpdatedByDeltas()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> writeReplicas1 = rbe(r(0, 20));
        PlacementDeltas addWrite1 = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas1)).build();
        ImmutableMultimap<NodeId, ReplicaNode> writeReplicas2 = rbe(r(20, 40));
        PlacementDeltas addWrite2 = PlacementDeltas.builder()
                                                   .put(params,
                                                        addWriteDelta(writeReplicas2)).build();
        ImmutableMultimap<NodeId, ReplicaNode> readReplicas = rbe(r(0, 40));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(readReplicas)).build();
        // first delta adds (0, 20] as write, second (20, 40] - make sure both are in placements when adding the read replica;
        assertPreExistingWriteReplica(startPlacements, addWrite1, addWrite2, addRead);
    }

    @Test(expected = Transformation.RejectedTransformationException.class)
    public void testDisallowAddingFullReadWithTransientWrite()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> transientWrite = rbeTransient(r(0, 20));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(transientWrite)).build();

        ImmutableMultimap<NodeId, ReplicaNode> fullRead = rbe(r(0,20));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(fullRead)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test
    public void testAllowAddingTransientReadWithTransientWrite()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> transientWrite = rbeTransient(r(0, 20));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(transientWrite)).build();

        ImmutableMultimap<NodeId, ReplicaNode> transientRead = rbeTransient(r(0,20));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(transientRead)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test
    public void testAllowAddingTransientReadWithFullWrite()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> fullWrite = rbe(r(0, 20));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(fullWrite)).build();

        ImmutableMultimap<NodeId, ReplicaNode> transientRead = rbeTransient(r(0,20));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(transientRead)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test(expected = Transformation.RejectedTransformationException.class)
    public void testHasSplitTransientWriteReplica()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        ImmutableMultimap<NodeId, ReplicaNode> writeReplicas1 = rbe(r(0, 20));
        ImmutableMultimap<NodeId, ReplicaNode> writeReplicas2 = rbeTransient(r(20, 40));
        PlacementDeltas addWriteFull = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas1)).build();
        PlacementDeltas addWriteTransient = PlacementDeltas.builder()
                                                           .put(params,
                                                           addWriteDelta(writeReplicas2)).build();

        ImmutableMultimap<NodeId, ReplicaNode> readReplicas = rbe(r(0, 40));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(readReplicas)).build();
        assertPreExistingWriteReplica(startPlacements, addWriteFull, addWriteTransient, addRead);
    }

    private void assertPreExistingWriteReplica(DataPlacements start, PlacementDeltas ... deltasInOrder)
    {
        new PlacementTransitionPlan(PlacementDeltas.empty(),
                                    PlacementDeltas.empty(),
                                    PlacementDeltas.empty(),
                                    PlacementDeltas.empty()).assertPreExistingWriteReplica(metadata.directory, start, deltasInOrder);
    }

    private PlacementDeltas.PlacementDelta addReadDelta(ImmutableMultimap<NodeId, ReplicaNode> replica)
    {
        return new PlacementDeltas.PlacementDelta(new NodeIdDelta(ImmutableMultimap.of(), replica), NodeIdDelta.empty());
    }

    private PlacementDeltas.PlacementDelta addWriteDelta(ImmutableMultimap<NodeId, ReplicaNode> replica)
    {
        return new PlacementDeltas.PlacementDelta(NodeIdDelta.empty(), new NodeIdDelta(ImmutableMultimap.of(), replica));
    }

    private ImmutableMultimap<NodeId, ReplicaNode> rbe(Range<Token> ... ranges)
    {
        ImmutableMultimap.Builder<NodeId, ReplicaNode> builder = ImmutableMultimap.builder();
        for (Range<Token> r : ranges)
            builder.put(node, new ReplicaNode(node, r, true));
        return builder.build();
    }

    private ImmutableMultimap<NodeId, ReplicaNode> rbeTransient(Range<Token> ... ranges)
    {
        ImmutableMultimap.Builder<NodeId, ReplicaNode> builder = ImmutableMultimap.builder();
        for (Range<Token> r : ranges)
            builder.put(node, new ReplicaNode(node, r, false));
        return builder.build();
    }

    private Range<Token> r(long start, long end)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(start), new Murmur3Partitioner.LongToken(end));
    }
}
