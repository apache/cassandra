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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.utils.FBUtilities;

public class PlacementTransitionPlanTest
{
    private static final ReplicationParams params = ReplicationParams.simple(2);
    private static final InetAddressAndPort ep = FBUtilities.getBroadcastAddressAndPort();
    @Test(expected = Transformation.RejectedTransformationException.class)
    public void testEmptyWriteReplica()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        RangesByEndpoint newReads = rbe(r(0, 20));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(newReads)).build();
        assertPreExistingWriteReplica(startPlacements, addRead);
    }

    @Test
    public void testHasWriteReplica()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        RangesByEndpoint newReplica = rbe(r(0, 20));
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
        RangesByEndpoint writeReplicas = rbe(r(0, 20), r(20, 40));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas)).build();
        RangesByEndpoint readReplicas = rbe(r(0, 40));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(readReplicas)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }
    @Test
    public void testAddSplitReadReplica()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        RangesByEndpoint writeReplicas = rbe(r(0, 40));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas)).build();
        RangesByEndpoint readReplicas = rbe(r(0, 20), r(20, 40));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(readReplicas)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test
    public void testAddSplitReadReplicaGap()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        RangesByEndpoint writeReplicas = rbe(r(0, 40));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas)).build();
        RangesByEndpoint readReplicas = rbe(r(0, 20), r(25, 40)); // this won't happen, but all read replicas are "covered" by the write replica above
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(readReplicas)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test(expected = Transformation.RejectedTransformationException.class)
    public void testHasSplitWriteReplicaWithGaps()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        RangesByEndpoint writeReplicas = rbe(r(0, 20), r(21, 40)); // token 21 missing
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas)).build();
        RangesByEndpoint readReplicas = rbe(r(0, 40));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(readReplicas)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test
    public void testPlacementsAreUpdatedByDeltas()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        RangesByEndpoint writeReplicas1 = rbe(r(0, 20));
        PlacementDeltas addWrite1 = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas1)).build();
        RangesByEndpoint writeReplicas2 = rbe(r(20, 40));
        PlacementDeltas addWrite2 = PlacementDeltas.builder()
                                                   .put(params,
                                                        addWriteDelta(writeReplicas2)).build();
        RangesByEndpoint readReplicas = rbe(r(0, 40));
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
        RangesByEndpoint transientWrite = rbeTransient(r(0, 20));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(transientWrite)).build();

        RangesByEndpoint fullRead = rbe(r(0,20));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(fullRead)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test
    public void testAllowAddingTransientReadWithTransientWrite()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        RangesByEndpoint transientWrite = rbeTransient(r(0, 20));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(transientWrite)).build();

        RangesByEndpoint transientRead = rbeTransient(r(0,20));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(transientRead)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test
    public void testAllowAddingTransientReadWithFullWrite()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        RangesByEndpoint fullWrite = rbe(r(0, 20));
        PlacementDeltas addWrite = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(fullWrite)).build();

        RangesByEndpoint transientRead = rbeTransient(r(0,20));
        PlacementDeltas addRead = PlacementDeltas.builder()
                                                 .put(params,
                                                      addReadDelta(transientRead)).build();
        assertPreExistingWriteReplica(startPlacements, addWrite, addRead);
    }

    @Test(expected = Transformation.RejectedTransformationException.class)
    public void testHasSplitTransientWriteReplica()
    {
        DataPlacements startPlacements = DataPlacements.EMPTY;
        RangesByEndpoint writeReplicas1 = rbe(r(0, 20));
        RangesByEndpoint writeReplicas2 = rbeTransient(r(20, 40));
        PlacementDeltas addWriteFull = PlacementDeltas.builder()
                                                  .put(params,
                                                       addWriteDelta(writeReplicas1)).build();
        PlacementDeltas addWriteTransient = PlacementDeltas.builder()
                                                           .put(params,
                                                           addWriteDelta(writeReplicas2)).build();

        RangesByEndpoint readReplicas = rbe(r(0, 40));
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
                                    PlacementDeltas.empty()).assertPreExistingWriteReplica(start, deltasInOrder);
    }

    private PlacementDeltas.PlacementDelta addReadDelta(RangesByEndpoint replica)
    {
        return new PlacementDeltas.PlacementDelta(new Delta(RangesByEndpoint.EMPTY, replica), Delta.empty());
    }

    private PlacementDeltas.PlacementDelta addWriteDelta(RangesByEndpoint replica)
    {
        return new PlacementDeltas.PlacementDelta(Delta.empty(), new Delta(RangesByEndpoint.EMPTY, replica));
    }

    private RangesByEndpoint rbe(Range<Token> ... ranges)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(ep);
        for (Range<Token> r : ranges)
            builder.add(Replica.fullReplica(ep, r));
        return new RangesByEndpoint(ImmutableMap.of(ep, builder.build()));
    }

    private RangesByEndpoint rbeTransient(Range<Token> ... ranges)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(ep);
        for (Range<Token> r : ranges)
            builder.add(Replica.transientReplica(ep, r));
        return new RangesByEndpoint(ImmutableMap.of(ep, builder.build()));
    }


    private Range<Token> r(long start, long end)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(start), new Murmur3Partitioner.LongToken(end));
    }
}
