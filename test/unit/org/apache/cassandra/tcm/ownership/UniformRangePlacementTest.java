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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.schema.ReplicationParams;

import static org.apache.cassandra.tcm.ownership.OwnershipUtils.rg;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.token;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UniformRangePlacementTest
{

    @Test
    public void testSplittingPlacementWithSingleRange()
    {
        // Special case of splitting a range that was created because the initial token list contained a single token,
        // which is either the min or max value in the token space. Any other single token would produce two ranges -
        // (MIN, t] & (t, MAX] - but because (x, x] denotes a wraparound, this case produces (MIN, MAX] and we need to
        // verify that we can safely split that when more tokens are introduced. This test supposes MIN = 0, MAX = 100
        PlacementForRange before = PlacementForRange.builder()
                                                    .withReplicaGroup(rg(0, 100, 1, 2, 3))
                                                    .build();
        // existing token is MIN (i.e. 0 for the purposes of this test)
        List<Token> tokens = ImmutableList.of(token(0), token(30), token(60), token(90));
        PlacementForRange after = PlacementForRange.splitRangesForPlacement(tokens, before);
        assertPlacement(after,
                        rg(0, 30, 1, 2, 3),
                        rg(30, 60, 1, 2, 3),
                        rg(60, 90, 1, 2, 3),
                        rg(90, 100, 1, 2, 3));


        // existing token is MAX (i.e. 100 for the purposes of this test).
        tokens = ImmutableList.of(token(30), token(60), token(90), token(100));
        after = PlacementForRange.splitRangesForPlacement(tokens, before);
        assertPlacement(after,
                        rg(0, 30, 1, 2, 3),
                        rg(30, 60, 1, 2, 3),
                        rg(60, 90, 1, 2, 3),
                        rg(90, 100, 1, 2, 3));
    }

    @Test
    public void testSplitSingleRange()
    {
        // Start with:  (0,100]   : (n1,n2,n3);
        //              (100,200] : (n1,n2,n3);
        //              (200,300] : (n1,n2,n3);
        //              (300,400] : (n1,n2,n3);
        PlacementForRange before = initialPlacement();
        // split (100,200] to (100,150], (150,200]
        List<Token> tokens = ImmutableList.of(token(100), token(150), token(200), token(300));
        PlacementForRange after = PlacementForRange.splitRangesForPlacement(tokens, before);
        assertPlacement(after,
                        rg(0, 100, 1, 2, 3),
                        rg(100, 150, 1, 2, 3),
                        rg(150, 200, 1, 2, 3),
                        rg(200, 300, 1, 2, 3),
                        rg(300, 400, 1, 2, 3));
    }

    @Test
    public void testSplitMultipleDisjointRanges()
    {
        // Start with:  (0,100]   : (n1,n2,n3);
        //              (100,200] : (n1,n2,n3);
        //              (200,300] : (n1,n2,n3);
        //              (300,400] : (n1,n2,n3);
        PlacementForRange before = initialPlacement();
        // split (100,200] to (100,150],(150,200]
        // and   (200,300] to (200,250],(250,300]
        List<Token> tokens = ImmutableList.of(token(100), token(150), token(200), token(250), token(300));
        PlacementForRange after = PlacementForRange.splitRangesForPlacement(tokens, before);
        assertPlacement(after,
                        rg(0, 100, 1, 2, 3),
                        rg(100, 150, 1, 2, 3),
                        rg(150, 200, 1, 2, 3),
                        rg(200, 250, 1, 2, 3),
                        rg(250, 300, 1, 2, 3),
                        rg(300, 400, 1, 2, 3));
    }

    @Test
    public void testSplitSingleRangeMultipleTimes()
    {
        // Start with:  (0,100]   : (n1,n2,n3);
        //              (100,200] : (n1,n2,n3);
        //              (200,300] : (n1,n2,n3);
        //              (300,400] : (n1,n2,n3);
        PlacementForRange before = initialPlacement();
        // split (100,200] to (100,125],(125,150],(150,200]
        List<Token> tokens = ImmutableList.of(token(100), token(125), token(150), token(200), token(300));
        PlacementForRange after = PlacementForRange.splitRangesForPlacement(tokens, before);
        assertPlacement(after,
                        rg(0, 100, 1, 2, 3),
                        rg(100, 125, 1, 2, 3),
                        rg(125, 150, 1, 2, 3),
                        rg(150, 200, 1, 2, 3),
                        rg(200, 300, 1, 2, 3),
                        rg(300, 400, 1, 2, 3));
    }

    @Test
    public void testSplitMultipleRangesMultipleTimes()
    {
        PlacementForRange before = initialPlacement();
        // split (100,200] to (100,125],(125,150],(150,200]
        // and   (200,300] to (200,225],(225,250],(250,300]
        List<Token> tokens = ImmutableList.of(token(100), token(125), token(150), token(200), token(225), token(250), token(300));
        PlacementForRange after = PlacementForRange.splitRangesForPlacement(tokens, before);
        assertPlacement(after,
                        rg(0, 100, 1, 2, 3),
                        rg(100, 125, 1, 2, 3),
                        rg(125, 150, 1, 2, 3),
                        rg(150, 200, 1, 2, 3),
                        rg(200, 225, 1, 2, 3),
                        rg(225, 250, 1, 2, 3),
                        rg(250, 300, 1, 2, 3),
                        rg(300, 400, 1, 2, 3));
    }

    @Test
    public void testSplitLastRangeMultipleTimes()
    {
        // Start with:  (0,100]   : (n1,n2,n3);
        //              (100,200] : (n1,n2,n3);
        //              (200,300] : (n1,n2,n3);
        //              (300,400] : (n1,n2,n3);
        PlacementForRange before = initialPlacement();
        // split (300,400] to (300,325],(325,350],(350,400]
        List<Token> tokens = ImmutableList.of(token(100), token(200), token(300), token(325), token(350));
        PlacementForRange after = PlacementForRange.splitRangesForPlacement(tokens, before);
        assertPlacement(after,
                        rg(0, 100, 1, 2, 3),
                        rg(100, 200, 1, 2, 3),
                        rg(200, 300, 1, 2, 3),
                        rg(300, 325, 1, 2, 3),
                        rg(325, 350, 1, 2, 3),
                        rg(350, 400, 1, 2, 3));
    }

    @Test
    public void testSplitFirstRangeMultipleTimes()
    {
        // Start with:  (0,100]   : (n1,n2,n3);
        //              (100,200] : (n1,n2,n3);
        //              (200,300] : (n1,n2,n3);
        //              (300,400] : (n1,n2,n3);
        PlacementForRange before = initialPlacement();
        // split (0,100] to (0,25],(25,50],(50,100]
        List<Token> tokens = ImmutableList.of(token(25), token(50), token(100), token(200), token(300));
        PlacementForRange after = PlacementForRange.splitRangesForPlacement(tokens, before);
        assertPlacement(after,
                        rg(0, 25, 1, 2, 3),
                        rg(25, 50, 1, 2, 3),
                        rg(50, 100, 1, 2, 3),
                        rg(100, 200, 1, 2, 3),
                        rg(200, 300, 1, 2, 3),
                        rg(300, 400, 1, 2, 3));
    }

    @Test
    public void testCombiningPlacements()
    {
        EndpointsForRange[] firstGroups = { rg(0, 100, 1, 2, 3),
                                            rg(100, 200, 1, 2, 3),
                                            rg(200, 300, 1, 2, 3),
                                            rg(300, 400, 1, 2, 3) };
        PlacementForRange p1 = PlacementForRange.builder().withReplicaGroups(Arrays.asList(firstGroups)).build();
        EndpointsForRange[] secondGroups = { rg(0, 100, 2, 3, 4),
                                             rg(100, 200, 2, 3, 5),
                                             rg(200, 300, 2, 3, 6),
                                             rg(300, 400, 2, 3, 7) };
        PlacementForRange p2 = PlacementForRange.builder().withReplicaGroups(Arrays.asList(secondGroups)).build();

        ReplicationParams params = ReplicationParams.simple(1);
        DataPlacements map1 = DataPlacements.builder(1).with(params, new DataPlacement(p1, p1)).build();
        DataPlacements map2 = DataPlacements.builder(1).with(params, new DataPlacement(p2, p2)).build();
        DataPlacement p3 = map1.combineReplicaGroups(map2).get(params);
        for (PlacementForRange placement : new PlacementForRange[]{ p3.reads, p3.writes })
        {
            assertPlacement(placement,
                            rg(0, 100, 1, 2, 3, 4),
                            rg(100, 200, 1, 2, 3, 5),
                            rg(200, 300, 1, 2, 3, 6),
                            rg(300, 400, 1, 2, 3, 7));
        }
    }

    @Test
    public void testSplittingNeedsSorting()
    {
        List<Token> tokens = ImmutableList.of(token(-4611686018427387905L), token(-3L));
        EndpointsForRange[] initial = { rg(-9223372036854775808L, -4611686018427387905L, 1),
                                        rg(-4611686018427387905L, -9223372036854775808L, 1)};
        DataPlacement.Builder builder = DataPlacement.builder();
        builder.writes.withReplicaGroups(Arrays.asList(initial));

        DataPlacement initialPlacement = builder.build();
        DataPlacement split = initialPlacement.splitRangesForPlacement(tokens);
        assertPlacement(split.writes, rg(-3, -9223372036854775808L, 1),
                        rg(-9223372036854775808L,-4611686018427387905L, 1),
                        rg(-4611686018427387905L, -3, 1));
    }

    @Test
    public void testInitialFullWrappingRange()
    {
        /*
        TOKENS=[-9223372036854775808, 3074457345618258602] PLACEMENT=[[Full(/127.0.0.1:7000,(-9223372036854775808,-9223372036854775808])]]
         */
        List<Token> tokens = ImmutableList.of(token(-9223372036854775808L), token(3074457345618258602L));
        EndpointsForRange[] initial = { rg(-9223372036854775808L, -9223372036854775808L, 1)};

        DataPlacement.Builder builder = DataPlacement.builder();
        builder.writes.withReplicaGroups(Arrays.asList(initial));

        DataPlacement initialPlacement = builder.build();
        DataPlacement split = initialPlacement.splitRangesForPlacement(tokens);
        assertPlacement(split.writes, rg(3074457345618258602L,-9223372036854775808L, 1), rg(-9223372036854775808L, 3074457345618258602L, 1));
    }

    @Test
    public void testWithMinToken()
    {
        EndpointsForRange initialRG = rg(Long.MIN_VALUE, Long.MIN_VALUE, 1, 2, 3);

        DataPlacement.Builder builder = DataPlacement.builder();
        builder.writes.withReplicaGroup(initialRG);

        DataPlacement initialPlacement = builder.build();
        List<Token> tokens = ImmutableList.of(token(Long.MIN_VALUE), token(0));
        DataPlacement newPlacement = initialPlacement.splitRangesForPlacement(tokens);
        assertEquals(2, newPlacement.writes.replicaGroups.values().size());
    }

    private PlacementForRange initialPlacement()
    {
        EndpointsForRange[] initialGroups = { rg(0, 100, 1, 2, 3),
                                              rg(100, 200, 1, 2, 3),
                                              rg(200, 300, 1, 2, 3),
                                              rg(300, 400, 1, 2, 3) };
        PlacementForRange placement = PlacementForRange.builder().withReplicaGroups(Arrays.asList(initialGroups)).build();
        assertPlacement(placement, initialGroups);
        return placement;
    }

    private void assertPlacement(PlacementForRange placement, EndpointsForRange...expected)
    {
        Collection<EndpointsForRange> replicaGroups = placement.replicaGroups.values();
        assertEquals(replicaGroups.size(), expected.length);
        int i = 0;
        boolean allMatch = true;
        for(EndpointsForRange group : replicaGroups)
            if (!Iterables.elementsEqual(group, expected[i++]))
                allMatch = false;

        assertTrue(String.format("Placement didn't match expected replica groups. " +
                                 "%nExpected: %s%nActual: %s", Arrays.asList(expected), replicaGroups),
                   allMatch);
    }
}
