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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;

/**
 * Compare different operations, and make sure that executing operations such as move, bootstrap, etc.,
 * is consistent with bootstrapping nodes with equivalent token ownership. Useful for testing operations
 * that are not yet a part of simulator, like transient replication.
 */
public class OperationalEquivalenceTest extends CMSTestBase
{
    private static final Random rng = new Random(1);

    static
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
    }

    @Test
    public void testMove() throws Exception
    {
        testMove(new TokenPlacementModel.SimpleReplicationFactor(2));
        testMove(new TokenPlacementModel.SimpleReplicationFactor(3));
        testMove(new TokenPlacementModel.SimpleReplicationFactor(5));

        testMove(new TokenPlacementModel.NtsReplicationFactor(1, 2));
        testMove(new TokenPlacementModel.NtsReplicationFactor(1, 3));
        testMove(new TokenPlacementModel.NtsReplicationFactor(1, 5));

        testMove(new TokenPlacementModel.NtsReplicationFactor(3, 2));
        testMove(new TokenPlacementModel.NtsReplicationFactor(3, 3));
        testMove(new TokenPlacementModel.NtsReplicationFactor(3, 5));

        testMove(new TokenPlacementModel.SimpleReplicationFactor(3, 1));
        testMove(new TokenPlacementModel.SimpleReplicationFactor(3, 2));
        testMove(new TokenPlacementModel.NtsReplicationFactor(3, 3, 1));
        testMove(new TokenPlacementModel.NtsReplicationFactor(3, 5, 2));
    }

    public void testMove(TokenPlacementModel.ReplicationFactor rf) throws Exception
    {
        TokenPlacementModel.NodeFactory nodeFactory = TokenPlacementModel.nodeFactory();

        ClusterMetadata withMove = null;
        List<TokenPlacementModel.Node> equivalentNodes = new ArrayList<>();
        int nodes = 30;
        try (CMSSut sut = new CMSSut(AtomicLongBackedProcessor::new, false, rf))
        {
            AtomicInteger counter = new AtomicInteger(0);
            for (int i = 0; i < nodes; i++)
            {
                int dc = toDc(i, rf);
                TokenPlacementModel.Node node = nodeFactory.make(counter.incrementAndGet(), dc, 1);
                sut.service.commit(new Register(new NodeAddresses(node.addr()), new Location(node.dc(), node.rack()), NodeVersion.CURRENT));
                sut.service.commit(new UnsafeJoin(node.nodeId(), Collections.singleton(node.longToken()), sut.service.placementProvider()));
                equivalentNodes.add(node);
            }

            TokenPlacementModel.Node toMove = equivalentNodes.get(rng.nextInt(equivalentNodes.size()));
            TokenPlacementModel.Node moved = toMove.withNewToken();
            equivalentNodes.set(equivalentNodes.indexOf(toMove), moved);

            Move plan = SimulatedOperation.prepareMove(sut, toMove, moved.longToken()).get();
            Iterator<?> iter = SimulatedOperation.toIter(sut.service, plan.startMove, plan.midMove, plan.finishMove);
            while (iter.hasNext())
                iter.next();

            withMove = ClusterMetadata.current();
        }

        assertPlacements(simulateAndCompare(rf, equivalentNodes).placements,
                         withMove.placements);
    }

    private static ClusterMetadata simulateAndCompare(TokenPlacementModel.ReplicationFactor rf, List<TokenPlacementModel.Node> nodes) throws Exception
    {
        Collections.shuffle(nodes, rng);
        try (CMSSut sut = new CMSSut(AtomicLongBackedProcessor::new, false, rf))
        {
            for (TokenPlacementModel.Node node : nodes)
            {
                sut.service.commit(new Register(new NodeAddresses(node.addr()), new Location(node.dc(), node.rack()), NodeVersion.CURRENT));
                sut.service.commit(new UnsafeJoin(node.nodeId(), Collections.singleton(node.longToken()), sut.service.placementProvider()));
            }

            return ClusterMetadata.current();
        }
    }

    private static void assertPlacements(DataPlacements l, DataPlacements r)
    {
        l.forEach((params, lPlacement) -> {
            DataPlacement rPlacement = r.get(params);
            lPlacement.reads.forEach((range, lReplicas) -> {
                EndpointsForRange rReplicas = rPlacement.reads.forRange(range).get();

                Assert.assertEquals(toReplicas(lReplicas.get()),
                                    toReplicas(rReplicas));
            });

            lPlacement.writes.forEach((range, lReplicas) -> {
                EndpointsForRange rReplicas = rPlacement.writes.forRange(range).get();

                Assert.assertEquals(toReplicas(lReplicas.get()),
                                    toReplicas(rReplicas));

            });
        });
    }

    public static List<Replica> toReplicas(EndpointsForRange ep)
    {
        return ep.stream().sorted(Replica::compareTo).collect(Collectors.toList());
    }
    private static int toDc(int i, TokenPlacementModel.ReplicationFactor rf)
    {
        return (i % rf.dcs()) + 1;
    }

}
