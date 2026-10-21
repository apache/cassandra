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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.CMSTestBase;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.addr;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.getLeavePlan;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.getMovePlan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class LocalRangesAllSettledTest
{
    private static final Logger logger = LoggerFactory.getLogger(LocalRangesAllSettledTest.class);

    private static final int[] INITIAL_NODES = new int[]{1, 2, 3, 4};

    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
    }

    @Before
    public void before() throws ExecutionException, InterruptedException
    {
        ClusterMetadataService.unsetInstance();
        new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, new TokenPlacementModel.SimpleReplicationFactor(3));

        // Join the first 4 nodes
        for (int i : INITIAL_NODES)
        {
            ClusterMetadataTestHelper.register(i, "dc" + i % 3, "rack0");
            ClusterMetadataTestHelper.join(i, i);
        }

        // Create keyspaces with various replication settings
        for (int i = 1; i <= 3; i++)
        {
            ClusterMetadataTestHelper.createKeyspace("simple_" + i, KeyspaceParams.simple(i));
            ClusterMetadataTestHelper.createKeyspace("nts_" + i, KeyspaceParams.nts("dc0", i, "dc1", i, "dc2", i));
        }

    }

    @Test
    public void testLeaving()
    {
        // Verify proposed ranges without any in flight operations
        AllLocalRanges initial = snapshotAllLocalRanges(LocalRangeStatus.CURRENT, INITIAL_NODES);
        AllLocalRanges proposed = snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES);
        assertEquals(initial, proposed);
        // Check against the actual write placements
        assertLocalRangesMatchPlacements(ClusterMetadata.current().placements(), initial, INITIAL_NODES);

        // Initiate an operation which affects ownership. This will add the MultiStepOperation which encodes any
        // necessary range movements so subsequent calls to ClusterMetadata::localRangesAllSettled
        // should return the expected local ranges after the operation has completed
        // pick a random node to leave (but not the CMS node (1), for simplicity's sake)
        int leaving = INITIAL_NODES[Math.max(1, new Random().nextInt(4))];
        logger.info("Selected node {} to leave", leaving);
        ClusterMetadataService.instance().commit(ClusterMetadataTestHelper.prepareLeave(leaving, true));
        proposed = snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES);
        assertNotEquals(initial, proposed);

        // Step through execution of the MSO, verifying after each step that the proposed ranges don't change
        UnbootstrapAndLeave plan = getLeavePlan(leaving);
        ClusterMetadataService.instance().commit(plan.startLeave);
        assertEquals(proposed, snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES));
        ClusterMetadataService.instance().commit(plan.midLeave);
        assertEquals(proposed, snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES));
        ClusterMetadataService.instance().commit(plan.finishLeave);
        assertEquals(proposed, snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES));

        // Verify that the final local ranges match what was proposed
        AllLocalRanges finalized = snapshotAllLocalRanges(LocalRangeStatus.CURRENT, INITIAL_NODES);
        assertEquals(proposed, finalized);

        // Finally, check against the actual write placements
        assertLocalRangesMatchPlacements(ClusterMetadata.current().placements(), finalized, INITIAL_NODES);
    }

    @Test
    public void testJoining()
    {
        // Verify proposed ranges without any in flight operations
        AllLocalRanges initial = snapshotAllLocalRanges(LocalRangeStatus.CURRENT, INITIAL_NODES);
        AllLocalRanges proposed = snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES);
        assertEquals(initial, proposed);
        // Check against the actual write placements
        assertLocalRangesMatchPlacements(ClusterMetadata.current().placements(), initial, INITIAL_NODES);

        // Initiate an operation which affects ownership. This will add the MultiStepOperation which encodes any
        // necessary range movements so subsequent calls to ClusterMetadata::localRangesAllSettled
        // should return the expected local ranges after the operation has completed
        int newNode = 10;
        ClusterMetadataTestHelper.register(newNode);
        int[] expandedNodes = Arrays.copyOf(INITIAL_NODES, INITIAL_NODES.length + 1);
        expandedNodes[expandedNodes.length - 1] = newNode;
        ClusterMetadataService.instance().commit(ClusterMetadataTestHelper.prepareJoin(newNode));
        proposed = snapshotAllLocalRanges(LocalRangeStatus.SETTLED, expandedNodes);
        assertNotEquals(initial, proposed);

        // Step through execution of the MSO, verifying after each step that the proposed ranges don't change
        BootstrapAndJoin plan = ClusterMetadataTestHelper.getBootstrapPlan(newNode);
        ClusterMetadataService.instance().commit(plan.startJoin);
        assertEquals(proposed, snapshotAllLocalRanges(LocalRangeStatus.SETTLED, expandedNodes));
        ClusterMetadataService.instance().commit(plan.midJoin);
        assertEquals(proposed, snapshotAllLocalRanges(LocalRangeStatus.SETTLED, expandedNodes));
        ClusterMetadataService.instance().commit(plan.finishJoin);
        assertEquals(proposed, snapshotAllLocalRanges(LocalRangeStatus.SETTLED, expandedNodes));

        // Verify that the final local ranges match what was proposed
        AllLocalRanges finalized = snapshotAllLocalRanges(LocalRangeStatus.CURRENT, expandedNodes);
        assertEquals(proposed, finalized);

        // Finally, check against the actual write placements
        assertLocalRangesMatchPlacements(ClusterMetadata.current().placements(), finalized, expandedNodes);
    }

    @Test
    public void testMoving()
    {
        // Verify proposed ranges without any in flight operations
        AllLocalRanges initial = snapshotAllLocalRanges(LocalRangeStatus.CURRENT, INITIAL_NODES);
        AllLocalRanges proposed = snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES);
        assertEquals(initial, proposed);
        // Check against the actual write placements
        assertLocalRangesMatchPlacements(ClusterMetadata.current().placements(), initial, INITIAL_NODES);

        // Initiate an operation which affects ownership. This will add the MultiStepOperation which encodes any
        // necessary range movements so subsequent calls to ClusterMetadata::localRangesAllSettled
        // should return the expected local ranges after the operation has completed
        // pick a random node to leave (but not the CMS node (1), for simplicity's sake)
        int moving = INITIAL_NODES[Math.max(1, new Random().nextInt(4))];
        Token newToken = ClusterMetadata.current().partitioner.getRandomToken();
        while (ClusterMetadata.current().tokenMap.tokens().contains(newToken))
            newToken = ClusterMetadata.current().partitioner.getRandomToken();
        logger.info("Selected node {} to move to token {} ", moving, newToken);
        ClusterMetadataService.instance().commit(ClusterMetadataTestHelper.prepareMove(moving, newToken));
        proposed = snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES);
        assertNotEquals(initial, proposed);

        // Step through execution of the MSO, verifying after each step that the proposed ranges don't change
        Move plan = getMovePlan(moving);
        ClusterMetadataService.instance().commit(plan.startMove);
        assertEquals(proposed, snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES));
        ClusterMetadataService.instance().commit(plan.midMove);
        assertEquals(proposed, snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES));
        ClusterMetadataService.instance().commit(plan.finishMove);
        assertEquals(proposed, snapshotAllLocalRanges(LocalRangeStatus.SETTLED, INITIAL_NODES));

        // Verify that the final local ranges match what was proposed
        AllLocalRanges finalized = snapshotAllLocalRanges(LocalRangeStatus.CURRENT, INITIAL_NODES);
        assertEquals(proposed, finalized);

        // Finally, check against the actual write placements
        assertLocalRangesMatchPlacements(ClusterMetadata.current().placements(), finalized, INITIAL_NODES);
    }

    private void assertLocalRangesMatchPlacements(DataPlacements placements,
                                                  AllLocalRanges allLocalRanges,
                                                  int... nodes)
    {
        for (int id : nodes)
        {
            RangeSetMap localRanges = allLocalRanges.get(id);
            InetAddressAndPort endpoint = addr(id);
            placements.forEach((replication, placement) -> {
                Set<Range<Token>> ranges = localRanges.get(replication);
                Set<Range<Token>> fromPlacement = placement.writes.byEndpoint().get(endpoint).ranges();
                assertEquals(ranges, fromPlacement);
            });
        }
    }

    private enum LocalRangeStatus { CURRENT, SETTLED }
    private static AllLocalRanges snapshotAllLocalRanges(LocalRangeStatus status, int... nodes)
    {
        InetAddressAndPort realLocalAddress = FBUtilities.getBroadcastAddressAndPort();
        AllLocalRanges snapshot = new AllLocalRanges();
        ClusterMetadata metadata = ClusterMetadata.current();
        for (int id : nodes)
        {
            InetAddressAndPort address = addr(id);
            // clear cached settled local ranges
            metadata.unsafeClearLocalRangesAllSettled();
            // temporarily set broadcast address to infer which node is "local"
            FBUtilities.setBroadcastInetAddressAndPort(address);
            RangeSetMap.Builder localRanges = RangeSetMap.builder();
            for (KeyspaceMetadata ksm : metadata.schema.getKeyspaces())
            {
                Set<Range<Token>> ranges = status == LocalRangeStatus.SETTLED
                                           ? metadata.localWriteRangesAllSettled(ksm).ranges()
                                           : metadata.localWriteRanges(ksm).ranges();
                localRanges.put(ksm.params.replication, ranges);
            }
            snapshot.put(id, localRanges.build());
        }
        // restore local address
        FBUtilities.setBroadcastInetAddressAndPort(realLocalAddress);
        metadata.unsafeClearLocalRangesAllSettled();
        return snapshot;
    }

    // A snapshot of the local ranges for each replication setting for each node
    private static class AllLocalRanges
    {
        Map<Integer, RangeSetMap> localWriteRanges = new HashMap<>();

        void put(int nodeId, RangeSetMap ranges)
        {
            localWriteRanges.put(nodeId, ranges);
        }

        RangeSetMap get(int nodeId)
        {
            return localWriteRanges.get(nodeId);
        }

        public final boolean equals(Object o)
        {
            if (!(o instanceof AllLocalRanges)) return false;

            return Objects.equals(localWriteRanges, ((AllLocalRanges)o).localWriteRanges);
        }

        public int hashCode()
        {
            return Objects.hashCode(localWriteRanges);
        }

        public String toString()
        {
            return "AllLocalRanges{" +
                   "localWriteRanges=" + localWriteRanges +
                   '}';
        }
    }

}
