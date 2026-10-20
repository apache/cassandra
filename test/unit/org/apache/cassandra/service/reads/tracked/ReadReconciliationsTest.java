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

package org.apache.cassandra.service.reads.tracked;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ReadReconciliationsTest
{
    private static final TableId TABLE_ID = TableId.generate();
    private static final CoordinatorLogId LOG_1 = CoordinatorLogId.fromLong(CoordinatorLogId.asLong(1, 1));
    private static final CoordinatorLogId LOG_2 = CoordinatorLogId.fromLong(CoordinatorLogId.asLong(2, 1));
    private static int LOCAL_NODE_ID;

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        org.apache.cassandra.SchemaLoader.prepareServer();
        LOCAL_NODE_ID = ClusterMetadata.current().myNodeId().id();
        ClusterMetadataTestHelper.register(InetAddressAndPort.getByNameUnchecked("127.0.0.2"));
        ClusterMetadataTestHelper.register(InetAddressAndPort.getByNameUnchecked("127.0.0.3"));
    }

    private static MutationSummary summary(CoordinatorLogId logId, int[] reconciled, int[] unreconciled)
    {
        MutationSummary.Builder builder = new MutationSummary.Builder(TABLE_ID);
        MutationSummary.CoordinatorSummary.Builder logBuilder = builder.builderForLog(logId);
        for (int i = 0; i < reconciled.length; i += 2)
            logBuilder.reconciled.add(reconciled[i], reconciled[i + 1]);
        for (int i = 0; i < unreconciled.length; i += 2)
            logBuilder.unreconciled.add(unreconciled[i], unreconciled[i + 1]);
        return builder.build();
    }

    @Test
    public void testCoordinatorTracksPerPeerPendingMutations()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 1001L);
        int dataNode = LOCAL_NODE_ID;
        int[] summaryNodes = new int[]{ 2, 3 };

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes);

        MutationSummary summaryPeer2 = summary(LOG_2, new int[]{}, new int[]{ 10, 10 });
        MutationSummary summaryPeer3 = summary(LOG_2, new int[]{}, new int[]{ 10, 10, 20, 20 });

        coordinator.acceptRemoteSummary(summaryPeer2, 2, true);
        coordinator.acceptRemoteSummary(summaryPeer3, 3, true);

        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        ReadReconciliations.Coordinator.PeerState peer3 = coordinator.getPeerState(3);

        assertNotNull(peer2);
        assertNotNull(peer3);

        // Peer 2 should have offset 10 pending
        assertTrue(peer2.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 10)));
        assertEquals(1, peer2.pendingLocalMutations.idCount());

        // Peer 3 should have offsets 10 and 20 pending (even though offset 10 was already requested by peer 2)
        assertTrue(peer3.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 10)));
        assertTrue(peer3.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 20)));
        assertEquals(2, peer3.pendingLocalMutations.idCount());

        // Now deliver mutation 10
        coordinator.acceptMutation(new ShortMutationId(LOG_2, 10));

        // Offset 10 should be removed from both peers
        assertFalse(peer2.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 10)));
        assertTrue(peer2.pendingLocalMutations.isEmpty());

        assertFalse(peer3.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 10)));
        assertTrue(peer3.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 20)));
        assertEquals(1, peer3.pendingLocalMutations.idCount());

        // Now deliver mutation 20
        coordinator.acceptMutation(new ShortMutationId(LOG_2, 20));
        assertTrue(peer3.pendingLocalMutations.isEmpty());
    }

    @Test
    public void testMutationReceivedBeforeSecondPeerSummaryArrives()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 1002L);
        int dataNode = LOCAL_NODE_ID;
        int[] summaryNodes = new int[]{ 2, 3 };

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes);

        MutationSummary summaryPeer2 = summary(LOG_2, new int[]{}, new int[]{ 10, 10 });
        MutationSummary summaryPeer3 = summary(LOG_2, new int[]{}, new int[]{ 10, 10 });

        coordinator.acceptRemoteSummary(summaryPeer2, 2, true);

        // Deliver mutation 10 before peer 3's summary arrives
        coordinator.acceptMutation(new ShortMutationId(LOG_2, 10));

        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        assertTrue(peer2.pendingLocalMutations.isEmpty());

        // Now peer 3's summary arrives with mutation 10
        coordinator.acceptRemoteSummary(summaryPeer3, 3, true);

        ReadReconciliations.Coordinator.PeerState peer3 = coordinator.getPeerState(3);
        assertNotNull(peer3);

        // Peer 3 should NOT have mutation 10 pending because it was already completed during this read
        assertFalse(peer3.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 10)));
        assertTrue(peer3.pendingLocalMutations.isEmpty());
    }

    @Test
    public void testMutationArrivesLocallyDuringCallbackRegistration()
    {
        // Tests the race window where a mutation was missing when acceptRemoteSummary was called,
        // but was witnessed locally before/during callback registration.
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 1003L);
        int dataNode = LOCAL_NODE_ID;
        int[] summaryNodes = new int[]{ 2 };

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes);

        MutationSummary summaryPeer2 = summary(LOG_2, new int[]{}, new int[]{ 50, 50 });
        coordinator.acceptRemoteSummary(summaryPeer2, 2, true);

        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        assertNotNull(peer2);
        assertTrue(peer2.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 50)));

        // Once the mutation arrives, pendingLocalMutations is immediately updated
        coordinator.acceptMutation(new ShortMutationId(LOG_2, 50));
        assertTrue(peer2.pendingLocalMutations.isEmpty());
    }
}
