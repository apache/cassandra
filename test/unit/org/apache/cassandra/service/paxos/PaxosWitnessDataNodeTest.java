/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.cassandra.service.paxos;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.service.paxos.PaxosPrepare.selectDataNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PaxosWitnessDataNodeTest
{
    private static final String KEYSPACE = "paxos_witness_test";
    private static final String SINGLE_FULL_KEYSPACE = "paxos_witness_single_full_test";
    private static final String TABLE = "tbl";

    private static InetAddressAndPort node1;
    private static InetAddressAndPort node2;
    private static InetAddressAndPort node3;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.daemonInitialization();
        // Not prepareServer(): it registers the local node on a random token, and these tests place all
        // three nodes deterministically so the replica set for the key is known
        ServerTestUtils.prepareServerNoRegister();
        ServerTestUtils.markCMS();
        MutationJournal.start();

        node1 = FBUtilities.getBroadcastAddressAndPort();
        node2 = ClusterMetadataTestHelper.addr(2);
        node3 = ClusterMetadataTestHelper.addr(3);

        ClusterMetadataTestHelper.register(node1, "datacenter1", "rack1");
        ClusterMetadataTestHelper.register(node2, "datacenter1", "rack1");
        ClusterMetadataTestHelper.register(node3, "datacenter1", "rack1");
        ClusterMetadataTestHelper.join(node1, new Murmur3Partitioner.LongToken(Long.MIN_VALUE / 2));
        ClusterMetadataTestHelper.join(node2, new Murmur3Partitioner.LongToken(0L));
        ClusterMetadataTestHelper.join(node3, new Murmur3Partitioner.LongToken(Long.MAX_VALUE / 2));

        ClusterMetadataTestHelper.createKeyspace("CREATE KEYSPACE " + KEYSPACE +
                                                 " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3/1'}" +
                                                 " AND replication_type = 'tracked'");
        SchemaTestUtil.announceNewTable(TableMetadata.builder(KEYSPACE, TABLE)
                                                     .addPartitionKeyColumn("k", Int32Type.instance)
                                                     .addRegularColumn("v", Int32Type.instance)
                                                     .build());

        // A second keyspace with only one full replica. Killing it leaves a quorum of witnesses, which is
        // the only way to reach a live poll with no full replica in it: replica ordering puts full
        // replicas first, so a first-match selection would otherwise always find one.
        ClusterMetadataTestHelper.createKeyspace("CREATE KEYSPACE " + SINGLE_FULL_KEYSPACE +
                                                 " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3/2'}" +
                                                 " AND replication_type = 'tracked'");
        SchemaTestUtil.announceNewTable(TableMetadata.builder(SINGLE_FULL_KEYSPACE, TABLE)
                                                     .addPartitionKeyColumn("k", Int32Type.instance)
                                                     .addRegularColumn("v", Int32Type.instance)
                                                     .build());
    }

    private static Paxos.Participants participants(Predicate<Replica> isAlive)
    {
        return participants(KEYSPACE, isAlive);
    }

    private static Paxos.Participants participants(String keyspace, Predicate<Replica> isAlive)
    {
        TableMetadata table = Schema.instance.getTableMetadata(keyspace, TABLE);
        assertNotNull("test schema was not created", table);
        Token token = table.partitioner.getToken(ByteBufferUtil.bytes(1));
        return Paxos.Participants.get(ClusterMetadata.current(), table, token, ConsistencyLevel.SERIAL, isAlive);
    }


    /**
     * Replica ordering puts full replicas first, so a live poll normally contains one whether or not the
     * selection filters for it. With a single full replica and that replica down, the surviving quorum is
     * all witnesses, and the selection has to report that no data node is available rather than picking a
     * witness that cannot answer the read.
     */
    @Test
    public void testNoDataNodeWhenOnlyWitnessesSurvive()
    {
        Paxos.Participants all = participants(SINGLE_FULL_KEYSPACE, replica -> true);
        assertEquals(1, fullReplicas(all).size());
        InetAddressAndPort onlyFull = fullReplicas(all).get(0).endpoint();

        Paxos.Participants participants = participants(SINGLE_FULL_KEYSPACE,
                                                      replica -> !replica.endpoint().equals(onlyFull));

        assertEquals(2, participants.sizeOfPoll());
        assertEquals(participants.sizeOfPoll(), witnesses(participants).size());

        Replica dataNode = selectDataNode(participants, participants.voterReplica(0).endpoint());
        assertNull("startTracked fails the prepare with \"Couldn't find a data node to use\" in this state",
                   dataNode);
    }

    private static List<Replica> fullReplicas(Paxos.Participants participants)
    {
        List<Replica> full = new ArrayList<>();
        for (int i = 0, size = participants.sizeOfPoll(); i < size; i++)
            if (participants.voterReplica(i).isFull())
                full.add(participants.voterReplica(i));
        return full;
    }

    private static List<Replica> witnesses(Paxos.Participants participants)
    {
        List<Replica> transientReplicas = new ArrayList<>();
        for (int i = 0, size = participants.sizeOfPoll(); i < size; i++)
            if (participants.voterReplica(i).isTransient())
                transientReplicas.add(participants.voterReplica(i));
        return transientReplicas;
    }


    @Test
    public void testWitnessVotesButIsNotTheDataNode()
    {
        Paxos.Participants participants = participants(replica -> true);

        assertEquals(3, participants.sizeOfPoll());
        assertEquals(1, witnesses(participants).size());

        Replica dataNode = selectDataNode(participants, node1);
        assertNotNull(dataNode);
        assertTrue(dataNode.isFull());
    }

    /**
     * The case a cluster test cannot reach: with one full replica down, quorum is still met by the
     * surviving full replica plus the witness, so the prepare proceeds and the data node has to be the
     * surviving full replica rather than the witness.
     */
    @Test
    public void testDataNodeIsTheSurvivingFullReplica()
    {
        Paxos.Participants all = participants(replica -> true);
        Replica witness = witnesses(all).get(0);
        Replica fullToKill = null;
        for (int i = 0, size = all.sizeOfPoll(); i < size; i++)
        {
            Replica replica = all.voterReplica(i);
            if (replica.isFull())
            {
                fullToKill = replica;
                break;
            }
        }
        assertNotNull(fullToKill);

        InetAddressAndPort dead = fullToKill.endpoint();
        Paxos.Participants participants = participants(replica -> !replica.endpoint().equals(dead));

        assertEquals(2, participants.sizeOfPoll());

        // The coordinator here is the witness, so the local-replica shortcut must not apply
        Replica dataNode = selectDataNode(participants, witness.endpoint());
        assertNotNull(dataNode);
        assertTrue(dataNode.isFull());
        assertNotEquals(dead, dataNode.endpoint());
    }

    /**
     * Paxos ballots and promises live in system.paxos, a node-local table, so being a witness for a data
     * range does not affect a node's ability to hold Paxos state for it.
     */
    @Test
    public void testPaxosStateKeyspaceIsNotTransientlyReplicated()
    {
        Keyspace systemKeyspace = Keyspace.open("system");
        assertFalse(systemKeyspace.getReplicationStrategy().hasTransientReplicas());
    }
}
