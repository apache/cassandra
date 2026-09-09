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

package org.apache.cassandra.db.streaming;

import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;

import static org.apache.cassandra.cql3.CQLTester.schemaChange;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Covers {@link CassandraStreamReceiver}'s detection of a sender/receiver disagreement about the mutation tracking
 * migration state.
 */
public class CassandraStreamReceiverTrackedTest
{
    private static final String KS = "cassandra_stream_receiver_tracked_test";
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.daemonInitialization();
        ServerTestUtils.prepareServer();
        MutationJournal.start();

        schemaChange("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
                     "AND replication_type = 'tracked'");
        schemaChange("CREATE TABLE " + KS + ".tbl (pk int PRIMARY KEY, val int)");
        cfs = Keyspace.open(KS).getColumnFamilyStore("tbl");
    }

    @Test
    public void testExpectsTrackedTransferForTrackedNonMigratingRanges()
    {
        List<Range<Token>> ranges = Collections.singletonList(new Range<>(new Murmur3Partitioner.LongToken(0),
                                                                          new Murmur3Partitioner.LongToken(100)));
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, untrackedRepairSession(), ranges, 1);
        assertTrue(receiver.expectsTrackedTransfer);
    }

    @Test
    public void testExpectsTrackedTransferWithNoRanges()
    {
        CassandraStreamReceiver receiver = new CassandraStreamReceiver(cfs, untrackedRepairSession(), Collections.emptyList(), 1);
        assertTrue(receiver.expectsTrackedTransfer);
    }

    private static StreamSession untrackedRepairSession()
    {
        StreamSession session = Mockito.mock(StreamSession.class);
        when(session.getStreamOperation()).thenReturn(StreamOperation.REPAIR);
        when(session.streamOperation()).thenReturn(StreamOperation.REPAIR);
        when(session.getPendingRepair()).thenReturn(ActiveRepairService.NO_PENDING_REPAIR);
        when(session.isTrackedTransfer()).thenReturn(false);
        return session;
    }
}
