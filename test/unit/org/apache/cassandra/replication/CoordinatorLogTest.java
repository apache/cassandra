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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.CoordinatorLog.CoordinatorLogPrimary;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CoordinatorLogTest
{
    private static final int LOCAL_HOST_ID = 1;
    private static final int REMOTE_HOST_ID_1 = 2;
    private static final int REMOTE_HOST_ID_2 = 3;

    private static final CoordinatorLogId LOCAL_LOG_ID = new CoordinatorLogId(LOCAL_HOST_ID, 1);
    private static final CoordinatorLogId REPLICA_LOG_ID = new CoordinatorLogId(REMOTE_HOST_ID_1, 1);

    private static final Participants PARTICIPANTS =
        new Participants(List.of(LOCAL_HOST_ID, REMOTE_HOST_ID_1, REMOTE_HOST_ID_2));

    private static final String KEYSPACE = "cltks";
    private static final String TABLE = "cltt";

    @BeforeClass
    public static void setUp() throws IOException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(3),
                                    TableMetadata.builder(KEYSPACE, TABLE)
                                                 .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                 .addClusteringColumn("ck", UTF8Type.instance)
                                                 .addRegularColumn("value", UTF8Type.instance)
                                                 .build());
        MutationJournal.instance.start();
    }

    private static Token tk(String key)
    {
        return new ByteOrderedPartitioner.BytesToken(ByteBufferUtil.bytes(key));
    }

    private static Offsets toOffsets(MutationId... ids)
    {
        Offsets.Mutable list = new Offsets.Mutable(LOCAL_LOG_ID);
        for (MutationId id : ids)
            list.add(id.offset());
        return list;
    }

    private static void assertUnreconciled(Token token, TableId tableId, CoordinatorLog log, boolean includePending, Offsets expectedReconciled, MutationId... expectedIds)
    {
        Offsets.Mutable reconciled = new Offsets.Mutable(LOCAL_LOG_ID);
        Offsets.Mutable unreconciled = new Offsets.Mutable(LOCAL_LOG_ID);
        log.collectOffsetsFor(token, tableId, includePending, unreconciled, reconciled);

        for (MutationId mid : expectedIds)
            assertTrue(unreconciled.contains(mid.offset()));

        assertEquals(toOffsets(expectedIds), unreconciled);
        assertEquals(expectedReconciled, reconciled);
    }

    @Test
    public void remoteReconciliationTest()
    {
        Token tk = tk("key");
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        TableId tableId = metadata.id;
        CoordinatorLogPrimary log = new CoordinatorLogPrimary(KEYSPACE, new Range<>(tk, tk), LOCAL_HOST_ID, LOCAL_LOG_ID, PARTICIPANTS);
        MutationId[] ids = new MutationId[] { log.nextId(), log.nextId(), log.nextId(), };

        List<Mutation> mutations = new ArrayList<>(ids.length);
        for (MutationId id : ids)
        {
            Mutation mutation = createMutation(id);
            mutations.add(mutation);
            log.startWriting(mutation);
        }

        Offsets.Mutable reconciled = new Offsets.Mutable(LOCAL_LOG_ID);
        // we've only started writing, so the ids shouldn't appear without includePending being true
        assertUnreconciled(tk, tableId, log, false, reconciled);
        assertUnreconciled(tk, tableId, log, true, reconciled, ids);

        for (Mutation mutation : mutations)
            log.finishWriting(mutation);

        // the call to finishWriting will have made the ids visible without the includePending flag
        assertUnreconciled(tk, tableId, log, false, reconciled, ids);

        log.receivedWriteResponse(ids[0], PARTICIPANTS.get(1));
        assertUnreconciled(tk, tableId, log, false, reconciled, ids);

        log.receivedWriteResponse(ids[0], PARTICIPANTS.get(2));
        reconciled.add(ids[0].offset());
        assertUnreconciled(tk, tableId, log, false, reconciled, ids[1], ids[2]);
    }

    private Mutation createMutation(MutationId id)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        return new RowUpdateBuilder(metadata, 0, "key")
            .clustering("ck")
            .add("value", "value")
            .build()
            .withMutationId(id);
    }

    @Test
    public void persistAndLoadPrimaryLogTest()
    {
        testPersistAndLoadRoundtrip(LOCAL_LOG_ID);
    }

    @Test
    public void persistAndLoadReplicaLogTest()
    {
        testPersistAndLoadRoundtrip(REPLICA_LOG_ID);
    }

    private void testPersistAndLoadRoundtrip(CoordinatorLogId logId)
    {
        Range<Token> range = new Range<>(tk("a"), tk("b"));

        Offsets.Mutable offsets1 = new Offsets.Mutable(logId);
        offsets1.add(1, 2, 3, 4);
        Offsets.Mutable offsets2 = new Offsets.Mutable(logId);
        offsets2.add(2, 3, 4, 5);
        Offsets.Mutable offsets3 = new Offsets.Mutable(logId);
        offsets3.add(3, 4, 5, 6);

        Node2OffsetsMap witnessed = new Node2OffsetsMap();
        witnessed.set(LOCAL_HOST_ID, offsets1);
        witnessed.set(REMOTE_HOST_ID_1, offsets2);
        witnessed.set(REMOTE_HOST_ID_2, offsets3);

        UnreconciledMutations unreconciled = new UnreconciledMutations();
        Mutation mutation1 = createMutation(new MutationId(logId.asLong(), MutationId.sequenceId(1, 0)));
        Mutation mutation2 = createMutation(new MutationId(logId.asLong(), MutationId.sequenceId(2, 0)));
        unreconciled.addDirectly(mutation1);
        unreconciled.addDirectly(mutation2);
        MutationJournal.instance.write(mutation1.id(), mutation1);
        MutationJournal.instance.write(mutation2.id(), mutation2);

        CoordinatorLog log =
            CoordinatorLog.recreate(KEYSPACE, range, LOCAL_HOST_ID, logId, PARTICIPANTS, witnessed, witnessed, unreconciled);

        Offsets.Mutable reconciled = new Offsets.Mutable(logId);
        reconciled.add(3, 4);
        assertEquals(reconciled, log.reconciledOffsets);

        validatePersistAndLoadRoundtrip(log);
        log.deleteFromSystemTable();
    }

    private static void validatePersistAndLoadRoundtrip(CoordinatorLog log)
    {
        log.persistToSystemTable();
        List<CoordinatorLog> logs = CoordinatorLog.loadFromSystemTable(KEYSPACE, log.range, LOCAL_HOST_ID);
        assertEquals(1, logs.size());
        CoordinatorLog loaded = logs.get(0);

        assertSame(log.getClass(), loaded.getClass());
        assertEquals(log.keyspace, loaded.keyspace);
        assertEquals(log.range, loaded.range);
        assertEquals(log.logId, loaded.logId);
        assertEquals(log.participants, loaded.participants);
        assertEquals(log.localNodeId, loaded.localNodeId);

        assertEquals(log.participants.size(), log.witnessedOffsets.size());
        assertEquals(log.participants.size(), log.persistedOffsets.size());
        assertEquals(loaded.participants.size(), loaded.witnessedOffsets.size());
        assertEquals(loaded.participants.size(), loaded.persistedOffsets.size());
        assertEquals(log.witnessedOffsets, loaded.witnessedOffsets);
        assertEquals(log.persistedOffsets, loaded.persistedOffsets);
        assertEquals(log.reconciledOffsets, loaded.reconciledOffsets);

        assertTrue(log.unreconciledMutations.equalsForTesting(loaded.unreconciledMutations));
    }
}
