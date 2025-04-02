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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.CoordinatorLog.CoordinatorLogPrimary;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CoordinatorLogTest
{
    private static final int LOCAL_HOST_ID = 1;
    private static final CoordinatorLogId LOG_ID = new CoordinatorLogId(LOCAL_HOST_ID, 1);
    private static final Participants PARTICIPANTS = new Participants(List.of(LOCAL_HOST_ID, 2, 3));

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
    }

    private static Token tk(String key)
    {
        return new ByteOrderedPartitioner.BytesToken(ByteBufferUtil.bytes(key));
    }

    private static Offsets toOffsets(MutationId... ids)
    {
        Offsets list = new Offsets(LOG_ID);
        for (MutationId id : ids)
            list.append(id.offset());
        return list;
    }

    private static void assertUnreconciled(Token token, TableId tableId, CoordinatorLog log, boolean includePending, Offsets expectedReconciled, MutationId... expectedIds)
    {
        Offsets reconciled = new Offsets(LOG_ID);
        Offsets unreconciled = new Offsets(LOG_ID);
        log.collectOffsetsFor(token, tableId, includePending, unreconciled, reconciled);

        for (MutationId mid : expectedIds)
            Assert.assertTrue(unreconciled.contains(mid.offset()));

        Assert.assertEquals(toOffsets(expectedIds), unreconciled);
        Assert.assertEquals(expectedReconciled, reconciled);
    }

    @Test
    public void remoteReconciliationTest()
    {
        Token tk = tk("key");
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        TableId tableId = metadata.id;
        CoordinatorLogPrimary log = new CoordinatorLogPrimary(LOCAL_HOST_ID, LOG_ID, PARTICIPANTS);
        MutationId[] ids = new MutationId[] { log.nextId(), log.nextId(), log.nextId(), };

        List<Mutation> mutations = new ArrayList<>(ids.length);
        for (MutationId id : ids)
        {
            Mutation mutation =
                new RowUpdateBuilder(metadata, 0, "key")
                .clustering("ck")
                .add("value", "value")
                .build()
                .withMutationId(id);

            mutations.add(mutation);
            log.startWriting(mutation);
        }

        Offsets reconciled = new Offsets(LOG_ID);
        // we've only started writing, so the ids shouldn't appear without includePending being true
        assertUnreconciled(tk, tableId, log, false, reconciled);
        assertUnreconciled(tk, tableId, log, true, reconciled, ids);

        for (Mutation mutation : mutations)
            log.finishWriting(mutation);

        // the call to finishWriting will have made the ids visible without the includePending flag
        assertUnreconciled(tk, tableId, log, false, reconciled, ids);

        log.witnessedRemoteMutation(ids[0], PARTICIPANTS.get(1));
        assertUnreconciled(tk, tableId, log, false, reconciled, ids);

        log.witnessedRemoteMutation(ids[0], PARTICIPANTS.get(2));
        reconciled.add(ids[0].offset());
        assertUnreconciled(tk, tableId, log, false, reconciled, ids[1], ids[2]);
    }
}
