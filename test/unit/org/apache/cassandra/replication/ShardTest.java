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
import java.util.function.LongSupplier;

import org.junit.BeforeClass;
import org.junit.Test;

import org.agrona.collections.MutableInteger;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class ShardTest
{
    private static final int LOCAL_HOST_ID = 1;
    private static final int REMOTE_HOST_ID_1 = 2;
    private static final int REMOTE_HOST_ID_2 = 3;

    private static final String KEYSPACE = "shard_test_ks";
    private static final String TABLE = "shard_test_table";

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

    @Test
    public void testPersistAndLoadSingleShard()
    {
        Range<Token> range = new Range<>(tk("a"), tk("z"));
        Participants participants = new Participants(List.of(LOCAL_HOST_ID, REMOTE_HOST_ID_1, REMOTE_HOST_ID_2));
        MutableInteger logId = new MutableInteger();
        LongSupplier logIdProvider = () -> CoordinatorLogId.asLong(LOCAL_HOST_ID, logId.getAndIncrement());

        Shard original = new Shard(LOCAL_HOST_ID, KEYSPACE, range, participants, logIdProvider, (s, l) -> {});
        original.persistToSystemTables();

        ArrayList<Shard> loadedShards = Shard.loadFromSystemTables(LOCAL_HOST_ID, logIdProvider, (s, l) -> {});
        assertEquals(1, loadedShards.size());
        Shard loaded = loadedShards.get(0);

        assertEquals(original.localNodeId, loaded.localNodeId);
        assertEquals(original.keyspace, loaded.keyspace);
        assertEquals(original.range, loaded.range);
        assertEquals(original.participants, loaded.participants);
        // TODO: compare the coordinator logs
    }

    @Test
    public void testLogRotation()
    {
        CoordinatorLog.overrideMaxOffsetForTesting(100);
        try
        {
            Range<Token> range = new Range<>(tk("a"), tk("z"));
            Participants participants = new Participants(List.of(LOCAL_HOST_ID, REMOTE_HOST_ID_1, REMOTE_HOST_ID_2));
            MutableInteger logId = new MutableInteger();
            LongSupplier logIdProvider = () -> CoordinatorLogId.asLong(LOCAL_HOST_ID, logId.getAndIncrement());
            Shard shard = new Shard(LOCAL_HOST_ID, KEYSPACE, range, participants, logIdProvider, (s, l) -> {
            });

            MutationId firstId = shard.nextId();
            for (int i = 0; i < 100; i++)
                assertEquals(firstId.hostLogId, shard.nextId().hostLogId);
            assertEquals(firstId.hostLogId + 1, shard.nextId().hostLogId);
        }
        finally
        {
            CoordinatorLog.overrideMaxOffsetForTesting(Integer.MAX_VALUE);
        }
    }
}