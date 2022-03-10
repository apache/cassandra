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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.hints.HintsTestUtil.assertHintsEqual;
import static org.apache.cassandra.hints.HintsTestUtil.assertPartitionsEqual;
import static org.apache.cassandra.net.Verb.HINT_REQ;

public class HintTest
{
    private static final String KEYSPACE = "hint_test";
    private static final String TABLE0 = "table_0";
    private static final String TABLE1 = "table_1";
    private static final String TABLE2 = "table_2";

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE0),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE2));
    }

    @Before
    public void resetGcGraceSeconds()
    {
        TokenMetadata tokenMeta = StorageService.instance.getTokenMetadata();
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        tokenMeta.clearUnsafe();
        tokenMeta.updateHostId(UUID.randomUUID(), local);
        tokenMeta.updateNormalTokens(BootStrapper.getRandomTokens(tokenMeta, 1), local);

        for (TableMetadata table : Schema.instance.getTablesAndViews(KEYSPACE))
            SchemaTestUtil.announceTableUpdate(table.unbuild().gcGraceSeconds(864000).build());
    }

    @Test
    public void testSerializer() throws IOException
    {
        long now = FBUtilities.timestampMicros();
        Mutation mutation = createMutation("testSerializer", now);
        Hint hint = Hint.create(mutation, now / 1000);

        // serialize
        int serializedSize = (int) Hint.serializer.serializedSize(hint, MessagingService.current_version);
        DataOutputBuffer dob = new DataOutputBuffer();
        Hint.serializer.serialize(hint, dob, MessagingService.current_version);
        assertEquals(serializedSize, dob.getLength());

        // deserialize
        DataInputPlus di = new DataInputBuffer(dob.buffer(), true);
        Hint deserializedHint = Hint.serializer.deserialize(di, MessagingService.current_version);

        // compare before/after
        assertHintsEqual(hint, deserializedHint);
    }

    @Test
    public void testApply()
    {
        long now = FBUtilities.timestampMicros();
        String key = "testApply";
        Mutation mutation = createMutation(key, now);
        Hint hint = Hint.create(mutation, now / 1000);

        // sanity check that there is no data inside yet
        assertNoPartitions(key, TABLE0);
        assertNoPartitions(key, TABLE1);
        assertNoPartitions(key, TABLE2);

        hint.apply();

        // assert that we can read the inserted partitions
        for (PartitionUpdate partition : mutation.getPartitionUpdates())
            assertPartitionsEqual(partition, readPartition(key, partition.metadata().name, partition.columns()));
    }

    @Test
    public void testApplyWithTruncation()
    {
        long now = FBUtilities.timestampMicros();
        String key = "testApplyWithTruncation";
        Mutation mutation = createMutation(key, now);

        // sanity check that there is no data inside yet
        assertNoPartitions(key, TABLE0);
        assertNoPartitions(key, TABLE1);
        assertNoPartitions(key, TABLE2);

        // truncate TABLE1
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE1).truncateBlocking();

        // create and apply a hint with creation time in the past (one second before the truncation)
        Hint.create(mutation, now / 1000 - 1).apply();

        // TABLE1 update should have been skipped and not applied, as expired
        assertNoPartitions(key, TABLE1);

        // TABLE0 and TABLE2 updates should have been applied successfully
        PartitionUpdate upd0 = mutation.getPartitionUpdate(Schema.instance.getTableMetadata(KEYSPACE, TABLE0));
        assertPartitionsEqual(upd0, readPartition(key, TABLE0, upd0.columns()));
        PartitionUpdate upd2 = mutation.getPartitionUpdate(Schema.instance.getTableMetadata(KEYSPACE, TABLE2));
        assertPartitionsEqual(upd2, readPartition(key, TABLE2, upd2.columns()));
    }

    @Test
    public void testApplyWithRegularExpiration()
    {
        long now = FBUtilities.timestampMicros();
        String key = "testApplyWithRegularExpiration";

        // sanity check that there is no data inside yet
        assertNoPartitions(key, TABLE0);
        assertNoPartitions(key, TABLE1);
        assertNoPartitions(key, TABLE2);

        // lower the GC GS on TABLE0 to 0 BEFORE the hint is created
        TableMetadata updated =
            Schema.instance
                  .getTableMetadata(KEYSPACE, TABLE0)
                  .unbuild()
                  .gcGraceSeconds(0)
                  .build();
        SchemaTestUtil.announceTableUpdate(updated);

        Mutation mutation = createMutation(key, now);
        Hint.create(mutation, now / 1000).apply();

        // all updates should have been skipped and not applied, as expired
        assertNoPartitions(key, TABLE0);
        assertNoPartitions(key, TABLE1);
        assertNoPartitions(key, TABLE2);
    }

    @Test
    public void testApplyWithGCGSReducedLater()
    {
        long now = FBUtilities.timestampMicros();
        String key = "testApplyWithGCGSReducedLater";

        // sanity check that there is no data inside yet
        assertNoPartitions(key, TABLE0);
        assertNoPartitions(key, TABLE1);
        assertNoPartitions(key, TABLE2);

        // lower the GC GS on TABLE0 AFTER the hint is already created
        TableMetadata updated =
            Schema.instance
                  .getTableMetadata(KEYSPACE, TABLE0)
                  .unbuild()
                  .gcGraceSeconds(0)
                  .build();
        SchemaTestUtil.announceTableUpdate(updated);

        Mutation mutation = createMutation(key, now);
        Hint hint = Hint.create(mutation, now / 1000);
        hint.apply();

        // all updates should have been skipped and not applied, as expired
        assertNoPartitions(key, TABLE0);
        assertNoPartitions(key, TABLE1);
        assertNoPartitions(key, TABLE2);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testChangedTopology() throws Exception
    {
        // create a hint
        long now = FBUtilities.timestampMicros();
        String key = "testChangedTopology";
        Mutation mutation = createMutation(key, now);
        Hint hint = Hint.create(mutation, now / 1000);

        // Prepare metadata with injected stale endpoint serving the mutation key.
        TokenMetadata tokenMeta = StorageService.instance.getTokenMetadata();
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        InetAddressAndPort endpoint = InetAddressAndPort.getByName("1.1.1.1");
        UUID localId = StorageService.instance.getLocalHostUUID();
        UUID targetId = UUID.randomUUID();
        tokenMeta.updateHostId(targetId, endpoint);
        tokenMeta.updateNormalTokens(ImmutableList.of(mutation.key().getToken()), endpoint);

        // sanity check that there is no data inside yet
        assertNoPartitions(key, TABLE0);
        assertNoPartitions(key, TABLE1);
        assertNoPartitions(key, TABLE2);

        assert StorageProxy.instance.getHintsInProgress() == 0;
        long totalHintCount = StorageProxy.instance.getTotalHints();
        // Process hint message.
        HintMessage message = new HintMessage(localId, hint);
        HINT_REQ.handler().doVerb(Message.out(HINT_REQ, message));

        // hint should not be applied as we no longer are a replica
        assertNoPartitions(key, TABLE0);
        assertNoPartitions(key, TABLE1);
        assertNoPartitions(key, TABLE2);

        // Attempt to send to new endpoint should have been made. Node is not live hence it should now be a hint.
        assertEquals(totalHintCount + 1, StorageProxy.instance.getTotalHints());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testChangedTopologyNotHintable() throws Exception
    {
        // create a hint
        long now = FBUtilities.timestampMicros();
        String key = "testChangedTopology";
        Mutation mutation = createMutation(key, now);
        Hint hint = Hint.create(mutation, now / 1000);

        // Prepare metadata with injected stale endpoint.
        TokenMetadata tokenMeta = StorageService.instance.getTokenMetadata();
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        InetAddressAndPort endpoint = InetAddressAndPort.getByName("1.1.1.1");
        UUID localId = StorageService.instance.getLocalHostUUID();
        UUID targetId = UUID.randomUUID();
        tokenMeta.updateHostId(targetId, endpoint);
        tokenMeta.updateNormalTokens(ImmutableList.of(mutation.key().getToken()), endpoint);

        // sanity check that there is no data inside yet
        assertNoPartitions(key, TABLE0);
        assertNoPartitions(key, TABLE1);
        assertNoPartitions(key, TABLE2);

        try
        {
            DatabaseDescriptor.setHintedHandoffEnabled(false);

            assert StorageMetrics.totalHintsInProgress.getCount() == 0;
            long totalHintCount = StorageMetrics.totalHints.getCount();
            // Process hint message.
            HintMessage message = new HintMessage(localId, hint);
            HINT_REQ.<HintMessage>handler().doVerb(
                    Message.builder(HINT_REQ, message).from(local).build());

            // hint should not be applied as we no longer are a replica
            assertNoPartitions(key, TABLE0);
            assertNoPartitions(key, TABLE1);
            assertNoPartitions(key, TABLE2);

            // Attempt to send to new endpoint should not have been made.
            assertEquals(totalHintCount, StorageMetrics.totalHints.getCount());
        }
        finally
        {
            DatabaseDescriptor.setHintedHandoffEnabled(true);
        }
    }

    @Test
    public void testCalculateHintExpiration()
    {
        // create a hint with gcgs
        long now = FBUtilities.timestampMicros();
        long nowInMillis = TimeUnit.MICROSECONDS.toMillis(now);
        int gcgs = 10; // It is less than the default mutation gcgs
        String key = "testExpiration";
        Mutation mutation = createMutation(key, now);
        // create a hint with explicit small gcgs
        Hint hint = Hint.create(mutation, nowInMillis, gcgs);
        assertEquals(nowInMillis + TimeUnit.SECONDS.toMillis(gcgs),
                     hint.expirationInMillis());

        // create a hint with mutation's gcgs.
        hint = Hint.create(mutation, nowInMillis);
        assertEquals(nowInMillis + TimeUnit.SECONDS.toMillis(mutation.smallestGCGS()),
                     hint.expirationInMillis());
    }

    private static Mutation createMutation(String key, long now)
    {
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(KEYSPACE, dk(key));

        builder.update(Schema.instance.getTableMetadata(KEYSPACE, TABLE0))
               .timestamp(now)
               .row("column0")
               .add("val", "value0");

        builder.update(Schema.instance.getTableMetadata(KEYSPACE, TABLE1))
               .timestamp(now + 1)
               .row("column1")
               .add("val", "value1");

        builder.update(Schema.instance.getTableMetadata(KEYSPACE, TABLE2))
               .timestamp(now + 2)
               .row("column2")
               .add("val", "value2");

        return builder.build();
    }

    private static ColumnFamilyStore cfs(String table)
    {
        return Schema.instance.getColumnFamilyStoreInstance(Schema.instance.getTableMetadata(KEYSPACE, table).id);
    }

    private static FilteredPartition readPartition(String key, String table, RegularAndStaticColumns columns)
    {
        String[] columnNames = new String[columns.size()];
        int i = 0;
        for (ColumnMetadata column : columns)
            columnNames[i++] = column.name.toString();

        return Util.getOnlyPartition(Util.cmd(cfs(table), key).columns(columnNames).build());
    }

    private static void assertNoPartitions(String key, String table)
    {
        ReadCommand cmd = Util.cmd(cfs(table), key).build();

        try (ReadExecutionController executionController = cmd.executionController();
             PartitionIterator iterator = cmd.executeInternal(executionController))
        {
            assertFalse(iterator.hasNext());
        }
    }
}
