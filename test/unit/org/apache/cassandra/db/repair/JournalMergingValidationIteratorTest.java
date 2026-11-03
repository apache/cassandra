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

package org.apache.cassandra.db.repair;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.ValidationPartitionIterator;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.replication.ValidationOffsets;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class JournalMergingValidationIteratorTest
{
    private static final AtomicInteger counter = new AtomicInteger();
    private static InetAddressAndPort coordinator;
    private static Token MT;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
        MutationJournal.start();
        MutationTrackingService.start();
        coordinator = InetAddressAndPort.getByName("10.0.0.1");
        MT = DatabaseDescriptor.getPartitioner().getMinimumToken();
    }

    private ColumnFamilyStore createTable()
    {
        String ks = "ks_" + counter.incrementAndGet();
        String tbl = "tbl";
        TableMetadata metadata =
            TableMetadata.builder(ks, tbl)
                         .addPartitionKeyColumn("k", Int32Type.instance)
                         .addRegularColumn("v", Int32Type.instance)
                         .params(TableParams.builder().gcGraceSeconds(0).build())
                         .build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1, ReplicationType.tracked), metadata);
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
        cfs.disableAutoCompaction();
        return cfs;
    }

    private void registerRepair(TimeUUID parentId, TimeUUID sessionID, ColumnFamilyStore cfs, Range<Token> range)
    {
        ActiveRepairService.instance().registerParentRepairSession(parentId,
                                                                   coordinator,
                                                                   List.of(cfs),
                                                                   Set.of(range),
                                                                   false,
                                                                   ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                   false,
                                                                   PreviewKind.REPAIRED);
    }

    private Mutation createInsertMutation(TableMetadata tableMetadata, int k, int v, long timestamp)
    {
        DecoratedKey key = tableMetadata.partitioner.decorateKey(ByteBufferUtil.bytes(k));
        MutationId mutationId = MutationTrackingService.instance().nextMutationId(tableMetadata.keyspace, key.getToken());
        org.apache.cassandra.db.SimpleBuilders.MutationBuilder builder = new org.apache.cassandra.db.SimpleBuilders.MutationBuilder(mutationId, tableMetadata.keyspace, key);
        builder.timestamp(timestamp);
        builder.update(tableMetadata).row().add("v", v);
        return builder.build();
    }

    private Mutation createDeleteMutation(TableMetadata tableMetadata, int k, long timestamp, long nowInSec)
    {
        DecoratedKey key = tableMetadata.partitioner.decorateKey(ByteBufferUtil.bytes(k));
        MutationId mutationId = MutationTrackingService.instance().nextMutationId(tableMetadata.keyspace, key.getToken());
        org.apache.cassandra.db.SimpleBuilders.MutationBuilder builder = new org.apache.cassandra.db.SimpleBuilders.MutationBuilder(mutationId, tableMetadata.keyspace, key);
        builder.timestamp(timestamp);
        org.apache.cassandra.db.partitions.PartitionUpdate.SimpleBuilder update = builder.update(tableMetadata);
        update.nowInSec(nowInSec);
        update.delete();
        return builder.build();
    }

    @Test
    public void testTombstoneInSSTableShadowsOlderWriteInJournal() throws Exception
    {
        ColumnFamilyStore cfs = createTable();
        Range<Token> range = new Range<>(MT, MT);

        // Write a deletion for key 1 with timestamp 2000L and localDeletionTime in the past into the table and flush to SSTable.
        long deleteTimestamp = 2000L;
        long localDeletionTime = 100;
        Mutation delete = createDeleteMutation(cfs.metadata(), 1, deleteTimestamp, localDeletionTime);
        delete.apply();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Assert.assertEquals(1, cfs.getLiveSSTables().size());

        // Write an insert into the journal with timestamp 1000L.
        // It is not flushed, so it only exists in the journal.
        Mutation insert = createInsertMutation(cfs.metadata(), 1, 1, 1000L);
        CoordinatorLogId coordLogId = CoordinatorLogId.fromLong(insert.id().logId());
        ShortMutationId journalId = new ShortMutationId(coordLogId.asLong(), insert.id().offset());
        MutationJournal.instance().write(journalId, insert);
        MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty();

        int maxOffset = Math.max(delete.id().offset(), insert.id().offset());
        ValidationOffsets validationOffsets = new ValidationOffsets(Map.of(coordLogId, new Offsets.Immutable(coordLogId, new int[]{ 0, maxOffset })));

        TimeUUID parentId = nextTimeUUID();
        TimeUUID sessionID = nextTimeUUID();
        registerRepair(parentId, sessionID, cfs, range);

        long nowInSec = 200; // nowInSec > localDeletionTime (100)
        boolean dontPurgeTombstones = false;

        try (ValidationPartitionIterator vi = cfs.getRepairManager().getValidationIterator(Collections.singleton(range), parentId, sessionID, false, nowInSec, dontPurgeTombstones, validationOffsets, null))
        {
            // If the SSTable iterator prematurely purges the tombstone before merging,
            // the tombstone will fail to shadow the insert from the journal, and the insert will be resurrected.
            // With correct purging on the final iterator, the tombstone shadows the insert, then is purged,
            // leaving no live or resurrected data.
            Assert.assertFalse("Deleted row was resurrected because tombstone was purged before merge", vi.hasNext());
        }
    }

    @Test
    public void testTombstoneInJournalIsPurgedOnFinalIterator() throws Exception
    {
        ColumnFamilyStore cfs = createTable();
        Range<Token> range = new Range<>(MT, MT);

        // Write an expired deletion into the journal.
        long deleteTimestamp = 2000L;
        long localDeletionTime = 100;
        Mutation delete = createDeleteMutation(cfs.metadata(), 2, deleteTimestamp, localDeletionTime);
        CoordinatorLogId coordLogId = CoordinatorLogId.fromLong(delete.id().logId());
        ShortMutationId journalId = new ShortMutationId(coordLogId.asLong(), delete.id().offset());
        MutationJournal.instance().write(journalId, delete);
        MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty();

        ValidationOffsets validationOffsets = new ValidationOffsets(Map.of(coordLogId, new Offsets.Immutable(coordLogId, new int[]{ 0, delete.id().offset() })));

        TimeUUID parentId = nextTimeUUID();
        TimeUUID sessionID = nextTimeUUID();
        registerRepair(parentId, sessionID, cfs, range);

        long nowInSec = 200;
        boolean dontPurgeTombstones = false;

        try (ValidationPartitionIterator vi = cfs.getRepairManager().getValidationIterator(Collections.singleton(range), parentId, sessionID, false, nowInSec, dontPurgeTombstones, validationOffsets, null))
        {
            // With final iterator purging, expired tombstones from the journal must be purged when dontPurgeTombstones is false.
            Assert.assertFalse("Expired tombstone from journal must be purged on final iterator", vi.hasNext());
        }
    }

    @Test
    public void testTombstonesRetainedWhenDontPurgeTombstonesIsTrue() throws Exception
    {
        ColumnFamilyStore cfs = createTable();
        Range<Token> range = new Range<>(MT, MT);

        long deleteTimestamp = 2000L;
        long localDeletionTime = 100;
        Mutation delete = createDeleteMutation(cfs.metadata(), 3, deleteTimestamp, localDeletionTime);
        CoordinatorLogId coordLogId = CoordinatorLogId.fromLong(delete.id().logId());
        ShortMutationId journalId = new ShortMutationId(coordLogId.asLong(), delete.id().offset());
        MutationJournal.instance().write(journalId, delete);
        MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty();

        ValidationOffsets validationOffsets = new ValidationOffsets(Map.of(coordLogId, new Offsets.Immutable(coordLogId, new int[]{ 0, delete.id().offset() })));

        TimeUUID parentId = nextTimeUUID();
        TimeUUID sessionID = nextTimeUUID();
        registerRepair(parentId, sessionID, cfs, range);

        long nowInSec = 200;
        boolean dontPurgeTombstones = true;

        try (ValidationPartitionIterator vi = cfs.getRepairManager().getValidationIterator(Collections.singleton(range), parentId, sessionID, false, nowInSec, dontPurgeTombstones, validationOffsets, null))
        {
            Assert.assertTrue("Tombstones must be retained when dontPurgeTombstones is true", vi.hasNext());
        }
    }
}
