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

package org.apache.cassandra.index.internal;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LogDomain;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TrackedIndexFlushTest
{
    private static final AtomicInteger keyspaceNumber = new AtomicInteger();
    private static final String INDEX_NAME = "tbl_v_index";

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        MutationJournal.start();
        MutationTrackingService.start();
    }

    private static ColumnFamilyStore newTableWithIndex(ReplicationType replicationType)
    {
        String ks = "tracked_index_flush_" + keyspaceNumber.incrementAndGet();

        TableMetadata.Builder builder =
        TableMetadata.builder(ks, "tbl")
                     .addPartitionKeyColumn("k", Int32Type.instance)
                     .addRegularColumn("v", Int32Type.instance);

        builder.indexes(Indexes.of(IndexMetadata.fromIndexTargets(
        Collections.singletonList(new IndexTarget(new ColumnIdentifier("v", true), IndexTarget.Type.VALUES)),
        INDEX_NAME,
        IndexMetadata.Kind.COMPOSITES,
        Collections.emptyMap())));

        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1, replicationType), builder);

        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
        cfs.disableAutoCompaction();
        indexStore(cfs).disableAutoCompaction();
        return cfs;
    }

    private static ColumnFamilyStore newTrackedTableWithIndex()
    {
        return newTableWithIndex(ReplicationType.tracked);
    }

    private static ColumnFamilyStore newUntrackedTableWithIndex()
    {
        return newTableWithIndex(ReplicationType.untracked);
    }

    private static ColumnFamilyStore indexStore(ColumnFamilyStore baseCfs)
    {
        return Iterables.getOnlyElement(baseCfs.indexManager.getAllIndexColumnFamilyStores());
    }

    private static ColumnFamilyStore newTrackedTableWithFlushedRow()
    {
        ColumnFamilyStore cfs = newTrackedTableWithIndex();
        write(cfs, 1, 1);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        return cfs;
    }

    private static void write(ColumnFamilyStore cfs, int k, int v)
    {
        TableMetadata metadata = cfs.metadata();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(k));
        MutationId id = MutationTrackingService.instance().nextMutationId(metadata.keyspace, key.getToken());
        SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(id, metadata.keyspace, key);
        PartitionUpdate.SimpleBuilder partition = builder.update(metadata);
        partition.row().add("v", v);
        Mutation mutation = builder.build();
        assertFalse(mutation.id().isNone());
        mutation.apply();
    }

    @Test
    public void untrackedIndexMemtableDomain()
    {
        ColumnFamilyStore cfs = newUntrackedTableWithIndex();

        Memtable index = indexStore(cfs).getTracker().getView().getCurrentMemtable();

        assertTrue(index.holds(LogDomain.COMMIT_LOG));
        assertFalse(index.holds(LogDomain.MUTATION_JOURNAL));
    }

    @Test
    public void trackedIndexMemtableDomain()
    {
        ColumnFamilyStore cfs = newTrackedTableWithIndex();

        Memtable index = indexStore(cfs).getTracker().getView().getCurrentMemtable();

        assertTrue(index.holds(LogDomain.MUTATION_JOURNAL));
        assertFalse(index.holds(LogDomain.COMMIT_LOG));
    }

    @Test
    public void rebuildingAnIndexOnATrackedTableSucceeds()
    {
        ColumnFamilyStore cfs = newTrackedTableWithFlushedRow();
        ColumnFamilyStore index = indexStore(cfs);
        assertFalse(cfs.getLiveSSTables().isEmpty());

        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(INDEX_NAME));

        int withSpan = 0;
        int withoutSpan = 0;
        for (SSTableReader sstable : index.getLiveSSTables())
        {
            IntervalSet<CommitLogPosition> intervals = sstable.getSSTableMetadata().commitLogIntervals;
            if (intervals.isEmpty())
            {
                withoutSpan++;
                continue;
            }
            withSpan++;
            for (CommitLogPosition start : intervals.starts())
                assertTrue(start.compareTo(intervals.upperBound().orElseThrow(AssertionError::new)) <= 0);
        }
        assertEquals(1, withoutSpan);
        assertEquals(1, withSpan);
    }

    @Test
    public void trackedIndexSSTableContainsNoOffsets()
    {
        ColumnFamilyStore cfs = newTrackedTableWithFlushedRow();
        ColumnFamilyStore index = indexStore(cfs);

        write(cfs, 2, 2);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertEquals("precondition: two index sstables to union intervals over", 2, index.getLiveSSTables().size());
        for (SSTableReader input : index.getLiveSSTables())
        {
            assertTrue(input.getSSTableMetadata().commitLogIntervals.isEmpty());
            assertTrue(input.getSSTableMetadata().coordinatorLogOffsets.isEmpty());
        }

        CompactionManager.instance.performMaximal(index);

        SSTableReader compacted = Iterables.getOnlyElement(index.getLiveSSTables());
        assertTrue(compacted.getSSTableMetadata().commitLogIntervals.isEmpty());
        assertTrue(compacted.getSSTableMetadata().coordinatorLogOffsets.isEmpty());
    }
}
