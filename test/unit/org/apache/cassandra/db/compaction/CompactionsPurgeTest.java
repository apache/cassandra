/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;
import static org.apache.cassandra.Util.dk;

public class CompactionsPurgeTest
{
    private static final String KEYSPACE1 = "CompactionsPurgeTest1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";
    private static final String KEYSPACE2 = "CompactionsPurgeTest2";
    private static final String KEYSPACE_CACHED = "CompactionsPurgeTestCached";
    private static final String CF_CACHED = "CachedCF";
    private static final String KEYSPACE_CQL = "cql_keyspace";
    private static final String CF_CQL = "table1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2));

        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1));

        SchemaLoader.createKeyspace(KEYSPACE_CACHED,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE_CACHED, CF_CACHED).caching(CachingParams.CACHE_EVERYTHING));

        SchemaLoader.createKeyspace(KEYSPACE_CQL,
                                    KeyspaceParams.simple(1),
                                    CreateTableStatement.parse("CREATE TABLE " + CF_CQL + " ("
                                                               + "k int PRIMARY KEY,"
                                                               + "v1 text,"
                                                               + "v2 int"
                                                               + ")", KEYSPACE_CQL));
    }

    @Test
    public void testMajorCompactionPurge()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        String key = "key1";

        // inserts
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 0, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }

        Util.flush(cfs);

        // deletes
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder.deleteRow(cfs.metadata(), 1, key, String.valueOf(i)).applyUnsafe();
        }
        Util.flush(cfs);

        // resurrect one column
        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 2, key);
        builder.clustering(String.valueOf(5))
               .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
               .build().applyUnsafe();

        Util.flush(cfs);

        // major compact and test that all columns but the resurrected one is completely gone
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE, false));
        cfs.invalidateCachedPartition(dk(key));

        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        assertEquals(1, partition.rowCount());
    }

    @Test
    public void testMajorCompactionPurgeTombstonesWithMaxTimestamp()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        String key = "key1";

        // inserts
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 0, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }
        Util.flush(cfs);

        // deletes
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder.deleteRow(cfs.metadata(), Long.MAX_VALUE, key, String.valueOf(i)).applyUnsafe();
        }
        Util.flush(cfs);

        // major compact - tombstones should be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE, false));

        // resurrect one column
        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 2, key);
        builder.clustering(String.valueOf(5))
               .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
               .build().applyUnsafe();

        Util.flush(cfs);

        cfs.invalidateCachedPartition(dk(key));

        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        assertEquals(1, partition.rowCount());
    }

    @Test
    public void testMajorCompactionPurgeTopLevelTombstoneWithMaxTimestamp()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        String key = "key1";

        // inserts
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 0, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }
        Util.flush(cfs);

        new Mutation.PartitionUpdateCollector(KEYSPACE1, dk(key))
            .add(PartitionUpdate.fullPartitionDelete(cfs.metadata(), dk(key), Long.MAX_VALUE, FBUtilities.nowInSeconds()))
            .build()
            .applyUnsafe();
        Util.flush(cfs);

        // major compact - tombstones should be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE, false));

        // resurrect one column
        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 2, key);
        builder.clustering(String.valueOf(5))
               .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
               .build().applyUnsafe();

        Util.flush(cfs);

        cfs.invalidateCachedPartition(dk(key));

        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        assertEquals(1, partition.rowCount());
    }

    @Test
    public void testMajorCompactionPurgeRangeTombstoneWithMaxTimestamp()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        String key = "key1";

        // inserts
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 0, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }
        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), Long.MAX_VALUE, dk(key))
            .addRangeTombstone(String.valueOf(0), String.valueOf(9)).build().applyUnsafe();
        Util.flush(cfs);

        // major compact - tombstones should be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE, false));

        // resurrect one column
        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 2, key);
        builder.clustering(String.valueOf(5))
               .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
               .build().applyUnsafe();

        Util.flush(cfs);

        cfs.invalidateCachedPartition(dk(key));

        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        assertEquals(1, partition.rowCount());
    }

    @Test
    public void testMinorCompactionPurge()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        for (int k = 1; k <= 2; ++k) {
            String key = "key" + k;

            // inserts
            for (int i = 0; i < 10; i++)
            {
                RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 0, key);
                builder.clustering(String.valueOf(i))
                        .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                        .build().applyUnsafe();
            }
            Util.flush(cfs);

            // deletes
            for (int i = 0; i < 10; i++)
            {
                RowUpdateBuilder.deleteRow(cfs.metadata(), 1, key, String.valueOf(i)).applyUnsafe();
            }

            Util.flush(cfs);
        }

        DecoratedKey key1 = Util.dk("key1");
        DecoratedKey key2 = Util.dk("key2");

        // flush, remember the current sstable and then resurrect one column
        // for first key. Then submit minor compaction on remembered sstables.
        Util.flush(cfs);
        Collection<SSTableReader> sstablesIncomplete = cfs.getLiveSSTables();

        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 2, "key1");
        builder.clustering(String.valueOf(5))
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build().applyUnsafe();

        Util.flush(cfs);
        try (CompactionTasks tasks = cfs.getCompactionStrategyManager().getUserDefinedTasks(sstablesIncomplete, Integer.MAX_VALUE))
        {
            Iterables.getOnlyElement(tasks).execute(ActiveCompactionsTracker.NOOP);
        }

        // verify that minor compaction does GC when key is provably not
        // present in a non-compacted sstable
        Util.assertEmpty(Util.cmd(cfs, key2).build());

        // verify that minor compaction still GC when key is present
        // in a non-compacted sstable but the timestamp ensures we won't miss anything
        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key1).build());
        assertEquals(1, partition.rowCount());
    }

    /**
     * verify that we don't drop tombstones during a minor compaction that might still be relevant
     */
    @Test
    public void testMinTimestampPurge()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        final boolean enforceStrictLiveness = cfs.metadata().enforceStrictLiveness();
        String key3 = "key3";

        // inserts
        new RowUpdateBuilder(cfs.metadata(), 8, key3)
            .clustering("c1")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build().applyUnsafe();

        new RowUpdateBuilder(cfs.metadata(), 8, key3)
        .clustering("c2")
        .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
        .build().applyUnsafe();

        Util.flush(cfs);
        // delete c1
        RowUpdateBuilder.deleteRow(cfs.metadata(), 10, key3, "c1").applyUnsafe();

        Util.flush(cfs);
        Collection<SSTableReader> sstablesIncomplete = cfs.getLiveSSTables();

        // delete c2 so we have new delete in a diffrent SSTable
        RowUpdateBuilder.deleteRow(cfs.metadata(), 9, key3, "c2").applyUnsafe();
        Util.flush(cfs);

        // compact the sstables with the c1/c2 data and the c1 tombstone
        try (CompactionTasks tasks = cfs.getCompactionStrategyManager().getUserDefinedTasks(sstablesIncomplete, Integer.MAX_VALUE))
        {
            Iterables.getOnlyElement(tasks).execute(ActiveCompactionsTracker.NOOP);
        }

        // We should have both the c1 and c2 tombstones still. Since the min timestamp in the c2 tombstone
        // sstable is older than the c1 tombstone, it is invalid to throw out the c1 tombstone.
        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key3).build());
        assertEquals(2, partition.rowCount());
        for (Row row : partition)
            assertFalse(row.hasLiveData(FBUtilities.nowInSeconds(), enforceStrictLiveness));
    }

    @Test
    public void testCompactionPurgeOneFile() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = "Standard2";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        String key = "key1";

        // inserts
        for (int i = 0; i < 5; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 0, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }

        // deletes
        for (int i = 0; i < 5; i++)
        {
            RowUpdateBuilder.deleteRow(cfs.metadata(), 1, key, String.valueOf(i)).applyUnsafe();
        }
        Util.flush(cfs);
        assertEquals(String.valueOf(cfs.getLiveSSTables()), 1, cfs.getLiveSSTables().size()); // inserts & deletes were in the same memtable -> only deletes in sstable

        // compact and test that the row is completely gone
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        assertTrue(cfs.getLiveSSTables().isEmpty());

        Util.assertEmpty(Util.cmd(cfs, key).build());
    }


    @Test
    public void testCompactionPurgeCachedRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        String keyspaceName = KEYSPACE_CACHED;
        String cfName = CF_CACHED;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        String key = "key3";

        // inserts
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 0, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }

        // deletes partition
        Mutation.PartitionUpdateCollector rm = new Mutation.PartitionUpdateCollector(KEYSPACE_CACHED, dk(key));
        rm.add(PartitionUpdate.fullPartitionDelete(cfs.metadata(), dk(key), 1, FBUtilities.nowInSeconds()));
        rm.build().applyUnsafe();

        // Adds another unrelated partition so that the sstable is not considered fully expired. We do not
        // invalidate the row cache in that latter case.
        new RowUpdateBuilder(cfs.metadata(), 0, "key4").clustering("c").add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER).build().applyUnsafe();

        // move the key up in row cache (it should not be empty since we have the partition deletion info)
        assertFalse(Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build()).isEmpty());

        // flush and major compact
        Util.flush(cfs);
        Util.compactAll(cfs, Integer.MAX_VALUE).get();

        // Since we've force purging (by passing MAX_VALUE for gc_before), the row should have been invalidated and we should have no deletion info anymore
        Util.assertEmpty(Util.cmd(cfs, key).build());
    }

    @Test
    public void testCompactionPurgeTombstonedRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        String keyspaceName = KEYSPACE1;
        String cfName = "Standard1";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        String key = "key3";

        // inserts
        for (int i = 0; i < 10; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), i, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }

        // deletes partition with timestamp such that not all columns are deleted
        Mutation.PartitionUpdateCollector rm = new Mutation.PartitionUpdateCollector(KEYSPACE1, dk(key));
        rm.add(PartitionUpdate.fullPartitionDelete(cfs.metadata(), dk(key), 4, FBUtilities.nowInSeconds()));
        rm.build().applyUnsafe();

        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        assertFalse(partition.partitionLevelDeletion().isLive());

        // flush and major compact (with tombstone purging)
        Util.flush(cfs);
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        assertFalse(Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build()).isEmpty());

        // re-inserts with timestamp lower than delete
        for (int i = 0; i < 5; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), i, key);
            builder.clustering(String.valueOf(i))
                   .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                   .build().applyUnsafe();
        }

        // Check that the second insert went in
        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        assertEquals(10, partition.rowCount());
    }


    @Test
    public void testRowTombstoneObservedBeforePurging()
    {
        String keyspace = "cql_keyspace";
        String table = "table1";
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // write a row out to one sstable
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (%d, '%s', %d)",
                                                     keyspace, table, 1, "foo", 1));
        Util.flush(cfs);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(1, result.size());

        // write a row tombstone out to a second sstable
        QueryProcessor.executeInternal(String.format("DELETE FROM %s.%s WHERE k = %d", keyspace, table, 1));
        Util.flush(cfs);

        // basic check that the row is considered deleted
        assertEquals(2, cfs.getLiveSSTables().size());
        result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(0, result.size());

        // compact the two sstables with a gcBefore that does *not* allow the row tombstone to be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, (int) (System.currentTimeMillis() / 1000) - 10000, false));

        // the data should be gone, but the tombstone should still exist
        assertEquals(1, cfs.getLiveSSTables().size());
        result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(0, result.size());

        // write a row out to one sstable
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (%d, '%s', %d)",
                                                     keyspace, table, 1, "foo", 1));
        Util.flush(cfs);
        assertEquals(2, cfs.getLiveSSTables().size());
        result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(1, result.size());

        // write a row tombstone out to a different sstable
        QueryProcessor.executeInternal(String.format("DELETE FROM %s.%s WHERE k = %d", keyspace, table, 1));
        Util.flush(cfs);

        // compact the two sstables with a gcBefore that *does* allow the row tombstone to be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, (int) (System.currentTimeMillis() / 1000) + 10000, false));

        // both the data and the tombstone should be gone this time
        assertEquals(0, cfs.getLiveSSTables().size());
        result = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(0, result.size());
    }
}
