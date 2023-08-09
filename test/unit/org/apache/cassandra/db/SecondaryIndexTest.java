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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.throwAssert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SecondaryIndexTest
{
    public static final String KEYSPACE1 = "SecondaryIndexTest1";
    public static final String WITH_COMPOSITE_INDEX = "WithCompositeIndex";
    public static final String WITH_MULTIPLE_COMPOSITE_INDEX = "WithMultipleCompositeIndex";
    public static final String WITH_KEYS_INDEX = "WithKeysIndex";
    public static final String COMPOSITE_INDEX_TO_BE_ADDED = "CompositeIndexToBeAdded";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE1, WITH_COMPOSITE_INDEX, true, true).gcGraceSeconds(0),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE1, COMPOSITE_INDEX_TO_BE_ADDED, false).gcGraceSeconds(0),
                                    SchemaLoader.compositeMultipleIndexCFMD(KEYSPACE1, WITH_MULTIPLE_COMPOSITE_INDEX).gcGraceSeconds(0),
                                    SchemaLoader.keysIndexCFMD(KEYSPACE1, WITH_KEYS_INDEX, true).gcGraceSeconds(0));
    }

    @Before
    public void truncateCFS()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(COMPOSITE_INDEX_TO_BE_ADDED).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_MULTIPLE_COMPOSITE_INDEX).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_KEYS_INDEX).truncateBlocking();
    }

    @Test
    public void testIndexScan()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);

        new RowUpdateBuilder(cfs.metadata(), 0, "k1").clustering("c").add("birthdate", 1L).add("notbirthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "k2").clustering("c").add("birthdate", 2L).add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "k3").clustering("c").add("birthdate", 1L).add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "k4").clustering("c").add("birthdate", 3L).add("notbirthdate", 2L).build().applyUnsafe();

        // basic single-expression query
        List<FilteredPartition> partitions = Util.getAll(Util.cmd(cfs).fromKeyExcl("k1").toKeyIncl("k3").columns("birthdate").build());
        assertEquals(2, partitions.size());
        Util.assertCellValue(2L, cfs, Util.row(partitions.get(0), "c"), "birthdate");
        Util.assertCellValue(1L, cfs, Util.row(partitions.get(1), "c"), "birthdate");

        // 2 columns, 3 results
        partitions = Util.getAll(Util.cmd(cfs).fromKeyExcl("k1").toKeyIncl("k4aaa").build());
        assertEquals(3, partitions.size());

        Row first = Util.row(partitions.get(0), "c");
        Util.assertCellValue(2L, cfs, first, "birthdate");
        Util.assertCellValue(2L, cfs, first, "notbirthdate");

        Row second = Util.row(partitions.get(1), "c");
        Util.assertCellValue(1L, cfs, second, "birthdate");
        Util.assertCellValue(2L, cfs, second, "notbirthdate");

        Row third = Util.row(partitions.get(2), "c");
        Util.assertCellValue(3L, cfs, third, "birthdate");
        Util.assertCellValue(2L, cfs, third, "notbirthdate");

        // Verify getIndexSearchers finds the data for our rc
        ReadCommand rc = Util.cmd(cfs).fromKeyIncl("k1")
                                      .toKeyIncl("k3")
                                      .columns("birthdate")
                                      .filterOn("birthdate", Operator.EQ, 1L)
                                      .build();

        Index.Searcher searcher = rc.indexSearcher();
        try (ReadExecutionController executionController = rc.executionController();
             UnfilteredPartitionIterator pi = searcher.search(executionController))
        {
            assertTrue(pi.hasNext());
            pi.next().close();
        }

        // Verify gt on idx scan
        partitions = Util.getAll(Util.cmd(cfs).fromKeyIncl("k1").toKeyIncl("k4aaa") .filterOn("birthdate", Operator.GT, 1L).build());
        int rowCount = 0;
        for (FilteredPartition partition : partitions)
        {
            for (Row row : partition)
            {
                ++rowCount;
                assert ByteBufferUtil.toLong(Util.cell(cfs, row, "birthdate").buffer()) > 1L;
            }
        }
        assertEquals(2, rowCount);

        // Filter on non-indexed, LT comparison
        Util.assertEmpty(Util.cmd(cfs).fromKeyExcl("k1").toKeyIncl("k4aaa")
                                      .filterOn("notbirthdate", Operator.NEQ, 2L)
                                      .build());

        // Hit on primary, fail on non-indexed filter
        Util.assertEmpty(Util.cmd(cfs).fromKeyExcl("k1").toKeyIncl("k4aaa")
                                      .filterOn("birthdate", Operator.EQ, 1L)
                                      .filterOn("notbirthdate", Operator.NEQ, 2L)
                                      .build());
    }

    @Test
    public void testLargeScan()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);
        ByteBuffer bBB = ByteBufferUtil.bytes("birthdate");
        ByteBuffer nbBB = ByteBufferUtil.bytes("notbirthdate");

        for (int i = 0; i < 100; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), FBUtilities.timestampMicros(), "key" + i)
                    .clustering("c")
                    .add("birthdate", 34L)
                    .add("notbirthdate", ByteBufferUtil.bytes((long) (i % 2)))
                    .build()
                    .applyUnsafe();
        }

        List<FilteredPartition> partitions = Util.getAll(Util.cmd(cfs)
                                                             .filterOn("birthdate", Operator.EQ, 34L)
                                                             .filterOn("notbirthdate", Operator.EQ, 1L)
                                                             .build());

        Set<DecoratedKey> keys = new HashSet<>();
        int rowCount = 0;

        for (FilteredPartition partition : partitions)
        {
            keys.add(partition.partitionKey());
            rowCount += partition.rowCount();
        }

        // extra check that there are no duplicate results -- see https://issues.apache.org/jira/browse/CASSANDRA-2406
        assertEquals(rowCount, keys.size());
        assertEquals(50, rowCount);
    }

    @Test
    public void testCompositeIndexDeletions() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);
        ByteBuffer bBB = ByteBufferUtil.bytes("birthdate");
        ColumnMetadata bDef = cfs.metadata().getColumn(bBB);
        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // Confirm addition works
        new RowUpdateBuilder(cfs.metadata(), 0, "k1").clustering("c").add("birthdate", 1L).build().applyUnsafe();
        assertIndexedOne(cfs, col, 1L);

        // delete the column directly
        RowUpdateBuilder.deleteRow(cfs.metadata(), 1, "k1", "c").applyUnsafe();
        assertIndexedNone(cfs, col, 1L);

        // verify that it's not being indexed under any other value either
        ReadCommand rc = Util.cmd(cfs).build();
        assertNull(rc.indexSearcher());

        // resurrect w/ a newer timestamp
        new RowUpdateBuilder(cfs.metadata(), 2, "k1").clustering("c").add("birthdate", 1L).build().apply();;
        assertIndexedOne(cfs, col, 1L);

        // verify that row and delete w/ older timestamp does nothing
        RowUpdateBuilder.deleteRow(cfs.metadata(), 1, "k1", "c").applyUnsafe();
        assertIndexedOne(cfs, col, 1L);

        // similarly, column delete w/ older timestamp should do nothing
        new RowUpdateBuilder(cfs.metadata(), 1, "k1").clustering("c").delete(bDef).build().applyUnsafe();
        assertIndexedOne(cfs, col, 1L);

        // delete the entire row (w/ newer timestamp this time)
        // todo - checking the # of index searchers for the command is probably not the best thing to test here
        RowUpdateBuilder.deleteRow(cfs.metadata(), 3, "k1", "c").applyUnsafe();
        rc = Util.cmd(cfs).build();
        assertNull(rc.indexSearcher());

        // make sure obsolete mutations don't generate an index entry
        // todo - checking the # of index searchers for the command is probably not the best thing to test here
        new RowUpdateBuilder(cfs.metadata(), 3, "k1").clustering("c").add("birthdate", 1L).build().apply();;
        rc = Util.cmd(cfs).build();
        assertNull(rc.indexSearcher());
    }

    @Test
    public void testCompositeIndexUpdate() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(WITH_COMPOSITE_INDEX);
        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // create a row and update the birthdate value, test that the index query fetches the new version
        new RowUpdateBuilder(cfs.metadata(), 1, "testIndexUpdate").clustering("c").add("birthdate", 100L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 2, "testIndexUpdate").clustering("c").add("birthdate", 200L).build().applyUnsafe();

        // Confirm old version fetch fails
        assertIndexedNone(cfs, col, 100L);

        // Confirm new works
        assertIndexedOne(cfs, col, 200L);

        // update the birthdate value with an OLDER timestamp, and test that the index ignores this
        assertIndexedNone(cfs, col, 300L);
        assertIndexedOne(cfs, col, 200L);
    }

    @Test
    public void testIndexUpdateOverwritingExpiringColumns() throws Exception
    {
        // see CASSANDRA-7268
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);
        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // create a row and update the birthdate value with an expiring column
        new RowUpdateBuilder(cfs.metadata(), 1L, 500, "K100").clustering("c").add("birthdate", 100L).build().applyUnsafe();
        assertIndexedOne(cfs, col, 100L);

        // requires a 1s sleep because we calculate local expiry time as (now() / 1000) + ttl
        TimeUnit.SECONDS.sleep(1);

        // now overwrite with the same name/value/ttl, but the local expiry time will be different
        new RowUpdateBuilder(cfs.metadata(), 1L, 500, "K100").clustering("c").add("birthdate", 100L).build().applyUnsafe();
        assertIndexedOne(cfs, col, 100L);

        // check that modifying the indexed value using the same timestamp behaves as expected
        new RowUpdateBuilder(cfs.metadata(), 1L, 500, "K101").clustering("c").add("birthdate", 101L).build().applyUnsafe();
        assertIndexedOne(cfs, col, 101L);

        TimeUnit.SECONDS.sleep(1);

        new RowUpdateBuilder(cfs.metadata(), 1L, 500, "K101").clustering("c").add("birthdate", 102L).build().applyUnsafe();
        // Confirm 101 is gone
        assertIndexedNone(cfs, col, 101L);

        // Confirm 102 is there
        assertIndexedOne(cfs, col, 102L);
    }

    @Test
    public void testDeleteOfInconsistentValuesInKeysIndex() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(WITH_KEYS_INDEX);

        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // create a row and update the "birthdate" value
        new RowUpdateBuilder(cfs.metadata(), 1, "k1").noRowMarker().add("birthdate", 1L).build().applyUnsafe();

        // force a flush, so our index isn't being read from a memtable
        Util.flushTable(keyspace, WITH_KEYS_INDEX);

        // now apply another update, but force the index update to be skipped
        keyspace.apply(new RowUpdateBuilder(cfs.metadata(), 2, "k1").noRowMarker().add("birthdate", 2L).build(),
                       true,
                       false);

        // Now searching the index for either the old or new value should return 0 rows
        // because the new value was not indexed and the old value should be ignored
        // (and in fact purged from the index cf).
        // first check for the old value
        assertIndexedNone(cfs, col, 1L);
        assertIndexedNone(cfs, col, 2L);

        // now, reset back to the original value, still skipping the index update, to
        // make sure the value was expunged from the index when it was discovered to be inconsistent
        keyspace.apply(new RowUpdateBuilder(cfs.metadata(), 3, "k1").noRowMarker().add("birthdate", 1L).build(),
                       true,
                       false);
        assertIndexedNone(cfs, col, 1L);
        ColumnFamilyStore indexCfs = cfs.indexManager.getAllIndexColumnFamilyStores().iterator().next();
        assertIndexCfsIsEmpty(indexCfs);
    }

    @Test
    public void testDeleteOfInconsistentValuesFromCompositeIndex() throws Exception
    {
        runDeleteOfInconsistentValuesFromCompositeIndexTest(false);
    }

    @Test
    public void testDeleteOfInconsistentValuesFromCompositeIndexOnStaticColumn() throws Exception
    {
        runDeleteOfInconsistentValuesFromCompositeIndexTest(true);
    }

    private void runDeleteOfInconsistentValuesFromCompositeIndexTest(boolean isStatic) throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = WITH_COMPOSITE_INDEX;

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        String colName = isStatic ? "static" : "birthdate";
        ByteBuffer col = ByteBufferUtil.bytes(colName);

        // create a row and update the author value
        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 0, "k1");
        if (!isStatic)
            builder = builder.clustering("c");
        builder.add(colName, 10l).build().applyUnsafe();

        // test that the index query fetches this version
        assertIndexedOne(cfs, col, 10l);

        // force a flush and retry the query, so our index isn't being read from a memtable
        Util.flushTable(keyspace, cfName);
        assertIndexedOne(cfs, col, 10l);

        // now apply another update, but force the index update to be skipped
        builder = new RowUpdateBuilder(cfs.metadata(), 0, "k1");
        if (!isStatic)
            builder = builder.clustering("c");
        builder.add(colName, 20l);
        keyspace.apply(builder.build(), true, false);

        // Now searching the index for either the old or new value should return 0 rows
        // because the new value was not indexed and the old value should be ignored
        // (and in fact purged from the index cf).
        // first check for the old value
        assertIndexedNone(cfs, col, 10l);
        assertIndexedNone(cfs, col, 20l);

        // now, reset back to the original value, still skipping the index update, to
        // make sure the value was expunged from the index when it was discovered to be inconsistent
        // TODO: Figure out why this is re-inserting
        builder = new RowUpdateBuilder(cfs.metadata(), 2, "k1");
        if (!isStatic)
            builder = builder.clustering("c");
        builder.add(colName, 10L);
        keyspace.apply(builder.build(), true, false);
        assertIndexedNone(cfs, col, 20l);

        ColumnFamilyStore indexCfs = cfs.indexManager.getAllIndexColumnFamilyStores().iterator().next();
        assertIndexCfsIsEmpty(indexCfs);
    }

    // See CASSANDRA-6098
    @Test
    public void testDeleteCompositeIndex() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);

        ByteBuffer colName = ByteBufferUtil.bytes("birthdate");

        // Insert indexed value.
        new RowUpdateBuilder(cfs.metadata(), 1, "k1").clustering("c").add("birthdate", 10l).build().applyUnsafe();

        // Now delete the value
        RowUpdateBuilder.deleteRow(cfs.metadata(), 2, "k1", "c").applyUnsafe();

        // We want the data to be gcable, but even if gcGrace == 0, we still need to wait 1 second
        // since we won't gc on a tie.
        try { Thread.sleep(1000); } catch (Exception e) {}

        // Read the index and we check we do get no value (and no NPE)
        // Note: the index will return the entry because it hasn't been deleted (we
        // haven't read yet nor compacted) but the data read itself will return null
        assertIndexedNone(cfs, colName, 10l);
    }

    @Test
    public void testDeleteKeysIndex() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_KEYS_INDEX);

        ByteBuffer colName = ByteBufferUtil.bytes("birthdate");

        // Insert indexed value.
        new RowUpdateBuilder(cfs.metadata(), 1, "k1").add("birthdate", 10l).build().applyUnsafe();

        // Now delete the value
        RowUpdateBuilder.deleteRow(cfs.metadata(), 2, "k1").applyUnsafe();

        // We want the data to be gcable, but even if gcGrace == 0, we still need to wait 1 second
        // since we won't gc on a tie.
        try { Thread.sleep(1000); } catch (Exception e) {}

        // Read the index and we check we do get no value (and no NPE)
        // Note: the index will return the entry because it hasn't been deleted (we
        // haven't read yet nor compacted) but the data read itself will return null
        assertIndexedNone(cfs, colName, 10l);
    }

    // See CASSANDRA-2628
    @Test
    public void testIndexScanWithLimitOne()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_COMPOSITE_INDEX);
        Mutation rm;

        new RowUpdateBuilder(cfs.metadata(), 0, "kk1").clustering("c").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "kk1").clustering("c").add("notbirthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "kk2").clustering("c").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "kk2").clustering("c").add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "kk3").clustering("c").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "kk3").clustering("c").add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "kk4").clustering("c").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "kk4").clustering("c").add("notbirthdate", 2L).build().applyUnsafe();

        // basic single-expression query, limit 1
        Util.getOnlyRow(Util.cmd(cfs)
                            .filterOn("birthdate", Operator.EQ, 1L)
                            .filterOn("notbirthdate", Operator.EQ, 1L)
                            .withLimit(1)
                            .build());
    }

    @Test
    public void testIndexCreate() throws IOException, InterruptedException, ExecutionException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COMPOSITE_INDEX_TO_BE_ADDED);

        // create a row and update the birthdate value, test that the index query fetches the new version
        new RowUpdateBuilder(cfs.metadata(), 0, "k1").clustering("c").add("birthdate", 1L).build().applyUnsafe();

        String indexName = "birthdate_index";
        ColumnMetadata old = cfs.metadata().getColumn(ByteBufferUtil.bytes("birthdate"));
        IndexMetadata indexDef =
            IndexMetadata.fromIndexTargets(
            Collections.singletonList(new IndexTarget(old.name, IndexTarget.Type.VALUES)),
                                           indexName,
                                           IndexMetadata.Kind.COMPOSITES,
                                           Collections.EMPTY_MAP);

        TableMetadata current = cfs.metadata();

        TableMetadata updated =
            current.unbuild()
                   .indexes(current.indexes.with(indexDef))
                   .build();
        SchemaTestUtil.announceTableUpdate(updated);

        // wait for the index to be built
        Index index = cfs.indexManager.getIndex(indexDef);
        do
        {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        while (!cfs.indexManager.isIndexQueryable(index));

        // we had a bug (CASSANDRA-2244) where index would get created but not flushed -- check for that
        // the way we find the index cfs is a bit convoluted at the moment
        ColumnFamilyStore indexCfs = cfs.indexManager.getIndex(indexDef)
                                                     .getBackingTable()
                                                     .orElseThrow(throwAssert("Index not found"));
        assertFalse(indexCfs.getLiveSSTables().isEmpty());
        assertIndexedOne(cfs, ByteBufferUtil.bytes("birthdate"), 1L);

        // validate that drop clears it out & rebuild works (CASSANDRA-2320)
        assertTrue(cfs.getBuiltIndexes().contains(indexName));
        cfs.indexManager.removeIndex(indexDef.name);
        assertFalse(cfs.getBuiltIndexes().contains(indexName));

        // rebuild & re-query
        Future future = cfs.indexManager.addIndex(indexDef, false);
        future.get();
        assertIndexedOne(cfs, ByteBufferUtil.bytes("birthdate"), 1L);
    }

    @Test
    public void testKeysSearcherSimple() throws Exception
    {
        //  Create secondary index and flush to disk
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(WITH_KEYS_INDEX);

        for (int i = 0; i < 10; i++)
            new RowUpdateBuilder(cfs.metadata(), 0, "k" + i).noRowMarker().add("birthdate", 1l).build().applyUnsafe();

        assertIndexedCount(cfs, ByteBufferUtil.bytes("birthdate"), 1l, 10);
        Util.flush(cfs);
        assertIndexedCount(cfs, ByteBufferUtil.bytes("birthdate"), 1l, 10);
    }

    @Test
    public void testSelectivityWithMultipleIndexes()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(WITH_MULTIPLE_COMPOSITE_INDEX);

        // creates rows such that birthday_index has 1 partition (key = 1L) with 4 rows -- mean row count = 4, and notbirthdate_index has 2 partitions with 2 rows each -- mean row count = 2
        new RowUpdateBuilder(cfs.metadata(), 0, "k1").clustering("c").add("birthdate", 1L).add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "k2").clustering("c").add("birthdate", 1L).add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "k3").clustering("c").add("birthdate", 1L).add("notbirthdate", 3L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "k4").clustering("c").add("birthdate", 1L).add("notbirthdate", 3L).build().applyUnsafe();

        Util.flush(cfs);
        ReadCommand rc = Util.cmd(cfs)
                             .fromKeyIncl("k1")
                             .toKeyIncl("k3")
                             .columns("birthdate")
                             .filterOn("birthdate", Operator.EQ, 1L)
                             .filterOn("notbirthdate", Operator.EQ, 0L)
                             .build();

        assertEquals("notbirthdate_key_index", rc.indexQueryPlan().getFirst().getIndexMetadata().name);
    }

    private void assertIndexedNone(ColumnFamilyStore cfs, ByteBuffer col, Object val)
    {
        assertIndexedCount(cfs, col, val, 0);
    }
    private void assertIndexedOne(ColumnFamilyStore cfs, ByteBuffer col, Object val)
    {
        assertIndexedCount(cfs, col, val, 1);
    }
    private void assertIndexedCount(ColumnFamilyStore cfs, ByteBuffer col, Object val, int count)
    {
        ColumnMetadata cdef = cfs.metadata().getColumn(col);

        ReadCommand rc = Util.cmd(cfs).filterOn(cdef.name.toString(), Operator.EQ, ((AbstractType) cdef.cellValueType()).decompose(val)).build();
        Index.Searcher searcher = rc.indexSearcher();
        if (count != 0)
            assertNotNull(searcher);

        try (ReadExecutionController executionController = rc.executionController();
             PartitionIterator iter = UnfilteredPartitionIterators.filter(searcher.search(executionController),
                                                                          FBUtilities.nowInSeconds()))
        {
            assertEquals(count, Util.size(iter));
        }
    }

    private void assertIndexCfsIsEmpty(ColumnFamilyStore indexCfs)
    {
        PartitionRangeReadCommand command = (PartitionRangeReadCommand)Util.cmd(indexCfs).build();
        try (ReadExecutionController controller = command.executionController();
             PartitionIterator iter = UnfilteredPartitionIterators.filter(Util.executeLocally(command, indexCfs, controller),
                                                                          FBUtilities.nowInSeconds()))
        {
            assertFalse(iter.hasNext());
        }
    }
}
