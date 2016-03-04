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

import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Joiner;
import com.google.common.collect.*;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.throwAssert;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Smoke tests of built-in secondary index implementations
 */
public class CassandraIndexTest extends CQLTester
{
    @Test
    public void indexOnRegularColumn() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c));")
                        .target("v")
                        .indexName("v_index")
                        .withFirstRow(row(0, 0, 0))
                        .withSecondRow(row(1, 1, 1))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("v=0")
                        .secondQueryExpression("v=1")
                        .updateExpression("SET v=2")
                        .postUpdateQueryExpression("v=2")
                        .run();
    }

    @Test
    public void indexOnFirstClusteringColumn() throws Throwable
    {
        // No update allowed on primary key columns, so this script has no update expression
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c));")
                        .target("c")
                        .indexName("c_index")
                        .withFirstRow(row(0, 0, 0))
                        .withSecondRow(row(1, 1, 1))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("c=0")
                        .secondQueryExpression("c=1")
                        .run();
    }

    @Test
    public void indexOnSecondClusteringColumn() throws Throwable
    {
        // No update allowed on primary key columns, so this script has no update expression
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2));")
                        .target("c2")
                        .indexName("c2_index")
                        .withFirstRow(row(0, 0, 0, 0))
                        .withSecondRow(row(1, 1, 1, 1))
                        .missingIndexMessage(String.format("PRIMARY KEY column \"%s\" cannot be restricted " +
                                                           "as preceding column \"%s\" is not restricted",
                                                           "c2", "c1"))
                        .firstQueryExpression("c2=0")
                        .secondQueryExpression("c2=1")
                        .run();
    }

    @Test
    public void indexOnFirstPartitionKeyColumn() throws Throwable
    {
        // No update allowed on primary key columns, so this script has no update expression
        new TestScript().tableDefinition("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, v int, PRIMARY KEY ((k1, k2), c1, c2));")
                        .target("k1")
                        .indexName("k1_index")
                        .withFirstRow(row(0, 0, 0, 0, 0))
                        .withSecondRow(row(1, 1, 1, 1, 1))
                        .missingIndexMessage("Partition key parts: k2 must be restricted as other parts are")
                        .firstQueryExpression("k1=0")
                        .secondQueryExpression("k1=1")
                        .run();
    }

    @Test
    public void indexOnSecondPartitionKeyColumn() throws Throwable
    {
        // No update allowed on primary key columns, so this script has no update expression
        new TestScript().tableDefinition("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, v int, PRIMARY KEY ((k1, k2), c1, c2));")
                        .target("k2")
                        .indexName("k2_index")
                        .withFirstRow(row(0, 0, 0, 0, 0))
                        .withSecondRow(row(1, 1, 1, 1, 1))
                        .missingIndexMessage("Partition key parts: k1 must be restricted as other parts are")
                        .firstQueryExpression("k2=0")
                        .secondQueryExpression("k2=1")
                        .run();
    }

    @Test
    public void indexOnNonFrozenListWithReplaceOperation() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, l list<int>, PRIMARY KEY (k, c));")
                        .target("l")
                        .indexName("l_index1")
                        .withFirstRow(row(0, 0, Lists.newArrayList(10, 20, 30)))
                        .withSecondRow(row(1, 1, Lists.newArrayList(11, 21, 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("l CONTAINS 10")
                        .secondQueryExpression("l CONTAINS 11")
                        .updateExpression("SET l = [40, 50, 60]")
                        .postUpdateQueryExpression("l CONTAINS 40")
                        .run();
    }

    @Test
    public void indexOnNonFrozenListWithInPlaceOperation() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, l list<int>, PRIMARY KEY (k, c));")
                        .target("l")
                        .indexName("l_index2")
                        .withFirstRow(row(0, 0, Lists.newArrayList(10, 20, 30)))
                        .withSecondRow(row(1, 1, Lists.newArrayList(11, 21, 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("l CONTAINS 10")
                        .secondQueryExpression("l CONTAINS 11")
                        .updateExpression("SET l = l - [10]")
                        .postUpdateQueryExpression("l CONTAINS 20")
                        .run();
    }

    @Test
    public void indexOnNonFrozenSetWithReplaceOperation() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, s set<int>, PRIMARY KEY (k, c));")
                        .target("s")
                        .indexName("s_index1")
                        .withFirstRow(row(0, 0, Sets.newHashSet(10, 20, 30)))
                        .withSecondRow(row(1, 1, Sets.newHashSet(11, 21, 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("s CONTAINS 10")
                        .secondQueryExpression("s CONTAINS 11")
                        .updateExpression("SET s = {40, 50, 60}")
                        .postUpdateQueryExpression("s CONTAINS 40")
                        .run();
    }

    @Test
    public void indexOnNonFrozenSetWithInPlaceOperation() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, s set<int>, PRIMARY KEY (k, c));")
                        .target("s")
                        .indexName("s_index2")
                        .withFirstRow(row(0, 0, Sets.newHashSet(10, 20, 30)))
                        .withSecondRow(row(1, 1, Sets.newHashSet(11, 21, 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("s CONTAINS 10")
                        .secondQueryExpression("s CONTAINS 11")
                        .updateExpression("SET s = s - {10}")
                        .postUpdateQueryExpression("s CONTAINS 20")
                        .run();
    }

    @Test
    public void indexOnNonFrozenMapValuesWithReplaceOperation() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, m map<text,int>, PRIMARY KEY (k, c));")
                        .target("m")
                        .indexName("m_index1")
                        .withFirstRow(row(0, 0, ImmutableMap.of("a", 10, "b", 20, "c", 30)))
                        .withSecondRow(row(1, 1, ImmutableMap.of("d", 11, "e", 21, "f", 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("m CONTAINS 10")
                        .secondQueryExpression("m CONTAINS 11")
                        .updateExpression("SET m = {'x':40, 'y':50, 'z':60}")
                        .postUpdateQueryExpression("m CONTAINS 40")
                        .run();
    }

    @Test
    public void indexOnNonFrozenMapValuesWithInPlaceOperation() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, m map<text,int>, PRIMARY KEY (k, c));")
                        .target("m")
                        .indexName("m_index2")
                        .withFirstRow(row(0, 0, ImmutableMap.of("a", 10, "b", 20, "c", 30)))
                        .withSecondRow(row(1, 1, ImmutableMap.of("d", 11, "e", 21, "f", 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("m CONTAINS 10")
                        .secondQueryExpression("m CONTAINS 11")
                        .updateExpression("SET m['a'] = 40")
                        .postUpdateQueryExpression("m CONTAINS 40")
                        .run();
    }

    @Test
    public void indexOnNonFrozenMapKeysWithReplaceOperation() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, m map<text,int>, PRIMARY KEY (k, c));")
                        .target("keys(m)")
                        .indexName("m_index3")
                        .withFirstRow(row(0, 0, ImmutableMap.of("a", 10, "b", 20, "c", 30)))
                        .withSecondRow(row(1, 1, ImmutableMap.of("d", 11, "e", 21, "f", 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("m CONTAINS KEY 'a'")
                        .secondQueryExpression("m CONTAINS KEY 'd'")
                        .updateExpression("SET m = {'x':40, 'y':50, 'z':60}")
                        .postUpdateQueryExpression("m CONTAINS KEY 'x'")
                        .run();
    }

    @Test
    public void indexOnNonFrozenMapKeysWithInPlaceOperation() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, m map<text,int>, PRIMARY KEY (k, c));")
                        .target("keys(m)")
                        .indexName("m_index4")
                        .withFirstRow(row(0, 0, ImmutableMap.of("a", 10, "b", 20, "c", 30)))
                        .withSecondRow(row(1, 1, ImmutableMap.of("d", 11, "e", 21, "f", 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("m CONTAINS KEY 'a'")
                        .secondQueryExpression("m CONTAINS KEY 'd'")
                        .updateExpression("SET m['a'] = NULL")
                        .postUpdateQueryExpression("m CONTAINS KEY 'b'")
                        .run();
    }

    @Test
    public void indexOnNonFrozenMapEntriesWithReplaceOperation() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, m map<text,int>, PRIMARY KEY (k, c));")
                        .target("entries(m)")
                        .indexName("m_index5")
                        .withFirstRow(row(0, 0, ImmutableMap.of("a", 10, "b", 20, "c", 30)))
                        .withSecondRow(row(1, 1, ImmutableMap.of("d", 11, "e", 21, "f", 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("m['a'] = 10")
                        .secondQueryExpression("m['d'] = 11")
                        .updateExpression("SET m = {'x':40, 'y':50, 'z':60}")
                        .postUpdateQueryExpression("m['x'] = 40")
                        .run();
    }

    @Test
    public void indexOnNonFrozenMapEntriesWithInPlaceOperation() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, m map<text,int>, PRIMARY KEY (k, c));")
                        .target("entries(m)")
                        .indexName("m_index6")
                        .withFirstRow(row(0, 0, ImmutableMap.of("a", 10, "b", 20, "c", 30)))
                        .withSecondRow(row(1, 1, ImmutableMap.of("d", 11, "e", 21, "f", 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("m['a'] = 10")
                        .secondQueryExpression("m['d'] = 11")
                        .updateExpression("SET m['a'] = 40")
                        .postUpdateQueryExpression("m['a'] = 40")
                        .run();
    }

    @Test
    public void indexOnFrozenList() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, l frozen<list<int>>, PRIMARY KEY (k, c));")
                        .target("full(l)")
                        .indexName("fl_index")
                        .withFirstRow(row(0, 0, Lists.newArrayList(10, 20, 30)))
                        .withSecondRow(row(1, 1, Lists.newArrayList(11, 21, 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("l = [10, 20, 30]")
                        .secondQueryExpression("l = [11, 21, 31]")
                        .updateExpression("SET l = [40, 50, 60]")
                        .postUpdateQueryExpression("l = [40, 50, 60]")
                        .run();
    }

    @Test
    public void indexOnFrozenSet() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, s frozen<set<int>>, PRIMARY KEY (k, c));")
                        .target("full(s)")
                        .indexName("fs_index")
                        .withFirstRow(row(0, 0, Sets.newHashSet(10, 20, 30)))
                        .withSecondRow(row(1, 1, Sets.newHashSet(11, 21, 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("s = {10, 20, 30}")
                        .secondQueryExpression("s = {11, 21, 31}")
                        .updateExpression("SET s = {40, 50, 60}")
                        .postUpdateQueryExpression("s = {40, 50, 60}")
                        .run();
    }

    @Test
    public void indexOnFrozenMap() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, c int, m frozen<map<text,int>>, PRIMARY KEY (k, c));")
                        .target("full(m)")
                        .indexName("fm_index")
                        .withFirstRow(row(0, 0, ImmutableMap.of("a", 10, "b", 20, "c", 30)))
                        .withSecondRow(row(1, 1, ImmutableMap.of("d", 11, "e", 21, "f", 31)))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("m = {'a':10, 'b':20, 'c':30}")
                        .secondQueryExpression("m = {'d':11, 'e':21, 'f':31}")
                        .updateExpression("SET m = {'x':40, 'y':50, 'z':60}")
                        .postUpdateQueryExpression("m = {'x':40, 'y':50, 'z':60}")
                        .run();
    }

    @Test
    public void indexOnRegularColumnWithCompactStorage() throws Throwable
    {
        new TestScript().tableDefinition("CREATE TABLE %s (k int, v int, PRIMARY KEY (k)) WITH COMPACT STORAGE;")
                        .target("v")
                        .indexName("cv_index")
                        .withFirstRow(row(0, 0))
                        .withSecondRow(row(1,1))
                        .missingIndexMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                        .firstQueryExpression("v=0")
                        .secondQueryExpression("v=1")
                        .updateExpression("SET v=2")
                        .postUpdateQueryExpression("v=2")
                        .run();
    }

    @Test
    public void createIndexesOnMultipleMapDimensions() throws Throwable
    {
        Object[] row1 = row(0, 0, ImmutableMap.of("a", 10, "b", 20, "c", 30));
        Object[] row2 = row(1, 1, ImmutableMap.of("d", 11, "e", 21, "f", 32));
        createTable("CREATE TABLE %s (k int, c int, m map<text, int>, PRIMARY KEY(k, c))");
        createIndex("CREATE INDEX mkey_index on %s(keys(m))");
        createIndex("CREATE INDEX mval_index on %s(m)");

        execute("INSERT INTO %s (k, c, m) VALUES (?, ?, ?)", row1);
        execute("INSERT INTO %s (k, c, m) VALUES (?, ?, ?)", row2);

        assertRows(execute("SELECT * FROM %s WHERE m CONTAINS KEY 'a'"), row1);
        assertRows(execute("SELECT * FROM %s WHERE m CONTAINS 20"), row1);
        assertRows(execute("SELECT * FROM %s WHERE m CONTAINS KEY 'f'"), row2);
        assertRows(execute("SELECT * FROM %s WHERE m CONTAINS 32"), row2);
    }

    @Test
    public void insertWithTombstoneRemovesEntryFromIndex() throws Throwable
    {
        int key = 0;
        int indexedValue = 99;
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        createIndex("CREATE INDEX v_index on %s(v)");
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", key, indexedValue);

        assertRows(execute("SELECT * FROM %s WHERE v = ?", indexedValue), row(key, indexedValue));
        execute("DELETE v FROM %s WHERE k=?", key);
        assertEmpty(execute("SELECT * FROM %s WHERE v = ?", indexedValue));
    }

    @Test
    public void updateTTLOnIndexedClusteringValue() throws Throwable
    {
        int basePk = 1;
        int indexedVal = 2;
        int initialTtl = 3600;
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k,c))");
        createIndex("CREATE INDEX c_index on %s(c)");
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, 0) USING TTL ?", basePk, indexedVal, initialTtl);
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        ColumnFamilyStore indexCfs = baseCfs.indexManager.listIndexes()
                                                         .iterator()
                                                         .next()
                                                         .getBackingTable()
                                                         .orElseThrow(throwAssert("No index found"));
        assertIndexRowTtl(indexCfs, indexedVal, initialTtl);

        int updatedTtl = 9999;
        execute("INSERT INTO %s (k, c ,v) VALUES (?, ?, 0) USING TTL ?", basePk, indexedVal, updatedTtl);

        assertIndexRowTtl(indexCfs, indexedVal, updatedTtl);
    }

    @Test
    public void indexBatchStatements() throws Throwable
    {
        // see CASSANDRA-10536
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s(c)");

        // Multi partition batch
        execute("BEGIN BATCH\n" +
                "UPDATE %1$s SET c = 0 WHERE a = 0 AND b = 0;\n" +
                "UPDATE %1$s SET c = 1 WHERE a = 1 AND b = 1;\n" +
                "APPLY BATCH");
        assertRows(execute("SELECT * FROM %s WHERE c = 0"), row(0, 0, 0));
        assertRows(execute("SELECT * FROM %s WHERE c = 1"), row(1, 1, 1));

        // Single Partition batch
        execute("BEGIN BATCH\n" +
                "UPDATE %1$s SET c = 2 WHERE a = 2 AND b = 0;\n" +
                "UPDATE %1$s SET c = 3 WHERE a = 2 AND b = 1;\n" +
                "APPLY BATCH");
        assertRows(execute("SELECT * FROM %s WHERE c = 2"), row(2, 0, 2));
        assertRows(execute("SELECT * FROM %s WHERE c = 3"), row(2, 1, 3));
    }

    @Test
    public void indexStatementsWithConditions() throws Throwable
    {
        // see CASSANDRA-10536
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s(c)");

        execute("INSERT INTO %s (a, b, c) VALUES (0, 0, 0) IF NOT EXISTS");
        assertRows(execute("SELECT * FROM %s WHERE c = 0"), row(0, 0, 0));

        execute("INSERT INTO %s (a, b, c) VALUES (0, 0, 1) IF NOT EXISTS");
        assertEmpty(execute("SELECT * FROM %s WHERE c = 1"));

        execute("UPDATE %s SET c = 1 WHERE a = 0 AND b =0 IF c = 0");
        assertRows(execute("SELECT * FROM %s WHERE c = 1"), row(0, 0, 1));
        assertEmpty(execute("SELECT * FROM %s WHERE c = 0"));

        execute("DELETE FROM %s WHERE a = 0 AND b = 0 IF c = 0");
        assertRows(execute("SELECT * FROM %s WHERE c = 1"), row(0, 0, 1));

        execute("DELETE FROM %s WHERE a = 0 AND b = 0 IF c = 1");
        assertEmpty(execute("SELECT * FROM %s WHERE c = 1"));

        execute("BEGIN BATCH\n" +
                "INSERT INTO %1$s (a, b, c) VALUES (2, 2, 2) IF NOT EXISTS;\n" +
                "INSERT INTO %1$s (a, b, c) VALUES (2, 3, 3)\n" +
                "APPLY BATCH");
        assertRows(execute("SELECT * FROM %s WHERE c = 2"), row(2, 2, 2));
        assertRows(execute("SELECT * FROM %s WHERE c = 3"), row(2, 3, 3));
    }

    @Test
    public void indexCorrectlyMarkedAsBuildAndRemoved() throws Throwable
    {
        String indexName = "build_remove_test_idx";
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE INDEX %s ON %%s(c)", indexName));
        waitForIndex(KEYSPACE, tableName, indexName);
        // check that there are no other rows in the built indexes table
        assertRows(execute(String.format("SELECT * FROM %s.\"%s\"", SystemKeyspace.NAME, SystemKeyspace.BUILT_INDEXES)),
                   row(KEYSPACE, indexName));

        // rebuild the index and verify the built status table
        getCurrentColumnFamilyStore().rebuildSecondaryIndex(indexName);
        waitForIndex(KEYSPACE, tableName, indexName);

        // check that there are no other rows in the built indexes table
        assertRows(execute(String.format("SELECT * FROM %s.\"%s\"", SystemKeyspace.NAME, SystemKeyspace.BUILT_INDEXES)),
                   row(KEYSPACE, indexName));
    }

    // this is slightly annoying, but we cannot read rows from the methods in Util as
    // ReadCommand#executeInternal uses metadata retrieved via the cfId, which the index
    // CFS inherits from the base CFS. This has the 'wrong' partitioner (the index table
    // uses LocalPartition, the base table a real one, so we cannot read from the index
    // table with executeInternal
    private void assertIndexRowTtl(ColumnFamilyStore indexCfs, int indexedValue, int ttl) throws Throwable
    {
        DecoratedKey indexKey = indexCfs.decorateKey(ByteBufferUtil.bytes(indexedValue));
        ClusteringIndexFilter filter = new ClusteringIndexSliceFilter(Slices.with(indexCfs.metadata.comparator,
                                                                                  Slice.ALL),
                                                                      false);
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(indexCfs.metadata,
                                                                               FBUtilities.nowInSeconds(),
                                                                               indexKey,
                                                                               ColumnFilter.all(indexCfs.metadata),
                                                                               filter);
        try (ReadExecutionController executionController = ReadExecutionController.forCommand(command);
             UnfilteredRowIterator iter = command.queryMemtableAndDisk(indexCfs, executionController.indexReadOpOrderGroup()))
        {
            while( iter.hasNext())
            {
                Unfiltered unfiltered = iter.next();
                assert (unfiltered.isRow());
                Row indexRow = (Row) unfiltered;
                assertEquals(ttl, indexRow.primaryKeyLivenessInfo().ttl());
            }
        }
    }

    private class TestScript
    {
        String tableDefinition;
        String indexName;
        String indexTarget;
        String queryExpression1;
        String queryExpression2;
        String updateExpression;
        String postUpdateQueryExpression;
        String missingIndexMessage;

        Object[] firstRow;
        Object[] secondRow;

        TestScript indexName(String indexName)
        {
            this.indexName = indexName;
            return this;
        }

        TestScript target(String indexTarget)
        {
            this.indexTarget = indexTarget;
            return this;
        }

        TestScript tableDefinition(String tableDefinition)
        {
            this.tableDefinition = tableDefinition;
            return this;
        }

        TestScript withFirstRow(Object[] row)
        {
            this.firstRow = row;
            return this;
        }

        TestScript withSecondRow(Object[] row)
        {
            this.secondRow = row;
            return this;
        }

        TestScript firstQueryExpression(String queryExpression)
        {
            queryExpression1 = queryExpression;
            return this;
        }

        TestScript secondQueryExpression(String queryExpression)
        {
            queryExpression2 = queryExpression;
            return this;
        }

        TestScript updateExpression(String updateExpression)
        {
            this.updateExpression = updateExpression;
            return this;
        }

        TestScript postUpdateQueryExpression(String queryExpression)
        {
            this.postUpdateQueryExpression = queryExpression;
            return this;
        }

        TestScript missingIndexMessage(String missingIndexMessage)
        {
            this.missingIndexMessage = missingIndexMessage;
            return this;
        }

        void run() throws Throwable
        {
            // check minimum required setup
            assertNotNull(indexName);
            assertNotNull(indexTarget);
            assertNotNull(queryExpression1);
            assertNotNull(queryExpression2);
            assertNotNull(firstRow);
            assertNotNull(secondRow);
            assertNotNull(tableDefinition);
            if (updateExpression != null)
                assertNotNull(postUpdateQueryExpression);

            // first, create the table as we need the CFMetaData to build the other cql statements
            createTable(tableDefinition);

            // now setup the cql statements the test will run through. Some are dependent on
            // the table definition, others are not.
            String createIndexCql = String.format("CREATE INDEX %s ON %%s(%s)", indexName, indexTarget);
            String dropIndexCql = String.format("DROP INDEX %s.%s", KEYSPACE, indexName);

            String selectFirstRowCql = String.format("SELECT * FROM %%s WHERE %s", queryExpression1);
            String selectSecondRowCql = String.format("SELECT * FROM %%s WHERE %s", queryExpression2);
            String insertCql = getInsertCql();
            String deleteRowCql = getDeleteRowCql();
            String deletePartitionCql = getDeletePartitionCql();

            // everything setup, run through the smoke test
            execute(insertCql, firstRow);
            // before creating the index, check we cannot query on the indexed column
            assertInvalidThrowMessage(missingIndexMessage, InvalidRequestException.class, selectFirstRowCql);

            // create the index, wait for it to be built then validate the indexed value
            createIndex(createIndexCql);
            waitForIndexBuild();
            assertRows(execute(selectFirstRowCql), firstRow);
            assertEmpty(execute(selectSecondRowCql));

            // flush and check again
            flush();
            assertRows(execute(selectFirstRowCql), firstRow);
            assertEmpty(execute(selectSecondRowCql));

            // force major compaction and query again
            compact();
            assertRows(execute(selectFirstRowCql), firstRow);
            assertEmpty(execute(selectSecondRowCql));

            // reload the base cfs and verify queries still work as expected
            getCurrentColumnFamilyStore().reload();
            assertRows(execute(selectFirstRowCql), firstRow);
            assertEmpty(execute(selectSecondRowCql));

            // drop the index and assert we can no longer query using it
            execute(dropIndexCql);
            assertInvalidThrowMessage(missingIndexMessage, InvalidRequestException.class, selectFirstRowCql);
            // reload the base cfs and verify again
            getCurrentColumnFamilyStore().reload();
            assertInvalidThrowMessage(missingIndexMessage, InvalidRequestException.class, selectFirstRowCql);

            flush();
            compact();

            // insert second row, re-create the index and query for both indexed values
            execute(insertCql, secondRow);
            createIndex(createIndexCql);
            waitForIndexBuild();
            assertRows(execute(selectFirstRowCql), firstRow);
            assertRows(execute(selectSecondRowCql), secondRow);

            // modify the indexed value in the first row, assert we can query by the new value & not the original one
            // note: this is not possible if the indexed column is part of the primary key, so we skip it in that case
            if (includesUpdate())
            {
                execute(getUpdateCql(), getPrimaryKeyValues(firstRow));
                assertEmpty(execute(selectFirstRowCql));
                // update the select statement to query using the updated value
                selectFirstRowCql = String.format("SELECT * FROM %%s WHERE %s", postUpdateQueryExpression);
                // we can't check the entire row b/c we've modified something.
                // so we just check the primary key columns, as they cannot have changed
                assertPrimaryKeyColumnsOnly(execute(selectFirstRowCql), firstRow);
            }

            // delete row, check that it cannot be found via index query
            execute(deleteRowCql, getPrimaryKeyValues(firstRow));
            assertEmpty(execute(selectFirstRowCql));

            // delete partition, check that its rows cannot be retrieved via index query
            execute(deletePartitionCql, getPartitionKeyValues(secondRow));
            assertEmpty(execute(selectSecondRowCql));

            // flush & compact, then verify that deleted values stay gone
            flush();
            compact();
            assertEmpty(execute(selectFirstRowCql));
            assertEmpty(execute(selectSecondRowCql));

            // add back both rows, reset the select for the first row to query on the original value & verify
            execute(insertCql, firstRow);
            selectFirstRowCql = String.format("SELECT * FROM %%s WHERE %s", queryExpression1);
            assertRows(execute(selectFirstRowCql), firstRow);
            execute(insertCql, secondRow);
            assertRows(execute(selectSecondRowCql), secondRow);

            // flush and compact, verify again & we're done
            flush();
            compact();
            assertRows(execute(selectFirstRowCql), firstRow);
            assertRows(execute(selectSecondRowCql), secondRow);
        }

        private void assertPrimaryKeyColumnsOnly(UntypedResultSet resultSet, Object[] row)
        {
            assertFalse(resultSet.isEmpty());
            CFMetaData cfm = getCurrentColumnFamilyStore().metadata;
            int columnCount = cfm.partitionKeyColumns().size();
            if (cfm.isCompound())
                columnCount += cfm.clusteringColumns().size();
            Object[] expected = copyValuesFromRow(row, columnCount);
            assertArrayEquals(expected, copyValuesFromRow(getRows(resultSet)[0], columnCount));
        }

        private String getInsertCql()
        {
            CFMetaData cfm = getCurrentColumnFamilyStore().metadata;
            String columns = Joiner.on(", ")
                                   .join(Iterators.transform(cfm.allColumnsInSelectOrder(),
                                                             (column) -> column.name.toString()));
            String markers = Joiner.on(", ").join(Iterators.transform(cfm.allColumnsInSelectOrder(),
                                                                      (column) -> {
                                                                          return "?";
                                                                      }));
            return String.format("INSERT INTO %%s (%s) VALUES (%s)", columns, markers);
        }

        private String getUpdateCql()
        {
            String whereClause = getPrimaryKeyColumns().map(column -> column.name.toString() + "=?")
                                                       .collect(Collectors.joining(" AND "));
            return String.format("UPDATE %%s %s WHERE %s", updateExpression, whereClause);
        }

        private String getDeleteRowCql()
        {
            return getPrimaryKeyColumns().map(column -> column.name.toString() + "=?")
                                         .collect(Collectors.joining(" AND ", "DELETE FROM %s WHERE ", ""));
        }

        private String getDeletePartitionCql()
        {
            CFMetaData cfm = getCurrentColumnFamilyStore().metadata;
            return StreamSupport.stream(cfm.partitionKeyColumns().spliterator(), false)
                                .map(column -> column.name.toString() + "=?")
                                .collect(Collectors.joining(" AND ", "DELETE FROM %s WHERE ", ""));
        }

        private Stream<ColumnDefinition> getPrimaryKeyColumns()
        {
            CFMetaData cfm = getCurrentColumnFamilyStore().metadata;
            if (cfm.isCompactTable())
                return cfm.partitionKeyColumns().stream();
            else
                return Stream.concat(cfm.partitionKeyColumns().stream(), cfm.clusteringColumns().stream());
        }

        private Object[] getPrimaryKeyValues(Object[] row)
        {
            CFMetaData cfm = getCurrentColumnFamilyStore().metadata;
            if (cfm.isCompactTable())
                return getPartitionKeyValues(row);

            return copyValuesFromRow(row, cfm.partitionKeyColumns().size() + cfm.clusteringColumns().size());
        }

        private Object[] getPartitionKeyValues(Object[] row)
        {
            CFMetaData cfm = getCurrentColumnFamilyStore().metadata;
            return copyValuesFromRow(row, cfm.partitionKeyColumns().size());
        }

        private Object[] copyValuesFromRow(Object[] row, int length)
        {
            Object[] values = new Object[length];
            System.arraycopy(row, 0, values, 0, length);
            return values;
        }

        private boolean includesUpdate()
        {
            return updateExpression != null;
        }

        // Spin waiting for named index to be built
        private void waitForIndexBuild() throws Throwable
        {
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            long maxWaitMillis = 10000;
            long startTime = System.currentTimeMillis();
            while (! cfs.indexManager.getBuiltIndexNames().contains(indexName))
            {
                Thread.sleep(100);
                long wait = System.currentTimeMillis() - startTime;
                if (wait > maxWaitMillis)
                    fail(String.format("Timed out waiting for index %s to build (%s)ms", indexName, wait));
            }
        }
    }
}
