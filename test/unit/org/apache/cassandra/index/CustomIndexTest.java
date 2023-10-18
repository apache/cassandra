/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Test;

import com.datastax.driver.core.exceptions.QueryValidationException;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.restrictions.IndexRestrictions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ColumnFamilyStore.FlushReason;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.cql3.statements.schema.IndexTarget.CUSTOM_INDEX_OPTION_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CustomIndexTest extends CQLTester
{
    @Test
    public void testInsertsOnCfsBackedIndex() throws Throwable
    {
        // test to ensure that we don't deadlock when flushing CFS backed custom indexers
        // see CASSANDRA-10181
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'org.apache.cassandra.index.internal.CustomCassandraIndex'");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 2, 0, 0);
    }

    @Test
    public void testTruncateWithNonCfsCustomIndex() throws Throwable
    {
        // deadlocks and times out the test in the face of the synchronisation
        // issues described in the comments on CASSANDRA-9669
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'org.apache.cassandra.index.StubIndex'");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 2);
        getCurrentColumnFamilyStore().truncateBlocking();
    }

    @Test
    public void indexControlsIfIncludedInBuildOnNewSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        String toInclude = "include";
        String toExclude = "exclude";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(b) USING '%s'",
                                  toInclude, IndexIncludedInBuild.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(b) USING '%s'",
                                  toExclude, IndexExcludedFromBuild.class.getName()));

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, 0);
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, 2);
        flush();

        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
        IndexIncludedInBuild included = (IndexIncludedInBuild)indexManager.getIndexByName(toInclude);
        included.reset();
        assertTrue(included.rowsInserted.isEmpty());

        IndexExcludedFromBuild excluded = (IndexExcludedFromBuild)indexManager.getIndexByName(toExclude);
        excluded.reset();
        assertTrue(excluded.rowsInserted.isEmpty());

        indexManager.rebuildIndexesBlocking(Sets.newHashSet(toInclude, toExclude));

        assertEquals(3, included.rowsInserted.size());
        assertTrue(excluded.rowsInserted.isEmpty());
    }

    @Test
    public void indexReceivesWriteTimeDeletionsCorrectly() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        String indexName = "test_index";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(d) USING '%s'",
                                  indexName, StubIndex.class.getName()));

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 2, 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 3, 3);

        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
        StubIndex index = (StubIndex)indexManager.getIndexByName(indexName);
        assertEquals(4, index.rowsInserted.size());
        assertTrue(index.partitionDeletions.isEmpty());
        assertTrue(index.rangeTombstones.isEmpty());

        execute("DELETE FROM %s WHERE a=0 AND b=0");
        assertTrue(index.partitionDeletions.isEmpty());
        assertEquals(1, index.rangeTombstones.size());

        execute("DELETE FROM %s WHERE a=0");
        assertEquals(1, index.partitionDeletions.size());
        assertEquals(1, index.rangeTombstones.size());
    }
    @Test
    public void nonCustomIndexesRequireExactlyOneTargetColumn() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY (k,c))");

        assertInvalidMessage("Only CUSTOM indexes support multiple columns", "CREATE INDEX multi_idx on %s(v1,v2)");
        assertInvalidMessage("Only CUSTOM indexes can be created without specifying a target column",
                           "CREATE INDEX no_targets on %s()");

        createIndex(String.format("CREATE CUSTOM INDEX multi_idx ON %%s(v1, v2) USING '%s'", StubIndex.class.getName()));
        assertIndexCreated("multi_idx", "v1", "v2");
    }

    @Test
    public void rejectDuplicateColumnsInTargetList() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY (k,c))");

        assertInvalidMessage("Duplicate column 'v1' in index target list",
                             String.format("CREATE CUSTOM INDEX ON %%s(v1, v1) USING '%s'",
                                           StubIndex.class.getName()));

        assertInvalidMessage("Duplicate column 'v1' in index target list",
                             String.format("CREATE CUSTOM INDEX ON %%s(v1, v1, c, c) USING '%s'",
                                           StubIndex.class.getName()));
    }

    @Test
    public void requireFullQualifierForFrozenCollectionTargets() throws Throwable
    {
        // this is really just to prove that we require the full modifier on frozen collection
        // targets whether the index is multicolumn or not
        createTable("CREATE TABLE %s(" +
                    " k int," +
                    " c int," +
                    " fmap frozen<map<int, text>>," +
                    " flist frozen<list<int>>," +
                    " fset frozen<set<int>>," +
                    " PRIMARY KEY(k,c))");

        assertInvalidMessage("Cannot create keys() index on frozen column fmap. " +
                             "Frozen collections are immutable and must be fully indexed",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, keys(fmap)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create entries() index on frozen column fmap. " +
                             "Frozen collections are immutable and must be fully indexed",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, entries(fmap)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create values() index on frozen column fmap. " +
                             "Frozen collections are immutable and must be fully indexed",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, fmap) USING'%s'", StubIndex.class.getName()));

        assertInvalidMessage("Cannot create keys() index on frozen column flist. " +
                             "Frozen collections are immutable and must be fully indexed",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, keys(flist)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create entries() index on frozen column flist. " +
                             "Frozen collections are immutable and must be fully indexed",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, entries(flist)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create values() index on frozen column flist. " +
                             "Frozen collections are immutable and must be fully indexed",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, flist) USING'%s'", StubIndex.class.getName()));

        assertInvalidMessage("Cannot create keys() index on frozen column fset. " +
                             "Frozen collections are immutable and must be fully indexed",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, keys(fset)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create entries() index on frozen column fset. " +
                             "Frozen collections are immutable and must be fully indexed",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, entries(fset)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create values() index on frozen column fset. " +
                             "Frozen collections are immutable and must be fully indexed",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, fset) USING'%s'", StubIndex.class.getName()));

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, full(fmap)) USING'%s'", StubIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, full(flist)) USING'%s'", StubIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, full(fset)) USING'%s'", StubIndex.class.getName()));
    }

    @Test
    public void defaultIndexNameContainsTargetColumns()
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY(k,c))");

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1, v2) USING '%s'", StubIndex.class.getName()));
        assertEquals(1, getCurrentColumnFamilyStore().metadata().indexes.size());
        assertIndexCreated(currentTable() + "_idx", "v1", "v2");

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v1, v2) USING '%s'", StubIndex.class.getName()));
        assertEquals(2, getCurrentColumnFamilyStore().metadata().indexes.size());
        assertIndexCreated(currentTable() + "_idx_1", "c", "v1", "v2");

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v2) USING '%s'", StubIndex.class.getName()));
        assertEquals(3, getCurrentColumnFamilyStore().metadata().indexes.size());
        assertIndexCreated(currentTable() + "_idx_2", "c", "v2");

        // duplicate the previous index with some additional options and check the name is generated as expected
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v2) USING '%s' WITH OPTIONS = {'foo':'bar'}",
                                  StubIndex.class.getName()));
        assertEquals(4, getCurrentColumnFamilyStore().metadata().indexes.size());
        Map<String, String> options = new HashMap<>();
        options.put("foo", "bar");
        assertIndexCreated(currentTable() + "_idx_3", options, "c", "v2");
    }

    @Test
    public void createMultiColumnIndexes()
    {
        // smoke test for various permutations of multicolumn indexes
        createTable("CREATE TABLE %s (" +
                    " pk1 int," +
                    " pk2 int," +
                    " c1 int," +
                    " c2 int," +
                    " v1 int," +
                    " v2 int," +
                    " mval map<text, int>," +
                    " lval list<int>," +
                    " sval set<int>," +
                    " fmap frozen<map<text,int>>," +
                    " flist frozen<list<int>>," +
                    " fset frozen<set<int>>," +
                    " PRIMARY KEY ((pk1, pk2), c1, c2))");

        testCreateIndex("idx_1", "pk1", "pk2");
        testCreateIndex("idx_2", "pk1", "c1");
        testCreateIndex("idx_3", "pk1", "c2");
        testCreateIndex("idx_4", "c1", "c2");
        testCreateIndex("idx_5", "c2", "v1");
        testCreateIndex("idx_6", "v1", "v2");
        testCreateIndex("idx_7", "pk2", "c2", "v2");

        createIndex(String.format("CREATE CUSTOM INDEX idx_8 ON %%s(" +
                                  "  pk1, c1, v1, values(mval), values(sval), values(lval)" +
                                  ") USING '%s'",
                                  StubIndex.class.getName()));
        assertIndexCreated("idx_8",
                           new HashMap<>(),
                           ImmutableList.of(indexTarget("pk1", IndexTarget.Type.SIMPLE),
                                            indexTarget("c1", IndexTarget.Type.SIMPLE),
                                            indexTarget("v1", IndexTarget.Type.SIMPLE),
                                            indexTarget("mval", IndexTarget.Type.VALUES),
                                            indexTarget("sval", IndexTarget.Type.VALUES),
                                            indexTarget("lval", IndexTarget.Type.VALUES)));

        createIndex(String.format("CREATE CUSTOM INDEX inc_frozen ON %%s(" +
                                  "  pk2, c2, v2, full(fmap), full(fset), full(flist)" +
                                  ") USING '%s'",
                                  StubIndex.class.getName()));
        assertIndexCreated("inc_frozen",
                           new HashMap<>(),
                           ImmutableList.of(indexTarget("pk2", IndexTarget.Type.SIMPLE),
                                            indexTarget("c2", IndexTarget.Type.SIMPLE),
                                            indexTarget("v2", IndexTarget.Type.SIMPLE),
                                            indexTarget("fmap", IndexTarget.Type.FULL),
                                            indexTarget("fset", IndexTarget.Type.FULL),
                                            indexTarget("flist", IndexTarget.Type.FULL)));

        createIndex(String.format("CREATE CUSTOM INDEX all_teh_things ON %%s(" +
                                  "  pk1, pk2, c1, c2, v1, v2, keys(mval), lval, sval, full(fmap), full(fset), full(flist)" +
                                  ") USING '%s'",
                                  StubIndex.class.getName()));
        assertIndexCreated("all_teh_things",
                           new HashMap<>(),
                           ImmutableList.of(indexTarget("pk1", IndexTarget.Type.SIMPLE),
                                            indexTarget("pk2", IndexTarget.Type.SIMPLE),
                                            indexTarget("c1", IndexTarget.Type.SIMPLE),
                                            indexTarget("c2", IndexTarget.Type.SIMPLE),
                                            indexTarget("v1", IndexTarget.Type.SIMPLE),
                                            indexTarget("v2", IndexTarget.Type.SIMPLE),
                                            indexTarget("mval", IndexTarget.Type.KEYS),
                                            indexTarget("lval", IndexTarget.Type.VALUES),
                                            indexTarget("sval", IndexTarget.Type.VALUES),
                                            indexTarget("fmap", IndexTarget.Type.FULL),
                                            indexTarget("fset", IndexTarget.Type.FULL),
                                            indexTarget("flist", IndexTarget.Type.FULL)));
    }

    @Test
    public void createMultiColumnIndexIncludingUserTypeColumn()
    {
        String myType = KEYSPACE + '.' + createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 frozen<" + myType + ">)");
        testCreateIndex("udt_idx", "v1", "v2");
    }

    @Test
    public void createIndexWithoutTargets() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY(k,c))");
        // only allowed for CUSTOM indexes
        assertInvalidMessage("Only CUSTOM indexes can be created without specifying a target column",
                             "CREATE INDEX ON %s()");

        // parentheses are mandatory
        assertInvalidSyntax("CREATE CUSTOM INDEX ON %%s USING '%s'", StubIndex.class.getName());
        createIndex(String.format("CREATE CUSTOM INDEX no_targets ON %%s() USING '%s'", StubIndex.class.getName()));
        assertIndexCreated("no_targets", new HashMap<>());
    }

    @Test
    public void testCustomIndexExpressionSyntax() throws Throwable
    {
        Object[] row = row(0, 0, 0, 0);
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        String indexName = currentTable() + "_custom_index";
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", row);


        assertInvalidMessage(String.format(IndexRestrictions.INDEX_NOT_FOUND, indexName, currentTableMetadata().toString()),
                             String.format("SELECT * FROM %%s WHERE expr(%s, 'foo bar baz')", indexName));

        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(c) USING '%s'", indexName, ColumnTargetedIndex.class.getName()));

        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  String.format(IndexRestrictions.INDEX_NOT_FOUND, "no_such_index", currentTableMetadata().toString()),
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(no_such_index, 'foo bar baz ')");

        // simple case
        assertRows(execute(String.format("SELECT * FROM %%s WHERE expr(%s, 'foo bar baz')", indexName)), row);
        assertRows(execute(String.format("SELECT * FROM %%s WHERE expr(\"%s\", 'foo bar baz')", indexName)), row);
        assertRows(execute(String.format("SELECT * FROM %%s WHERE expr(%s, $$foo \" ~~~ bar Baz$$)", indexName)), row);

        // multiple expressions on the same index
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  IndexRestrictions.MULTIPLE_EXPRESSIONS,
                                  QueryValidationException.class,
                                  String.format("SELECT * FROM %%s WHERE expr(%1$s, 'foo') AND expr(%1$s, 'bar')",
                                                indexName));

        // multiple expressions on different indexes
        createIndex(String.format("CREATE CUSTOM INDEX other_custom_index ON %%s(d) USING '%s'", ColumnTargetedIndex.class.getName()));
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  IndexRestrictions.MULTIPLE_EXPRESSIONS,
                                  QueryValidationException.class,
                                  String.format("SELECT * FROM %%s WHERE expr(%s, 'foo') AND expr(other_custom_index, 'bar')",
                                                indexName));

        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                  QueryValidationException.class,
                                  String.format("SELECT * FROM %%s WHERE expr(%s, 'foo') AND d=0", indexName));
        assertRows(execute(String.format("SELECT * FROM %%s WHERE expr(%s, 'foo') AND d=0 ALLOW FILTERING", indexName)), row);
    }

    /**
     * A {@link StubIndex} that only supports expressions on its target column.
     */
    public static final class ColumnTargetedIndex extends StubIndex
    {
        private final ColumnMetadata indexedColumn;

        public ColumnTargetedIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
            indexedColumn = TargetParser.parse(baseCfs.metadata(), metadata).left;
        }

        @Override
        public boolean supportsExpression(ColumnMetadata column, Operator operator)
        {
            return column.equals(indexedColumn) && super.supportsExpression(column, operator);
        }
    }

    @Test
    public void customIndexDoesntSupportCustomExpressions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        String indexName = currentTable() + "_custom_index";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(c) USING '%s'",
                                  indexName,
                                  NoCustomExpressionsIndex.class.getName()));
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  String.format( IndexRestrictions.CUSTOM_EXPRESSION_NOT_SUPPORTED, indexName),
                                  QueryValidationException.class,
                                  String.format("SELECT * FROM %%s WHERE expr(%s, 'foo bar baz')", indexName));
    }

    @Test
    public void customIndexRejectsExpressionSyntax() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        String indexName = currentTable() + "_custom_index";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(c) USING '%s'",
                                  indexName,
                                  AlwaysRejectIndex.class.getName()));
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "None shall pass",
                                  QueryValidationException.class,
                                  String.format("SELECT * FROM %%s WHERE expr(%s, 'foo bar baz')", indexName));
    }

    @Test
    public void customExpressionsMustTargetCustomIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX non_custom_index ON %s(c)");
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  String.format(IndexRestrictions.NON_CUSTOM_INDEX_IN_EXPRESSION, "non_custom_index"),
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(non_custom_index, 'c=0')");
    }

    @Test
    public void customExpressionsDisallowedInModifications() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        String indexName = currentTable() + "_custom_index";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(c) USING '%s'",
                                  indexName, StubIndex.class.getName()));

        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  ModificationStatement.CUSTOM_EXPRESSIONS_NOT_ALLOWED,
                                  QueryValidationException.class,
                                  String.format("DELETE FROM %%s WHERE expr(%s, 'foo bar baz ')", indexName));
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  ModificationStatement.CUSTOM_EXPRESSIONS_NOT_ALLOWED,
                                  QueryValidationException.class,
                                  String.format("UPDATE %%s SET d=0 WHERE expr(%s, 'foo bar baz ')", indexName));
    }

    @Test
    public void indexSelectionPrefersMostSelectiveIndex() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c int, PRIMARY KEY (a))");
        createIndex(String.format("CREATE CUSTOM INDEX %s_more_selective ON %%s(b) USING '%s'",
                                  currentTable(),
                                  SettableSelectivityIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX %s_less_selective ON %%s(c) USING '%s'",
                                  currentTable(),
                                  SettableSelectivityIndex.class.getName()));
        SettableSelectivityIndex moreSelective =
            (SettableSelectivityIndex)getCurrentColumnFamilyStore().indexManager.getIndexByName(currentTable() + "_more_selective");
        SettableSelectivityIndex lessSelective =
            (SettableSelectivityIndex)getCurrentColumnFamilyStore().indexManager.getIndexByName(currentTable() + "_less_selective");
        assertEquals(0, moreSelective.searchersProvided);
        assertEquals(0, lessSelective.searchersProvided);

        // the more selective index should be chosen
        moreSelective.setEstimatedResultRows(1);
        lessSelective.setEstimatedResultRows(1000);
        execute("SELECT * FROM %s WHERE b=0 AND c=0 ALLOW FILTERING");
        assertEquals(1, moreSelective.searchersProvided);
        assertEquals(0, lessSelective.searchersProvided);

        // and adjusting the selectivity should have an observable effect
        moreSelective.setEstimatedResultRows(10000);
        execute("SELECT * FROM %s WHERE b=0 AND c=0 ALLOW FILTERING");
        assertEquals(1, moreSelective.searchersProvided);
        assertEquals(1, lessSelective.searchersProvided);
    }

    @Test
    public void customExpressionForcesIndexSelection() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c int, PRIMARY KEY (a))");
        createIndex(String.format("CREATE CUSTOM INDEX %s_more_selective ON %%s(b) USING '%s'",
                                  currentTable(),
                                  SettableSelectivityIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX %s_less_selective ON %%s(c) USING '%s'",
                                  currentTable(),
                                  SettableSelectivityIndex.class.getName()));
        SettableSelectivityIndex moreSelective =
            (SettableSelectivityIndex)getCurrentColumnFamilyStore().indexManager.getIndexByName(currentTable() + "_more_selective");
        SettableSelectivityIndex lessSelective =
            (SettableSelectivityIndex)getCurrentColumnFamilyStore().indexManager.getIndexByName(currentTable() + "_less_selective");
        assertEquals(0, moreSelective.searchersProvided);
        assertEquals(0, lessSelective.searchersProvided);

        // without a custom expression, the more selective index should be chosen
        moreSelective.setEstimatedResultRows(1);
        lessSelective.setEstimatedResultRows(1000);
        execute("SELECT * FROM %s WHERE b=0 AND c=0 ALLOW FILTERING");
        assertEquals(1, moreSelective.searchersProvided);
        assertEquals(0, lessSelective.searchersProvided);

        // when a custom expression is present, its target index should be preferred
        execute(String.format("SELECT * FROM %%s WHERE b=0 AND expr(%s_less_selective, 'expression') ALLOW FILTERING", currentTable()));
        assertEquals(1, moreSelective.searchersProvided);
        assertEquals(1, lessSelective.searchersProvided);
    }

    @Test
    public void testCustomExpressionValueType() throws Throwable
    {
        // verify that the type of the expression value is determined by Index::customExpressionValueType
        createTable("CREATE TABLE %s (k int, v1 uuid, v2 blob, PRIMARY KEY(k))");
        createIndex(String.format("CREATE CUSTOM INDEX int_index ON %%s() USING '%s'",
                                  Int32ExpressionIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX text_index ON %%s() USING '%s'",
                                  UTF8ExpressionIndex.class.getName()));

        execute("SELECT * FROM %s WHERE expr(text_index, 'foo')");
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "Invalid INTEGER constant (99) for \"custom index expression\" of type text",
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(text_index, 99)");

        execute("SELECT * FROM %s WHERE expr(int_index, 99)");
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "Invalid STRING constant (foo) for \"custom index expression\" of type int",
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(int_index, 'foo')");
    }

    @Test
    public void reloadIndexMetadataOnBaseCfsReload()
    {
        // verify that whenever the base table TableMetadata is reloaded, a reload of the index
        // metadata is performed
        createTable("CREATE TABLE %s (k int, v1 int, PRIMARY KEY(k))");
        createIndex(String.format("CREATE CUSTOM INDEX reload_counter ON %%s() USING '%s'",
                                  CountMetadataReloadsIndex.class.getName()));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CountMetadataReloadsIndex index = (CountMetadataReloadsIndex)cfs.indexManager.getIndexByName("reload_counter");
        assertEquals(0, index.reloads.get());

        // reloading the CFS, even without any metadata changes invokes the index's metadata reload task
        cfs.reload();
        assertEquals(1, index.reloads.get());
    }

    @Test
    public void notifyIndexersOfPartitionAndRowRemovalDuringCleanup() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k,c))");
        createIndex(String.format("CREATE CUSTOM INDEX cleanup_index ON %%s() USING '%s'", StubIndex.class.getName()));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StubIndex index  = (StubIndex)cfs.indexManager.getIndexByName("cleanup_index");

        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 1, 1);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 2, 2);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 3, 3, 3);
        assertEquals(4, index.rowsInserted.size());
        assertEquals(0, index.partitionDeletions.size());

        ReadCommand cmd = Util.cmd(cfs, 0).build();
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            cfs.indexManager.deletePartition(iterator.next(), FBUtilities.nowInSeconds());
        }

        assertEquals(1, index.partitionDeletions.size());
        assertEquals(3, index.rowsDeleted.size());
        for (int i = 0; i < 3; i++)
            assertEquals(index.rowsDeleted.get(i).clustering(), index.rowsInserted.get(i).clustering());
    }

    @Test
    public void notifyIndexersOfExpiredRowsDuringCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, PRIMARY KEY (k,c))");
        createIndex(String.format("CREATE CUSTOM INDEX row_ttl_test_index ON %%s() USING '%s'", StubIndex.class.getName()));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StubIndex index  = (StubIndex)cfs.indexManager.getIndexByName("row_ttl_test_index");

        execute("INSERT INTO %s (k, c) VALUES (?, ?) USING TTL 1", 0, 0);
        execute("INSERT INTO %s (k, c) VALUES (?, ?)", 0, 1);
        execute("INSERT INTO %s (k, c) VALUES (?, ?)", 0, 2);
        execute("INSERT INTO %s (k, c) VALUES (?, ?)", 3, 3);
        assertEquals(4, index.rowsInserted.size());
        // flush so that we end up with an expiring row in the first sstable
        flush();

        // let the row with the ttl expire, then force a compaction
        TimeUnit.SECONDS.sleep(2);
        compact();

        // the index should have been notified of the expired row
        assertEquals(1, index.rowsDeleted.size());
        Integer deletedClustering = Int32Type.instance.compose(index.rowsDeleted.get(0).clustering().bufferAt(0));
        assertEquals(0, deletedClustering.intValue());
    }

    @Test
    public void validateOptions()
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY(k,c))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v2) USING '%s' WITH OPTIONS = {'foo':'bar'}",
                                  IndexWithValidateOptions.class.getName()));
        assertNotNull(IndexWithValidateOptions.options);
        assertEquals("bar", IndexWithValidateOptions.options.get("foo"));
    }

    @Test
    public void validateOptionsWithTableMetadata()
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY(k,c))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v2) USING '%s' WITH OPTIONS = {'foo':'bar'}",
                                  IndexWithOverloadedValidateOptions.class.getName()));
        TableMetadata table = getCurrentColumnFamilyStore().metadata();
        assertEquals(table, IndexWithOverloadedValidateOptions.table);
        assertNotNull(IndexWithOverloadedValidateOptions.options);
        assertEquals("bar", IndexWithOverloadedValidateOptions.options.get("foo"));
    }

    @Test
    public void testFailing2iFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int)");
        createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(value) USING 'org.apache.cassandra.index.CustomIndexTest$BrokenCustom2I'");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, i);

        try
        {
            flush();
            fail("Exception should have been propagated");
        }
        catch (Throwable t)
        {
            assertTrue(t.getMessage().contains("Broken2I"));
        }

        // SSTables remain uncommitted.
        assertEquals(1, getCurrentColumnFamilyStore().getDirectories().getDirectoryForNewSSTables().tryList().length);
    }

    @Test
    public void indexBuildingPagesLargePartitions() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v int, PRIMARY KEY(k,c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SecondaryIndexManager indexManager = cfs.indexManager;
        int totalRows = SimulateConcurrentFlushingIndex.ROWS_IN_PARTITION;
        // Insert a single wide partition to be indexed
        for (int i = 0; i < totalRows; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);
        Util.flush(cfs);

        // Create the index, which won't automatically start building
        String indexName = "build_single_partition_idx";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(v) USING '%s'",
                                  indexName, SimulateConcurrentFlushingIndex.class.getName()));
        SimulateConcurrentFlushingIndex index = (SimulateConcurrentFlushingIndex) indexManager.getIndexByName(indexName);

        // Index the partition with an Indexer which artificially simulates additional concurrent
        // flush activity by periodically issuing barriers on the read & write op groupings
        DecoratedKey targetKey = getCurrentColumnFamilyStore().decorateKey(ByteBufferUtil.bytes(0));
        indexManager.indexPartition(targetKey, Collections.singleton(index), totalRows / 10);

        // When indexing is done check that:
        // * The base table's read ordering at finish was > the one at the start (i.e. that
        //   we didn't hold a single read OpOrder.Group for the whole operation.
        // * That multiple write OpOrder.Groups were used to perform the writes to the index
        // * That all operations are complete, that none of the relevant OpOrder.Groups are
        //   marked as blocking progress and that all the barriers' ops are considered done.
        assertTrue(index.readOrderingAtFinish.compareTo(index.readOrderingAtStart) > 0);
        assertTrue(index.writeGroups.size() > 1);
        assertFalse(index.readOrderingAtFinish.isBlocking());
        index.writeGroups.forEach(group -> assertFalse(group.isBlocking()));
        index.readBarriers.forEach(b -> assertTrue(b.getSyncPoint().isFinished()));
        index.writeBarriers.forEach(b -> {
            b.await(); // Keyspace.writeOrder is global, so this might be temporally blocked by other tests
            assertTrue(b.getSyncPoint().isFinished());
        });
    }

    @Test
    public void partitionIndexTest() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v int, s int static, PRIMARY KEY(k,c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 1, 2, 2);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 1, 3, 3);

        execute("INSERT INTO %s (k, c) VALUES (?, ?)", 2, 2);

        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 3, 1, 1);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 3, 2, 2);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 3, 3, 3);
        execute("DELETE FROM %s WHERE k = ? AND c >= ?", 3, 3);
        execute("DELETE FROM %s WHERE k = ? AND c <= ?", 3, 1);

        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 4, 1, 1);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 4, 2, 2);
        execute("DELETE FROM %s WHERE k = ? AND c = ?", 4, 1);

        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 5, 1, 1);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 5, 2, 2);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 5, 3, 3);
        execute("DELETE FROM %s WHERE k = ?", 5);

        Util.flush(cfs);

        String indexName = "partition_index_test_idx";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(v) USING '%s'",
                                  indexName, StubIndex.class.getName()));

        SecondaryIndexManager indexManager = cfs.indexManager;
        StubIndex index = (StubIndex) indexManager.getIndexByName(indexName);

        DecoratedKey targetKey;
        for (int pageSize = 1; pageSize <= 5; pageSize++)
        {
            targetKey = getCurrentColumnFamilyStore().decorateKey(ByteBufferUtil.bytes(1));
            indexManager.indexPartition(targetKey, Collections.singleton(index), pageSize);
            assertEquals(3, index.rowsInserted.size());
            assertEquals(0, index.rangeTombstones.size());
            assertTrue(index.partitionDeletions.get(0).isLive());
            index.reset();
        }

        for (int pageSize = 1; pageSize <= 5; pageSize++)
        {
            targetKey = getCurrentColumnFamilyStore().decorateKey(ByteBufferUtil.bytes(2));
            indexManager.indexPartition(targetKey, Collections.singleton(index), pageSize);
            assertEquals(1, index.rowsInserted.size());
            assertEquals(0, index.rangeTombstones.size());
            assertTrue(index.partitionDeletions.get(0).isLive());
            index.reset();
        }

        for (int pageSize = 1; pageSize <= 5; pageSize++)
        {
            targetKey = getCurrentColumnFamilyStore().decorateKey(ByteBufferUtil.bytes(3));
            indexManager.indexPartition(targetKey, Collections.singleton(index), pageSize);
            assertEquals(1, index.rowsInserted.size());
            assertEquals(2, index.rangeTombstones.size());
            assertTrue(index.partitionDeletions.get(0).isLive());
            index.reset();
        }

        for (int pageSize = 1; pageSize <= 5; pageSize++)
        {
            targetKey = getCurrentColumnFamilyStore().decorateKey(ByteBufferUtil.bytes(5));
            indexManager.indexPartition(targetKey, Collections.singleton(index), pageSize);
            assertEquals(1, index.partitionDeletions.size());
            assertFalse(index.partitionDeletions.get(0).isLive());
            index.reset();
        }
    }

    @Test
    public void partitionIsNotOverIndexed() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v int, PRIMARY KEY(k,c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SecondaryIndexManager indexManager = cfs.indexManager;

        int totalRows = 1;

        // Insert a single row partition to be indexed
        for (int i = 0; i < totalRows; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);
        Util.flush(cfs);

        // Create the index, which won't automatically start building
        String indexName = "partition_overindex_test_idx";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(v) USING '%s'",
                                  indexName, StubIndex.class.getName()));
        StubIndex index = (StubIndex) indexManager.getIndexByName(indexName);

        // Index the partition
        DecoratedKey targetKey = getCurrentColumnFamilyStore().decorateKey(ByteBufferUtil.bytes(0));
        indexManager.indexPartition(targetKey, Collections.singleton(index), totalRows);

        // Assert only one partition is counted
        assertEquals(1, index.beginCalls);
        assertEquals(1, index.finishCalls);
    }

    @Test
    public void rangeTombstoneTest() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v int, v2 int, PRIMARY KEY(k,c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SecondaryIndexManager indexManager = cfs.indexManager;

        // Insert a single range tombstone
        execute("DELETE FROM %s WHERE k=1 and c > 2");
        Util.flush(cfs);

        // Create the index, which won't automatically start building
        String indexName = "range_tombstone_idx";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(v) USING '%s'",
                                  indexName, StubIndex.class.getName()));
        String indexName2 = "range_tombstone_idx2";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(v2) USING '%s'",
                                  indexName2, StubIndex.class.getName()));

        StubIndex index = (StubIndex) indexManager.getIndexByName(indexName);
        StubIndex index2 = (StubIndex) indexManager.getIndexByName(indexName2);

        // Index the partition
        DecoratedKey targetKey = getCurrentColumnFamilyStore().decorateKey(ByteBufferUtil.bytes(1));
        indexManager.indexPartition(targetKey, Sets.newHashSet(index, index2), 1);

        // and both indexes should have the same range tombstone
        assertEquals(index.rangeTombstones, index2.rangeTombstones);
    }


    // Used for index creation above
    @SuppressWarnings("unused")
    public static class BrokenCustom2I extends StubIndex
    {
        public BrokenCustom2I(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public Callable<?> getBlockingFlushTask()
        {
            throw new RuntimeException("Broken2I");
        }
    }

    private void testCreateIndex(String indexName, String... targetColumnNames)
    {
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(%s) USING '%s'",
                                  indexName, String.join(",", targetColumnNames), StubIndex.class.getName()));
        assertIndexCreated(indexName, targetColumnNames);
    }

    private void assertIndexCreated(String name, String... targetColumnNames)
    {
        assertIndexCreated(name, new HashMap<>(), targetColumnNames);
    }

    private void assertIndexCreated(String name, Map<String, String> options, String... targetColumnNames)
    {
        List<IndexTarget> targets = Arrays.stream(targetColumnNames)
                                          .map(s -> new IndexTarget(ColumnIdentifier.getInterned(s, true), IndexTarget.Type.SIMPLE))
                                          .collect(Collectors.toList());
        assertIndexCreated(name, options, targets);
    }

    private void assertIndexCreated(String name, Map<String, String> options, List<IndexTarget> targets)
    {
        // all tests here use StubIndex as the custom index class,
        // so add that to the map of options
        options.put(CUSTOM_INDEX_OPTION_NAME, StubIndex.class.getName());
        IndexMetadata expected = IndexMetadata.fromIndexTargets(targets, name, IndexMetadata.Kind.CUSTOM, options);
        Indexes indexes = getCurrentColumnFamilyStore().metadata().indexes;
        for (IndexMetadata actual : indexes)
            if (actual.equals(expected))
                return;

        fail(String.format("Index %s not found", expected));
    }

    private static IndexTarget indexTarget(String name, IndexTarget.Type type)
    {
        return new IndexTarget(ColumnIdentifier.getInterned(name, true), type);
    }

    public static final class CountMetadataReloadsIndex extends StubIndex
    {
        private final AtomicInteger reloads = new AtomicInteger(0);

        public CountMetadataReloadsIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public void reset()
        {
            super.reset();
            reloads.set(0);
        }

        public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
        {
            return reloads::incrementAndGet;
        }
    }

    public static final class IndexIncludedInBuild extends StubIndex
    {
        public IndexIncludedInBuild(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public boolean shouldBuildBlocking()
        {
            return true;
        }
    }

    public static final class UTF8ExpressionIndex extends StubIndex
    {
        public UTF8ExpressionIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public AbstractType<?> customExpressionValueType()
        {
            return UTF8Type.instance;
        }
    }

    public static final class Int32ExpressionIndex extends StubIndex
    {
        public Int32ExpressionIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public AbstractType<?> customExpressionValueType()
        {
            return Int32Type.instance;
        }
    }

    public static final class SettableSelectivityIndex extends StubIndex
    {
        private int searchersProvided = 0;
        private long estimatedResultRows = 0;

        public SettableSelectivityIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public void setEstimatedResultRows(long estimate)
        {
            estimatedResultRows = estimate;
        }

        public long getEstimatedResultRows()
        {
            return estimatedResultRows;
        }

        public Searcher searcherFor(ReadCommand command)
        {
                searchersProvided++;
                return super.searcherFor(command);
        }
    }

    public static final class IndexExcludedFromBuild extends StubIndex
    {
        public IndexExcludedFromBuild(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public boolean shouldBuildBlocking()
        {
            return false;
        }
    }

    public static final class NoCustomExpressionsIndex extends StubIndex
    {
        public NoCustomExpressionsIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public AbstractType<?> customExpressionValueType()
        {
            return null;
        }
    }

    public static final class AlwaysRejectIndex extends StubIndex
    {
        public AlwaysRejectIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public void validate(ReadCommand command) throws InvalidRequestException
        {
            throw new InvalidRequestException("None shall pass");
        }

        public Searcher searcherFor(ReadCommand command)
        {
            throw new InvalidRequestException("None shall pass (though I'd have expected to fail faster)");
        }
    }

    public static final class IndexWithValidateOptions extends StubIndex
    {
        public static Map<String, String> options;

        public IndexWithValidateOptions(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @SuppressWarnings("unused")
        public static Map<String, String> validateOptions(Map<String, String> options)
        {
            IndexWithValidateOptions.options = options;
            return new HashMap<>();
        }
    }

    public static final class IndexWithOverloadedValidateOptions extends StubIndex
    {
        public static TableMetadata table;
        public static Map<String, String> options;

        public IndexWithOverloadedValidateOptions(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @SuppressWarnings("unused")
        public static Map<String, String> validateOptions(Map<String, String> options, TableMetadata table)
        {
            IndexWithOverloadedValidateOptions.options = options;
            IndexWithOverloadedValidateOptions.table = table;
            return new HashMap<>();
        }
    }

    public static final class SimulateConcurrentFlushingIndex extends StubIndex
    {
        ColumnFamilyStore baseCfs;
        AtomicInteger indexedRowCount = new AtomicInteger(0);

        OpOrder.Group readOrderingAtStart = null;
        OpOrder.Group readOrderingAtFinish = null;
        Set<OpOrder.Group> writeGroups = new HashSet<>();
        List<OpOrder.Barrier> readBarriers = new ArrayList<>();
        List<OpOrder.Barrier> writeBarriers = new ArrayList<>();

        static final int ROWS_IN_PARTITION = 1000;

        public SimulateConcurrentFlushingIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
            this.baseCfs = baseCfs;
        }

        // When indexing an entire partition 2 potential problems can be caused by
        // whilst holding a single read & a single write OpOrder.Group.
        // * By holding a write group too long, flushes are blocked
        // * Holding a read group for too long prevents the memory from flushed memtables
        //   from being reclaimed.
        // See CASSANDRA-12796 for details.
        // To test that the index builder pages through a large partition, using
        // finer grained OpOrder.Groups we write a "large" partition to disk, then
        // kick off an index build on it, using this indexer.
        // To simulate concurrent flush activity, we periodically issue barriers on
        // the current read and write groups.
        // When we're done indexing the partition, the test checks the states of the
        // various OpOrder.Groups, which it can obtain from this index.

        @Override
        public Indexer indexerFor(final DecoratedKey key,
                                  RegularAndStaticColumns columns,
                                  long nowInSec,
                                  WriteContext ctx,
                                  IndexTransaction.Type transactionType,
                                  Memtable memtable)
        {
            CassandraWriteContext cassandraWriteContext = (CassandraWriteContext) ctx;
            if (readOrderingAtStart == null)
                readOrderingAtStart = baseCfs.readOrdering.getCurrent();

            writeGroups.add(cassandraWriteContext.getGroup());

            return new Indexer()
            {
                public void begin()
                {
                    // to simulate other activity on base table during indexing, issue
                    // barriers on the read and write orderings. This is analogous to
                    // what happens when other flushes are being processed during the
                    // indexing of a partition
                    OpOrder.Barrier readBarrier = baseCfs.readOrdering.newBarrier();
                    readBarrier.issue();
                    readBarriers.add(readBarrier);
                    OpOrder.Barrier writeBarrier = Keyspace.writeOrder.newBarrier();
                    writeBarrier.issue();
                    writeBarriers.add(writeBarrier);
                }

                public void insertRow(Row row)
                {
                    indexedRowCount.incrementAndGet();
                }

                public void finish()
                {
                    // we've indexed all rows in the target partition,
                    // grab the read OpOrder.Group for the base CFS so
                    // we can compare it with the starting group
                    if (indexedRowCount.get() < ROWS_IN_PARTITION)
                        readOrderingAtFinish = baseCfs.readOrdering.getCurrent();
                }

                public void partitionDelete(DeletionTime deletionTime) { }

                public void rangeTombstone(RangeTombstone tombstone) { }

                public void updateRow(Row oldRowData, Row newRowData) { }

                public void removeRow(Row row) { }

            };
        }
    }


    @Test
    public void testFlushObserver() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s int static, v int, PRIMARY KEY (k, c))");
        String indexName = "test_index_with_flush_observer";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(v) USING '%s'",
                                  indexName, IndexWithFlushObserver.class.getName()));

        execute("INSERT INTO %s (k, c, s, v) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (k, c, s, v) VALUES (?, ?, ?, ?)", 0, 1, 1, 1);
        execute("INSERT INTO %s (k, c, s, v) VALUES (?, ?, ?, ?)", 1, 0, 2, 2);
        execute("INSERT INTO %s (k, c, s, v) VALUES (?, ?, ?, ?)", 1, 1, 3, 3);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SecondaryIndexManager indexManager = cfs.indexManager;
        IndexWithFlushObserver index = (IndexWithFlushObserver) indexManager.getIndexByName(indexName);

        assertEquals(0, index.beginFlushCalls.get());
        assertEquals(0, index.flushedPartitions.get());
        assertEquals(0, index.flushedStaticRows.get());
        assertEquals(0, index.flushedUnfiltereds.get());
        assertEquals(0, index.completeFlushCalls.get());

        cfs.forceBlockingFlush(FlushReason.UNIT_TESTS);

        assertEquals(1, index.beginFlushCalls.get());
        assertEquals(2, index.flushedPartitions.get());
        assertEquals(2, index.flushedStaticRows.get());
        assertEquals(4, index.flushedUnfiltereds.get());
        assertEquals(1, index.completeFlushCalls.get());

        execute("DELETE FROM %s WHERE k=?", 0);
        execute("DELETE FROM %s WHERE k=? AND c>=?", 1, 1);
        index.reset();
        cfs.forceBlockingFlush(FlushReason.UNIT_TESTS);

        assertEquals(1, index.beginFlushCalls.get());
        assertEquals(2, index.flushedPartitions.get());
        assertEquals(0, index.flushedStaticRows.get()); // flushed data has no static values..
        assertEquals(2, index.flushedUnfiltereds.get());
        assertEquals(1, index.completeFlushCalls.get());
    }

    /**
     * A {@link StubIndex} using a {@link SSTableFlushObserver} that just keeps count of operations.
     */
    public static final class IndexWithFlushObserver extends StubIndex
    {

        AtomicInteger beginFlushCalls = new AtomicInteger();
        AtomicInteger flushedPartitions = new AtomicInteger();
        AtomicInteger flushedStaticRows = new AtomicInteger();
        AtomicInteger flushedUnfiltereds = new AtomicInteger();
        AtomicInteger completeFlushCalls = new AtomicInteger();

        public IndexWithFlushObserver(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public void reset()
        {
            super.reset();
            beginFlushCalls.set(0);
            flushedPartitions.set(0);
            flushedStaticRows.set(0);
            flushedUnfiltereds.set(0);
            completeFlushCalls.set(0);
        }

        @Override
        public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker)
        {
            return new SSTableFlushObserver() {

                @Override
                public void begin()
                {
                    beginFlushCalls.incrementAndGet();
                }

                @Override
                public void startPartition(DecoratedKey key, long keyPosition, long keyPositionForSASI)
                {
                    flushedPartitions.incrementAndGet();
                }

                @Override
                public void staticRow(Row staticRow)
                {
                    flushedStaticRows.incrementAndGet();
                }

                @Override
                public void nextUnfilteredCluster(Unfiltered unfiltered)
                {
                    flushedUnfiltereds.incrementAndGet();
                }

                @Override
                public void complete()
                {
                    completeFlushCalls.incrementAndGet();
                }
            };
        }
    }

    /**
     * Verify that writes for indexes in the same {@link Index.Group} are grouped.
     */
    @Test
    public void testGroupedWrites() throws Throwable
    {
        // create the schema with two indexes in the same group
        String indexClassName = IndexWithSharedGroup.class.getName();
        createTable("CREATE TABLE %s (k int, c int, s int static, v int, PRIMARY KEY (k,c))");
        createIndex(String.format("CREATE CUSTOM INDEX grouped_index_c ON %%s(c) USING '%s'", indexClassName));
        createIndex(String.format("CREATE CUSTOM INDEX grouped_index_v ON %%s(v) USING '%s'", indexClassName));

        // retrieve the indexes and their shared group
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SecondaryIndexManager indexManager = cfs.indexManager;
        StubIndex index1 = (IndexWithSharedGroup) indexManager.getIndexByName("grouped_index_c");
        StubIndex index2 = (IndexWithSharedGroup) indexManager.getIndexByName("grouped_index_v");
        IndexWithSharedGroup.Group group = indexManager.listIndexGroups()
                                                       .stream()
                                                       .filter(g -> g instanceof IndexWithSharedGroup.Group)
                                                       .map(g -> (IndexWithSharedGroup.Group) g)
                                                       .findAny()
                                                       .orElseThrow(AssertionError::new);

        // verify that row insertions get to the index group and they are propagated to their members
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 1, 1);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 1, 0, 3);
        assertEquals(3, group.rowsInserted.get());
        assertEquals(3, index1.rowsInserted.size());
        assertEquals(3, index2.rowsInserted.size());

        // verify that row updates get to the index group and they are propagated to their members
        execute("UPDATE %s SET v=? WHERE k=? AND c=?", 10, 0, 0);
        execute("UPDATE %s SET v=? WHERE k=? AND c=?", 10, 1, 0);
        assertEquals(2, group.rowsUpdated.get());
        assertEquals(2, index1.rowsUpdated.size());
        assertEquals(2, index2.rowsUpdated.size());

        // verify that partition deletions get to the index group and its members
        ReadCommand cmd = Util.cmd(cfs, 0).build();
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            cfs.indexManager.deletePartition(iterator.next(), FBUtilities.nowInSeconds());
        }
        assertEquals(1, group.partitionDeletions.get());
        assertEquals(1, index1.partitionDeletions.size());
        assertEquals(1, index2.partitionDeletions.size());

        // verify that the row deletions produced by the previous partition deletion get to the group and its members
        assertEquals(2, group.rowsDeleted.get());
        assertEquals(2, index1.rowsDeleted.size());
        assertEquals(2, index2.rowsDeleted.size());

        // verify that range tombstones get to the index group and its members
        execute("DELETE FROM %s WHERE k=? AND c>?", 0, 0);
        execute("DELETE FROM %s WHERE k=? AND c>?", 1, 1);
        assertEquals(2, group.rangeTombstones.get());
        assertEquals(2, index1.rangeTombstones.size());
        assertEquals(2, index2.rangeTombstones.size());

        // verify the total number of begin calls
        assertEquals(10, group.beginCalls.get());
        assertEquals(10, index1.beginCalls);
        assertEquals(10, index2.beginCalls);

        // verify the total number of finish calls
        assertEquals(10, group.finishCalls.get());
        assertEquals(10, index1.finishCalls);
        assertEquals(10, index2.finishCalls);

        // flush the previous data to get rid of it, reset the group counters and flush a new memtable
        cfs.forceBlockingFlush(FlushReason.UNIT_TESTS);
        group.reset();
        execute("INSERT INTO %s (k, s) VALUES (?, ?)", 1, 0);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 1, 0, 0);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 1, 1, 0);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 2, 0, 0);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 2, 1, 0);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 2, 2, 0);
        execute("DELETE FROM %s WHERE k=? AND c=?", 2, 3);
        execute("DELETE FROM %s WHERE k=?", 3);
        cfs.forceBlockingFlush(FlushReason.UNIT_TESTS);

        // verify that the flush observer calls get only once to the group
        assertEquals(1, group.beginFlushCalls.get());
        assertEquals(3, group.flushedPartitions.get());
        assertEquals(3, group.flushedStaticRows.get());
        assertEquals(6, group.flushedUnfiltereds.get());
        assertEquals(1, group.completeFlushCalls.get());

        // verify that the index rebuilds can be directed only to the first index
        group.reset();
        indexManager.rebuildIndexesBlocking(Collections.singleton(index1.getIndexMetadata().name));
        assertEquals(8, group.rowsInserted.get());
        assertEquals(8, index1.rowsInserted.size());
        assertEquals(0, index2.rowsInserted.size());

        // verify that the index rebuilds can be directed only to the second index
        group.reset();
        indexManager.rebuildIndexesBlocking(Collections.singleton(index2.getIndexMetadata().name));
        assertEquals(8, group.rowsInserted.get());
        assertEquals(0, index1.rowsInserted.size());
        assertEquals(8, index2.rowsInserted.size());
    }

    @Test
    public void testIndexGroupsInstancesManagement() throws Throwable
    {
        String indexClassName = IndexWithSharedGroup.class.getName();
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int, v5 int)");
        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;

        // create two indexes belonging to the same group and verify that only one group is added to the manager
        String idx1 = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", indexClassName));
        String idx2 = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v2) USING '%s'", indexClassName));
        Supplier<IndexWithSharedGroup.Group> groupSupplier =
                () -> indexManager.listIndexGroups().stream()
                                                    .filter(g -> g instanceof IndexWithSharedGroup.Group)
                                                    .map(g -> (IndexWithSharedGroup.Group) g)
                                                    .findAny()
                                                    .orElse(null);
        IndexWithSharedGroup.Group group = groupSupplier.get();
        // verify that only one group has been added to the manager
        assertEquals(2, indexManager.listIndexes().size());
        assertEquals(1, indexManager.listIndexGroups().size());
        assertEquals(2, group.indexes.size());

        // create two indexes belonging to their own singleton group and verify that two groups are added to the manager
        String idx3 = createIndex("CREATE INDEX ON %s(v3)");
        String idx4 = createIndex("CREATE INDEX ON %s(v4)");
        assertEquals(4, indexManager.listIndexes().size());
        assertEquals(3, indexManager.listIndexGroups().size());

        // create another index to the shared group and verify that they are added to the existing group instance
        String idx5 = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v5) USING '%s'", indexClassName));
        assertEquals(5, indexManager.listIndexes().size());
        assertEquals(3, indexManager.listIndexGroups().size());
        assertEquals(3, group.indexes.size());

        // drop one of the shared group members and verify that the manager still has the same group count
        dropIndex("DROP INDEX %s." + idx1);
        assertEquals(4, indexManager.listIndexes().size());
        assertEquals(3, indexManager.listIndexGroups().size());
        assertEquals(2, group.indexes.size());

        // drop the standalone indexes and verify that their singleton groups are removed from the manager
        dropIndex("DROP INDEX %s." + idx3);
        dropIndex("DROP INDEX %s." + idx4);
        assertEquals(2, indexManager.listIndexes().size());
        assertEquals(1, indexManager.listIndexGroups().size());

        // drop the remaining members of the shared group and verify that it no longer exists in the manager
        dropIndex("DROP INDEX %s." + idx2);
        dropIndex("DROP INDEX %s." + idx5);
        assertEquals(0, indexManager.listIndexes().size());
        assertEquals(0, indexManager.listIndexGroups().size());
        assertEquals(0, group.indexes.size());

        // create the sharing group members again and verify that they are added to a new group instance
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(v1) USING '%s'", idx1, indexClassName));
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(v2) USING '%s'", idx2, indexClassName));
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(v3) USING '%s'", idx3, indexClassName));
        IndexWithSharedGroup.Group newGroup = indexManager.listIndexGroups()
                                                          .stream()
                                                          .filter(g -> g instanceof IndexWithSharedGroup.Group)
                                                          .map(g -> (IndexWithSharedGroup.Group) g)
                                                          .findAny()
                                                          .orElseThrow(AssertionError::new);
        assertEquals(3, indexManager.listIndexes().size());
        assertEquals(1, indexManager.listIndexGroups().size());
        assertEquals(3, newGroup.indexes.size());
    }

    /**
     * {@link StubIndex} implementation that uses the same {@link Index.Group} for all its instances.
     * That group keeps count of the calls and passes them to its members.
     */
    public static final class IndexWithSharedGroup extends StubIndex
    {
        public IndexWithSharedGroup(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public boolean shouldBuildBlocking()
        {
            return true;
        }

        @Override
        public void register(IndexRegistry registry)
        {
            registry.registerIndex(this, new Group.Key(Group.class), Group::new);
        }

        @Override
        public void unregister(IndexRegistry registry)
        {
            registry.unregisterIndex(this, new Group.Key(Group.class));
        }

        private static class Group implements Index.Group
        {
            Map<String, IndexWithSharedGroup> indexes = Maps.newConcurrentMap();

            AtomicInteger beginCalls = new AtomicInteger();
            AtomicInteger finishCalls = new AtomicInteger();
            AtomicInteger partitionDeletions = new AtomicInteger();
            AtomicInteger rangeTombstones = new AtomicInteger();
            AtomicInteger rowsInserted = new AtomicInteger();
            AtomicInteger rowsDeleted = new AtomicInteger();
            AtomicInteger rowsUpdated = new AtomicInteger();

            AtomicInteger beginFlushCalls = new AtomicInteger();
            AtomicInteger flushedPartitions = new AtomicInteger();
            AtomicInteger flushedStaticRows = new AtomicInteger();
            AtomicInteger flushedUnfiltereds = new AtomicInteger();
            AtomicInteger completeFlushCalls = new AtomicInteger();

            public void reset()
            {
                beginCalls.set(0);
                finishCalls.set(0);
                partitionDeletions.set(0);
                rangeTombstones.set(0);
                rowsInserted.set(0);
                rowsDeleted.set(0);
                rowsUpdated.set(0);
                beginFlushCalls.set(0);
                flushedPartitions.set(0);
                flushedStaticRows.set(0);
                flushedUnfiltereds.set(0);
                completeFlushCalls.set(0);
                indexes.values().forEach(IndexWithSharedGroup::reset);
            }

            @Override
            public Set<Index> getIndexes()
            {
                return ImmutableSet.copyOf(indexes.values());
            }

            @Override
            public void addIndex(Index index)
            {
                indexes.put(index.getIndexMetadata().name, (IndexWithSharedGroup) index);
            }

            @Override
            public void removeIndex(Index index)
            {
                indexes.remove(index.getIndexMetadata().name);
            }

            @Override
            public boolean containsIndex(Index index)
            {
                return indexes.containsKey(index.getIndexMetadata().name);
            }

            @Override
            public boolean isSingleton()
            {
                return false;
            }

            @Override
            public Index.Indexer indexerFor(Predicate<Index> indexSelector,
                                            DecoratedKey key,
                                            RegularAndStaticColumns columns,
                                            long nowInSec,
                                            WriteContext context,
                                            IndexTransaction.Type transactionType,
                                            Memtable memtable)
            {
                Set<Index.Indexer> indexers = indexes.values()
                                                     .stream()
                                                     .filter(indexSelector)
                                                     .map(i -> i.indexerFor(key, columns, nowInSec, context, transactionType, memtable))
                                                     .filter(Objects::nonNull)
                                                     .collect(Collectors.toSet());

                return indexers.isEmpty() ? null : new Index.Indexer() {

                    @Override
                    public void begin()
                    {
                        beginCalls.incrementAndGet();
                        indexers.forEach(Indexer::begin);
                    }

                    @Override
                    public void partitionDelete(DeletionTime deletionTime)
                    {
                        partitionDeletions.incrementAndGet();
                        indexers.forEach(indexer -> indexer.partitionDelete(deletionTime));
                    }

                    @Override
                    public void rangeTombstone(RangeTombstone tombstone)
                    {
                        rangeTombstones.incrementAndGet();
                        indexers.forEach(indexer -> indexer.rangeTombstone(tombstone));
                    }

                    @Override
                    public void insertRow(Row row)
                    {
                        rowsInserted.incrementAndGet();
                        indexers.forEach(indexer -> indexer.insertRow(row));
                    }

                    @Override
                    public void removeRow(Row row)
                    {
                        rowsDeleted.incrementAndGet();
                        indexers.forEach(indexer -> indexer.removeRow(row));
                    }

                    @Override
                    public void updateRow(Row oldRow, Row newRow)
                    {
                        rowsUpdated.incrementAndGet();
                        indexers.forEach(indexer -> indexer.updateRow(oldRow, newRow));
                    }

                    @Override
                    public void finish()
                    {
                        finishCalls.incrementAndGet();
                        indexers.forEach(Indexer::finish);
                    }
                };
            }

            @Override
            public QueryPlan queryPlanFor(RowFilter rowFilter)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker, TableMetadata tableMetadata)
            {
                Set<SSTableFlushObserver> observers = indexes.values()
                                                             .stream()
                                                             .map(i -> i.getFlushObserver(descriptor, tracker))
                                                             .filter(Objects::nonNull)
                                                             .collect(Collectors.toSet());

                return new SSTableFlushObserver() {

                    @Override
                    public void begin()
                    {
                        beginFlushCalls.incrementAndGet();
                        observers.forEach(SSTableFlushObserver::begin);
                    }

                    @Override
                    public void startPartition(DecoratedKey key, long position, long keyPositionForSASI)
                    {
                        flushedPartitions.incrementAndGet();
                        observers.forEach(o -> o.startPartition(key, position, keyPositionForSASI));
                    }

                    @Override
                    public void staticRow(Row staticRow)
                    {
                        flushedStaticRows.incrementAndGet();
                        observers.forEach(o -> o.staticRow(staticRow));
                    }

                    @Override
                    public void nextUnfilteredCluster(Unfiltered unfiltered)
                    {
                        flushedUnfiltereds.incrementAndGet();
                        observers.forEach(o -> o.nextUnfilteredCluster(unfiltered));
                    }

                    @Override
                    public void complete()
                    {
                        completeFlushCalls.incrementAndGet();
                        observers.forEach(SSTableFlushObserver::complete);
                    }
                };
            }

            @Override
            public Set<Component> getComponents()
            {
                return Collections.emptySet();
            }
        }
    }
}
