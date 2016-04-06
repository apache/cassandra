package org.apache.cassandra.index;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import com.datastax.driver.core.exceptions.QueryValidationException;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.restrictions.IndexRestrictions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.throwAssert;
import static org.apache.cassandra.cql3.statements.IndexTarget.CUSTOM_INDEX_OPTION_NAME;
import static org.junit.Assert.assertEquals;
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
        createIndex("CREATE CUSTOM INDEX myindex ON %s(c) USING 'org.apache.cassandra.index.internal.CustomCassandraIndex'");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 2, 0, 0);
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

        indexManager.buildAllIndexesBlocking(getCurrentColumnFamilyStore().getLiveSSTables());

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

        assertInvalidMessage("Duplicate column v1 in index target list",
                             String.format("CREATE CUSTOM INDEX ON %%s(v1, v1) USING '%s'",
                                           StubIndex.class.getName()));

        assertInvalidMessage("Duplicate column v1 in index target list",
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
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, keys(fmap)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create entries() index on frozen column fmap. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, entries(fmap)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create values() index on frozen column fmap. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, fmap) USING'%s'", StubIndex.class.getName()));

        assertInvalidMessage("Cannot create keys() index on frozen column flist. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, keys(flist)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create entries() index on frozen column flist. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, entries(flist)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create values() index on frozen column flist. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, flist) USING'%s'", StubIndex.class.getName()));

        assertInvalidMessage("Cannot create keys() index on frozen column fset. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, keys(fset)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create entries() index on frozen column fset. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, entries(fset)) USING'%s'",
                                           StubIndex.class.getName()));
        assertInvalidMessage("Cannot create values() index on frozen column fset. " +
                             "Frozen collections only support full() indexes",
                             String.format("CREATE CUSTOM INDEX ON %%s(c, fset) USING'%s'", StubIndex.class.getName()));

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, full(fmap)) USING'%s'", StubIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, full(flist)) USING'%s'", StubIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, full(fset)) USING'%s'", StubIndex.class.getName()));
    }

    @Test
    public void defaultIndexNameContainsTargetColumns() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY(k,c))");

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1, v2) USING '%s'", StubIndex.class.getName()));
        assertEquals(1, getCurrentColumnFamilyStore().metadata.getIndexes().size());
        assertIndexCreated(currentTable() + "_idx", "v1", "v2");

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v1, v2) USING '%s'", StubIndex.class.getName()));
        assertEquals(2, getCurrentColumnFamilyStore().metadata.getIndexes().size());
        assertIndexCreated(currentTable() + "_idx_1", "c", "v1", "v2");

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v2) USING '%s'", StubIndex.class.getName()));
        assertEquals(3, getCurrentColumnFamilyStore().metadata.getIndexes().size());
        assertIndexCreated(currentTable() + "_idx_2", "c", "v2");

        // duplicate the previous index with some additional options and check the name is generated as expected
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v2) USING '%s' WITH OPTIONS = {'foo':'bar'}",
                                  StubIndex.class.getName()));
        assertEquals(4, getCurrentColumnFamilyStore().metadata.getIndexes().size());
        Map<String, String> options = new HashMap<>();
        options.put("foo", "bar");
        assertIndexCreated(currentTable() + "_idx_3", options, "c", "v2");
    }

    @Test
    public void createMultiColumnIndexes() throws Throwable
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
        testCreateIndex("idx_8", "pk1", "c1", "v1", "mval", "sval", "lval");

        createIndex(String.format("CREATE CUSTOM INDEX inc_frozen ON %%s(" +
                                  "  pk2, c2, v2, full(fmap), full(fset), full(flist)" +
                                  ") USING '%s'",
                                  StubIndex.class.getName()));
        assertIndexCreated("inc_frozen",
                           new HashMap<>(),
                           ImmutableList.of(indexTarget("pk2", IndexTarget.Type.VALUES),
                                            indexTarget("c2", IndexTarget.Type.VALUES),
                                            indexTarget("v2", IndexTarget.Type.VALUES),
                                            indexTarget("fmap", IndexTarget.Type.FULL),
                                            indexTarget("fset", IndexTarget.Type.FULL),
                                            indexTarget("flist", IndexTarget.Type.FULL)));

        createIndex(String.format("CREATE CUSTOM INDEX all_teh_things ON %%s(" +
                                  "  pk1, pk2, c1, c2, v1, v2, keys(mval), lval, sval, full(fmap), full(fset), full(flist)" +
                                  ") USING '%s'",
                                  StubIndex.class.getName()));
        assertIndexCreated("all_teh_things",
                           new HashMap<>(),
                           ImmutableList.of(indexTarget("pk1", IndexTarget.Type.VALUES),
                                            indexTarget("pk2", IndexTarget.Type.VALUES),
                                            indexTarget("c1", IndexTarget.Type.VALUES),
                                            indexTarget("c2", IndexTarget.Type.VALUES),
                                            indexTarget("v1", IndexTarget.Type.VALUES),
                                            indexTarget("v2", IndexTarget.Type.VALUES),
                                            indexTarget("mval", IndexTarget.Type.KEYS),
                                            indexTarget("lval", IndexTarget.Type.VALUES),
                                            indexTarget("sval", IndexTarget.Type.VALUES),
                                            indexTarget("fmap", IndexTarget.Type.FULL),
                                            indexTarget("fset", IndexTarget.Type.FULL),
                                            indexTarget("flist", IndexTarget.Type.FULL)));
    }

    @Test
    public void createMultiColumnIndexIncludingUserTypeColumn() throws Throwable
    {
        String myType = KEYSPACE + '.' + createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 frozen<" + myType + ">)");
        testCreateIndex("udt_idx", "v1", "v2");
        Indexes indexes = getCurrentColumnFamilyStore().metadata.getIndexes();
        IndexMetadata expected = IndexMetadata.fromIndexTargets(getCurrentColumnFamilyStore().metadata,
                                                                ImmutableList.of(indexTarget("v1", IndexTarget.Type.VALUES),
                                                                                 indexTarget("v2", IndexTarget.Type.VALUES)),
                                                                "udt_idx",
                                                                IndexMetadata.Kind.CUSTOM,
                                                                ImmutableMap.of(CUSTOM_INDEX_OPTION_NAME,
                                                                                StubIndex.class.getName()));
        IndexMetadata actual = indexes.get("udt_idx").orElseThrow(throwAssert("Index udt_idx not found"));
        assertEquals(expected, actual);
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
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", row);

        assertInvalidMessage(String.format(IndexRestrictions.INDEX_NOT_FOUND, "custom_index", keyspace(), currentTable()),
                             "SELECT * FROM %s WHERE expr(custom_index, 'foo bar baz')");

        createIndex(String.format("CREATE CUSTOM INDEX custom_index ON %%s(c) USING '%s'", StubIndex.class.getName()));

        assertInvalidThrowMessage(Server.CURRENT_VERSION,
                                  String.format(IndexRestrictions.INDEX_NOT_FOUND, "no_such_index", keyspace(), currentTable()),
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(no_such_index, 'foo bar baz ')");

        // simple case
        assertRows(execute("SELECT * FROM %s WHERE expr(custom_index, 'foo bar baz')"), row);
        assertRows(execute("SELECT * FROM %s WHERE expr(\"custom_index\", 'foo bar baz')"), row);
        assertRows(execute("SELECT * FROM %s WHERE expr(custom_index, $$foo \" ~~~ bar Baz$$)"), row);

        // multiple expressions on the same index
        assertInvalidThrowMessage(Server.CURRENT_VERSION,
                                  IndexRestrictions.MULTIPLE_EXPRESSIONS,
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(custom_index, 'foo') AND expr(custom_index, 'bar')");

        // multiple expressions on different indexes
        createIndex(String.format("CREATE CUSTOM INDEX other_custom_index ON %%s(d) USING '%s'", StubIndex.class.getName()));
        assertInvalidThrowMessage(Server.CURRENT_VERSION,
                                  IndexRestrictions.MULTIPLE_EXPRESSIONS,
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(custom_index, 'foo') AND expr(other_custom_index, 'bar')");

        assertInvalidThrowMessage(Server.CURRENT_VERSION,
                                  StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(custom_index, 'foo') AND d=0");
        assertRows(execute("SELECT * FROM %s WHERE expr(custom_index, 'foo') AND d=0 ALLOW FILTERING"), row);
    }

    @Test
    public void customIndexDoesntSupportCustomExpressions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE CUSTOM INDEX custom_index ON %%s(c) USING '%s'",
                                  NoCustomExpressionsIndex.class.getName()));
        assertInvalidThrowMessage(Server.CURRENT_VERSION,
                                  String.format( IndexRestrictions.CUSTOM_EXPRESSION_NOT_SUPPORTED, "custom_index"),
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(custom_index, 'foo bar baz')");
    }

    @Test
    public void customIndexRejectsExpressionSyntax() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE CUSTOM INDEX custom_index ON %%s(c) USING '%s'",
                                  AlwaysRejectIndex.class.getName()));
        assertInvalidThrowMessage(Server.CURRENT_VERSION,
                                  "None shall pass",
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(custom_index, 'foo bar baz')");
    }

    @Test
    public void customExpressionsMustTargetCustomIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX non_custom_index ON %s(c)");
        assertInvalidThrowMessage(Server.CURRENT_VERSION,
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

        assertInvalidThrowMessage(Server.CURRENT_VERSION,
                                  ModificationStatement.CUSTOM_EXPRESSIONS_NOT_ALLOWED,
                                  QueryValidationException.class,
                                  String.format("DELETE FROM %%s WHERE expr(%s, 'foo bar baz ')", indexName));
        assertInvalidThrowMessage(Server.CURRENT_VERSION,
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
        assertInvalidThrowMessage(Server.CURRENT_VERSION,
                                  "Invalid INTEGER constant (99) for \"custom index expression\" of type text",
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(text_index, 99)");

        execute("SELECT * FROM %s WHERE expr(int_index, 99)");
        assertInvalidThrowMessage(Server.CURRENT_VERSION,
                                  "Invalid STRING constant (foo) for \"custom index expression\" of type int",
                                  QueryValidationException.class,
                                  "SELECT * FROM %s WHERE expr(int_index, 'foo')");
    }

    @Test
    public void reloadIndexMetadataOnBaseCfsReload() throws Throwable
    {
        // verify that whenever the base table CFMetadata is reloaded, a reload of the index
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
        Integer deletedClustering = Int32Type.instance.compose(index.rowsDeleted.get(0).clustering().get(0));
        assertEquals(0, deletedClustering.intValue());
    }

    @Test
    public void validateOptions() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY(k,c))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v2) USING '%s' WITH OPTIONS = {'foo':'bar'}",
                                  IndexWithValidateOptions.class.getName()));
        assertNotNull(IndexWithValidateOptions.options);
        assertEquals("bar", IndexWithValidateOptions.options.get("foo"));
    }

    @Test
    public void validateOptionsWithCFMetaData() throws Throwable
    {
        createTable("CREATE TABLE %s(k int, c int, v1 int, v2 int, PRIMARY KEY(k,c))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c, v2) USING '%s' WITH OPTIONS = {'foo':'bar'}",
                                  IndexWithOverloadedValidateOptions.class.getName()));
        CFMetaData cfm = getCurrentColumnFamilyStore().metadata;
        assertEquals(cfm, IndexWithOverloadedValidateOptions.cfm);
        assertNotNull(IndexWithOverloadedValidateOptions.options);
        assertEquals("bar", IndexWithOverloadedValidateOptions.options.get("foo"));
    }

    private void testCreateIndex(String indexName, String... targetColumnNames) throws Throwable
    {
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(%s) USING '%s'",
                                  indexName,
                                  Arrays.stream(targetColumnNames).collect(Collectors.joining(",")),
                                  StubIndex.class.getName()));
        assertIndexCreated(indexName, targetColumnNames);
    }

    private void assertIndexCreated(String name, String... targetColumnNames)
    {
        assertIndexCreated(name, new HashMap<>(), targetColumnNames);
    }

    private void assertIndexCreated(String name, Map<String, String> options, String... targetColumnNames)
    {
        List<IndexTarget> targets = Arrays.stream(targetColumnNames)
                                          .map(s -> new IndexTarget(ColumnIdentifier.getInterned(s, true),
                                                                    IndexTarget.Type.VALUES))
                                          .collect(Collectors.toList());
        assertIndexCreated(name, options, targets);
    }

    private void assertIndexCreated(String name, Map<String, String> options, List<IndexTarget> targets)
    {
        // all tests here use StubIndex as the custom index class,
        // so add that to the map of options
        options.put(CUSTOM_INDEX_OPTION_NAME, StubIndex.class.getName());
        CFMetaData cfm = getCurrentColumnFamilyStore().metadata;
        IndexMetadata expected = IndexMetadata.fromIndexTargets(cfm, targets, name, IndexMetadata.Kind.CUSTOM, options);
        Indexes indexes = getCurrentColumnFamilyStore().metadata.getIndexes();
        for (IndexMetadata actual : indexes)
            if (actual.equals(expected))
                return;

        fail(String.format("Index %s not found in CFMetaData", expected));
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

        public static Map<String, String> validateOptions(Map<String, String> options)
        {
            IndexWithValidateOptions.options = options;
            return new HashMap<>();
        }
    }

    public static final class IndexWithOverloadedValidateOptions extends StubIndex
    {
        public static CFMetaData cfm;
        public static Map<String, String> options;

        public IndexWithOverloadedValidateOptions(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public static Map<String, String> validateOptions(Map<String, String> options, CFMetaData cfm)
        {
            IndexWithOverloadedValidateOptions.options = options;
            IndexWithOverloadedValidateOptions.cfm = cfm;
            return new HashMap<>();
        }
    }
}
