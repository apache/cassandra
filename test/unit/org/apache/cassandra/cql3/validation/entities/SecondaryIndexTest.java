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
package org.apache.cassandra.cql3.validation.entities;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.index.internal.CustomCassandraIndex;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;

import static org.apache.cassandra.Util.throwAssert;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SecondaryIndexTest extends CQLTester
{
    public static final int TOO_BIG = 1024 * 65;
    
    @BeforeClass
    public static void setDefaultSecondaryIndex()
    {
        DatabaseDescriptor.setDefaultSecondaryIndex(CassandraIndex.NAME);
    }

    @Test
    public void testCreateAndDropIndex() throws Throwable
    {
        testCreateAndDropIndex("test", false);
        testCreateAndDropIndex("test2", true);
    }

    @Test
    public void testCreateAndDropIndexWithQuotedIdentifier() throws Throwable
    {
        testCreateAndDropIndex("\"quoted_ident\"", false);
        testCreateAndDropIndex("\"quoted_ident2\"", true);
    }

    @Test
    public void testCreateAndDropIndexWithCamelCaseIdentifier() throws Throwable
    {
        testCreateAndDropIndex("CamelCase", false);
        testCreateAndDropIndex("CamelCase2", true);
    }

    /**
     * Test creating and dropping an index with the specified name.
     *
     * @param indexName         the index name
     * @param addKeyspaceOnDrop add the keyspace name in the drop statement
     * @throws Throwable if an error occurs
     */
    private void testCreateAndDropIndex(String indexName, boolean addKeyspaceOnDrop) throws Throwable
    {
        assertInvalidMessage(format("Index '%s.%s' doesn't exist",
                                    KEYSPACE,
                                    removeQuotes(indexName.toLowerCase(Locale.US))),
                             format("DROP INDEX %s.%s", KEYSPACE, indexName));

        createTable("CREATE TABLE %s (a int primary key, b int);");
        createIndexAsync("CREATE INDEX " + indexName + " ON %s(b);");
        createIndexAsync("CREATE INDEX IF NOT EXISTS " + indexName + " ON %s(b);");

        assertInvalidMessage(format("Index '%s' already exists",
                                    removeQuotes(indexName.toLowerCase(Locale.US))),
                             "CREATE INDEX " + indexName + " ON %s(b)");

        // IF NOT EXISTS should apply in cases where the new index differs from an existing one in name only
        String otherIndexName = "index_" + nanoTime();
        assertEquals(1, getCurrentColumnFamilyStore().metadata().indexes.size());
        createIndexAsync("CREATE INDEX IF NOT EXISTS " + otherIndexName + " ON %s(b)");
        assertEquals(1, getCurrentColumnFamilyStore().metadata().indexes.size());
        assertInvalidMessage(format("Index %s is a duplicate of existing index %s",
                                    removeQuotes(otherIndexName.toLowerCase(Locale.US)),
                                    removeQuotes(indexName.toLowerCase(Locale.US))),
                             "CREATE INDEX " + otherIndexName + " ON %s(b)");

        execute("INSERT INTO %s (a, b) values (?, ?);", 0, 0);
        execute("INSERT INTO %s (a, b) values (?, ?);", 1, 1);
        execute("INSERT INTO %s (a, b) values (?, ?);", 2, 2);
        execute("INSERT INTO %s (a, b) values (?, ?);", 3, 1);

        assertRows(execute("SELECT * FROM %s where b = ?", 1), row(1, 1), row(3, 1));

        if (addKeyspaceOnDrop)
        {
            dropIndex(format("DROP INDEX %s.%s", KEYSPACE, indexName));
        }
        else
        {
            execute("USE " + KEYSPACE);
            execute(format("DROP INDEX %s", indexName));
        }

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s where b = ?", 1);
        dropIndex(format("DROP INDEX IF EXISTS %s.%s", KEYSPACE, indexName));
        assertInvalidMessage(format("Index '%s.%s' doesn't exist",
                                    KEYSPACE,
                                    removeQuotes(indexName.toLowerCase(Locale.US))),
                             format("DROP INDEX %s.%s", KEYSPACE, indexName));
    }

    /**
     * Removes the quotes from the specified index name.
     *
     * @param indexName the index name from which the quotes must be removed.
     * @return the unquoted index name.
     */
    private static String removeQuotes(String indexName)
    {
        return StringUtils.remove(indexName, '\"');
    }

    @Test
    public void shouldCreateCassandraIndexExplicitly() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int)");
        createIndex("CREATE INDEX byAge ON %s(age) USING 'legacy_local_table'");

        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
        assertTrue(indexManager.getIndexByName("byage") instanceof CassandraIndex);

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, 'Frodo', 'Baggins', 32)", id1);
        assertEmpty(execute("SELECT firstname FROM %s WHERE userid = ? AND age = 33", id1));
    }

    @Test
    public void shouldCreateCassandraIndexWhenNotDefault() throws Throwable
    {
        DatabaseDescriptor.setDefaultSecondaryIndex(StorageAttachedIndex.NAME);
        
        try
        {
            createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int)");
            createIndex("CREATE INDEX byAge ON %s(age) USING 'legacy_local_table'");

            SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
            assertTrue(indexManager.getIndexByName("byage") instanceof CassandraIndex);

            UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
            execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, 'Frodo', 'Baggins', 32)", id1);
            assertEmpty(execute("SELECT firstname FROM %s WHERE userid = ? AND age = 33", id1));
        }
        finally
        {
            DatabaseDescriptor.setDefaultSecondaryIndex(CassandraIndex.NAME);
        }
    }

    /**
     * Check that you can query for an indexed column even with a key EQ clause,
     * migrated from cql_tests.py:TestCQL.static_cf_test()
     */
    @Test
    public void testSelectWithEQ() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int)");
        createIndex("CREATE INDEX byAge ON %s(age)");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, 'Frodo', 'Baggins', 32)", id1);
        execute("UPDATE %s SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = ?", id2);

        beforeAndAfterFlush(() -> {
            assertEmpty(execute("SELECT firstname FROM %s WHERE userid = ? AND age = 33", id1));

            assertRows(execute("SELECT firstname FROM %s WHERE userid = ? AND age = 33", id2),
                       row("Samwise"));
        });
    }

    /**
     * Check CREATE INDEX without name and validate the index can be dropped,
     * migrated from cql_tests.py:TestCQL.nameless_index_test()
     */
    @Test
    public void testNamelessIndex() throws Throwable
    {
        createTable(" CREATE TABLE %s (id text PRIMARY KEY, birth_year int)");

        createIndex("CREATE INDEX on %s (birth_year)");

        execute("INSERT INTO %s (id, birth_year) VALUES ('Tom', 42)");
        execute("INSERT INTO %s (id, birth_year) VALUES ('Paul', 24)");
        execute("INSERT INTO %s (id, birth_year) VALUES ('Bob', 42)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT id FROM %s WHERE birth_year = 42"),
                       row("Tom"),
                       row("Bob"));
        });

        execute("DROP INDEX %s_birth_year_idx");

        assertInvalid("SELECT id FROM users WHERE birth_year = 42");
    }

    /**
     * Test range queries with 2ndary indexes (#4257),
     * migrated from cql_tests.py:TestCQL.range_query_2ndary_test()
     */
    @Test
    public void testRangeQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, row int, setid int)");
        createIndex("CREATE INDEX indextest_setid_idx ON %s (setid)");

        execute("INSERT INTO %s (id, row, setid) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (id, row, setid) VALUES (?, ?, ?)", 1, 1, 0);
        execute("INSERT INTO %s (id, row, setid) VALUES (?, ?, ?)", 2, 2, 0);
        execute("INSERT INTO %s (id, row, setid) VALUES (?, ?, ?)", 3, 3, 0);

        assertInvalid("SELECT * FROM %s WHERE setid = 0 AND row < 1");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE setid = 0 AND row < 1 ALLOW FILTERING"),
                       row(0, 0, 0));
        });
    }

    /**
     * Check for unknown compression parameters options (#4266),
     * migrated from cql_tests.py:TestCQL.compression_option_validation_test()
     */
    @Test
    public void testUnknownCompressionOptions() throws Throwable
    {
        String tableName = createTableName();
        assertInvalidThrow(SyntaxException.class, format("CREATE TABLE %s (key varchar PRIMARY KEY, password varchar, gender varchar) WITH compression_parameters:sstable_compressor = 'DeflateCompressor'", tableName));

        assertInvalidThrow(ConfigurationException.class, format("CREATE TABLE %s (key varchar PRIMARY KEY, password varchar, gender varchar) WITH compression = { 'sstable_compressor': 'DeflateCompressor' }",
                                                                tableName));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.indexes_composite_test()
     */
    @Test
    public void testIndexOnComposite() throws Throwable
    {
        createTable("CREATE TABLE %s (blog_id int, timestamp int, author text, content text, PRIMARY KEY (blog_id, timestamp))");

        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 0, "bob", "1st post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 1, "tom", "2nd post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 2, "bob", "3rd post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 3, "tom", "4th post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 0, "bob", "5th post");

        createIndex("CREATE INDEX authoridx ON %s (author)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT blog_id, timestamp FROM %s WHERE author = 'bob'"),
                       row(1, 0),
                       row(0, 0),
                       row(0, 2));
        });

        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 1, "tom", "6th post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 2, "tom", "7th post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 3, "bob", "8th post");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT blog_id, timestamp FROM %s WHERE author = 'bob'"),
                       row(1, 0),
                       row(1, 3),
                       row(0, 0),
                       row(0, 2));
        });

        execute("DELETE FROM %s WHERE blog_id = 0 AND timestamp = 2");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT blog_id, timestamp FROM %s WHERE author = 'bob'"),
                       row(1, 0),
                       row(1, 3),
                       row(0, 0));
        });
    }

    /**
     * Test for the validation bug of #4709,
     * migrated from cql_tests.py:TestCQL.refuse_in_with_indexes_test()
     */
    @Test
    public void testInvalidIndexSelect() throws Throwable
    {
        createTable("create table %s (pk varchar primary key, col1 varchar, col2 varchar)");
        createIndex("create index on %s (col1)");
        createIndex("create index on %s (col2)");

        execute("insert into %s (pk, col1, col2) values ('pk1','foo1','bar1')");
        execute("insert into %s (pk, col1, col2) values ('pk1a','foo1','bar1')");
        execute("insert into %s (pk, col1, col2) values ('pk1b','foo1','bar1')");
        execute("insert into %s (pk, col1, col2) values ('pk1c','foo1','bar1')");
        execute("insert into %s (pk, col1, col2) values ('pk2','foo2','bar2')");
        execute("insert into %s (pk, col1, col2) values ('pk3','foo3','bar3')");
        assertInvalid("select * from %s where col2 in ('bar1', 'bar2')");

        //Migrated from cql_tests.py:TestCQL.bug_6050_test()
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");

        createIndex("CREATE INDEX ON %s (a)");
        assertInvalid("SELECT * FROM %s WHERE a = 3 AND b IN (1, 3)");

    }

    /**
     * Migrated from cql_tests.py:TestCQL.edge_2i_on_complex_pk_test()
     */
    @Test
    public void testIndexesOnComplexPrimaryKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 int, pk1 int, ck0 int, ck1 int, ck2 int, value int, PRIMARY KEY ((pk0, pk1), ck0, ck1, ck2))");

        execute("CREATE INDEX ON %s (pk0)");
        execute("CREATE INDEX ON %s (ck0)");
        execute("CREATE INDEX ON %s (ck1)");
        execute("CREATE INDEX ON %s (ck2)");

        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (0, 1, 2, 3, 4, 5)");
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (1, 2, 3, 4, 5, 0)");
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (2, 3, 4, 5, 0, 1)");
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (3, 4, 5, 0, 1, 2)");
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (4, 5, 0, 1, 2, 3)");
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (5, 0, 1, 2, 3, 4)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT value FROM %s WHERE pk0 = 2"),
                       row(1));

            assertRows(execute("SELECT value FROM %s WHERE ck0 = 0"),
                       row(3));

            assertRows(execute("SELECT value FROM %s WHERE pk0 = 3 AND pk1 = 4 AND ck1 = 0"),
                       row(2));

            assertRows(execute("SELECT value FROM %s WHERE pk0 = 5 AND pk1 = 0 AND ck0 = 1 AND ck2 = 3 ALLOW FILTERING"),
                       row(4));
        });
    }

    /**
     * Test for CASSANDRA-5240,
     * migrated from cql_tests.py:TestCQL.bug_5240_test()
     */
    @Test
    public void testIndexOnCompoundRowKey() throws Throwable
    {
        createTable("CREATE TABLE %s (interval text, seq int, id int, severity int, PRIMARY KEY ((interval, seq), id) ) WITH CLUSTERING ORDER BY (id DESC)");

        execute("CREATE INDEX ON %s (severity)");

        execute("insert into %s (interval, seq, id , severity) values('t',1, 1, 1)");
        execute("insert into %s (interval, seq, id , severity) values('t',1, 2, 1)");
        execute("insert into %s (interval, seq, id , severity) values('t',1, 3, 2)");
        execute("insert into %s (interval, seq, id , severity) values('t',1, 4, 3)");
        execute("insert into %s (interval, seq, id , severity) values('t',2, 1, 3)");
        execute("insert into %s (interval, seq, id , severity) values('t',2, 2, 3)");
        execute("insert into %s (interval, seq, id , severity) values('t',2, 3, 1)");
        execute("insert into %s (interval, seq, id , severity) values('t',2, 4, 2)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("select * from %s where severity = 3 and interval = 't' and seq =1"),
                       row("t", 1, 4, 3));

        });
    }

    /**
     * Migrated from cql_tests.py:TestCQL.secondary_index_counters()
     */
    @Test
    public void testIndexOnCountersInvalid() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c counter)");
        assertInvalid("CREATE INDEX ON test(c)");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.collection_indexing_test()
     */
    @Test
    public void testIndexOnCollections() throws Throwable
    {
        createTable(" CREATE TABLE %s ( k int, v int, l list<int>, s set<text>, m map<text, int>, PRIMARY KEY (k, v))");

        createIndex("CREATE INDEX ON %s (l)");
        createIndex("CREATE INDEX ON %s (s)");
        createIndex("CREATE INDEX ON %s (m)");

        execute("INSERT INTO %s (k, v, l, s, m) VALUES (0, 0, [1, 2],    {'a'},      {'a' : 1})");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (0, 1, [3, 4],    {'b', 'c'}, {'a' : 1, 'b' : 2})");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (0, 2, [1],       {'a', 'c'}, {'c' : 3})");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (1, 0, [1, 2, 4], {},         {'b' : 1})");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (1, 1, [4, 5],    {'d'},      {'a' : 1, 'b' : 3})");

        beforeAndAfterFlush(() -> {
            // lists
            assertRows(execute("SELECT k, v FROM %s WHERE l CONTAINS 1"), row(1, 0), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND l CONTAINS 1"), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE l CONTAINS 2"), row(1, 0), row(0, 0));
            assertEmpty(execute("SELECT k, v FROM %s WHERE l CONTAINS 6"));

            // sets
            assertRows(execute("SELECT k, v FROM %s WHERE s CONTAINS 'a'"), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND s CONTAINS 'a'"), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE s CONTAINS 'd'"), row(1, 1));
            assertEmpty(execute("SELECT k, v FROM %s  WHERE s CONTAINS 'e'"));

            // maps
            assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS 1"), row(1, 0), row(1, 1), row(0, 0), row(0, 1));
            assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS 1"), row(0, 0), row(0, 1));
            assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS 2"), row(0, 1));
            assertEmpty(execute("SELECT k, v FROM %s  WHERE m CONTAINS 4"));
        });
    }

    @Test
    public void testSelectOnMultiIndexOnCollectionsWithNull() throws Throwable
    {
        createTable(" CREATE TABLE %s ( k int, v int, x text, l list<int>, s set<text>, m map<text, int>, PRIMARY KEY (k, v))");

        createIndex("CREATE INDEX ON %s (x)");
        createIndex("CREATE INDEX ON %s (v)");
        createIndex("CREATE INDEX ON %s (s)");
        createIndex("CREATE INDEX ON %s (m)");


        execute("INSERT INTO %s (k, v, x, l, s, m) VALUES (0, 0, 'x', [1, 2],    {'a'},      {'a' : 1})");
        execute("INSERT INTO %s (k, v, x, l, s, m) VALUES (0, 1, 'x', [3, 4],    {'b', 'c'}, {'a' : 1, 'b' : 2})");
        execute("INSERT INTO %s (k, v, x, l, s, m) VALUES (0, 2, 'x', [1],       {'a', 'c'}, {'c' : 3})");
        execute("INSERT INTO %s (k, v, x, l, s, m) VALUES (1, 0, 'x', [1, 2, 4], {},         {'b' : 1})");
        execute("INSERT INTO %s (k, v, x, l, s, m) VALUES (1, 1, 'x', [4, 5],    {'d'},      {'a' : 1, 'b' : 3})");
        execute("INSERT INTO %s (k, v, x, l, s, m) VALUES (1, 2, 'x', null,      null,       null)");

        beforeAndAfterFlush(() -> {
            // lists
            assertRows(execute("SELECT k, v FROM %s WHERE x = 'x' AND l CONTAINS 1 ALLOW FILTERING"), row(1, 0), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE x = 'x' AND k = 0 AND l CONTAINS 1 ALLOW FILTERING"), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE x = 'x' AND l CONTAINS 2 ALLOW FILTERING"), row(1, 0), row(0, 0));
            assertEmpty(execute("SELECT k, v FROM %s WHERE x = 'x' AND l CONTAINS 6 ALLOW FILTERING"));

            // sets
            assertRows(execute("SELECT k, v FROM %s WHERE x = 'x' AND s CONTAINS 'a' ALLOW FILTERING" ), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE x = 'x' AND k = 0 AND s CONTAINS 'a' ALLOW FILTERING"), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE x = 'x' AND s CONTAINS 'd' ALLOW FILTERING"), row(1, 1));
            assertEmpty(execute("SELECT k, v FROM %s  WHERE x = 'x' AND s CONTAINS 'e' ALLOW FILTERING"));

            // maps
            assertRows(execute("SELECT k, v FROM %s WHERE x = 'x' AND m CONTAINS 1 ALLOW FILTERING"), row(1, 0), row(1, 1), row(0, 0), row(0, 1));
            assertRows(execute("SELECT k, v FROM %s WHERE x = 'x' AND k = 0 AND m CONTAINS 1 ALLOW FILTERING"), row(0, 0), row(0, 1));
            assertRows(execute("SELECT k, v FROM %s WHERE x = 'x' AND m CONTAINS 2 ALLOW FILTERING"), row(0, 1));
            assertEmpty(execute("SELECT k, v FROM %s  WHERE x = 'x' AND m CONTAINS 4 ALLOW FILTERING"));
        });
    }

    /**
     * Migrated from cql_tests.py:TestCQL.map_keys_indexing()
     */
    @Test
    public void testIndexOnMapKeys() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, m map<text, int>, PRIMARY KEY (k, v))");

        createIndex("CREATE INDEX ON %s(keys(m))");

        execute("INSERT INTO %s (k, v, m) VALUES (0, 0, {'a' : 1})");
        execute("INSERT INTO %s (k, v, m) VALUES (0, 1, {'a' : 1, 'b' : 2})");
        execute("INSERT INTO %s (k, v, m) VALUES (0, 2, {'c' : 3})");
        execute("INSERT INTO %s (k, v, m) VALUES (1, 0, {'b' : 1})");
        execute("INSERT INTO %s (k, v, m) VALUES (1, 1, {'a' : 1, 'b' : 3})");

        // maps
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS KEY 'a'"), row(1, 1), row(0, 0), row(0, 1));
            assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS KEY 'a'"), row(0, 0), row(0, 1));
            assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS KEY 'c'"), row(0, 2));
            assertEmpty(execute("SELECT k, v FROM %s  WHERE m CONTAINS KEY 'd'"));
        });
    }

    /**
     * Test for #6950 bug,
     * migrated from cql_tests.py:TestCQL.key_index_with_reverse_clustering()
     */
    @Test
    public void testIndexOnKeyWithReverseClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY ((k1, k2), v) ) WITH CLUSTERING ORDER BY (v DESC)");

        createIndex("CREATE INDEX ON %s (k2)");

        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 0, 1)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 1, 2)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 0, 3)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (1, 0, 4)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (1, 1, 5)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (2, 0, 7)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (2, 1, 8)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (3, 0, 1)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE k2 = 0 AND v >= 2 ALLOW FILTERING"),
                       row(2, 0, 7),
                       row(0, 0, 3),
                       row(1, 0, 4));
        });
    }

    /**
     * Test for CASSANDRA-6612,
     * migrated from cql_tests.py:TestCQL.bug_6612_test()
     */
    @Test
    public void testSelectCountOnIndexedColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (username text, session_id text, app_name text, account text, last_access timestamp, created_on timestamp, PRIMARY KEY (username, session_id, app_name, account))");

        createIndex("create index ON %s (app_name)");
        createIndex("create index ON %s (last_access)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("select count(*) from %s where app_name='foo' and account='bar' and last_access > 4 allow filtering"), row(0L));
        });

        execute("insert into %s (username, session_id, app_name, account, last_access, created_on) values ('toto', 'foo', 'foo', 'bar', 12, 13)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("select count(*) from %s where app_name='foo' and account='bar' and last_access > 4 allow filtering"), row(1L));
        });
    }

    @Test
    public void testSyntaxVariationsForIndexOnCollectionsValue() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, m map<int, int>, l list<int>, s set<int>, PRIMARY KEY (k))");
        createAndDropCollectionValuesIndex("m");
        createAndDropCollectionValuesIndex("l");
        createAndDropCollectionValuesIndex("s");
    }

    private void createAndDropCollectionValuesIndex(String columnName) throws Throwable
    {
        String indexName = columnName + "_idx";
        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
        createIndex(format("CREATE INDEX %s on %%s(%s)", indexName, columnName));
        IndexMetadata indexDef = indexManager.getIndexByName(indexName).getIndexMetadata();
        assertEquals(format("values(%s)", columnName), indexDef.options.get(IndexTarget.TARGET_OPTION_NAME));
        dropIndex(format("DROP INDEX %s.%s", KEYSPACE, indexName));
        assertFalse(indexManager.hasIndexes());
        createIndex(format("CREATE INDEX %s on %%s(values(%s))", indexName, columnName));
        assertEquals(indexDef, indexManager.getIndexByName(indexName).getIndexMetadata());
        dropIndex(format("DROP INDEX %s.%s", KEYSPACE, indexName));
    }

    @Test
    public void testCreateIndexWithQuotedColumnNames() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    " k int," +
                    " v int, " +
                    " lower_case_map map<int, int>," +
                    " \"MixedCaseMap\" map<int, int>," +
                    " lower_case_frozen_list frozen<list<int>>," +
                    " \"UPPER_CASE_FROZEN_LIST\" frozen<list<int>>," +
                    " \"set name with spaces\" set<int>," +
                    " \"column_name_with\"\"escaped quote\" int," +
                    " PRIMARY KEY (k))");

        createAndDropIndexWithQuotedColumnIdentifier("\"v\"");
        createAndDropIndexWithQuotedColumnIdentifier("keys(\"lower_case_map\")");
        createAndDropIndexWithQuotedColumnIdentifier("keys(\"MixedCaseMap\")");
        createAndDropIndexWithQuotedColumnIdentifier("full(\"lower_case_frozen_list\")");
        createAndDropIndexWithQuotedColumnIdentifier("full(\"UPPER_CASE_FROZEN_LIST\")");
        createAndDropIndexWithQuotedColumnIdentifier("values(\"set name with spaces\")");
        createAndDropIndexWithQuotedColumnIdentifier("\"column_name_with\"\"escaped quote\"");
    }

    private void createAndDropIndexWithQuotedColumnIdentifier(String target) throws Throwable
    {
        String indexName = "test_mixed_case_idx";
        createIndex(format("CREATE INDEX %s ON %%s(%s)", indexName, target));
        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
        IndexMetadata indexDef = indexManager.getIndexByName(indexName).getIndexMetadata();
        dropIndex(format("DROP INDEX %s.%s", KEYSPACE, indexName));
        // verify we can re-create the index using the target string
        createIndex(format("CREATE INDEX %s ON %%s(%s)",
                           indexName, indexDef.options.get(IndexTarget.TARGET_OPTION_NAME)));
        assertEquals(indexDef, indexManager.getIndexByName(indexName).getIndexMetadata());
        dropIndex(format("DROP INDEX %s.%s", KEYSPACE, indexName));
    }


    /**
     * Test for CASSANDRA-5732, Can not query secondary index
     * migrated from cql_tests.py:TestCQL.bug_5732_test(),
     * which was executing with a row cache size of 100 MiB
     * and restarting the node, here we just cleanup the cache.
     */
    @Test
    public void testCanQuerySecondaryIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int,)");

        execute("ALTER TABLE %s WITH CACHING = { 'keys': 'ALL', 'rows_per_partition': 'ALL' }");
        execute("INSERT INTO %s (k,v) VALUES (0,0)");
        execute("INSERT INTO %s (k,v) VALUES (1,1)");

        createIndex("CREATE INDEX testindex on %s (v)");

        assertRows(execute("SELECT k FROM %s WHERE v = 0"), row(0));
        cleanupCache();
        assertRows(execute("SELECT k FROM %s WHERE v = 0"), row(0));
    }

    // CASSANDRA-8280/8081
    // reject updates with indexed values where value > 64k
    // make sure we check conditional and unconditional statements,
    // both singly and in batches (CASSANDRA-10536)
    @Test
    public void testIndexOnCompositeValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(c)");
        failInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
        failInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?) IF NOT EXISTS", ByteBuffer.allocate(TOO_BIG));
        failInsert("BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b, c) VALUES (0, 0, ?);\n" +
                   "APPLY BATCH",
                   ByteBuffer.allocate(TOO_BIG));
        failInsert("BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b, c) VALUES (0, 0, ?) IF NOT EXISTS;\n" +
                   "APPLY BATCH",
                   ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnPartitionKeyInsertValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY ((a, b)))");
        createIndex("CREATE INDEX ON %s(a)");
        succeedInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?) IF NOT EXISTS", ByteBuffer.allocate(TOO_BIG));
        succeedInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
        succeedInsert("BEGIN BATCH\n" +
                      "INSERT INTO %s (a, b, c) VALUES (0, 0, ?);\n" +
                      "APPLY BATCH", ByteBuffer.allocate(TOO_BIG));

        // the indexed value passes validation, but the batch size will
        // exceed the default failure threshold, so temporarily raise it
        // (the non-conditional batch doesn't hit this because
        // BatchStatement::executeLocally skips the size check but CAS
        // path does not)
        long batchSizeThreshold = DatabaseDescriptor.getBatchSizeFailThreshold();
        try
        {
            DatabaseDescriptor.setBatchSizeFailThresholdInKiB((TOO_BIG / 1024) * 2);
            succeedInsert("BEGIN BATCH\n" +
                          "INSERT INTO %s (a, b, c) VALUES (1, 1, ?) IF NOT EXISTS;\n" +
                          "APPLY BATCH", ByteBuffer.allocate(TOO_BIG));
        }
        finally
        {
            DatabaseDescriptor.setBatchSizeFailThresholdInKiB((int) (batchSizeThreshold / 1024));
        }
    }

    @Test
    public void testIndexOnPartitionKeyWithStaticColumnAndNoRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, c int, s int static, v int, PRIMARY KEY((pk1, pk2), c))");
        createIndex("CREATE INDEX ON %s (pk2)");
        execute("INSERT INTO %s (pk1, pk2, c, s, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 1, 9, 1);
        execute("INSERT INTO %s (pk1, pk2, c, s, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 2, 9, 2);
        execute("INSERT INTO %s (pk1, pk2, s) VALUES (?, ?, ?)", 2, 1, 9);
        execute("INSERT INTO %s (pk1, pk2, c, s, v) VALUES (?, ?, ?, ?, ?)", 3, 1, 1, 9, 1);

        assertRows(execute("SELECT * FROM %s WHERE pk2 = ?", 1),
                   row(2, 1, null, 9, null),
                   row(1, 1, 1, 9, 1),
                   row(1, 1, 2, 9, 2),
                   row(3, 1, 1, 9, 1));

        execute("UPDATE %s SET s=?, v=? WHERE pk1=? AND pk2=? AND c=?", 9, 1, 1, 10, 2);
        assertRows(execute("SELECT * FROM %s WHERE pk2 = ?", 10), row(1, 10, 2, 9, 1));

        execute("UPDATE %s SET s=? WHERE pk1=? AND pk2=?", 9, 1, 20);
        assertRows(execute("SELECT * FROM %s WHERE pk2 = ?", 20), row(1, 20, null, 9, null));
    }

    @Test
    public void testIndexOnClusteringColumnInsertValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s(b)");
        succeedInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?) IF NOT EXISTS", ByteBuffer.allocate(TOO_BIG));
        succeedInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
        succeedInsert("BEGIN BATCH\n" +
                      "INSERT INTO %s (a, b, c) VALUES (0, 0, ?);\n" +
                      "APPLY BATCH", ByteBuffer.allocate(TOO_BIG));

        // the indexed value passes validation, but the batch size will
        // exceed the default failure threshold, so temporarily raise it
        // (the non-conditional batch doesn't hit this because
        // BatchStatement::executeLocally skips the size check but CAS
        // path does not)
        long batchSizeThreshold = DatabaseDescriptor.getBatchSizeFailThreshold();
        try
        {
            DatabaseDescriptor.setBatchSizeFailThresholdInKiB((TOO_BIG / 1024) * 2);
            succeedInsert("BEGIN BATCH\n" +
                          "INSERT INTO %s (a, b, c) VALUES (1, 1, ?) IF NOT EXISTS;\n" +
                          "APPLY BATCH", ByteBuffer.allocate(TOO_BIG));
        }
        finally
        {
            DatabaseDescriptor.setBatchSizeFailThresholdInKiB((int)(batchSizeThreshold / 1024));
        }
    }

    @Test
    public void testIndexOnFullCollectionEntryInsertCollectionValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b frozen<map<int, blob>>, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(full(b))");
        Map<Integer, ByteBuffer> map = new HashMap<>();
        map.put(0, ByteBuffer.allocate(1024 * 65));
        failInsert("INSERT INTO %s (a, b) VALUES (0, ?)", map);
        failInsert("INSERT INTO %s (a, b) VALUES (0, ?) IF NOT EXISTS", map);
        failInsert("BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b) VALUES (0, ?);\n" +
                   "APPLY BATCH", map);
        failInsert("BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b) VALUES (0, ?) IF NOT EXISTS;\n" +
                   "APPLY BATCH", map);
    }

    @Test
    public void prepareStatementsWithLIKEClauses() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, c1 text, c2 text, v1 text, v2 text, v3 int, PRIMARY KEY (a, c1, c2))");
        createIndex(format("CREATE CUSTOM INDEX c1_idx on %%s(c1) USING '%s' WITH OPTIONS = {'mode' : 'PREFIX'}",
                           SASIIndex.class.getName()));
        createIndex(format("CREATE CUSTOM INDEX c2_idx on %%s(c2) USING '%s' WITH OPTIONS = {'mode' : 'CONTAINS'}",
                           SASIIndex.class.getName()));
        createIndex(format("CREATE CUSTOM INDEX v1_idx on %%s(v1) USING '%s' WITH OPTIONS = {'mode' : 'PREFIX'}",
                           SASIIndex.class.getName()));
        createIndex(format("CREATE CUSTOM INDEX v2_idx on %%s(v2) USING '%s' WITH OPTIONS = {'mode' : 'CONTAINS'}",
                           SASIIndex.class.getName()));
        createIndex(format("CREATE CUSTOM INDEX v3_idx on %%s(v3) USING '%s'", SASIIndex.class.getName()));

        forcePreparedValues();
        // prefix mode indexes support prefix/contains/matches
        assertInvalidMessage("c1 LIKE '%<term>' abc is only supported on properly indexed columns",
                             "SELECT * FROM %s WHERE c1 LIKE ?",
                             "%abc");
        assertInvalidMessage("c1 LIKE '%<term>%' abc is only supported on properly indexed columns",
                             "SELECT * FROM %s WHERE c1 LIKE ?",
                             "%abc%");
        execute("SELECT * FROM %s WHERE c1 LIKE ?", "abc%");
        execute("SELECT * FROM %s WHERE c1 LIKE ?", "abc");
        assertInvalidMessage("v1 LIKE '%<term>' abc is only supported on properly indexed columns",
                             "SELECT * FROM %s WHERE v1 LIKE ?",
                             "%abc");
        assertInvalidMessage("v1 LIKE '%<term>%' abc is only supported on properly indexed columns",
                             "SELECT * FROM %s WHERE v1 LIKE ?",
                             "%abc%");
        execute("SELECT * FROM %s WHERE v1 LIKE ?", "abc%");
        execute("SELECT * FROM %s WHERE v1 LIKE ?", "abc");

        // contains mode indexes support prefix/suffix/contains/matches
        execute("SELECT * FROM %s WHERE c2 LIKE ?", "abc%");
        execute("SELECT * FROM %s WHERE c2 LIKE ?", "%abc");
        execute("SELECT * FROM %s WHERE c2 LIKE ?", "%abc%");
        execute("SELECT * FROM %s WHERE c2 LIKE ?", "abc");
        execute("SELECT * FROM %s WHERE v2 LIKE ?", "abc%");
        execute("SELECT * FROM %s WHERE v2 LIKE ?", "%abc");
        execute("SELECT * FROM %s WHERE v2 LIKE ?", "%abc%");
        execute("SELECT * FROM %s WHERE v2 LIKE ?", "abc");

        // LIKE is not supported on indexes of non-literal values
        // this is rejected before binding, so the value isn't available in the error message
        assertInvalidMessage("LIKE restriction is only supported on properly indexed columns. v3 LIKE ? is not valid",
                             "SELECT * FROM %s WHERE v3 LIKE ?",
                             "%abc");
        assertInvalidMessage("LIKE restriction is only supported on properly indexed columns. v3 LIKE ? is not valid",
                             "SELECT * FROM %s WHERE v3 LIKE ?",
                             "%abc%");
        assertInvalidMessage("LIKE restriction is only supported on properly indexed columns. v3 LIKE ? is not valid",
                             "SELECT * FROM %s WHERE v3 LIKE ?",
                             "%abc%");
        assertInvalidMessage("LIKE restriction is only supported on properly indexed columns. v3 LIKE ? is not valid",
                             "SELECT * FROM %s WHERE v3 LIKE ?",
                             "abc");
    }

    public void failInsert(String insertCQL, Object...args) throws Throwable
    {
        try
        {
            execute(insertCQL, args);
            fail("Expected statement to fail validation");
        }
        catch (Exception e)
        {
            // as expected
        }
    }

    public void succeedInsert(String insertCQL, Object...args) throws Throwable
    {
        execute(insertCQL, args);
        flush();
    }

    /**
     * Migrated from cql_tests.py:TestCQL.clustering_indexing_test()
     */
    @Test
    public void testIndexesOnClustering() throws Throwable
    {
        createTable("CREATE TABLE %s ( id1 int, id2 int, author text, time bigint, v1 text, v2 text, PRIMARY KEY ((id1, id2), author, time))");

        createIndex("CREATE INDEX ON %s (time)");
        execute("CREATE INDEX ON %s (id2)");

        execute("INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 0, 'A', 'A')");
        execute("INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 1, 'B', 'B')");
        execute("INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 1, 'bob', 2, 'C', 'C')");
        execute("INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 0, 'tom', 0, 'D', 'D')");
        execute("INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 1, 'tom', 1, 'E', 'E')");

        assertRows(execute("SELECT v1 FROM %s WHERE time = 1"),
                   row("B"), row("E"));

        assertRows(execute("SELECT v1 FROM %s WHERE id2 = 1"),
                   row("C"), row("E"));

        assertRows(execute("SELECT v1 FROM %s WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 0"),
                   row("A"));

        // Test for CASSANDRA-8206
        execute("UPDATE %s SET v2 = null WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 1");

        assertRows(execute("SELECT v1 FROM %s WHERE id2 = 0"),
                   row("A"), row("B"), row("D"));

        assertRows(execute("SELECT v1 FROM %s WHERE time = 1"),
                   row("B"), row("E"));
    }

    @Test
    public void testMultipleIndexesOnOneColumn() throws Throwable
    {
        String indexClassName = StubIndex.class.getName();
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY ((a), b))");
        // uses different options otherwise the two indexes are considered duplicates
        createIndex(format("CREATE CUSTOM INDEX c_idx_1 ON %%s(c) USING '%s' WITH OPTIONS = {'foo':'a'}", indexClassName));
        createIndex(format("CREATE CUSTOM INDEX c_idx_2 ON %%s(c) USING '%s' WITH OPTIONS = {'foo':'b'}", indexClassName));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableMetadata cfm = cfs.metadata();
        StubIndex index1 = (StubIndex)cfs.indexManager.getIndex(cfm.indexes
                                                                   .get("c_idx_1")
                                                                   .orElseThrow(throwAssert("index not found")));
        StubIndex index2 = (StubIndex)cfs.indexManager.getIndex(cfm.indexes
                                                                   .get("c_idx_2")
                                                                   .orElseThrow(throwAssert("index not found")));
        Object[] row1a = row(0, 0, 0);
        Object[] row1b = row(0, 0, 1);
        Object[] row2 = row(2, 2, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", row1a);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", row1b);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", row2);

        assertEquals(2, index1.rowsInserted.size());
        assertColumnValue(0, "c", index1.rowsInserted.get(0), cfm);
        assertColumnValue(2, "c", index1.rowsInserted.get(1), cfm);

        assertEquals(2, index2.rowsInserted.size());
        assertColumnValue(0, "c", index2.rowsInserted.get(0), cfm);
        assertColumnValue(2, "c", index2.rowsInserted.get(1), cfm);

        assertEquals(1, index1.rowsUpdated.size());
        assertColumnValue(0, "c", index1.rowsUpdated.get(0).left, cfm);
        assertColumnValue(1, "c", index1.rowsUpdated.get(0).right, cfm);

        assertEquals(1, index2.rowsUpdated.size());
        assertColumnValue(0, "c", index2.rowsUpdated.get(0).left, cfm);
        assertColumnValue(1, "c", index2.rowsUpdated.get(0).right, cfm);
    }

    @Test
    public void testDeletions() throws Throwable
    {
        // Test for bugs like CASSANDRA-10694.  These may not be readily visible with the built-in secondary index
        // implementation because of the stale entry handling.

        String indexClassName = StubIndex.class.getName();
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY ((a), b))");
        createIndex(format("CREATE CUSTOM INDEX c_idx ON %%s(c) USING '%s'", indexClassName));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableMetadata cfm = cfs.metadata();
        StubIndex index1 = (StubIndex) cfs.indexManager.getIndex(cfm.indexes
                .get("c_idx")
                .orElseThrow(throwAssert("index not found")));

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP 1", 0, 0, 0);
        assertEquals(1, index1.rowsInserted.size());

        execute("DELETE FROM %s USING TIMESTAMP 2 WHERE a = ? AND b = ?", 0, 0);
        assertEquals(1, index1.rowsUpdated.size());
        Pair<Row, Row> update = index1.rowsUpdated.get(0);
        Row existingRow = update.left;
        Row newRow = update.right;

        // check the existing row from the update call
        assertTrue(existingRow.deletion().isLive());
        assertEquals(DeletionTime.LIVE, existingRow.deletion().time());
        assertEquals(1L, existingRow.primaryKeyLivenessInfo().timestamp());

        // check the new row from the update call
        assertFalse(newRow.deletion().isLive());
        assertEquals(2L, newRow.deletion().time().markedForDeleteAt());
        assertFalse(newRow.cells().iterator().hasNext());

        // delete the same row again
        execute("DELETE FROM %s USING TIMESTAMP 3 WHERE a = ? AND b = ?", 0, 0);
        assertEquals(2, index1.rowsUpdated.size());
        update = index1.rowsUpdated.get(1);
        existingRow = update.left;
        newRow = update.right;

        // check the new row from the update call
        assertFalse(existingRow.deletion().isLive());
        assertEquals(2L, existingRow.deletion().time().markedForDeleteAt());
        assertFalse(existingRow.cells().iterator().hasNext());

        // check the new row from the update call
        assertFalse(newRow.deletion().isLive());
        assertEquals(3L, newRow.deletion().time().markedForDeleteAt());
        assertFalse(newRow.cells().iterator().hasNext());
    }

    @Test
    public void testUpdatesToMemtableData() throws Throwable
    {
        // verify the contract specified by Index.Indexer::updateRow(oldRowData, newRowData),
        // when a row in the memtable is updated, the indexer should be informed of:
        // * new columns
        // * removed columns
        // * columns whose value, timestamp or ttl have been modified.
        // Any columns which are unchanged by the update are not passed to the Indexer
        // Note that for simplicity this test resets the index between each scenario
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 int, PRIMARY KEY (k,c))");
        createIndex(format("CREATE CUSTOM INDEX test_index ON %%s() USING '%s'", StubIndex.class.getName()));
        execute("INSERT INTO %s (k, c, v1, v2) VALUES (0, 0, 0, 0) USING TIMESTAMP 0");

        ColumnMetadata v1 = getCurrentColumnFamilyStore().metadata().getColumn(new ColumnIdentifier("v1", true));
        ColumnMetadata v2 = getCurrentColumnFamilyStore().metadata().getColumn(new ColumnIdentifier("v2", true));

        StubIndex index = (StubIndex)getCurrentColumnFamilyStore().indexManager.getIndexByName("test_index");
        assertEquals(1, index.rowsInserted.size());

        // Overwrite a single value, leaving the other untouched
        execute("UPDATE %s USING TIMESTAMP 1 SET v1=1 WHERE k=0 AND c=0");
        assertEquals(1, index.rowsUpdated.size());
        Row oldRow = index.rowsUpdated.get(0).left;
        assertEquals(1, oldRow.columnCount());
        validateCell(oldRow.getCell(v1), v1, ByteBufferUtil.bytes(0), 0);
        Row newRow = index.rowsUpdated.get(0).right;
        assertEquals(1, newRow.columnCount());
        validateCell(newRow.getCell(v1), v1, ByteBufferUtil.bytes(1), 1);
        index.reset();

        // Overwrite both values
        execute("UPDATE %s USING TIMESTAMP 2 SET v1=2, v2=2 WHERE k=0 AND c=0");
        assertEquals(1, index.rowsUpdated.size());
        oldRow = index.rowsUpdated.get(0).left;
        assertEquals(2, oldRow.columnCount());
        validateCell(oldRow.getCell(v1), v1, ByteBufferUtil.bytes(1), 1);
        validateCell(oldRow.getCell(v2), v2, ByteBufferUtil.bytes(0), 0);
        newRow = index.rowsUpdated.get(0).right;
        assertEquals(2, newRow.columnCount());
        validateCell(newRow.getCell(v1), v1, ByteBufferUtil.bytes(2), 2);
        validateCell(newRow.getCell(v2), v2, ByteBufferUtil.bytes(2), 2);
        index.reset();

        // Delete one value
        execute("DELETE v1 FROM %s USING TIMESTAMP 3 WHERE k=0 AND c=0");
        assertEquals(1, index.rowsUpdated.size());
        oldRow = index.rowsUpdated.get(0).left;
        assertEquals(1, oldRow.columnCount());
        validateCell(oldRow.getCell(v1), v1, ByteBufferUtil.bytes(2), 2);
        newRow = index.rowsUpdated.get(0).right;
        assertEquals(1, newRow.columnCount());
        Cell<?> newCell = newRow.getCell(v1);
        assertTrue(newCell.isTombstone());
        assertEquals(3, newCell.timestamp());
        index.reset();

        // Modify the liveness of the primary key, the delta rows should contain
        // no cell data as only the pk was altered, but it should illustrate the
        // change to the liveness info
        execute("INSERT INTO %s(k, c) VALUES (0, 0) USING TIMESTAMP 4");
        assertEquals(1, index.rowsUpdated.size());
        oldRow = index.rowsUpdated.get(0).left;
        assertEquals(0, oldRow.columnCount());
        assertEquals(0, oldRow.primaryKeyLivenessInfo().timestamp());
        newRow = index.rowsUpdated.get(0).right;
        assertEquals(0, newRow.columnCount());
        assertEquals(4, newRow.primaryKeyLivenessInfo().timestamp());
    }

    @Test
    public void testIndexQueriesWithIndexNotReady() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, value int, PRIMARY KEY (pk, ck))");

        for (int i = 0; i < 10; i++)
            for (int j = 0; j < 10; j++)
                execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", i, j, i + j);

        createIndexAsync("CREATE CUSTOM INDEX testIndex ON %s (value) USING '" + IndexBlockingOnInitialization.class.getName() + "'");
        try
        {
            execute("SELECT value FROM %s WHERE value = 2");
            fail();
        }
        catch (IndexNotAvailableException e)
        {
            assertTrue(true);
        }
        finally
        {
            execute("DROP index " + KEYSPACE + ".testIndex");
        }
    }

    @Test // A Bad init could leave an index only accepting reads
    public void testReadOnlyIndex() throws Throwable
    {
        // On successful initialization both reads and writes go through
        createTable("CREATE TABLE %s (pk int, ck int, value int, PRIMARY KEY (pk, ck))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s (value) USING '" + ReadOnlyOnFailureIndex.class.getName() + "'");
        execute("SELECT value FROM %s WHERE value = 1");
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 1, 1);
        ReadOnlyOnFailureIndex index = (ReadOnlyOnFailureIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);
        assertEquals(1, index.rowsInserted.size());

        // Upon rebuild, both reads and writes still go through
        getCurrentColumnFamilyStore().indexManager.rebuildIndexesBlocking(ImmutableSet.of(indexName));
        assertEquals(1, index.rowsInserted.size());
        execute("SELECT value FROM %s WHERE value = 1");
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 2, 1, 1);
        assertEquals(2, index.rowsInserted.size());
        dropIndex(format("DROP INDEX %s.%s", KEYSPACE, indexName));

        // On bad initial build writes are not forwarded to the index
        ReadOnlyOnFailureIndex.failInit = true;
        indexName = createIndexAsync("CREATE CUSTOM INDEX ON %s (value) USING '" + ReadOnlyOnFailureIndex.class.getName() + "'");
        index = (ReadOnlyOnFailureIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);
        waitForIndexBuilds(indexName);
        assertInvalidThrow(IndexNotAvailableException.class, "SELECT value FROM %s WHERE value = 1");
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 1, 1);
        assertEquals(0, index.rowsInserted.size());

        // Upon recovery, we can index data again
        index.reset();
        getCurrentColumnFamilyStore().indexManager.rebuildIndexesBlocking(ImmutableSet.of(indexName));
        assertEquals(2, index.rowsInserted.size());
        execute("SELECT value FROM %s WHERE value = 1");
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 2, 1, 1);
        assertEquals(3, index.rowsInserted.size());
        dropIndex(format("DROP INDEX %s.%s", KEYSPACE, indexName));
    }

    @Test  // A Bad init could leave an index only accepting writes
    public void testWriteOnlyIndex() throws Throwable
    {
        // On successful initialization both reads and writes go through
        createTable("CREATE TABLE %s (pk int, ck int, value int, PRIMARY KEY (pk, ck))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s (value) USING '" + WriteOnlyOnFailureIndex.class.getName() + "'");
        execute("SELECT value FROM %s WHERE value = 1");
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 1, 1);
        WriteOnlyOnFailureIndex index = (WriteOnlyOnFailureIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);
        assertEquals(1, index.rowsInserted.size());

        // Upon rebuild, both reads and writes still go through
        getCurrentColumnFamilyStore().indexManager.rebuildIndexesBlocking(ImmutableSet.of(indexName));
        assertEquals(1, index.rowsInserted.size());
        execute("SELECT value FROM %s WHERE value = 1");
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 2, 1, 1);
        assertEquals(2, index.rowsInserted.size());
        dropIndex(format("DROP INDEX %s.%s", KEYSPACE, indexName));

        // On bad initial build writes are forwarded to the index
        WriteOnlyOnFailureIndex.failInit = true;
        indexName = createIndexAsync("CREATE CUSTOM INDEX ON %s (value) USING '" + WriteOnlyOnFailureIndex.class.getName() + "'");
        index = (WriteOnlyOnFailureIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);
        waitForIndexBuilds(indexName);
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 1, 1);
        assertEquals(1, index.rowsInserted.size());
        assertInvalidThrow(IndexNotAvailableException.class, "SELECT value FROM %s WHERE value = 1");

        // Upon recovery, we can query data again
        index.reset();
        getCurrentColumnFamilyStore().indexManager.rebuildIndexesBlocking(ImmutableSet.of(indexName));
        assertEquals(2, index.rowsInserted.size());
        execute("SELECT value FROM %s WHERE value = 1");
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 2, 1, 1);
        assertEquals(3, index.rowsInserted.size());
        dropIndex(format("DROP INDEX %s.%s", KEYSPACE, indexName));
    }

    @Test
    public void droppingIndexInvalidatesPreparedStatements() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY ((a), b))");
        String indexName = createIndex("CREATE INDEX ON %s(c)");
        MD5Digest cqlId = prepareStatement("SELECT * FROM %s.%s WHERE c=?").statementId;

        assertNotNull(QueryProcessor.instance.getPrepared(cqlId));

        dropIndex("DROP INDEX %s." + indexName);

        assertNull(QueryProcessor.instance.getPrepared(cqlId));
    }

    // See CASSANDRA-11021
    @Test
    public void testIndexesOnNonStaticColumnsWhereSchemaIncludesStaticColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int static, d int, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX b_idx on %s(b)");
        createIndex("CREATE INDEX d_idx on %s(d)");

        execute("INSERT INTO %s (a, b, c ,d) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 1, 1, 1)");
        assertRows(execute("SELECT * FROM %s WHERE b = 0"), row(0, 0, 0, 0));
        assertRows(execute("SELECT * FROM %s WHERE d = 1"), row(1, 1, 1, 1));

        execute("UPDATE %s SET c = 2 WHERE a = 0");
        execute("UPDATE %s SET c = 3, d = 4 WHERE a = 1 AND b = 1");
        assertRows(execute("SELECT * FROM %s WHERE b = 0"), row(0, 0, 2, 0));
        assertRows(execute("SELECT * FROM %s WHERE d = 4"), row(1, 1, 3, 4));

        execute("DELETE FROM %s WHERE a = 0");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1");
        assertEmpty(execute("SELECT * FROM %s WHERE b = 0"));
        assertEmpty(execute("SELECT * FROM %s WHERE d = 3"));
    }

    @Test
    public void testWithEmptyRestrictionValueAndSecondaryIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY ((pk), c))");
        createIndex("CREATE INDEX on %s(c)");
        createIndex("CREATE INDEX on %s(v)");

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"));
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", bytes("foo123"), bytes("2"), bytes("1"));

        beforeAndAfterFlush(() -> {
            // Test clustering columns restrictions
            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c = text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) = (text_as_blob(''));"));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c IN (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) IN ((text_as_blob('')), (text_as_blob('1')));"),
                       row(bytes("foo123"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) >= (text_as_blob('')) AND v = text_as_blob('1') ALLOW FILTERING;"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("1")));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c <= text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) <= (text_as_blob('')) AND v = text_as_blob('1') ALLOW FILTERING;"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) < (text_as_blob('')) AND v = text_as_blob('1') ALLOW FILTERING;"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c < text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('') AND c < text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) > (text_as_blob('')) AND (c) < (text_as_blob('')) AND v = text_as_blob('1') ALLOW FILTERING;"));
        });

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"));

        beforeAndAfterFlush(() -> {

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c = text_as_blob('');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) = (text_as_blob(''));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c IN (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) IN ((text_as_blob('')), (text_as_blob('1')));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) >= (text_as_blob('')) AND v = text_as_blob('1') ALLOW FILTERING;"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c <= text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) <= (text_as_blob('')) AND v = text_as_blob('1') ALLOW FILTERING;"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1")));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c < text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) < (text_as_blob('')) AND v = text_as_blob('1') ALLOW FILTERING;"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('') AND c < text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) >= (text_as_blob('')) AND c < text_as_blob('') AND v = text_as_blob('1') ALLOW FILTERING;"));

            // Test restrictions on non-primary key value
            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND v = text_as_blob('');"));
        });

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                bytes("foo123"), bytes("3"), EMPTY_BYTE_BUFFER);

        beforeAndAfterFlush(() -> {

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND v = text_as_blob('');"),
                       row(bytes("foo123"), bytes("3"), EMPTY_BYTE_BUFFER));
        });
    }

    @Test
    public void testPartitionKeyWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY ((a, b)))");
        createIndex("CREATE INDEX ON %s (a);");
        createIndex("CREATE INDEX ON %s (b);");

        execute("INSERT INTO %s (a, b, c) VALUES (1,2,3)");
        execute("INSERT INTO %s (a, b, c) VALUES (2,3,4)");
        execute("INSERT INTO %s (a, b, c) VALUES (5,6,7)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE a = 1"),
                       row(1, 2, 3));
            assertRows(execute("SELECT * FROM %s WHERE b = 3"),
                       row(2, 3, 4));

        });
    }

    @Test
    public void testAllowFilteringOnPartitionKeyWithSecondaryIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, c1 int, c2 int, v int, " +
                    "PRIMARY KEY ((pk1, pk2), c1, c2))");
        createIndex("CREATE INDEX v_idx_1 ON %s (v);");

        for (int i = 1; i <= 5; i++)
        {
            for (int j = 1; j <= 2; j++)
            {
                execute("INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", j, 1, 1, 1, i);
                execute("INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", j, 1, 1, i, i);
                execute("INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", j, 1, i, i, i);
                execute("INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", j, i, i, i, i);
            }
        }

        beforeAndAfterFlush(() -> {
            assertEmpty(execute("SELECT * FROM %s WHERE pk1 = 1 AND  c1 > 0 AND c1 < 5 AND c2 = 1 AND v = 3 ALLOW FILTERING;"));

            assertRows(execute("SELECT * FROM %s WHERE pk1 = 1 AND  c1 > 0 AND c1 < 5 AND c2 = 3 AND v = 3 ALLOW FILTERING;"),
                       row(1, 3, 3, 3, 3),
                       row(1, 1, 1, 3, 3),
                       row(1, 1, 3, 3, 3));

            assertEmpty(execute("SELECT * FROM %s WHERE pk1 = 1 AND  c2 > 1 AND c2 < 5 AND v = 1 ALLOW FILTERING;"));

            assertRows(execute("SELECT * FROM %s WHERE pk1 = 1 AND  c1 > 1 AND c2 > 2 AND v = 3 ALLOW FILTERING;"),
                       row(1, 3, 3, 3, 3),
                       row(1, 1, 3, 3, 3));

            assertRows(execute("SELECT * FROM %s WHERE pk1 = 1 AND  pk2 > 1 AND c2 > 2 AND v = 3 ALLOW FILTERING;"),
                       row(1, 3, 3, 3, 3));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk2 > 1 AND  c1 IN(0,1,2) AND v <= 3 ALLOW FILTERING;"),
                                    row(1, 2, 2, 2, 2),
                                    row(2, 2, 2, 2, 2));

            assertRows(execute("SELECT * FROM %s WHERE pk1 >= 2 AND pk2 <=3 AND  c1 IN(0,1,2) AND c2 IN(0,1,2) AND v < 3  ALLOW FILTERING;"),
                       row(2, 2, 2, 2, 2),
                       row(2, 1, 1, 2, 2),
                       row(2, 1, 2, 2, 2));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE pk1 >= 1 AND pk2 <=3 AND  c1 IN(0,1,2) AND c2 IN(0,1,2) AND v = 3");
        });
    }

    @Test
    public void testAllowFilteringOnPartitionKeyWithIndexForContains() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v set<int>, PRIMARY KEY ((k1, k2)))");
        createIndex("CREATE INDEX ON %s(k2)");

        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 0, set(1, 2, 3));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 1, set(2, 3, 4));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 0, set(3, 4, 5));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 1, set(4, 5, 6));

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE k2 > ?", 0);

            assertRows(execute("SELECT * FROM %s WHERE k2 > ? ALLOW FILTERING", 0),
                       row(0, 1, set(2, 3, 4)),
                       row(1, 1, set(4, 5, 6)));

            assertRows(execute("SELECT * FROM %s WHERE k2 >= ? AND v CONTAINS ? ALLOW FILTERING", 1, 6),
                       row(1, 1, set(4, 5, 6)));

            assertEmpty(execute("SELECT * FROM %s WHERE k2 < ? AND v CONTAINS ? ALLOW FILTERING", 0, 7));
        });
    }

    @Test
    public void testIndexOnStaticColumnWithPartitionWithoutRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY(pk, c))");
        createIndex("CREATE INDEX ON %s (s)");

        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 1, 9, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 2, 9, 2);
        execute("INSERT INTO %s (pk, s) VALUES (?, ?)", 2, 9);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 3, 1, 9, 1);
        flush();

        assertRows(execute("SELECT * FROM %s WHERE s = ?", 9),
                   row(1, 1, 9, 1),
                   row(1, 2, 9, 2),
                   row(2, null, 9, null),
                   row(3, 1, 9, 1));

        execute("DELETE FROM %s WHERE pk = ?", 3);

        assertRows(execute("SELECT * FROM %s WHERE s = ?", 9),
                   row(1, 1, 9, 1),
                   row(1, 2, 9, 2),
                   row(2, null, 9, null));
    }

    @Test
    public void testIndexOnRegularColumnWithPartitionWithoutRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY(pk, c))");
        createIndex("CREATE INDEX ON %s (v)");

        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 1, 9, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 2, 9, 2);
        execute("INSERT INTO %s (pk, s) VALUES (?, ?)", 2, 9);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 3, 1, 9, 1);
        flush();

        execute("DELETE FROM %s WHERE pk = ? and c = ?", 3, 1);

        assertRows(execute("SELECT * FROM %s WHERE v = ?", 1),
                   row(1, 1, 9, 1));
    }

    @Test
    public void testIndexOnDurationColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, d duration)");
        assertInvalidMessage("Secondary indexes are not supported on duration columns",
                             "CREATE INDEX ON %s (d)");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<duration>)");
        assertInvalidMessage("Secondary indexes are not supported on collections containing durations",
                             "CREATE INDEX ON %s (l)");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<int, duration>)");
        assertInvalidMessage("Secondary indexes are not supported on collections containing durations",
                             "CREATE INDEX ON %s (m)");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, t tuple<int, duration>)");
        assertInvalidMessage("Secondary indexes are not supported on tuples containing durations",
                             "CREATE INDEX ON %s (t)");

        String udt = createType("CREATE TYPE %s (i int, d duration)");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, t " + udt + ")");
        assertInvalidMessage("Secondary indexes are not supported on UDTs containing durations",
                             "CREATE INDEX ON %s (t)");
    }

    @Test
    public void testIndexOnFrozenUDT() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<" + type + ">)");

        Object udt1 = userType("a", 1);
        Object udt2 = userType("a", 2);

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, udt1);
        String indexName = createIndex("CREATE INDEX ON %s (v)");

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 1, udt2);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 1, udt1);

        assertRows(execute("SELECT * FROM %s WHERE v = ?", udt1), row(1, udt1), row(0, udt1));
        assertEmpty(execute("SELECT * FROM %s WHERE v = ?", udt2));

        execute("DELETE FROM %s WHERE k = 0");
        assertRows(execute("SELECT * FROM %s WHERE v = ?", udt1), row(1, udt1));

        dropIndex("DROP INDEX %s." + indexName);
        assertInvalidMessage(format("Index '%s.%s' doesn't exist", KEYSPACE, indexName),
                             format("DROP INDEX %s.%s", KEYSPACE, indexName));
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE v = ?", udt1);
    }

    @Test
    public void testIndexOnFrozenCollectionOfUDT() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<set<frozen<" + type + ">>>)");

        Object udt1 = userType("a", 1);
        Object udt2 = userType("a", 2);

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 1, set(udt1, udt2));
        assertInvalidMessage("Frozen collections are immutable and must be fully indexed", "CREATE INDEX idx ON %s (keys(v))");
        assertInvalidMessage("Frozen collections are immutable and must be fully indexed", "CREATE INDEX idx ON %s (values(v))");
        String indexName = createIndex("CREATE INDEX ON %s (full(v))");

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 2, set(udt2));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE v CONTAINS ?", udt1);

        assertRows(execute("SELECT * FROM %s WHERE v = ?", set(udt1, udt2)), row(1, set(udt1, udt2)));
        assertRows(execute("SELECT * FROM %s WHERE v = ?", set(udt2)), row(2, set(udt2)));

        execute("DELETE FROM %s WHERE k = 2");
        assertEmpty(execute("SELECT * FROM %s WHERE v = ?", set(udt2)));

        dropIndex("DROP INDEX %s." + indexName);
        assertInvalidMessage(format("Index '%s.%s' doesn't exist", KEYSPACE, indexName),
                             format("DROP INDEX %s.%s", KEYSPACE, indexName));
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE v CONTAINS ?", udt1);
    }

    @Test
    public void testIndexOnNonFrozenCollectionOfFrozenUDT() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<frozen<" + type + ">>)");

        Object udt1 = userType("a", 1);
        Object udt2 = userType("a", 2);

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 1, set(udt1));
        assertInvalidMessage("Cannot create index on keys of column v with non-map type",
                             "CREATE INDEX ON %s (keys(v))");
        assertInvalidMessage("full() indexes can only be created on frozen collections",
                             "CREATE INDEX ON %s (full(v))");
        String indexName = createIndex("CREATE INDEX ON %s (values(v))");

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 2, set(udt2));
        execute("UPDATE %s SET v = v + ? WHERE k = ?", set(udt2), 1);

        assertRows(execute("SELECT * FROM %s WHERE v CONTAINS ?", udt1), row(1, set(udt1, udt2)));
        assertRows(execute("SELECT * FROM %s WHERE v CONTAINS ?", udt2), row(1, set(udt1, udt2)), row(2, set(udt2)));

        execute("DELETE FROM %s WHERE k = 1");
        assertEmpty(execute("SELECT * FROM %s WHERE v CONTAINS ?", udt1));
        assertRows(execute("SELECT * FROM %s WHERE v CONTAINS ?", udt2), row(2, set(udt2)));

        dropIndex("DROP INDEX %s." + indexName);
        assertInvalidMessage(format("Index '%s.%s' doesn't exist", KEYSPACE, indexName),
                             format("DROP INDEX %s.%s", KEYSPACE, indexName));
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE v CONTAINS ?", udt1);
    }

    @Test
    public void testIndexOnNonFrozenUDT() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ")");
        assertInvalidMessage("Cannot create index on non-frozen UDT column v", "CREATE INDEX ON %s (v)");
        assertInvalidMessage("Cannot create keys() index on v. Non-collection columns only support simple indexes", "CREATE INDEX ON %s (keys(v))");
        assertInvalidMessage("Cannot create values() index on v. Non-collection columns only support simple indexes", "CREATE INDEX ON %s (values(v))");
        assertInvalidMessage("full() indexes can only be created on frozen collections", "CREATE INDEX ON %s (full(v))");
    }

    @Test
    public void testIndexOnPartitionKeyInsertExpiringColumn() throws Throwable
    {
        testIndexOnPartitionKeyInsertExpiringColumn(false);
    }

    @Test
    public void testIndexOnPartitionKeyInsertExpiringColumnWithFlush() throws Throwable
    {
        testIndexOnPartitionKeyInsertExpiringColumn(true);
    }

    private void testIndexOnPartitionKeyInsertExpiringColumn(boolean flushBeforeUpdate) throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, a int, b int, PRIMARY KEY ((k1, k2)))");
        createIndex("CREATE INDEX ON %s(k1)");
        execute("INSERT INTO %s (k1, k2, a, b) VALUES (1, 2, 3, 4)");
        assertRows(execute("SELECT * FROM %s WHERE k1 = 1"), row(1, 2, 3, 4));

        if (flushBeforeUpdate)
            flush();

        execute("UPDATE %s USING TTL 1 SET b = 10 WHERE k1 = 1 AND k2 = 2");
        Thread.sleep(1000);
        assertRows(execute("SELECT * FROM %s WHERE k1 = 1"), row(1, 2, 3, null));
    }

    @Test
    public void testIndexOnPartitionKeyOverridingExpiredRow() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY ((k1, k2)))");
        createIndex("CREATE INDEX ON %s(k1)");

        execute("UPDATE %s USING TTL 1 SET v = 3 WHERE k1 = 1 AND k2 = 2");
        Thread.sleep(1500);

        assertEmpty(execute("SELECT * FROM %s WHERE k1 = 1"));

        execute("UPDATE %s SET v = 3 WHERE k1 = 1 AND k2 = 2");
        assertRows(execute("SELECT * FROM %s WHERE k1 = 1"), row(1, 2, 3));
    }

    @Test
    public void testIndexOnPartitionKeyOverridingDeletedRow() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c int, v int, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE INDEX ON %s(k1)");

        execute("INSERT INTO %s(k1, k2, c, v) VALUES (1, 2, 3, 4)");
        execute("DELETE FROM %s WHERE k1 = 1 AND k2 = 2 AND c = 3");
        execute("UPDATE %s SET v = 4 WHERE k1 = 1 AND k2 = 2 AND c = 3");

        assertRows(execute("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c = 3"), row(1, 2, 3, 4));
        assertRows(execute("SELECT * FROM %s WHERE k1 = 1"), row(1, 2, 3, 4));
    }

    @Test
    public void testIndexOnClusteringKeyInsertExpiringColumn() throws Throwable
    {
        testIndexOnClusteringKeyInsertExpiringColumn(false);
    }

    @Test
    public void testIndexOnClusteringKeyInsertExpiringColumnWithFlush() throws Throwable
    {
        testIndexOnClusteringKeyInsertExpiringColumn(true);
    }

    private void testIndexOnClusteringKeyInsertExpiringColumn(boolean flushBeforeUpdate) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(ck)");
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (1, 2, 3, 4)");
        assertRows(execute("SELECT * FROM %s WHERE ck = 2"), row(1, 2, 3, 4));

        if (flushBeforeUpdate)
            flush();

        execute("UPDATE %s USING TTL 1 SET b = 10 WHERE pk = 1 AND ck = 2");
        Thread.sleep(1000);
        assertRows(execute("SELECT * FROM %s WHERE ck = 2"), row(1, 2, 3, null));
    }

    @Test
    public void testIndexOnClusteringKeyOverridingExpiredRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(ck)");

        execute("UPDATE %s USING TTL 1 SET v = 3 WHERE pk = 1 AND ck = 2");
        Thread.sleep(1000);

        assertEmpty(execute("SELECT * FROM %s WHERE ck = 2"));

        execute("UPDATE %s SET v = 3 WHERE pk = 1 AND ck = 2");
        assertRows(execute("SELECT * FROM %s WHERE ck = 2"), row(1, 2, 3));
    }

    @Test
    public void testIndexOnClusteringKeyOverridingDeletedRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(ck)");

        execute("INSERT INTO %s(pk, ck, v) VALUES (1, 2, 3)");
        execute("DELETE FROM %s WHERE pk = 1 AND ck = 2");
        execute("UPDATE %s SET v = 3 WHERE pk = 1 AND ck = 2");

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck = 2"), row(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE ck = 2"), row(1, 2, 3));
    }

    @Test
    public void testIndexOnRegularColumnInsertExpiringColumn() throws Throwable
    {
        testIndexOnRegularColumnInsertExpiringColumn(false);
    }

    @Test
    public void testIndexOnRegularColumnInsertExpiringColumnWithFlush() throws Throwable
    {
        testIndexOnRegularColumnInsertExpiringColumn(true);
    }

    private void testIndexOnRegularColumnInsertExpiringColumn(boolean flushBeforeUpdate) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(a)");
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (1, 2, 3, 4)");
        assertRows(execute("SELECT * FROM %s WHERE a = 3"), row(1, 2, 3, 4));

        if (flushBeforeUpdate)
            flush();

        execute("UPDATE %s USING TTL 1 SET b = 10 WHERE pk = 1 AND ck = 2");
        Thread.sleep(1000);
        assertRows(execute("SELECT * FROM %s WHERE a = 3"), row(1, 2, 3, null));

        if (flushBeforeUpdate)
            flush();

        execute("UPDATE %s USING TTL 1 SET a = 5 WHERE pk = 1 AND ck = 2");
        Thread.sleep(1000);
        assertEmpty(execute("SELECT * FROM %s WHERE a = 3"));
        assertEmpty(execute("SELECT * FROM %s WHERE a = 5"));
    }

    @Test
    public void testIndexOnRegularColumnOverridingExpiredRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(v)");

        execute("UPDATE %s USING TTL 1 SET v = 3 WHERE pk = 1 AND ck = 2");
        Thread.sleep(1000);

        assertEmpty(execute("SELECT * FROM %s WHERE v = 3"));

        execute("UPDATE %s SET v = 3 WHERE pk = 1 AND ck = 2");
        assertRows(execute("SELECT * FROM %s WHERE v = 3"), row(1, 2, 3));
    }

    @Test
    public void testIndexOnRegularColumnOverridingDeletedRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(v)");

        execute("INSERT INTO %s(pk, ck, v) VALUES (1, 2, 3)");
        execute("DELETE FROM %s WHERE pk=1 AND ck=2");
        execute("UPDATE %s SET v=3 WHERE pk=1 AND ck=2");

        assertRows(execute("SELECT * FROM %s WHERE pk=1 AND ck=2"), row(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE v=3"), row(1, 2, 3));
    }

    private ResultMessage.Prepared prepareStatement(String cql)
    {
        return QueryProcessor.instance.prepare(format(cql, KEYSPACE, currentTable()),
                                               ClientState.forInternalCalls());
    }

    private void validateCell(Cell<?> cell, ColumnMetadata def, ByteBuffer val, long timestamp)
    {
        assertNotNull(cell);
        assertEquals(0, def.type.compare(cell.buffer(), val));
        assertEquals(timestamp, cell.timestamp());
    }

    private static void assertColumnValue(int expected, String name, Row row, TableMetadata cfm)
    {
        ColumnMetadata col = cfm.getColumn(new ColumnIdentifier(name, true));
        AbstractType<?> type = col.type;
        assertEquals(expected, type.compose(row.getCell(col).buffer()));
    }

    /**
     * <code>CassandraIndex</code> that blocks during the initialization.
     */
    public static class IndexBlockingOnInitialization extends CustomCassandraIndex
    {
        private final CountDownLatch latch = new CountDownLatch(1);

        public IndexBlockingOnInitialization(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
        {
            super(baseCfs, indexDef);
        }

        @Override
        public Callable<?> getInitializationTask()
        {
            return () -> {
                latch.await();
                return null;
            };
        }

        @Override
        public Callable<?> getInvalidateTask()
        {
            latch.countDown();
            return super.getInvalidateTask();
        }
    }

    /**
     * {@code StubIndex} that only supports some load. Could be intentional or a result of a bad init.
     */
    public static class LoadTypeConstrainedIndex extends StubIndex
    {
        static volatile boolean failInit = false;
        final LoadType supportedLoadOnFailure;

        LoadTypeConstrainedIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef, LoadType supportedLoadOnFailure)
        {
            super(baseCfs, indexDef);
            this.supportedLoadOnFailure = supportedLoadOnFailure;
        }

        @Override
        public LoadType getSupportedLoadTypeOnFailure(boolean isInitialBuild)
        {
            return supportedLoadOnFailure;
        }

        @Override
        public void reset()
        {
            super.reset();
            failInit = false;
        }

        @Override
        public Callable<?> getInitializationTask()
        {
            if (failInit)
                return () -> {throw new IllegalStateException("Index is configured to fail.");};

            return null;
        }

        public boolean shouldBuildBlocking()
        {
            return true;
        }
    }

    public static class ReadOnlyOnFailureIndex extends LoadTypeConstrainedIndex
    {
        public ReadOnlyOnFailureIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
        {
            super(baseCfs, indexDef, LoadType.READ);
        }
    }

    public static class WriteOnlyOnFailureIndex extends LoadTypeConstrainedIndex
    {
        public WriteOnlyOnFailureIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
        {
            super(baseCfs, indexDef, LoadType.WRITE);
        }
    }
}
