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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SecondaryIndexTest extends CQLTester
{
    private static final int TOO_BIG = 1024 * 65;

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
     * @param indexName the index name
     * @param addKeyspaceOnDrop add the keyspace name in the drop statement
     * @throws Throwable if an error occurs
     */
    private void testCreateAndDropIndex(String indexName, boolean addKeyspaceOnDrop) throws Throwable
    {
        execute("USE system");
        assertInvalidMessage("Index '" + removeQuotes(indexName.toLowerCase(Locale.US)) + "' could not be found", "DROP INDEX " + indexName + ";");

        createTable("CREATE TABLE %s (a int primary key, b int);");
        createIndex("CREATE INDEX " + indexName + " ON %s(b);");
        createIndex("CREATE INDEX IF NOT EXISTS " + indexName + " ON %s(b);");

        assertInvalidMessage("Index already exists", "CREATE INDEX " + indexName + " ON %s(b)");

        execute("INSERT INTO %s (a, b) values (?, ?);", 0, 0);
        execute("INSERT INTO %s (a, b) values (?, ?);", 1, 1);
        execute("INSERT INTO %s (a, b) values (?, ?);", 2, 2);
        execute("INSERT INTO %s (a, b) values (?, ?);", 3, 1);

        assertRows(execute("SELECT * FROM %s where b = ?", 1), row(1, 1), row(3, 1));
        assertInvalidMessage("Index '" + removeQuotes(indexName.toLowerCase(Locale.US)) + "' could not be found in any of the tables of keyspace 'system'", "DROP INDEX " + indexName);

        if (addKeyspaceOnDrop)
        {
            dropIndex("DROP INDEX " + KEYSPACE + "." + indexName);
        }
        else
        {
            execute("USE " + KEYSPACE);
            dropIndex("DROP INDEX " + indexName);
        }

        assertInvalidMessage("No secondary indexes on the restricted columns support the provided operators",
                             "SELECT * FROM %s where b = ?", 1);
        dropIndex("DROP INDEX IF EXISTS " + indexName);
        assertInvalidMessage("Index '" + removeQuotes(indexName.toLowerCase(Locale.US)) + "' could not be found", "DROP INDEX " + indexName);
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

        assertEmpty(execute("SELECT firstname FROM %s WHERE userid = ? AND age = 33", id1));

        assertRows(execute("SELECT firstname FROM %s WHERE userid = ? AND age = 33", id2),
                   row("Samwise"));
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

        assertRows(execute("SELECT id FROM %s WHERE birth_year = 42"),
                   row("Tom"),
                   row("Bob"));

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

        assertRows(execute("SELECT * FROM %s WHERE setid = 0 AND row < 1 ALLOW FILTERING"),
                   row(0, 0, 0));
    }

    /**
     * Check for unknown compression parameters options (#4266),
     * migrated from cql_tests.py:TestCQL.compression_option_validation_test()
     */
    @Test
    public void testUnknownCompressionOptions() throws Throwable
    {
        String tableName = createTableName();
        assertInvalidThrow(SyntaxException.class, String.format(
                                                               "CREATE TABLE %s (key varchar PRIMARY KEY, password varchar, gender varchar) WITH compression_parameters:sstable_compressor = 'DeflateCompressor'", tableName));


        assertInvalidThrow(ConfigurationException.class, String.format(
                                                                      "CREATE TABLE %s (key varchar PRIMARY KEY, password varchar, gender varchar) WITH compression = { 'sstable_compressor': 'DeflateCompressor' }", tableName));
    }

    /**
     * Check one can use arbitrary name for datacenter when creating keyspace (#4278),
     * migrated from cql_tests.py:TestCQL.keyspace_creation_options_test()
     */
    @Test
    public void testDataCenterName() throws Throwable
    {
       execute("CREATE KEYSPACE Foo WITH replication = { 'class' : 'NetworkTopologyStrategy', 'us-east' : 1, 'us-west' : 1 };");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.indexes_composite_test()
     */
    @Test
    public void testIndexOnComposite() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (blog_id int, timestamp int, author text, content text, PRIMARY KEY (blog_id, timestamp))");

        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 0, "bob", "1st post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 1, "tom", "2nd post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 2, "bob", "3rd post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 3, "tom", "4th post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 0, "bob", "5th post");

        createIndex("CREATE INDEX authoridx ON %s (author)");

        assertTrue(waitForIndex(keyspace(), tableName, "authoridx"));

        assertRows(execute("SELECT blog_id, timestamp FROM %s WHERE author = 'bob'"),
                   row(1, 0),
                   row(0, 0),
                   row(0, 2));

        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 1, "tom", "6th post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 2, "tom", "7th post");
        execute("INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 3, "bob", "8th post");

        assertRows(execute("SELECT blog_id, timestamp FROM %s WHERE author = 'bob'"),
                   row(1, 0),
                   row(1, 3),
                   row(0, 0),
                   row(0, 2));

        execute("DELETE FROM %s WHERE blog_id = 0 AND timestamp = 2");

        assertRows(execute("SELECT blog_id, timestamp FROM %s WHERE author = 'bob'"),
                   row(1, 0),
                   row(1, 3),
                   row(0, 0));
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

        assertRows(execute("SELECT value FROM %s WHERE pk0 = 2"),
                   row(1));

        assertRows(execute("SELECT value FROM %s WHERE ck0 = 0"),
                   row(3));

        assertRows(execute("SELECT value FROM %s WHERE pk0 = 3 AND pk1 = 4 AND ck1 = 0"),
                   row(2));

        assertRows(execute("SELECT value FROM %s WHERE pk0 = 5 AND pk1 = 0 AND ck0 = 1 AND ck2 = 3 ALLOW FILTERING"),
                   row(4));
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

        assertRows(execute("select * from %s where severity = 3 and interval = 't' and seq =1"),
                   row("t", 1, 4, 3));
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
        assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS KEY 'a'"), row(1, 1), row(0, 0), row(0, 1));
        assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS KEY 'a'"), row(0, 0), row(0, 1));
        assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS KEY 'c'"), row(0, 2));
        assertEmpty(execute("SELECT k, v FROM %s  WHERE m CONTAINS KEY 'd'"));

        // we're not allowed to create a value index if we already have a key one
        assertInvalid("CREATE INDEX ON %s(m)");
    }

    /**
     * Test for #6950 bug,
     * migrated from cql_tests.py:TestCQL.key_index_with_reverse_clustering()
     */
    @Test
    public void testIndexOnKeyWithReverseClustering() throws Throwable
    {
        createTable(" CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY ((k1, k2), v) ) WITH CLUSTERING ORDER BY (v DESC)");

        createIndex("CREATE INDEX ON %s (k2)");

        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 0, 1)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 1, 2)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 0, 3)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (1, 0, 4)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (1, 1, 5)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (2, 0, 7)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (2, 1, 8)");
        execute("INSERT INTO %s (k1, k2, v) VALUES (3, 0, 1)");

        assertRows(execute("SELECT * FROM %s WHERE k2 = 0 AND v >= 2 ALLOW FILTERING"),
                   row(2, 0, 7),
                   row(0, 0, 3),
                   row(1, 0, 4));
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

        assertRows(execute("select count(*) from %s where app_name='foo' and account='bar' and last_access > 4 allow filtering"), row(0L));

        execute("insert into %s (username, session_id, app_name, account, last_access, created_on) values ('toto', 'foo', 'foo', 'bar', 12, 13)");

        assertRows(execute("select count(*) from %s where app_name='foo' and account='bar' and last_access > 4 allow filtering"), row(1L));
    }

    /**
     * Test for CASSANDRA-5732, Can not query secondary index
     * migrated from cql_tests.py:TestCQL.bug_5732_test(),
     * which was executing with a row cache size of 100 MB
     * and restarting the node, here we just cleanup the cache.
     */
    @Test
    public void testCanQuerySecondaryIndex() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int,)");

        execute("ALTER TABLE %s WITH CACHING='ALL'");
        execute("INSERT INTO %s (k,v) VALUES (0,0)");
        execute("INSERT INTO %s (k,v) VALUES (1,1)");

        createIndex("CREATE INDEX testindex on %s (v)");
        assertTrue(waitForIndex(keyspace(), tableName, "testindex"));

        assertRows(execute("SELECT k FROM %s WHERE v = 0"), row(0));
        cleanupCache();
        assertRows(execute("SELECT k FROM %s WHERE v = 0"), row(0));
    }

    // CASSANDRA-8280/8081
    // reject updates with indexed values where value > 64k
    @Test
    public void testIndexOnCompositeValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(c)");
        failInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnClusteringColumnInsertPartitionKeyAndClusteringsOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a blob, b blob, c blob, d int, PRIMARY KEY (a, b, c))");
        createIndex("CREATE INDEX ON %s(b)");

        // CompositeIndexOnClusteringKey creates index entries composed of the
        // PK plus all of the non-indexed clustering columns from the primary row
        // so we should reject where len(a) + len(c) > 65560 as this will form the
        // total clustering in the index table
        ByteBuffer a = ByteBuffer.allocate(100);
        ByteBuffer b = ByteBuffer.allocate(10);
        ByteBuffer c = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT - 99);

        failInsert("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, 0)", a, b, c);
    }

    @Test
    public void testCompactTableWithValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b blob, PRIMARY KEY (a)) WITH COMPACT STORAGE");
        createIndex("CREATE INDEX ON %s(b)");
        failInsert("INSERT INTO %s (a, b) VALUES (0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnCollectionValueInsertPartitionKeyAndCollectionKeyOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a blob , b map<blob, int>, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(b)");

        // A collection key > 64k by itself will be rejected from
        // the primary table.
        // To test index validation we need to ensure that
        // len(b) < 64k, but len(a) + len(b) > 64k as that will
        // form the clustering in the index table
        ByteBuffer a = ByteBuffer.allocate(100);
        ByteBuffer b = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT - 100);

        failInsert("UPDATE %s SET b[?] = 0 WHERE a = ?", b, a);
    }

    @Test
    public void testIndexOnCollectionKeyInsertPartitionKeyAndClusteringOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a blob, b blob, c map<blob, int>, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s(KEYS(c))");

        // Basically the same as the case with non-collection clustering
        // CompositeIndexOnCollectionKeyy creates index entries composed of the
        // PK plus all of the clustering columns from the primary row, except the
        // collection element - which becomes the partition key in the index table
        ByteBuffer a = ByteBuffer.allocate(100);
        ByteBuffer b = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT - 100);
        ByteBuffer c = ByteBuffer.allocate(10);

        failInsert("UPDATE %s SET c[?] = 0 WHERE a = ? and b = ?", c, a, b);
    }

    @Test
    public void testIndexOnPartitionKeyInsertValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY ((a, b)))");
        createIndex("CREATE INDEX ON %s(a)");
        succeedInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnClusteringColumnInsertValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s(b)");
        succeedInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnFullCollectionEntryInsertCollectionValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b frozen<map<int, blob>>, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(full(b))");
        Map<Integer, ByteBuffer> map = new HashMap();
        map.put(0, ByteBuffer.allocate(1024 * 65));
        failInsert("INSERT INTO %s (a, b) VALUES (0, ?)", map);
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

    /**
     * Migrated from cql_tests.py:TestCQL.invalid_clustering_indexing_test()
     */
    @Test
    public void testIndexesOnClusteringInvalid() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE");
        assertInvalid("CREATE INDEX ON %s (a)");
        assertInvalid("CREATE INDEX ON %s (b)");

        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        assertInvalid("CREATE INDEX ON %s (a)");
        assertInvalid("CREATE INDEX ON %s (b)");
        assertInvalid("CREATE INDEX ON %s (c)");

        createTable("CREATE TABLE %s (a int, b int, c int static , PRIMARY KEY (a, b))");
        assertInvalid("CREATE INDEX ON %s (c)");
    }

}
