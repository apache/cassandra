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
package org.apache.cassandra.cql3.validation.operations;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.exceptions.InvalidRequestException;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Test column ranges and ordering with static column in table
 */
public class SelectTest extends CQLTester
{

    @Test
    public void testSingleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c))");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p2"),
            row("p2", null, "sv2", null)
        );

        // Ascending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p2"),
            row("p2", null, "sv2", null)
        );

        // Descending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p2"),
            row("p2", null, "sv2", null)
        );

        // No order with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k0"));

        // Ascending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k0"));

        // Descending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k0"));

        // IN

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?)", "p1", "k1", "k2"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c ASC", "p1", "k1", "k2"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c DESC", "p1", "k1", "k2"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );
    }

    @Test
    public void testSingleClusteringReversed() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p2"),
            row("p2", null, "sv2", null)
        );

        // Ascending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p2"),
            row("p2", null, "sv2", null)
        );

        // Descending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p2"),
            row("p2", null, "sv2", null)
        );

        // No order with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k0"));

        // Ascending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k0"));

        // Descending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k0"));

        // IN

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?)", "p1", "k1", "k2"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c ASC", "p1", "k1", "k2"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c DESC", "p1", "k1", "k2"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );
    }

    /**
     * Check query with KEY IN clause
     * migrated from cql_tests.py:TestCQL.select_key_in_test()
     */
    @Test
    public void testSelectKeyIn() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int)");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, 'Frodo', 'Baggins', 32)", id1);
        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, 'Samwise', 'Gamgee', 33)", id2);

        assertRowCount(execute("SELECT firstname, lastname FROM %s WHERE userid IN (?, ?)", id1, id2), 2);
    }

    /**
     * Check query with KEY IN clause for wide row tables
     * migrated from cql_tests.py:TestCQL.in_clause_wide_rows_test()
     */
    @Test
    public void testSelectKeyInForWideRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c IN (5, 2, 8)"),
                   row(2), row(5), row(8));

        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, ?, ?)", i, i);

        assertEmpty(execute("SELECT v FROM %s WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3"));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c1 = 0 AND c2 IN (5, 2, 8)"),
                   row(2), row(5), row(8));
    }

    /**
     * Check SELECT respects inclusive and exclusive bounds
     * migrated from cql_tests.py:TestCQL.exclusive_slice_test()
     */
    @Test
    public void testSelectBounds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);

        assertRowCount(execute("SELECT v FROM %s WHERE k = 0"), 10);

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c <= 6"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c <= 6"),
                   row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c < 6"),
                   row(2), row(3), row(4), row(5));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c < 6"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c <= 6 LIMIT 2"),
                   row(3), row(4));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c < 6 ORDER BY c DESC LIMIT 2"),
                   row(5), row(4));
    }

    @Test
    public void testSetContains() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories set<text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, set("lmn"));

        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "xyz", "lmn"));

        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS ?", "lmn"),
                   row("test", 5, set("lmn"))
        );

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "lmn"),
                   row("test", 5, set("lmn"))
        );

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, "lmn"),
                   row("test", 5, set("lmn"))
        );

        assertInvalidMessage("Unsupported null value for column categories",
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, null);

        assertInvalidMessage("Unsupported unset value for column categories",
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, unset());

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS ?", "xyz", "lmn", "notPresent");
        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS ? ALLOW FILTERING", "xyz", "lmn", "notPresent"));
    }

    @Test
    public void testListContains() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories list<text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, list("lmn"));

        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "xyz", "lmn"));

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?;", "test", "lmn"),
                   row("test", 5, list("lmn"))
        );

        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS ?", "lmn"),
                   row("test", 5, list("lmn"))
        );

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?;", "test", 5, "lmn"),
                   row("test", 5, list("lmn"))
        );

        assertInvalidMessage("Unsupported null value for column categories",
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, null);

        assertInvalidMessage("Unsupported unset value for column categories",
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, unset());

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ?",
                             "test", 5, "lmn", "notPresent");
        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ? ALLOW FILTERING",
                            "test", 5, "lmn", "notPresent"));
    }

    @Test
    public void testListContainsWithFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (e int PRIMARY KEY, f list<text>, s int)");
        createIndex("CREATE INDEX ON %s(f)");
        for(int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (e, f, s) VALUES (?, ?, ?)", i, list("Dubai"), 4);
        }
        for(int i = 3; i < 5; i++)
        {
            execute("INSERT INTO %s (e, f, s) VALUES (?, ?, ?)", i, list("Dubai"), 3);
        }
        assertRows(execute("SELECT * FROM %s WHERE f CONTAINS ? AND s=? allow filtering", "Dubai", 3),
                   row(4, list("Dubai"), 3),
                   row(3, list("Dubai"), 3));
    }

    @Test
    public void testMapKeyContains() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(keys(categories))");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));

        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "xyz", "lmn"));

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "test", "lmn"),
                   row("test", 5, map("lmn", "foo"))
        );
        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS KEY ?", "lmn"),
                   row("test", 5, map("lmn", "foo"))
        );

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ?", "test", 5, "lmn"),
                   row("test", 5, map("lmn", "foo"))
        );

        assertInvalidMessage("Unsupported null value for column categories",
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ?", "test", 5, null);

        assertInvalidMessage("Unsupported unset value for column categories",
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ?", "test", 5, unset());

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ? AND categories CONTAINS KEY ?",
                             "test", 5, "lmn", "notPresent");
        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ? AND categories CONTAINS KEY ? ALLOW FILTERING",
                            "test", 5, "lmn", "notPresent"));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ? AND categories CONTAINS ?",
                             "test", 5, "lmn", "foo");
    }

    @Test
    public void testMapValueContains() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));

        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "xyz", "foo"));

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "foo"),
                   row("test", 5, map("lmn", "foo"))
        );

        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS ?", "foo"),
                   row("test", 5, map("lmn", "foo"))
        );

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, "foo"),
                   row("test", 5, map("lmn", "foo"))
        );

        assertInvalidMessage("Unsupported null value for column categories",
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, null);

        assertInvalidMessage("Unsupported unset value for column categories",
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, unset());

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ?"
                            , "test", 5, "foo", "notPresent");

        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ? ALLOW FILTERING"
                           , "test", 5, "foo", "notPresent"));
    }

    // See CASSANDRA-7525
    @Test
    public void testQueryMultipleIndexTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");

        // create an index on
        createIndex("CREATE INDEX id_index ON %s(id)");
        createIndex("CREATE INDEX categories_values_index ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));

        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS ? AND id = ? ALLOW FILTERING", "foo", 5),
                   row("test", 5, map("lmn", "foo"))
        );

        assertRows(
                  execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND id = ? ALLOW FILTERING", "test", "foo", 5),
                  row("test", 5, map("lmn", "foo"))
        );
    }

    // See CASSANDRA-8033
    @Test
    public void testFilterForContains() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v set<int>, PRIMARY KEY ((k1, k2)))");
        createIndex("CREATE INDEX ON %s(k2)");

        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 0, set(1, 2, 3));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 1, set(2, 3, 4));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 0, set(3, 4, 5));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 1, set(4, 5, 6));

        assertRows(execute("SELECT * FROM %s WHERE k2 = ?", 1),
                   row(0, 1, set(2, 3, 4)),
                   row(1, 1, set(4, 5, 6))
        );

        assertRows(execute("SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 6),
                   row(1, 1, set(4, 5, 6))
        );

        assertEmpty(execute("SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 7));
    }

    // See CASSANDRA-8073
    @Test
    public void testIndexLookupWithClusteringPrefix() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d set<int>, PRIMARY KEY (a, b, c))");
        createIndex("CREATE INDEX ON %s(d)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, set(1, 2, 3));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, set(3, 4, 5));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, set(1, 2, 3));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, set(3, 4, 5));

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND d CONTAINS ?", 0, 1, 3),
                   row(0, 1, 0, set(1, 2, 3)),
                   row(0, 1, 1, set(3, 4, 5))
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND d CONTAINS ?", 0, 1, 2),
                   row(0, 1, 0, set(1, 2, 3))
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND d CONTAINS ?", 0, 1, 5),
                   row(0, 1, 1, set(3, 4, 5))
        );
    }

    @Test
    public void testContainsKeyAndContainsWithIndexOnMapKey() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(keys(categories))");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));
        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 6, map("lmn", "foo2"));

        assertInvalidMessage("Predicates on non-primary-key columns (categories) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "foo");

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "test", "lmn"),
                   row("test", 5, map("lmn", "foo")),
                   row("test", 6, map("lmn", "foo2")));
        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ? AND categories CONTAINS ? ALLOW FILTERING",
                           "test", "lmn", "foo"),
                   row("test", 5, map("lmn", "foo")));
        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS KEY ? ALLOW FILTERING",
                           "test", "foo", "lmn"),
                   row("test", 5, map("lmn", "foo")));
    }

    @Test
    public void testContainsKeyAndContainsWithIndexOnMapValue() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));
        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 6, map("lmn2", "foo"));

        assertInvalidMessage("Predicates on non-primary-key columns (categories) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "test", "lmn");

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "foo"),
                   row("test", 5, map("lmn", "foo")),
                   row("test", 6, map("lmn2", "foo")));
        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ? AND categories CONTAINS ? ALLOW FILTERING",
                           "test", "lmn", "foo"),
                   row("test", 5, map("lmn", "foo")));
        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS KEY ? ALLOW FILTERING",
                           "test", "foo", "lmn"),
                   row("test", 5, map("lmn", "foo")));
    }

    /**
     * Test token ranges
     * migrated from cql_tests.py:TestCQL.token_range_test()
     */
    @Test
    public void testTokenRange() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int PRIMARY KEY, c int, v int)");

        int c = 100;
        for (int i = 0; i < c; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, i, i);

        Object[][] res = getRows(execute("SELECT k FROM %s"));
        assertEquals(c, res.length);

        Object[] inOrder = new Object[res.length];
        for (int i = 0; i < res.length; i++)
            inOrder[i] = res[i][0];

        Long min_token = Long.MIN_VALUE;

        res = getRows(execute(String.format("SELECT k FROM %s.%s WHERE token(k) >= %d",
                                            keyspace(), currentTable(), min_token)));
        assertEquals(c, res.length);

        res = getRows(execute(String.format("SELECT k FROM %s.%s WHERE token(k) >= token(%d) AND token(k) < token(%d)",
                                            keyspace(), currentTable(), inOrder[32], inOrder[65])));

        for (int i = 32; i < 65; i++)
            Assert.assertEquals(inOrder[i], res[i - 32][0]);
    }

    /**
     * Test select count
     * migrated from cql_tests.py:TestCQL.count_test()
     */
    @Test
    public void testSelectCount() throws Throwable
    {
        createTable(" CREATE TABLE %s (kind text, time int, value1 int, value2 int, PRIMARY KEY(kind, time))");

        execute("INSERT INTO %s (kind, time, value1, value2) VALUES ('ev1', ?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (kind, time, value1, value2) VALUES ('ev1', ?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (kind, time, value1) VALUES ('ev1', ?, ?)", 2, 2);
        execute("INSERT INTO %s (kind, time, value1, value2) VALUES ('ev1', ?, ?, ?)", 3, 3, 3);
        execute("INSERT INTO %s (kind, time, value1) VALUES ('ev1', ?, ?)", 4, 4);
        execute("INSERT INTO %s (kind, time, value1, value2) VALUES ('ev2', 0, 0, 0)");

        assertRows(execute("SELECT COUNT(*) FROM %s WHERE kind = 'ev1'"),
                   row(5L));

        assertRows(execute("SELECT COUNT(1) FROM %s WHERE kind IN ('ev1', 'ev2') AND time=0"),
                   row(2L));
    }

    /**
     * Range test query from #4372
     * migrated from cql_tests.py:TestCQL.range_query_test()
     */
    @Test
    public void testRangeQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, f text, PRIMARY KEY (a, b, c, d, e) )");

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 2, '2')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, '1')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 2, 1, '1')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 3, '3')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 5, '5')");

        assertRows(execute("SELECT a, b, c, d, e, f FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 AND e >= 2"),
                   row(1, 1, 1, 1, 2, "2"),
                   row(1, 1, 1, 1, 3, "3"),
                   row(1, 1, 1, 1, 5, "5"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.composite_row_key_test()
     */
    @Test
    public void testCompositeRowKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c int, v int, PRIMARY KEY ((k1, k2), c))");

        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (k1, k2, c, v) VALUES (?, ?, ?, ?)", 0, i, i, i);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 2, 2, 2),
                   row(0, 3, 3, 3),
                   row(0, 0, 0, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE k1 = 0 and k2 IN (1, 3)"),
                   row(0, 1, 1, 1),
                   row(0, 3, 3, 3));

        assertInvalid("SELECT * FROM %s WHERE k2 = 3");

        assertRows(execute("SELECT * FROM %s WHERE token(k1, k2) = token(0, 1)"),
                   row(0, 1, 1, 1));


        assertRows(execute("SELECT * FROM %s WHERE token(k1, k2) > ?", Long.MIN_VALUE),
                   row(0, 2, 2, 2),
                   row(0, 3, 3, 3),
                   row(0, 0, 0, 0),
                   row(0, 1, 1, 1));
    }

    /**
     * Test for #4532, NPE when trying to select a slice from a composite table
     * migrated from cql_tests.py:TestCQL.bug_4532_test()
     */
    @Test
    public void testSelectSliceFromComposite() throws Throwable
    {
        createTable("CREATE TABLE %s (status ascii, ctime bigint, key ascii, nil ascii, PRIMARY KEY (status, ctime, key))");

        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345678,'key1','')");
        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345678,'key2','')");
        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345679,'key3','')");
        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345679,'key4','')");
        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345679,'key5','')");
        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345680,'key6','')");

        assertInvalid("SELECT * FROM %s WHERE ctime>=12345679 AND key='key3' AND ctime<=12345680 LIMIT 3;");
        assertInvalid("SELECT * FROM %s WHERE ctime=12345679  AND key='key3' AND ctime<=12345680 LIMIT 3");
    }

    /**
     * Test for #4716 bug and more generally for good behavior of ordering,
     * migrated from cql_tests.py:TestCQL.reversed_compact_test()
     */
    @Test
    public void testReverseCompact() throws Throwable
    {
        createTable("CREATE TABLE %s ( k text, c int, v int, PRIMARY KEY (k, c) ) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c DESC)");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES ('foo', ?, ?)", i, i);

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo'"),
                   row(5), row(4), row(3));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo'"),
                   row(6), row(5), row(4), row(3), row(2));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC"),
                   row(5), row(4), row(3));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC"),
                   row(6), row(5), row(4), row(3), row(2));

        createTable("CREATE TABLE %s ( k text, c int, v int, PRIMARY KEY (k, c) ) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s(k, c, v) VALUES ('foo', ?, ?)", i, i);

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo'"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo'"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC"),
                   row(5), row(4), row(3));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC"),
                   row(6), row(5), row(4), row(3), row(2));
    }

    /**
     * Test for the bug from #4760 and #4759,
     * migrated from cql_tests.py:TestCQL.reversed_compact_multikey_test()
     */
    @Test
    public void testReversedCompactMultikey() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, c1 int, c2 int, value text, PRIMARY KEY(key, c1, c2) ) WITH COMPACT STORAGE AND CLUSTERING ORDER BY(c1 DESC, c2 DESC)");

        for (int i = 0; i < 3; i++)
            for (int j = 0; j < 3; j++)
                execute("INSERT INTO %s (key, c1, c2, value) VALUES ('foo', ?, ?, 'bar')", i, j);

        // Equalities
        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1"),
                   row(1, 2), row(1, 1), row(1, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1 ORDER BY c1 ASC, c2 ASC"),
                   row(1, 0), row(1, 1), row(1, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1 ORDER BY c1 DESC, c2 DESC"),
                   row(1, 2), row(1, 1), row(1, 0));

        // GT
        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1"),
                   row(2, 2), row(2, 1), row(2, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1 ORDER BY c1 ASC, c2 ASC"),
                   row(2, 0), row(2, 1), row(2, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1 ORDER BY c1 DESC, c2 DESC"),
                   row(2, 2), row(2, 1), row(2, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1"),
                   row(2, 2), row(2, 1), row(2, 0), row(1, 2), row(1, 1), row(1, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC, c2 ASC"),
                   row(1, 0), row(1, 1), row(1, 2), row(2, 0), row(2, 1), row(2, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC"),
                   row(1, 0), row(1, 1), row(1, 2), row(2, 0), row(2, 1), row(2, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 DESC, c2 DESC"),
                   row(2, 2), row(2, 1), row(2, 0), row(1, 2), row(1, 1), row(1, 0));

        // LT
        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1"),
                   row(0, 2), row(0, 1), row(0, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1 ORDER BY c1 ASC, c2 ASC"),
                   row(0, 0), row(0, 1), row(0, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1 ORDER BY c1 DESC, c2 DESC"),
                   row(0, 2), row(0, 1), row(0, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1"),
                   row(1, 2), row(1, 1), row(1, 0), row(0, 2), row(0, 1), row(0, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC, c2 ASC"),
                   row(0, 0), row(0, 1), row(0, 2), row(1, 0), row(1, 1), row(1, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC"),
                   row(0, 0), row(0, 1), row(0, 2), row(1, 0), row(1, 1), row(1, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 DESC, c2 DESC"),
                   row(1, 2), row(1, 1), row(1, 0), row(0, 2), row(0, 1), row(0, 0));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_4882_test()
     */
    @Test
    public void testDifferentOrdering() throws Throwable
    {
        createTable(" CREATE TABLE %s ( k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2) ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");

        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 1, 1, 1)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 2, 2)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 1, 3, 3)");

        assertRows(execute("select * from %s where k = 0 limit 1"),
                   row(0, 0, 2, 2));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.allow_filtering_test()
     */
    @Test
    public void testAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        for (int i = 0; i < 3; i++)
            for (int j = 0; j < 3; j++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, j, j);

        // Don't require filtering, always allowed
        String[] queries = new String[]
                           {
                           "SELECT * FROM %s WHERE k = 1",
                           "SELECT * FROM %s WHERE k = 1 AND c > 2",
                           "SELECT * FROM %s WHERE k = 1 AND c = 2"
                           };

        for (String q : queries)
        {
            execute(q);
            execute(q + " ALLOW FILTERING");
        }

        // Require filtering, allowed only with ALLOW FILTERING
        queries = new String[]
                  {
                  "SELECT * FROM %s WHERE c = 2",
                  "SELECT * FROM %s WHERE c > 2 AND c <= 4"
                  };

        for (String q : queries)
        {
            assertInvalid(q);
            execute(q + " ALLOW FILTERING");
        }

        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int,)");
        createIndex("CREATE INDEX ON %s (a)");

        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i * 10, i * 100);

        // Don't require filtering, always allowed
        queries = new String[]
                  {
                  "SELECT * FROM %s WHERE k = 1",
                  "SELECT * FROM %s WHERE a = 20"
                  };

        for (String q : queries)
        {
            execute(q);
            execute(q + " ALLOW FILTERING");
        }

        // Require filtering, allowed only with ALLOW FILTERING
        queries = new String[]
                  {
                  "SELECT * FROM %s WHERE a = 20 AND b = 200"
                  };

        for (String q : queries)
        {
            assertInvalid(q);
            execute(q + " ALLOW FILTERING");
        }
    }

    /**
     * Test for bug from #5122,
     * migrated from cql_tests.py:TestCQL.composite_partition_key_validation_test()
     */
    @Test
    public void testSelectOnCompositeInvalid() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b text, c uuid, PRIMARY KEY ((a, b)))");

        execute("INSERT INTO %s (a, b , c ) VALUES (1, 'aze', 4d481800-4c5f-11e1-82e0-3f484de45426)");
        execute("INSERT INTO %s (a, b , c ) VALUES (1, 'ert', 693f5800-8acb-11e3-82e0-3f484de45426)");
        execute("INSERT INTO %s (a, b , c ) VALUES (1, 'opl', d4815800-2d8d-11e0-82e0-3f484de45426)");

        assertRowCount(execute("SELECT * FROM %s"), 3);
        assertInvalid("SELECT * FROM %s WHERE a=1");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multi_in_test()
     */
    @Test
    public void testMultiSelects() throws Throwable
    {
        doTestVariousSelects(false);
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multi_in_compact_test()
     */
    @Test
    public void testMultiSelectsCompactStorage() throws Throwable
    {
        doTestVariousSelects(true);
    }


    public void doTestVariousSelects(boolean compact) throws Throwable
    {
        createTable(
                   "CREATE TABLE %s (group text, zipcode text, state text, fips_regions int, city text, PRIMARY KEY (group, zipcode, state, fips_regions))"
                   + (compact
                      ? " WITH COMPACT STORAGE"
                      : ""));

        String str = "INSERT INTO %s (group, zipcode, state, fips_regions, city) VALUES (?, ?, ?, ?, ?)";
        execute(str, "test", "06029", "CT", 9, "Ellington");
        execute(str, "test", "06031", "CT", 9, "Falls Village");
        execute(str, "test", "06902", "CT", 9, "Stamford");
        execute(str, "test", "06927", "CT", 9, "Stamford");
        execute(str, "test", "10015", "NY", 36, "New York");
        execute(str, "test", "07182", "NJ", 34, "Newark");
        execute(str, "test", "73301", "TX", 48, "Austin");
        execute(str, "test", "94102", "CA", 06, "San Francisco");

        execute(str, "test2", "06029", "CT", 9, "Ellington");
        execute(str, "test2", "06031", "CT", 9, "Falls Village");
        execute(str, "test2", "06902", "CT", 9, "Stamford");
        execute(str, "test2", "06927", "CT", 9, "Stamford");
        execute(str, "test2", "10015", "NY", 36, "New York");
        execute(str, "test2", "07182", "NJ", 34, "Newark");
        execute(str, "test2", "73301", "TX", 48, "Austin");
        execute(str, "test2", "94102", "CA", 06, "San Francisco");

        assertRowCount(execute("select zipcode from %s"), 16);
        assertRowCount(execute("select zipcode from %s where group='test'"), 8);
        assertInvalid("select zipcode from %s where zipcode='06902'");
        assertRowCount(execute("select zipcode from %s where zipcode='06902' ALLOW FILTERING"), 2);
        assertRowCount(execute("select zipcode from %s where group='test' and zipcode='06902'"), 1);
        assertRowCount(execute("select zipcode from %s where group='test' and zipcode IN ('06902','73301','94102')"), 3);
        assertRowCount(execute("select zipcode from %s where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA')"), 2);
        assertRowCount(execute("select zipcode from %s where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions = 9"), 1);
        assertRowCount(execute("select zipcode from %s where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') ORDER BY zipcode DESC"), 2);
        assertRowCount(execute("select zipcode from %s where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions > 0"), 2);
        assertEmpty(execute("select zipcode from %s where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions < 0"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multi_in_compact_non_composite_test()
     */
    @Test
    public void testMultiSelectsNonCompositeCompactStorage() throws Throwable
    {
        createTable("CREATE TABLE %s (key int, c int, v int, PRIMARY KEY (key, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (key, c, v) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (key, c, v) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (key, c, v) VALUES (0, 2, 2)");

        assertRows(execute("SELECT * FROM %s WHERE key=0 AND c IN (0, 2)"),
                   row(0, 0, 0), row(0, 2, 2));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.ticket_5230_test()
     */
    @Test
    public void testMultipleClausesOnPrimaryKey() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, c text, v text, PRIMARY KEY(key, c))");

        execute("INSERT INTO %s (key, c, v) VALUES ('foo', '1', '1')");
        execute("INSERT INTO %s(key, c, v) VALUES ('foo', '2', '2')");
        execute("INSERT INTO %s(key, c, v) VALUES ('foo', '3', '3')");

        assertRows(execute("SELECT c FROM %s WHERE key = 'foo' AND c IN ('1', '2')"),
                   row("1"), row("2"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_5404()
     */
    @Test
    public void testSelectWithToken() throws Throwable
    {
        createTable("CREATE TABLE %s (key text PRIMARY KEY)");

        // We just want to make sure this doesn 't NPE server side
        assertInvalid("select * from %s where token(key) > token(int(3030343330393233)) limit 1");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.clustering_order_and_functions_test()
     */
    @Test
    public void testFunctionsWithClusteringDesc() throws Throwable
    {
        createTable("CREATE TABLE %s ( k int, t timeuuid, PRIMARY KEY (k, t) ) WITH CLUSTERING ORDER BY (t DESC)");

        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (k, t) VALUES (?, now())", i);

        execute("SELECT dateOf(t) FROM %s");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.select_with_alias_test()
     */
    @Test
    public void testSelectWithAlias() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, name text)");

        for (int id = 0; id < 5; id++)
            execute("INSERT INTO %s (id, name) VALUES (?, ?) USING TTL 10 AND TIMESTAMP 0", id, "name" + id);

        // test aliasing count( *)
        UntypedResultSet rs = execute("SELECT count(*) AS user_count FROM %s");
        assertEquals("user_count", rs.metadata().get(0).name.toString());
        assertEquals(5L, rs.one().getLong(rs.metadata().get(0).name.toString()));

        // test aliasing regular value
        rs = execute("SELECT name AS user_name FROM %s WHERE id = 0");
        assertEquals("user_name", rs.metadata().get(0).name.toString());
        assertEquals("name0", rs.one().getString(rs.metadata().get(0).name.toString()));

        // test aliasing writetime
        rs = execute("SELECT writeTime(name) AS name_writetime FROM %s WHERE id = 0");
        assertEquals("name_writetime", rs.metadata().get(0).name.toString());
        assertEquals(0, rs.one().getInt(rs.metadata().get(0).name.toString()));

        // test aliasing ttl
        rs = execute("SELECT ttl(name) AS name_ttl FROM %s WHERE id = 0");
        assertEquals("name_ttl", rs.metadata().get(0).name.toString());
        int ttl = rs.one().getInt(rs.metadata().get(0).name.toString());
        assertTrue(ttl == 9 || ttl == 10);

        // test aliasing a regular function
        rs = execute("SELECT intAsBlob(id) AS id_blob FROM %s WHERE id = 0");
        assertEquals("id_blob", rs.metadata().get(0).name.toString());
        assertEquals(ByteBuffer.wrap(new byte[4]), rs.one().getBlob(rs.metadata().get(0).name.toString()));

        // test that select throws a meaningful exception for aliases in where clause
        assertInvalidMessage("Aliases aren't allowed in the where clause",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE user_id = 0");

        // test that select throws a meaningful exception for aliases in order by clause
        assertInvalidMessage("Aliases are not allowed in order by clause",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE id IN (0) ORDER BY user_name");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.select_distinct_test()
     */
    @Test
    public void testSelectDistinct() throws Throwable
    {
        // Test a regular(CQL3) table.
        createTable("CREATE TABLE %s (pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY((pk0, pk1), ck0))");

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 0, 0)", i, i);
            execute("INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 1, 1)", i, i);
        }

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 1"),
                   row(0, 0));

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 3"),
                   row(0, 0),
                   row(2, 2),
                   row(1, 1));

        // Test selection validation.
        assertInvalidMessage("queries must request all the partition key columns", "SELECT DISTINCT pk0 FROM %s");
        assertInvalidMessage("queries must only request partition key columns", "SELECT DISTINCT pk0, pk1, ck0 FROM %s");

        //Test a 'compact storage' table.
        createTable("CREATE TABLE %s (pk0 int, pk1 int, val int, PRIMARY KEY((pk0, pk1))) WITH COMPACT STORAGE");

        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (pk0, pk1, val) VALUES (?, ?, ?)", i, i, i);

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 1"),
                   row(0, 0));

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 3"),
                   row(0, 0),
                   row(2, 2),
                   row(1, 1));

        // Test a 'wide row' thrift table.
        createTable("CREATE TABLE %s (pk int, name text, val int, PRIMARY KEY(pk, name)) WITH COMPACT STORAGE");

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (pk, name, val) VALUES (?, 'name0', 0)", i);
            execute("INSERT INTO %s (pk, name, val) VALUES (?, 'name1', 1)", i);
        }

        assertRows(execute("SELECT DISTINCT pk FROM %s LIMIT 1"),
                   row(1));

        assertRows(execute("SELECT DISTINCT pk FROM %s LIMIT 3"),
                   row(1),
                   row(0),
                   row(2));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.select_distinct_with_deletions_test()
     */
    @Test
    public void testSelectDistinctWithDeletions() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c int, v int)");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, i, i);

        Object[][] rows = getRows(execute("SELECT DISTINCT k FROM %s"));
        Assert.assertEquals(10, rows.length);
        Object key_to_delete = rows[3][0];

        execute("DELETE FROM %s WHERE k=?", key_to_delete);

        rows = getRows(execute("SELECT DISTINCT k FROM %s"));
        Assert.assertEquals(9, rows.length);

        rows = getRows(execute("SELECT DISTINCT k FROM %s LIMIT 5"));
        Assert.assertEquals(5, rows.length);

        rows = getRows(execute("SELECT DISTINCT k FROM %s"));
        Assert.assertEquals(9, rows.length);
    }

    @Test
    public void testSelectDistinctWithWhereClause() throws Throwable {
        createTable("CREATE TABLE %s (k int, a int, b int, PRIMARY KEY (k, a))");
        createIndex("CREATE INDEX ON %s (b)");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i, i);
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i * 10, i * 10);
        }

        String distinctQueryErrorMsg = "SELECT DISTINCT with WHERE clause only supports restriction by partition key.";
        assertInvalidMessage(distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE a >= 80 ALLOW FILTERING");

        assertInvalidMessage(distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE k IN (1, 2, 3) AND a = 10");

        assertInvalidMessage(distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE b = 5");

        assertRows(execute("SELECT DISTINCT k FROM %s WHERE k = 1"),
                   row(1));
        assertRows(execute("SELECT DISTINCT k FROM %s WHERE k IN (5, 6, 7)"),
                   row(5),
                   row(6),
                   row(7));

        // With static columns
        createTable("CREATE TABLE %s (k int, a int, s int static, b int, PRIMARY KEY (k, a))");
        createIndex("CREATE INDEX ON %s (b)");
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (k, a, b, s) VALUES (?, ?, ?, ?)", i, i, i, i);
            execute("INSERT INTO %s (k, a, b, s) VALUES (?, ?, ?, ?)", i, i * 10, i * 10, i * 10);
        }

        assertRows(execute("SELECT DISTINCT s FROM %s WHERE k = 5"),
                   row(50));
        assertRows(execute("SELECT DISTINCT s FROM %s WHERE k IN (5, 6, 7)"),
                   row(50),
                   row(60),
                   row(70));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_6327_test()
     */
    @Test
    public void testSelectInClauseAtOne() throws Throwable
    {
        createTable("CREATE TABLE %s ( k int, v int, PRIMARY KEY (k, v))");

        execute("INSERT INTO %s (k, v) VALUES (0, 0)");

        flush();

        assertRows(execute("SELECT v FROM %s WHERE k=0 AND v IN (1, 0)"),
                   row(0));
    }

    /**
     * Test for the #6579 'select count' paging bug,
     * migrated from cql_tests.py:TestCQL.select_count_paging_test()
     */
    @Test
    public void testSelectCountPaging() throws Throwable
    {
        createTable("create table %s (field1 text, field2 timeuuid, field3 boolean, primary key(field1, field2))");
        createIndex("create index test_index on %s (field3)");

        execute("insert into %s (field1, field2, field3) values ('hola', now(), false)");
        execute("insert into %s (field1, field2, field3) values ('hola', now(), false)");

        assertRows(execute("select count(*) from %s where field3 = false limit 1"),
                   row(2L));
    }

    /**
     * Test for #7105 bug,
     * migrated from cql_tests.py:TestCQL.clustering_order_in_test()
     */
    @Test
    public void testClusteringOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY ((a, b), c) ) with clustering order by (c desc)");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 3)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 5, 6)");

        assertRows(execute("SELECT * FROM %s WHERE a=1 AND b=2 AND c IN (3)"),
                   row(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE a=1 AND b=2 AND c IN (3, 4)"),
                   row(1, 2, 3));
    }

    /**
     * Test for #7105 bug,
     * SELECT with IN on final column of composite and compound primary key fails
     * migrated from cql_tests.py:TestCQL.bug7105_test()
     */
    @Test
    public void testSelectInFinalColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 2, 3, 3)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 4, 6, 5)");

        assertRows(execute("SELECT * FROM %s WHERE a=1 AND b=2 ORDER BY b DESC"),
                   row(1, 2, 3, 3));
    }

    @Test
    public void testAlias() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, name text)");

        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (id, name) VALUES (?, ?) USING TTL 10 AND TIMESTAMP 0", i, Integer.toString(i));

        assertInvalidMessage("Aliases aren't allowed in the where clause",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE user_id = 0");

        // test that select throws a meaningful exception for aliases in order by clause
        assertInvalidMessage("Aliases are not allowed in order by clause",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE id IN (0) ORDER BY user_name");

    }

    @Test
    public void testFilteringWithoutIndices() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e map<int, int>, PRIMARY KEY (a, b))");

        // Checks filtering
        assertInvalidMessage("Predicates on non-primary-key columns (c, d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = 1 AND d = 2 ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE a = 1 AND b = 1 AND c = 2 ALLOW FILTERING");
        assertInvalidMessage("IN predicates on non-primary-key columns (c) is not yet supported",
                             "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (2, 3) ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > 2 ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS 1 ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS KEY 1 ALLOW FILTERING");

        // Checks filtering with null
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING", unset());
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING", unset());
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS ? ALLOW FILTERING", unset());
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS KEY ? ALLOW FILTERING", unset());

        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 2, 8)");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 6, 4)");

        assertRows(execute("SELECT * FROM %s WHERE c = 4 ALLOW FILTERING"),
                   row(1, 2, 4),
                   row(3, 6, 4));

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c", "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column c", "SELECT * FROM %s WHERE c > null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = ?", unset());
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > ?", unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING", unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING", unset());
    }

    @Test
    public void testFilteringOnStaticColumnWithoutIndices() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s int static, c int, PRIMARY KEY (a, b))");

        // Checks filtering
        assertInvalidMessage("Predicates on non-primary-key columns (c, s) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = 1 AND s = 2 ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (s) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE a = 1 AND b = 1 AND s = 2 ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (s) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE s > 2 ALLOW FILTERING");

        // Checks filtering with null
        assertInvalidMessage("Predicates on non-primary-key columns (s) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE s = null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (s) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE s > null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Predicates on non-primary-key columns (s) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE s = ? ALLOW FILTERING", unset());
        assertInvalidMessage("Predicates on non-primary-key columns (s) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE s > ? ALLOW FILTERING", unset());
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndices() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, 7)");

        // Lets add some tombstones to make sure that filtering handle them properly
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 2, 7)");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1");
        execute("DELETE FROM %s WHERE a = 2 AND b = 2");

        flush();

        // Checks filtering
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4 ALLOW FILTERING");

        assertInvalidMessage("IN predicates on non-primary-key columns (c) is not yet supported",
                             "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > 4 ALLOW FILTERING");

        // Checks filtering with null
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, 7)");

        // Lets add some tombstones to make sure that filtering handle them properly
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (5, 2, 7)");
        execute("DELETE FROM %s WHERE a = 0");
        execute("DELETE FROM %s WHERE a = 5");

        flush();

        // Checks filtering
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4");

        assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 4 ALLOW FILTERING"),
                   row(1, 2, 4));

        assertInvalidMessage("IN predicates on non-primary-key columns (c) is not yet supported",
                             "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7)");

        assertInvalidMessage("IN predicates on non-primary-key columns (c) is not yet supported",
                             "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING");

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > 4");

        assertRows(execute("SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                   row(2, 1, 6),
                   row(4, 1, 7));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE b < 3 AND c <= 4");

        assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= 4 ALLOW FILTERING"),
                   row(1, 2, 4),
                   row(3, 2, 4));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c >= 3 AND c <= 6");

        assertRows(execute("SELECT * FROM %s WHERE c >= 3 AND c <= 6 ALLOW FILTERING"),
                   row(1, 2, 4),
                   row(2, 1, 6),
                   row(3, 2, 4));

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testFilteringWithoutIndicesWithCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c list<int>, d set<int>, e map<int, int>, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, [1, 6], {2, 12}, {1: 6})");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, [3, 2], {6, 4}, {3: 2})");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, [1, 2], {2, 4}, {1: 2})");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 3, [3, 6], {6, 12}, {3: 6})");

        flush();

        // Checks filtering for lists
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING");

        // Checks filtering for sets
        assertInvalidMessage("Predicates on non-primary-key columns (d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE d CONTAINS 4 ALLOW FILTERING");

        // Checks filtering for maps
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS 2 ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS KEY 2 ALLOW FILTERING");

        // Checks filtering with null
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE d CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS KEY null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e[null] = 2 ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e[1] = null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE d CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS KEY ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e[?] = 2 ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e[1] = ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testFilteringWithoutIndicesWithFrozenCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c frozen<list<int>>, d frozen<set<int>>, e frozen<map<int, int>>, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, [1, 6], {2, 12}, {1: 6})");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, [3, 2], {6, 4}, {3: 2})");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, [1, 2], {2, 4}, {1: 2})");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 3, [3, 6], {6, 12}, {3: 6})");

        flush();

        // Checks filtering for lists
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = [3, 2] ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > [1, 5] AND c < [3, 6] ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING");

        // Checks filtering for sets
        assertInvalidMessage("Predicates on non-primary-key columns (d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE d = {6, 4} ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE d > {4, 5} AND d < {6} ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE d CONTAINS 4 ALLOW FILTERING");

        // Checks filtering for maps
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e = {1 : 2} ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                "SELECT * FROM %s WHERE e > {1 : 4} AND e < {3 : 6} ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS 2 ALLOW FILTERING");

        assertInvalidMessage("Map-entry equality predicates on frozen map column e are not supported",
                             "SELECT * FROM %s WHERE e[1] = 6 ALLOW FILTERING");

        // Checks filtering with null
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE d = null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE d CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e = null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS KEY null ALLOW FILTERING");
        assertInvalidMessage("Map-entry equality predicates on frozen map column e are not supported",
                             "SELECT * FROM %s WHERE e[null] = 2 ALLOW FILTERING");
        assertInvalidMessage("Map-entry equality predicates on frozen map column e are not supported",
                             "SELECT * FROM %s WHERE e[1] = null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE d = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (d) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE d CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (e) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE e CONTAINS KEY ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Map-entry equality predicates on frozen map column e are not supported",
                             "SELECT * FROM %s WHERE e[?] = 2 ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Map-entry equality predicates on frozen map column e are not supported",
                             "SELECT * FROM %s WHERE e[1] = ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndicesAndWithLists() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<list<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, [6, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, [4, 1])");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, [7, 1])");

        flush();

        // Checks filtering
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = [4, 1] ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > [4, 2] ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE b <= 3 AND c < [6, 2] ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING");

        assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

        // Checks filtering with null
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<list<int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, [6, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, [4, 1])");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, [7, 1])");

        flush();

        // Checks filtering
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = [4, 2]");

        assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = [4, 2] ALLOW FILTERING"),
                   row(1, 2, list(4, 2)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > [4, 2]");

        assertRows(execute("SELECT * FROM %s WHERE c > [4, 2] ALLOW FILTERING"),
                   row(2, 1, list(6, 2)),
                   row(4, 1, list(7, 1)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE b < 3 AND c <= [4, 2]");

        assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= [4, 2] ALLOW FILTERING"),
                   row(1, 2, list(4, 2)),
                   row(3, 2, list(4, 1)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c >= [4, 3] AND c <= [7]");

        assertRows(execute("SELECT * FROM %s WHERE c >= [4, 3] AND c <= [7] ALLOW FILTERING"),
                   row(2, 1, list(6, 2)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                "SELECT * FROM %s WHERE c CONTAINS 2");

        assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                   row(1, 2, list(4, 2)),
                   row(2, 1, list(6, 2)));

        assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

        assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                   row(2, 1, list(6, 2)));

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndicesAndWithSets() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<set<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, {6, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, {4, 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, {7, 1})");

        flush();

        // Checks filtering
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4, 1} ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > {4, 2} ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c >= {4, 2} AND c <= {6, 4} ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING");

        assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

        // Checks filtering with null
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<set<int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, {6, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, {4, 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, {7, 1})");

        flush();

        // Checks filtering
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4, 2}");

        assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4, 2} ALLOW FILTERING"),
                   row(1, 2, set(4, 2)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > {4, 2}");

        assertRows(execute("SELECT * FROM %s WHERE c > {4, 2} ALLOW FILTERING"),
                   row(2, 1, set(6, 2)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE b < 3 AND c <= {4, 2}");

        assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= {4, 2} ALLOW FILTERING"),
                   row(1, 2, set(4, 2)),
                   row(4, 1, set(1, 7)),
                   row(3, 2, set(4, 1)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c >= {4, 3} AND c <= {7}");

        assertRows(execute("SELECT * FROM %s WHERE c >= {5, 2} AND c <= {7} ALLOW FILTERING"),
                   row(2, 1, set(6, 2)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                "SELECT * FROM %s WHERE c CONTAINS 2");

        assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                   row(1, 2, set(4, 2)),
                   row(2, 1, set(6, 2)));

        assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

        assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                   row(2, 1, set(6, 2)));

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testIndexQueryWithValueOver64K() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c blob, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX test ON %s (c)");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, bytes(1));
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, bytes(2));

        assertInvalidMessage("Index expression values may not be larger than 64K",
                             "SELECT * FROM %s WHERE c = ?  ALLOW FILTERING", TOO_BIG);
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndicesAndWithMaps() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<map<int, int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, {6 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, {4 : 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, {7 : 1})");

        flush();

        // Checks filtering
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4 : 1} ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > {4 : 2} ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE b <= 3 AND c < {6 : 2} ALLOW FILTERING");

        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING");

        // Checks filtering with null
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Predicates on non-primary-key columns (c) are not yet supported for non secondary index queries",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<map<int, int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, {6 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, {4 : 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, {7 : 1})");

        flush();

        // Checks filtering
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4 : 2}");

        assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4 : 2} ALLOW FILTERING"),
                   row(1, 2, map(4, 2)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > {4 : 2}");

        assertRows(execute("SELECT * FROM %s WHERE c > {4 : 2} ALLOW FILTERING"),
                   row(2, 1, map(6, 2)),
                   row(4, 1, map(7, 1)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE b < 3 AND c <= {4 : 2}");

        assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= {4 : 2} ALLOW FILTERING"),
                   row(1, 2, map(4, 2)),
                   row(3, 2, map(4, 1)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c >= {4 : 3} AND c <= {7 : 1}");

        assertRows(execute("SELECT * FROM %s WHERE c >= {5 : 2} AND c <= {7 : 0} ALLOW FILTERING"),
                   row(2, 1, map(6, 2)));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                "SELECT * FROM %s WHERE c CONTAINS 2");

        assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                   row(1, 2, map(4, 2)),
                   row(2, 1, map(6, 2)));

        assertRows(execute("SELECT * FROM %s WHERE c CONTAINS KEY 4 ALLOW FILTERING"),
                   row(1, 2, map(4, 2)),
                   row(3, 2, map(4, 1)));

        assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                   row(2, 1, map(6, 2)));

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS KEY null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ? ALLOW FILTERING",
                             unset());
    }

    /**
     * Check select with and without compact storage, with different column
     * order. See CASSANDRA-10988
     */
    @Test
    public void testClusteringOrderWithSlice() throws Throwable
    {
        for (String compactOption : new String[] { "", " COMPACT STORAGE AND" })
        {
            // non-compound, ASC order
            createTable("CREATE TABLE %s (a text, b int, PRIMARY KEY (a, b)) WITH" +
                        compactOption +
                        " CLUSTERING ORDER BY (b ASC)");

            execute("INSERT INTO %s (a, b) VALUES ('a', 2)");
            execute("INSERT INTO %s (a, b) VALUES ('a', 3)");
            assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                       row("a", 2),
                       row("a", 3));

            assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b DESC"),
                       row("a", 3),
                       row("a", 2));

            // non-compound, DESC order
            createTable("CREATE TABLE %s (a text, b int, PRIMARY KEY (a, b)) WITH" +
                        compactOption +
                        " CLUSTERING ORDER BY (b DESC)");

            execute("INSERT INTO %s (a, b) VALUES ('a', 2)");
            execute("INSERT INTO %s (a, b) VALUES ('a', 3)");
            assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                       row("a", 3),
                       row("a", 2));

            assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                       row("a", 2),
                       row("a", 3));

            // compound, first column DESC order
            createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b, c)) WITH" +
                        compactOption +
                        " CLUSTERING ORDER BY (b DESC)"
            );

            execute("INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)");
            execute("INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)");
            assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                       row("a", 3, 5),
                       row("a", 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                       row("a", 2, 4),
                       row("a", 3, 5));

            // compound, mixed order
            createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b, c)) WITH" +
                        compactOption +
                        " CLUSTERING ORDER BY (b ASC, c DESC)"
            );

            execute("INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)");
            execute("INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)");
            assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                       row("a", 2, 4),
                       row("a", 3, 5));

            assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                       row("a", 2, 4),
                       row("a", 3, 5));
        }
    }

    @Test
    public void testOverlyLargeSelectPK() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, PRIMARY KEY ((a), b))");

        assertInvalidThrow(InvalidRequestException.class,
                           "SELECT * FROM %s WHERE a = ?", new String(TOO_BIG.array()));
    }

    @Test
    public void testOverlyLargeSelectCK() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, PRIMARY KEY ((a), b))");

        assertInvalidThrow(InvalidRequestException.class,
                           "SELECT * FROM %s WHERE a = 'foo' AND b = ?", new String(TOO_BIG.array()));
    }

    @Test
    public void testOverlyLargeSelectKeyIn() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, d text, PRIMARY KEY ((a, b, c), d))");

        assertInvalidThrow(InvalidRequestException.class,
                           "SELECT * FROM %s WHERE a = 'foo' AND b= 'bar' AND c IN (?, ?)",
                           new String(TOO_BIG.array()), new String(TOO_BIG.array()));
    }

    @Test
    public void testFilteringWithSecondaryIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, " +
                    "c1 int, " +
                    "c2 int, " +
                    "c3 int, " +
                    "v int, " +
                    "PRIMARY KEY (pk, c1, c2, c3))");
        createIndex("CREATE INDEX v_idx_1 ON %s (v);");

        for (int i = 1; i <= 5; i++)
        {
            execute("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 1, 1, i);
            execute("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 1, i, i);
            execute("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 1, 1, i, i, i);
            execute("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 1, i, i, i, i);
        }

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 > 0 AND c1 < 5 AND c2 = 1 AND v = 3 ALLOW FILTERING;"),
                   row(1, 1, 1, 3, 3));

        assertEmpty(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 > 1 AND c1 < 5 AND c2 = 1 AND v = 3 ALLOW FILTERING;"));

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 > 1 AND c2 > 2 AND c3 > 2 AND v = 3 ALLOW FILTERING;"),
                   row(1, 3, 3, 3, 3));

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 > 1 AND c2 > 2 AND c3 = 3 AND v = 3 ALLOW FILTERING;"),
                   row(1, 3, 3, 3, 3));

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 IN(0,1,2) AND c2 = 1 AND v = 3 ALLOW FILTERING;"),
                   row(1, 1, 1, 3, 3));

        assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 IN(0,1,2) AND c2 = 1 AND v = 3"),
                   row(1, 1, 1, 3, 3));

        assertInvalidMessage("Clustering column \"c2\" cannot be restricted (preceding column \"c1\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE pk = 1 AND  c1 > 0 AND c1 < 5 AND c2 = 1 ALLOW FILTERING;");

        assertInvalidMessage("PRIMARY KEY column \"c2\" cannot be restricted as preceding column \"c1\" is not restricted",
                             "SELECT * FROM %s WHERE pk = 1 AND  c2 = 1 ALLOW FILTERING;");
    }

    @Test
    public void testIndexQueryWithCompositePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (p1 int, p2 int, v int, PRIMARY KEY ((p1, p2)))");
        assertInvalidMessage("Partition key parts: p2 must be restricted as other parts are",
                             "SELECT * FROM %s WHERE p1 = 1 AND v = 3 ALLOW FILTERING");

        createIndex("CREATE INDEX ON %s(v)");

        execute("INSERT INTO %s(p1, p2, v) values (?, ?, ?)", 1, 1, 3);
        execute("INSERT INTO %s(p1, p2, v) values (?, ?, ?)", 1, 2, 3);
        execute("INSERT INTO %s(p1, p2, v) values (?, ?, ?)", 2, 1, 3);

        assertRows(execute("SELECT * FROM %s WHERE p1 = 1 AND v = 3 ALLOW FILTERING"),
                   row(1, 2, 3),
                   row(1, 1, 3));
    }

    @Test
    public void testEmptyRestrictionValue() throws Throwable
    {
        for (String options : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY ((pk), c))" + options);
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"), bytes("2"), bytes("2"));

            for (boolean flush : new boolean[]{false, true})
            {
                if (flush)
                    flush();

                assertInvalidMessage("Key may not be empty", "SELECT * FROM %s WHERE pk = textAsBlob('');");
                assertInvalidMessage("Key may not be empty", "SELECT * FROM %s WHERE pk IN (textAsBlob(''), textAsBlob('1'));");

                assertInvalidMessage("Key may not be empty",
                                     "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                     EMPTY_BYTE_BUFFER, bytes("2"), bytes("2"));

                // Test clustering columns restrictions
                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) = (textAsBlob(''));"));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) IN ((textAsBlob('')), (textAsBlob('1')));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('');"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) > (textAsBlob(''));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('');"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob(''));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) <= (textAsBlob(''));"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) < (textAsBlob(''));"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('') AND c < textAsBlob('');"));
            }

            if (options.contains("COMPACT"))
            {
                assertInvalidMessage("Missing PRIMARY KEY part c",
                                     "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                     bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4"));
            }
            else
            {
                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                        bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4"));

                for (boolean flush : new boolean[]{false, true})
                {
                    if (flush)
                        flush();

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) = (textAsBlob(''));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) IN ((textAsBlob('')), (textAsBlob('1')));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('');"),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) > (textAsBlob(''));"),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('');"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob(''));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('');"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) <= (textAsBlob(''));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('');"));

                    assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) < (textAsBlob(''));"));

                    assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('') AND c < textAsBlob('');"));
                }
            }

            // Test restrictions on non-primary key value
            assertInvalidMessage("Predicates on non-primary-key columns (v) are not yet supported for non secondary index queries",
                                 "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('') ALLOW FILTERING;");
        }
    }

    @Test
    public void testEmptyRestrictionValueWithMultipleClusteringColumns() throws Throwable
    {
        for (String options : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))" + options);
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("2"));

            for (boolean flush : new boolean[]{false, true})
            {
                if (flush)
                    flush();

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 = textAsBlob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) = (textAsBlob('1'), textAsBlob(''));"));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1');"),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 IN (textAsBlob(''), textAsBlob('1'));"),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')));"),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('');"),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 > textAsBlob('');"),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'));"),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 >= textAsBlob('');"),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 <= textAsBlob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) <= (textAsBlob('1'), textAsBlob(''));"));
            }

            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                    bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4"));

            for (boolean flush : new boolean[]{false, true})
            {
                if (flush)
                    flush();

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('');"),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('') AND c2 = textAsBlob('1');"),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) = (textAsBlob(''), textAsBlob('1'));"),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1');"),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')));"),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'));"),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) >= (textAsBlob(''), textAsBlob('1'));"),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) <= (textAsBlob(''), textAsBlob('1'));"),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) < (textAsBlob(''), textAsBlob('1'));"));
            }
        }
    }

    @Test
    public void testEmptyRestrictionValueWithOrderBy() throws Throwable
    {
        for (String options : new String[] { "",
                                             " WITH COMPACT STORAGE",
                                             " WITH CLUSTERING ORDER BY (c DESC)",
                                             " WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c DESC)"})
        {
            String orderingClause = options.contains("ORDER") ? "" : "ORDER BY c DESC" ;

            createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY ((pk), c))" + options);
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"),
                    bytes("1"),
                    bytes("1"));
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"),
                    bytes("2"),
                    bytes("2"));

            for (boolean flush : new boolean[]{false, true})
            {
                if (flush)
                    flush();

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('')" + orderingClause),
                           row(bytes("foo123"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('')" + orderingClause),
                           row(bytes("foo123"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('')" + orderingClause));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('')" + orderingClause));
            }

            if (options.contains("COMPACT"))
            {
                assertInvalidMessage("Missing PRIMARY KEY part c",
                                     "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                     bytes("foo123"),
                                     EMPTY_BYTE_BUFFER,
                                     bytes("4"));
            }
            else
            {
                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                        bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4"));

                for (boolean flush : new boolean[]{false, true})
                {
                    if (flush)
                        flush();

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('')" + orderingClause),
                               row(bytes("foo123"), bytes("2"), bytes("2")),
                               row(bytes("foo123"), bytes("1"), bytes("1")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('')" + orderingClause),
                               row(bytes("foo123"), bytes("2"), bytes("2")),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertEmpty(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('')" + orderingClause));

                    assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('')" + orderingClause),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));
                }
            }
        }
    }

    @Test
    public void testEmptyRestrictionValueWithMultipleClusteringColumnsAndOrderBy() throws Throwable
    {
        for (String options : new String[] { "",
                " WITH COMPACT STORAGE",
                " WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)",
                " WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c1 DESC, c2 DESC)"})
        {
            String orderingClause = options.contains("ORDER") ? "" : "ORDER BY c1 DESC, c2 DESC" ;

            createTable("CREATE TABLE %s (pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))" + options);
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("2"));

            for (boolean flush : new boolean[]{false, true})
            {
                if (flush)
                    flush();

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 > textAsBlob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 > textAsBlob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 = textAsBlob('1') AND c2 >= textAsBlob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));
            }

            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                    bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4"));

            for (boolean flush : new boolean[]{false, true})
            {
                if (flush)
                    flush();

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c1 IN (textAsBlob(''), textAsBlob('1')) AND c2 = textAsBlob('1')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) IN ((textAsBlob(''), textAsBlob('1')), (textAsBlob('1'), textAsBlob('1')))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) > (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c1, c2) >= (textAsBlob(''), textAsBlob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));
            }
        }
    }
}
