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
import org.junit.Assert;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.exceptions.InvalidRequestException;

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

    @Test
    public void testSetContainsWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories set<text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, set("lmn"));

        beforeAndAfterFlush(() -> {
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
        });
    }

    @Test
    public void testListContainsWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories list<text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, list("lmn"));
        beforeAndAfterFlush(() -> {
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
        });
    }

    @Test
    public void testListContainsWithIndexAndFiltering() throws Throwable
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

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE f CONTAINS ? AND s=? allow filtering", "Dubai", 3),
                       row(4, list("Dubai"), 3),
                       row(3, list("Dubai"), 3));
        });
    }

    @Test
    public void testMapKeyContainsWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(keys(categories))");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));

        beforeAndAfterFlush(() -> {
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
        });
    }

    @Test
    public void testMapValueContainsWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));

        beforeAndAfterFlush(() -> {
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
                                 "SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ?",
                                 "test", 5, "foo", "notPresent");

            assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ? ALLOW FILTERING",
                                "test", 5, "foo", "notPresent"));
        });
    }

    // See CASSANDRA-7525
    @Test
    public void testQueryMultipleIndexTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");

        // create an index on
        createIndex("CREATE INDEX ON %s(id)");
        createIndex("CREATE INDEX ON %s(categories)");

        beforeAndAfterFlush(() -> {

            execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));

            assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS ? AND id = ? ALLOW FILTERING", "foo", 5),
                       row("test", 5, map("lmn", "foo")));

            assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND id = ? ALLOW FILTERING", "test", "foo", 5),
                       row("test", 5, map("lmn", "foo")));
        });
    }

    // See CASSANDRA-8033
    @Test
    public void testFilterWithIndexForContains() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v set<int>, PRIMARY KEY ((k1, k2)))");
        createIndex("CREATE INDEX ON %s(k2)");

        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 0, set(1, 2, 3));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 1, set(2, 3, 4));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 0, set(3, 4, 5));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 1, set(4, 5, 6));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE k2 = ?", 1),
                       row(0, 1, set(2, 3, 4)),
                       row(1, 1, set(4, 5, 6))
            );

            assertRows(execute("SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 6),
                       row(1, 1, set(4, 5, 6))
            );

            assertEmpty(execute("SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 7));
        });
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

        beforeAndAfterFlush(() -> {
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
        });
    }

    @Test
    public void testContainsKeyAndContainsWithIndexOnMapKey() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(keys(categories))");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));
        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 6, map("lmn", "foo2"));

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
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
        });
    }

    @Test
    public void testContainsKeyAndContainsWithIndexOnMapValue() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));
        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 6, map("lmn2", "foo"));

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
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
        });
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
        createTable("CREATE TABLE %s (group text, zipcode text, state text, fips_regions int, city text, PRIMARY KEY (group, zipcode, state, fips_regions))");

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

        execute("SELECT to_timestamp(t) FROM %s");
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
        rs = execute("SELECT int_as_blob(id) AS id_blob FROM %s WHERE id = 0");
        assertEquals("id_blob", rs.metadata().get(0).name.toString());
        assertEquals(ByteBuffer.wrap(new byte[4]), rs.one().getBlob(rs.metadata().get(0).name.toString()));

        // test that select throws a meaningful exception for aliases in where clause
        assertInvalidMessage("Undefined column name user_id",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE user_id = 0");

        // test that select throws a meaningful exception for aliases in order by clause
        assertInvalidMessage("Undefined column name user_name",
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

        String distinctQueryErrorMsg = "SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns.";
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

    @Test
    public void testSelectDistinctWithWhereClauseOnStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, a int, s int static, s1 int static, b int, PRIMARY KEY (k, a))");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", i, i, i, i, i);
            execute("INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", i, i * 10, i * 10, i * 10, i * 10);
        }

        execute("INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", 2, 10, 10, 10, 10);

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT DISTINCT k, s, s1 FROM %s WHERE s = 90 AND s1 = 90 ALLOW FILTERING"),
                       row(9, 90, 90));

            assertRows(execute("SELECT DISTINCT k, s, s1 FROM %s WHERE s = 90 AND s1 = 90 ALLOW FILTERING"),
                       row(9, 90, 90));

            assertRows(execute("SELECT DISTINCT k, s, s1 FROM %s WHERE s = 10 AND s1 = 10 ALLOW FILTERING"),
                       row(1, 10, 10),
                       row(2, 10, 10));

            assertRows(execute("SELECT DISTINCT k, s, s1 FROM %s WHERE k = 1 AND s = 10 AND s1 = 10 ALLOW FILTERING"),
                       row(1, 10, 10));
        });
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
        createIndex("create index on %s (field3)");

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

        assertInvalidMessage("Undefined column name user_id",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE user_id = 0");

        // test that select throws a meaningful exception for aliases in order by clause
        assertInvalidMessage("Undefined column name user_name",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE id IN (0) ORDER BY user_name");

    }

    @Test
    public void testAllowFilteringOnPartitionKeyOnStaticColumnsWithRowsWithOnlyStaticValues() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s int static, c int, d int, primary key (a, b))");

        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (a, s) VALUES (?, ?)", i, i);
            if (i != 2)
                for (int j = 0; j < 4; j++)
                    execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, j, i + j);
        }

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a >= 1 AND c = 2 AND s >= 1 ALLOW FILTERING"),
                                    row(1, 2, 1, 2, 3),
                                    row(3, 2, 3, 2, 5),
                                    row(4, 2, 4, 2, 6));

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c = 2 AND s >= 1 LIMIT 2 ALLOW FILTERING"),
                       row(1, 2, 1, 2, 3),
                       row(4, 2, 4, 2, 6));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE a >= 3 AND c = 2 AND s >= 1 LIMIT 2 ALLOW FILTERING"),
                                    row(4, 2, 4, 2, 6),
                                    row(3, 2, 3, 2, 5));
        });
    }

    @Test
    public void testFilteringOnStaticColumnsWithRowsWithOnlyStaticValues() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s int static, c int, d int, primary key (a, b))");

        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (a, s) VALUES (?, ?)", i, i);
            if (i != 2)
                for (int j = 0; j < 4; j++)
                    execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, j, i + j);
        }

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE c = 2 AND s >= 1 LIMIT 2 ALLOW FILTERING"),
                       row(1, 2, 1, 2, 3),
                       row(4, 2, 4, 2, 6));
        });
    }

    @Test
    public void testFilteringWithoutIndices() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, s int static, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 2, 4, 8)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 3, 6, 12)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 4, 4, 8)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 3, 7, 12)");
        execute("UPDATE %s SET s = 1 WHERE a = 1");
        execute("UPDATE %s SET s = 2 WHERE a = 2");
        execute("UPDATE %s SET s = 3 WHERE a = 3");

        // Adds tomstones
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 1, 4, 8)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 2, 7, 12)");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1");
        execute("DELETE FROM %s WHERE a = 2 AND b = 2");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c = 4 AND d = 8");

            assertRows(execute("SELECT * FROM %s WHERE c = 4 AND d = 8 ALLOW FILTERING"),
                       row(1, 2, 1, 4, 8),
                       row(1, 4, 1, 4, 8));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND d = 8");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND d = 8 ALLOW FILTERING"),
                       row(1, 4, 1, 4, 8));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE s = 1 AND d = 12");

            assertRows(execute("SELECT * FROM %s WHERE s = 1 AND d = 12 ALLOW FILTERING"),
                       row(1, 3, 1, 6, 12));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7)");

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING"),
                       row(1, 3, 1, 6, 12),
                       row(2, 3, 2, 7, 12));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4");

            assertRows(execute("SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       row(1, 3, 1, 6, 12),
                       row(2, 3, 2, 7, 12));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE s > 1");

            assertRows(execute("SELECT * FROM %s WHERE s > 1 ALLOW FILTERING"),
                       row(2, 3, 2, 7, 12),
                       row(3, null, 3, null, null));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= 4");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 1, 4, 8));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6");

            assertRows(execute("SELECT * FROM %s WHERE c >= 3 AND c <= 6 ALLOW FILTERING"),
                       row(1, 2, 1, 4, 8),
                       row(1, 3, 1, 6, 12),
                       row(1, 4, 1, 4, 8));

            assertRows(execute("SELECT * FROM %s WHERE s >= 1 LIMIT 2 ALLOW FILTERING"),
                       row(1, 2, 1, 4, 8),
                       row(1, 3, 1, 6, 12));
        });

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
                             "SELECT * FROM %s WHERE s > null");
        assertInvalidMessage("Unsupported null value for column s",
                             "SELECT * FROM %s WHERE s > null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column s",
                             "SELECT * FROM %s WHERE s = ? ALLOW FILTERING",
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

        beforeAndAfterFlush(() -> {

            // Checks filtering for lists
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 3 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            // Checks filtering for sets
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE d CONTAINS 4");

            assertRows(execute("SELECT * FROM %s WHERE d CONTAINS 4 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE d CONTAINS 4 AND d CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            // Checks filtering for maps
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE e CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE e CONTAINS 2 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE e CONTAINS KEY 1 ALLOW FILTERING"),
                       row(1, 2, list(1, 6), set(2, 12), map(1, 6)),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE e[1] = 6 ALLOW FILTERING"),
                       row(1, 2, list(1, 6), set(2, 12), map(1, 6)));

            assertRows(execute("SELECT * FROM %s WHERE e CONTAINS KEY 1 AND e CONTAINS 2 ALLOW FILTERING"),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND d CONTAINS 4 AND e CONTAINS KEY 3 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column d",
                             "SELECT * FROM %s WHERE d CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column e",
                             "SELECT * FROM %s WHERE e CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column e",
                             "SELECT * FROM %s WHERE e CONTAINS KEY null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null map key for column e",
                             "SELECT * FROM %s WHERE e[null] = 2 ALLOW FILTERING");
        assertInvalidMessage("Unsupported null map value for column e",
                             "SELECT * FROM %s WHERE e[1] = null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column d",
                             "SELECT * FROM %s WHERE d CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column e",
                             "SELECT * FROM %s WHERE e CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column e",
                             "SELECT * FROM %s WHERE e CONTAINS KEY ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset map key for column e",
                             "SELECT * FROM %s WHERE e[?] = 2 ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset map value for column e",
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

        beforeAndAfterFlush(() -> {

            // Checks filtering for lists
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c = [3, 2]");

            assertRows(execute("SELECT * FROM %s WHERE c = [3, 2] ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > [1, 5] AND c < [3, 6]");

            assertRows(execute("SELECT * FROM %s WHERE c > [1, 5] AND c < [3, 6] ALLOW FILTERING"),
                       row(1, 2, list(1, 6), set(2, 12), map(1, 6)),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c >= [1, 6] AND c < [3, 3] ALLOW FILTERING"),
                       row(1, 2, list(1, 6), set(2, 12), map(1, 6)),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 3 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            // Checks filtering for sets
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE d = {6, 4}");

            assertRows(execute("SELECT * FROM %s WHERE d = {6, 4} ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE d > {4, 5} AND d < {6}");

            assertRows(execute("SELECT * FROM %s WHERE d > {4, 5} AND d < {6} ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            assertRows(execute("SELECT * FROM %s WHERE d >= {2, 12} AND d <= {4, 6} ALLOW FILTERING"),
                       row(1, 2, list(1, 6), set(2, 12), map(1, 6)),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE d CONTAINS 4");

            assertRows(execute("SELECT * FROM %s WHERE d CONTAINS 4 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE d CONTAINS 4 AND d CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            // Checks filtering for maps
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE e = {1 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE e = {1 : 2} ALLOW FILTERING"),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE e > {1 : 4} AND e < {3 : 6}");

            assertRows(execute("SELECT * FROM %s WHERE e > {1 : 4} AND e < {3 : 6} ALLOW FILTERING"),
                       row(1, 2, list(1, 6), set(2, 12), map(1, 6)),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            assertRows(execute("SELECT * FROM %s WHERE e >= {1 : 6} AND e <= {3 : 2} ALLOW FILTERING"),
                       row(1, 2, list(1, 6), set(2, 12), map(1, 6)),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE e CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE e CONTAINS 2 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE e CONTAINS KEY 1 ALLOW FILTERING"),
                       row(1, 2, list(1, 6), set(2, 12), map(1, 6)),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertInvalidMessage("Map-entry equality predicates on frozen map column e are not supported",
                                 "SELECT * FROM %s WHERE e[1] = 6 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE e CONTAINS KEY 1 AND e CONTAINS 2 ALLOW FILTERING"),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND d CONTAINS 4 AND e CONTAINS KEY 3 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column d",
                             "SELECT * FROM %s WHERE d = null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column d",
                             "SELECT * FROM %s WHERE d CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column e",
                             "SELECT * FROM %s WHERE e = null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column e",
                             "SELECT * FROM %s WHERE e CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column e",
                             "SELECT * FROM %s WHERE e CONTAINS KEY null ALLOW FILTERING");
        assertInvalidMessage("Map-entry equality predicates on frozen map column e are not supported",
                             "SELECT * FROM %s WHERE e[null] = 2 ALLOW FILTERING");
        assertInvalidMessage("Map-entry equality predicates on frozen map column e are not supported",
                             "SELECT * FROM %s WHERE e[1] = null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column d",
                             "SELECT * FROM %s WHERE d = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column d",
                             "SELECT * FROM %s WHERE d CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column e",
                             "SELECT * FROM %s WHERE e = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column e",
                             "SELECT * FROM %s WHERE e CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column e",
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
    public void testIndexQueryWithValueOver64K() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c blob, PRIMARY KEY (a, b))");
        String idx = createIndex("CREATE INDEX ON %s (c)");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, bytes(1));
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, bytes(2));

        assertInvalidMessage("Index expression values may not be larger than 64K",
                             "SELECT * FROM %s WHERE c = ?  ALLOW FILTERING", TOO_BIG);

        dropIndex("DROP INDEX %s." + idx);
        assertEmpty(execute("SELECT * FROM %s WHERE c = ?  ALLOW FILTERING", TOO_BIG));
    }

    @Test
    public void testPKQueryWithValueOver64K() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, PRIMARY KEY (a, b))");

        assertInvalidThrow(InvalidRequestException.class,
                           "SELECT * FROM %s WHERE a = ?", new String(TOO_BIG.array()));
    }

    @Test
    public void testCKQueryWithValueOver64K() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, PRIMARY KEY (a, b))");

        execute("SELECT * FROM %s WHERE a = 'foo' AND b = ?", new String(TOO_BIG.array()));
    }

    @Test
    public void testAllowFilteringOnPartitionKeyWithDistinct() throws Throwable
    {
        // Test a regular(CQL3) table.
        createTable("CREATE TABLE %s (pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY((pk0, pk1), ck0))");

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 0, 0)", i, i);
            execute("INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 1, 1)", i, i);
        }

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk1 = 1 LIMIT 3");

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk0 > 0 AND pk1 = 1 LIMIT 3");

            assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s WHERE pk0 = 1 LIMIT 1 ALLOW FILTERING"),
                    row(1, 1));

            assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s WHERE pk1 = 1 LIMIT 3 ALLOW FILTERING"),
                    row(1, 1));

            assertEmpty(execute("SELECT DISTINCT pk0, pk1 FROM %s WHERE pk0 < 0 AND pk1 = 1 LIMIT 3 ALLOW FILTERING"));

            // Test selection validation.
            assertInvalidMessage("queries must request all the partition key columns",
                    "SELECT DISTINCT pk0 FROM %s ALLOW FILTERING");
            assertInvalidMessage("queries must only request partition key columns",
                    "SELECT DISTINCT pk0, pk1, ck0 FROM %s ALLOW FILTERING");
        });
    }

    @Test
    public void testAllowFilteringOnPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))");

        execute("INSERT INTO %s (a,b,c,d) VALUES (11, 12, 13, 14)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (11, 15, 16, 17)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (21, 22, 23, 24)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (31, 32, 33, 34)");

        beforeAndAfterFlush(() -> {

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE b in (11,12)");

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE a = 11");

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE a > 11");

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE a > 11 and b = 1");

            assertRows(execute("SELECT * FROM %s WHERE a = 11 ALLOW FILTERING"),
                    row(11, 12, 13, 14),
                    row(11, 15, 16, 17));

            assertRows(execute("SELECT * FROM %s WHERE a in (11) and b in (12,15,22)"),
                    row(11, 12, 13, 14),
                    row(11, 15, 16, 17));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b in (12,15,22)");

            assertRows(execute("SELECT * FROM %s WHERE a in (11) and b in (12,15,22) ALLOW FILTERING"),
                    row(11, 12, 13, 14),
                    row(11, 15, 16, 17));

            assertRows(execute("SELECT * FROM %s WHERE a in (11) ALLOW FILTERING"),
                    row(11, 12, 13, 14),
                    row(11, 15, 16, 17));

            assertRows(execute("SELECT * FROM %s WHERE b = 15 ALLOW FILTERING"),
                    row(11, 15, 16, 17));

            assertRows(execute("SELECT * FROM %s WHERE b >= 15 ALLOW FILTERING"),
                    row(11, 15, 16, 17),
                    row(31, 32, 33, 34),
                    row(21, 22, 23, 24));

            assertRows(execute("SELECT * FROM %s WHERE a >= 11 ALLOW FILTERING"),
                    row(11, 12, 13, 14),
                    row(11, 15, 16, 17),
                    row(31, 32, 33, 34),
                    row(21, 22, 23, 24));

            assertRows(execute("SELECT * FROM %s WHERE a >= 11 AND b <= 15 ALLOW FILTERING"),
                    row(11, 12, 13, 14),
                    row(11, 15, 16, 17));

            assertRows(execute("SELECT * FROM %s WHERE a <= 11 AND b >= 14 ALLOW FILTERING"),
                    row(11, 15, 16, 17));

            Object[][] res = getRows(execute("SELECT * FROM %s WHERE a < 11 ALLOW FILTERING"));
            assertEquals(0, res.length);
            res = getRows(execute("SELECT * FROM %s WHERE b > 32 ALLOW FILTERING"));
            assertEquals(0, res.length);
        });

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column a",
                             "SELECT * FROM %s WHERE a = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column a",
                             "SELECT * FROM %s WHERE a > ? ALLOW FILTERING",
                             unset());

        // No clustering key
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b)))");

        execute("INSERT INTO %s (a,b,c,d) VALUES (11, 12, 13, 14)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (11, 15, 16, 17)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (21, 22, 23, 24)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (31, 32, 33, 34)");

        beforeAndAfterFlush(() -> {

             assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE b in (11,12)");

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE a = 11");

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE a > 11");

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE a > 11 and b = 1");

            assertRows(execute("SELECT * FROM %s WHERE a = 11 ALLOW FILTERING"),
                    row(11, 12, 13, 14),
                    row(11, 15, 16, 17));

            assertRows(execute("SELECT * FROM %s WHERE a >= 11 ALLOW FILTERING"),
                    row(11, 12, 13, 14),
                    row(11, 15, 16, 17),
                    row(31, 32, 33, 34),
                    row(21, 22, 23, 24));

            assertRows(execute("SELECT * FROM %s WHERE a >= 11 AND b <= 15 ALLOW FILTERING"),
                    row(11, 12, 13, 14),
                    row(11, 15, 16, 17));

            assertRows(execute("SELECT * FROM %s WHERE a <= 11 AND b >= 14 ALLOW FILTERING"),
                    row(11, 15, 16, 17));
        });

        // ----------------------------------------------
        // one partition key
        // ----------------------------------------------
        createTable("CREATE TABLE %s (a int primary key, b int, c int)");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, 7)");

        // Adds tomstones
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (5, 2, 7)");
        execute("DELETE FROM %s WHERE a = 0");
        execute("DELETE FROM %s WHERE a = 5");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE a = 1 ALLOW FILTERING"),
                    row(1, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 ALLOW FILTERING"),
                    row(1, 2, 4),
                    row(2, 1, 6),
                    row(4, 1, 7),
                    row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b >= 2  ALLOW FILTERING"),
                    row(1, 2, 4),
                    row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b >= 2 AND c <= 4 ALLOW FILTERING"),
                    row(1, 2, 4),
                    row(3, 2, 4));
        });
    }

    @Test
    public void testAllowFilteringOnPartitionAndClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c, d))");

        execute("INSERT INTO %s (a,b,c,d,e) VALUES (11, 12, 13, 14, 15)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (11, 15, 16, 17, 18)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (21, 22, 23, 24, 25)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (31, 32, 33, 34, 35)");

        beforeAndAfterFlush(() -> {

            assertRows(execute("SELECT * FROM %s WHERE a = 11 AND b = 15 AND c = 16"),
                    row(11, 15, 16, 17, 18));

            assertInvalidMessage(
                    "Clustering column \"d\" cannot be restricted (preceding column \"c\" is restricted by a non-EQ relation)",
                    "SELECT * FROM %s WHERE a = 11 AND b = 12 AND c > 13 AND d = 14");

            assertRows(execute("SELECT * FROM %s WHERE a = 11 AND b = 15 AND c = 16 AND d > 16"),
                    row(11, 15, 16, 17, 18));

            assertRows(execute("SELECT * FROM %s WHERE a = 11 AND b = 15 AND c > 13 AND d >= 17 ALLOW FILTERING"),
                    row(11, 15, 16, 17, 18));
            assertInvalidMessage(
                    "Clustering column \"d\" cannot be restricted (preceding column \"c\" is restricted by a non-EQ relation)",
                    "SELECT * FROM %s WHERE a = 11 AND b = 12 AND c > 13 AND d > 17");

            assertRows(execute("SELECT * FROM %s WHERE c > 30 AND d >= 34 ALLOW FILTERING"),
                    row(31, 32, 33, 34, 35));

            assertRows(execute("SELECT * FROM %s WHERE a <= 11 AND c > 15 AND d >= 16 ALLOW FILTERING"),
                    row(11, 15, 16, 17, 18));

            assertRows(execute("SELECT * FROM %s WHERE a <= 11 AND b >= 15 AND c > 15 AND d >= 16 ALLOW FILTERING"),
                    row(11, 15, 16, 17, 18));

            assertRows(execute("SELECT * FROM %s WHERE a <= 100 AND b >= 15 AND c > 0 AND d <= 100 ALLOW FILTERING"),
                    row(11, 15, 16, 17, 18),
                    row(31, 32, 33, 34, 35),
                    row(21, 22, 23, 24, 25));

            assertInvalidMessage(
                    "Clustering column \"d\" cannot be restricted (preceding column \"c\" is restricted by a non-EQ relation)",
                    "SELECT * FROM %s WHERE a <= 11 AND c > 15 AND d >= 16");
        });

        // test clutering order
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c, d)) WITH CLUSTERING ORDER BY (c DESC, d ASC)");

        execute("INSERT INTO %s (a,b,c,d,e) VALUES (11, 11, 13, 14, 15)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (11, 11, 14, 17, 18)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (11, 12, 15, 14, 15)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (11, 12, 16, 17, 18)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (21, 11, 23, 24, 25)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (21, 11, 24, 34, 35)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (21, 12, 25, 24, 25)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (21, 12, 26, 34, 35)");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE b >= 12 ALLOW FILTERING"),
                                    row(11, 12, 15, 14, 15),
                                    row(11, 12, 16, 17, 18),
                                    row(21, 12, 25, 24, 25),
                                    row(21, 12, 26, 34, 35));
        });
    }

    @Test
    public void testAllowFilteringOnPartitionKeyWithoutIndicesWithCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c list<int>, d set<int>, e map<int, int>, PRIMARY KEY ((a, b)))");

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, [1, 6], {2, 12}, {1: 6})");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, [3, 2], {6, 4}, {3: 2})");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, [1, 2], {2, 4}, {1: 2})");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 3, [3, 6], {6, 12}, {3: 6})");

        beforeAndAfterFlush(() -> {

            // Checks filtering for lists
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE b < 0 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE b >= 4 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(
                    execute("SELECT * FROM %s WHERE a > 0 AND b <= 3 AND c CONTAINS 2 AND c CONTAINS 3 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            // Checks filtering for sets
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                    "SELECT * FROM %s WHERE a = 1 AND d CONTAINS 4");

            assertRows(execute("SELECT * FROM %s WHERE d CONTAINS 4 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE d CONTAINS 4 AND d CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));

            // Checks filtering for maps
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE e CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a < 2 AND b >= 3 AND e CONTAINS 2 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND e CONTAINS KEY 1 ALLOW FILTERING"),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)),
                       row(1, 2, list(1, 6), set(2, 12), map(1, 6)));

            assertRows(execute("SELECT * FROM %s WHERE a in (1) AND b in (2) AND e[1] = 6 ALLOW FILTERING"),
                       row(1, 2, list(1, 6), set(2, 12), map(1, 6)));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND e CONTAINS KEY 1 AND e CONTAINS 2 ALLOW FILTERING"),
                       row(1, 4, list(1, 2), set(2, 4), map(1, 2)));

            assertRows(
                    execute("SELECT * FROM %s WHERE a >= 1 AND b in (3) AND c CONTAINS 2 AND d CONTAINS 4 AND e CONTAINS KEY 3 ALLOW FILTERING"),
                       row(1, 3, list(3, 2), set(6, 4), map(3, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column d",
                             "SELECT * FROM %s WHERE b < 1 AND d CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column e",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column e",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e CONTAINS KEY null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null map key for column e",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e[null] = 2 ALLOW FILTERING");
        assertInvalidMessage("Unsupported null map value for column e",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e[1] = null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column d",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND d CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column e",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column e",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e CONTAINS KEY ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset map key for column e",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e[?] = 2 ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset map value for column e",
                             "SELECT * FROM %s WHERE a >= 1 AND b < 1 AND e[1] = ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testAllowFilteringOnPartitionKeyWithCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, cnt counter, PRIMARY KEY ((a, b), c))");

        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 14L, 11, 12, 13);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 21, 22, 23);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 27L, 21, 22, 26);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 34L, 31, 32, 33);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 41, 42, 43);

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt = 24"),
                       row(41, 42, 43, 24L),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 22 AND cnt = 24"),
                       row(41, 42, 43, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND b < 25 AND cnt = 24"),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND c < 25 AND cnt = 24"),
                       row(21, 22, 23, 24L));

            assertInvalidMessage(
            "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.",
            "SELECT * FROM %s WHERE a = 21 AND b > 10 AND cnt > 23 ORDER BY c DESC ALLOW FILTERING");

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a = 21 AND b = 22 AND cnt > 23 ORDER BY c DESC"),
                       row(21, 22, 26, 27L),
                       row(21, 22, 23, 24L));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt > 20 AND cnt < 30"),
                       row(41, 42, 43, 24L),
                       row(21, 22, 23, 24L),
                       row(21, 22, 26, 27L));
        });
    }

    @Test
    public void filteringOnClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a,b,c,d) VALUES (11, 12, 13, 14)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (11, 15, 16, 17)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (21, 22, 23, 24)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (31, 32, 33, 34)");

        beforeAndAfterFlush(() -> {

            assertRows(execute("SELECT * FROM %s WHERE a = 11 AND b = 15"),
                       row(11, 15, 16, 17));

            assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE a = 11 AND b > 12 AND c = 15");

            assertRows(execute("SELECT * FROM %s WHERE a = 11 AND b = 15 AND c > 15"),
                       row(11, 15, 16, 17));

            assertRows(execute("SELECT * FROM %s WHERE a = 11 AND b > 12 AND c > 13 AND d = 17 ALLOW FILTERING"),
                       row(11, 15, 16, 17));
            assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE a = 11 AND b > 12 AND c > 13 and d = 17");

            assertRows(execute("SELECT * FROM %s WHERE b > 20 AND c > 30 ALLOW FILTERING"),
                       row(31, 32, 33, 34));
            assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE b > 20 AND c > 30");

            assertRows(execute("SELECT * FROM %s WHERE b > 20 AND c < 30 ALLOW FILTERING"),
                       row(21, 22, 23, 24));
            assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE b > 20 AND c < 30");

            assertRows(execute("SELECT * FROM %s WHERE b > 20 AND c = 33 ALLOW FILTERING"),
                       row(31, 32, 33, 34));
            assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE b > 20 AND c = 33");

            assertRows(execute("SELECT * FROM %s WHERE c = 33 ALLOW FILTERING"),
                       row(31, 32, 33, 34));
            assertInvalidMessage("PRIMARY KEY column \"c\" cannot be restricted as preceding column \"b\" is not restricted",
                                 "SELECT * FROM %s WHERE c = 33");
        });

        // --------------------------------------------------
        // Clustering column within and across partition keys
        // --------------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a,b,c,d) VALUES (11, 12, 13, 14)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (11, 15, 16, 17)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (11, 18, 19, 20)");

        execute("INSERT INTO %s (a,b,c,d) VALUES (21, 22, 23, 24)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (21, 25, 26, 27)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (21, 28, 29, 30)");

        execute("INSERT INTO %s (a,b,c,d) VALUES (31, 32, 33, 34)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (31, 35, 36, 37)");
        execute("INSERT INTO %s (a,b,c,d) VALUES (31, 38, 39, 40)");

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a = 21 AND c > 23"),
                       row(21, 25, 26, 27),
                       row(21, 28, 29, 30));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a = 21 AND c > 23 ORDER BY b DESC"),
                       row(21, 28, 29, 30),
                       row(21, 25, 26, 27));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 16 and c < 36"),
                       row(11, 18, 19, 20),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27),
                       row(21, 28, 29, 30),
                       row(31, 32, 33, 34));
        });
    }

    @Test
    public void filteringWithMultiColumnSlices() throws Throwable
    {
        //----------------------------------------
        // Multi-column slices for clustering keys
        //----------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d))");

        execute("INSERT INTO %s (a,b,c,d,e) VALUES (11, 12, 13, 14, 15)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (21, 22, 23, 24, 25)");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (31, 32, 33, 34, 35)");

        beforeAndAfterFlush(() -> {

            assertRows(execute("SELECT * FROM %s WHERE b = 22 AND d = 24 ALLOW FILTERING"),
                       row(21, 22, 23, 24, 25));
            assertInvalidMessage("PRIMARY KEY column \"d\" cannot be restricted as preceding column \"c\" is not restricted",
                                 "SELECT * FROM %s WHERE b = 22 AND d = 24");

            assertRows(execute("SELECT * FROM %s WHERE (b, c) > (20, 30) AND d = 34 ALLOW FILTERING"),
                       row(31, 32, 33, 34, 35));
            assertInvalidMessage("Clustering column \"d\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE (b, c) > (20, 30) AND d = 34");
        });
    }

    @Test
    public void containsFilteringForClusteringKeys() throws Throwable
    {
        //-------------------------------------------------
        // Frozen collections filtering for clustering keys
        //-------------------------------------------------

        // first clustering column
        createTable("CREATE TABLE %s (a int, b frozen<list<int>>, c int, PRIMARY KEY (a, b, c))");
        execute("INSERT INTO %s (a,b,c) VALUES (?, ?, ?)", 11, list(1, 3), 14);
        execute("INSERT INTO %s (a,b,c) VALUES (?, ?, ?)", 21, list(2, 3), 24);
        execute("INSERT INTO %s (a,b,c) VALUES (?, ?, ?)", 21, list(3, 3), 34);

        beforeAndAfterFlush(() -> {

            assertRows(execute("SELECT * FROM %s WHERE a = 21 AND b CONTAINS 2 ALLOW FILTERING"),
                       row(21, list(2, 3), 24));
            assertInvalidMessage("Clustering columns can only be restricted with CONTAINS with a secondary index or filtering",
                                 "SELECT * FROM %s WHERE a = 21 AND b CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE b CONTAINS 2 ALLOW FILTERING"),
                       row(21, list(2, 3), 24));
            assertInvalidMessage("Clustering columns can only be restricted with CONTAINS with a secondary index or filtering",
                                 "SELECT * FROM %s WHERE b CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE b CONTAINS 3 ALLOW FILTERING"),
                       row(11, list(1, 3), 14),
                       row(21, list(2, 3), 24),
                       row(21, list(3, 3), 34));
        });

        // non-first clustering column
        createTable("CREATE TABLE %s (a int, b int, c frozen<list<int>>, d int, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 11, 12, list(1, 3), 14);
        execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 21, 22, list(2, 3), 24);
        execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 21, 22, list(3, 3), 34);

        beforeAndAfterFlush(() -> {

            assertRows(execute("SELECT * FROM %s WHERE a = 21 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(21, 22, list(2, 3), 24));
            assertInvalidMessage("Clustering columns can only be restricted with CONTAINS with a secondary index or filtering",
                                 "SELECT * FROM %s WHERE a = 21 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE b > 20 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(21, 22, list(2, 3), 24));
            assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE b > 20 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 3 ALLOW FILTERING"),
                       row(11, 12, list(1, 3), 14),
                       row(21, 22, list(2, 3), 24),
                       row(21, 22, list(3, 3), 34));
        });

        createTable("CREATE TABLE %s (a int, b int, c frozen<map<text, text>>, d int, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 11, 12, map("1", "3"), 14);
        execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 21, 22, map("2", "3"), 24);
        execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", 21, 22, map("3", "3"), 34);

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE b > 20 AND c CONTAINS KEY '2' ALLOW FILTERING"),
                       row(21, 22, map("2", "3"), 24));
            assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                                 "SELECT * FROM %s WHERE b > 20 AND c CONTAINS KEY '2'");
        });
    }

    @Test
    public void testContainsOnPartitionKey() throws Throwable
    {
        testContainsOnPartitionKey("CREATE TABLE %s (pk frozen<map<int, int>>, ck int, v int, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void testContainsOnPartitionKeyPart() throws Throwable
    {
        testContainsOnPartitionKey("CREATE TABLE %s (pk frozen<map<int, int>>, ck int, v int, PRIMARY KEY ((pk, ck)))");
    }

    private void testContainsOnPartitionKey(String schema) throws Throwable
    {
        createTable(schema);

        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", map(1, 2), 1, 1);
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", map(1, 2), 2, 2);

        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", map(1, 2, 3, 4), 1, 3);
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", map(1, 2, 3, 4), 2, 3);

        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", map(5, 6), 5, 5);
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", map(7, 8), 6, 6);

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE pk CONTAINS KEY 1");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk CONTAINS KEY 1 ALLOW FILTERING"),
                                    row(map(1, 2), 1, 1),
                                    row(map(1, 2), 2, 2),
                                    row(map(1, 2, 3, 4), 1, 3),
                                    row(map(1, 2, 3, 4), 2, 3));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk CONTAINS KEY 1 AND pk CONTAINS 4 ALLOW FILTERING"),
                                    row(map(1, 2, 3, 4), 1, 3),
                                    row(map(1, 2, 3, 4), 2, 3));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk CONTAINS KEY 1 AND pk CONTAINS KEY 3 ALLOW FILTERING"),
                                    row(map(1, 2, 3, 4), 1, 3),
                                    row(map(1, 2, 3, 4), 2, 3));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk CONTAINS KEY 1 AND v = 3 ALLOW FILTERING"),
                                    row(map(1, 2, 3, 4), 1, 3),
                                    row(map(1, 2, 3, 4), 2, 3));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk CONTAINS KEY 1 AND ck = 1 AND v = 3 ALLOW FILTERING"),
                                    row(map(1, 2, 3, 4), 1, 3));
        });
    }

    @Test
    public void filteringWithOrderClause() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d list<int>, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, 13, list(1,4));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, 23, list(2,4));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, 26, list(2,7));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, 33, list(3,4));

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT a, b, c, d FROM %s WHERE a = 21 AND c > 20 ORDER BY b DESC"),
                       row(21, 25, 26, list(2, 7)),
                       row(21, 22, 23, list(2, 4)));

            assertRows(executeFilteringOnly("SELECT a, b, c, d FROM %s WHERE a IN(21, 31) AND c > 20 ORDER BY b DESC"),
                       row(31, 32, 33, list(3, 4)),
                       row(21, 25, 26, list(2, 7)),
                       row(21, 22, 23, list(2, 4)));
        });
    }


    @Test
    public void filteringOnStaticColumnTest() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, s int static, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c, d, s) VALUES (11, 12, 13, 14, 15)");
        execute("INSERT INTO %s (a, b, c, d, s) VALUES (21, 22, 23, 24, 25)");
        execute("INSERT INTO %s (a, b, c, d, s) VALUES (21, 26, 27, 28, 29)");
        execute("INSERT INTO %s (a, b, c, d, s) VALUES (31, 32, 33, 34, 35)");
        execute("INSERT INTO %s (a, b, c, d, s) VALUES (11, 42, 43, 44, 45)");

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT a, b, c, d, s FROM %s WHERE s = 29"),
                       row(21, 22, 23, 24, 29),
                       row(21, 26, 27, 28, 29));
            assertRows(executeFilteringOnly("SELECT a, b, c, d, s FROM %s WHERE b > 22 AND s = 29"),
                       row(21, 26, 27, 28, 29));
            assertRows(executeFilteringOnly("SELECT a, b, c, d, s FROM %s WHERE b > 10 and b < 26 AND s = 29"),
                       row(21, 22, 23, 24, 29));
            assertRows(executeFilteringOnly("SELECT a, b, c, d, s FROM %s WHERE c > 10 and c < 27 AND s = 29"),
                       row(21, 22, 23, 24, 29));
            assertRows(executeFilteringOnly("SELECT a, b, c, d, s FROM %s WHERE c > 10 and c < 43 AND s = 29"),
                       row(21, 22, 23, 24, 29),
                       row(21, 26, 27, 28, 29));
            assertRows(executeFilteringOnly("SELECT a, b, c, d, s FROM %s WHERE c > 10 AND s > 15 AND s < 45"),
                       row(21, 22, 23, 24, 29),
                       row(21, 26, 27, 28, 29),
                       row(31, 32, 33, 34, 35));
            assertRows(executeFilteringOnly("SELECT a, b, c, d, s FROM %s WHERE a = 21 AND s > 15 AND s < 45 ORDER BY b DESC"),
                       row(21, 26, 27, 28, 29),
                       row(21, 22, 23, 24, 29));
            assertRows(executeFilteringOnly("SELECT a, b, c, d, s FROM %s WHERE c > 13 and d < 44"),
                       row(21, 22, 23, 24, 29),
                       row(21, 26, 27, 28, 29),
                       row(31, 32, 33, 34, 35));
        });
    }

    @Test
    public void containsFilteringOnNonClusteringColumn() throws Throwable {
        createTable("CREATE TABLE %s (a int, b int, c int, d list<int>, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, 13, list(1,4));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, 23, list(2,4));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, 26, list(2,7));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, 33, list(3,4));

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT a, b, c, d FROM %s WHERE b > 20 AND d CONTAINS 2"),
                       row(21, 22, 23, list(2, 4)),
                       row(21, 25, 26, list(2, 7)));

            assertRows(executeFilteringOnly("SELECT a, b, c, d FROM %s WHERE b > 20 AND d CONTAINS 2 AND d contains 4"),
                       row(21, 22, 23, list(2, 4)));
        });
    }

    @Test
    public void testCustomIndexWithFiltering() throws Throwable
    {
        // Test for CASSANDRA-11310 compatibility with 2i
        createTable("CREATE TABLE %s (a text, b int, c text, d int, PRIMARY KEY (a, b, c));");
        createIndex("CREATE INDEX ON %s(c)");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", "a", 0, "b", 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", "a", 1, "b", 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", "a", 2, "b", 3);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", "c", 3, "b", 4);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", "d", 4, "d", 5);

        beforeAndAfterFlush(() -> {
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a='a' AND b > 0 AND c = 'b'"),
                       row("a", 1, "b", 2),
                       row("a", 2, "b", 3));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c = 'b' AND d = 4"),
                       row("c", 3, "b", 4));
        });
    }

    @Test
    public void testFilteringWithCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c))");

        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 14L, 11, 12, 13);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 21, 22, 23);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 27L, 21, 25, 26);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 34L, 31, 32, 33);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 41, 42, 43);

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt = 24"),
                       row(21, 22, 23, 24L),
                       row(41, 42, 43, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 22 AND cnt = 24"),
                       row(41, 42, 43, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND b < 25 AND cnt = 24"),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND c < 25 AND cnt = 24"),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a = 21 AND b > 10 AND cnt > 23 ORDER BY b DESC"),
                       row(21, 25, 26, 27L),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt > 20 AND cnt < 30"),
                       row(21, 22, 23, 24L),
                       row(21, 25, 26, 27L),
                       row(41, 42, 43, 24L));
        });
    }

    private UntypedResultSet executeFilteringOnly(String statement) throws Throwable
    {
        assertInvalid(statement);
        return execute(statement + " ALLOW FILTERING");
    }

    /**
     * Check select with ith different column order. See CASSANDRA-10988
     */
    @Test
    public void testClusteringOrderWithSlice() throws Throwable
    {
        // non-compound, ASC order
        createTable("CREATE TABLE %s (a text, b int, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b ASC)");

        execute("INSERT INTO %s (a, b) VALUES ('a', 2)");
        execute("INSERT INTO %s (a, b) VALUES ('a', 3)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 2),
                   row("a", 3));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b DESC"),
                   row("a", 3),
                   row("a", 2));

        // non-compound, DESC order
        createTable("CREATE TABLE %s (a text, b int, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)");

        execute("INSERT INTO %s (a, b) VALUES ('a', 2)");
        execute("INSERT INTO %s (a, b) VALUES ('a', 3)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 3),
                   row("a", 2));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2),
                   row("a", 3));

        // compound, first column DESC order
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b DESC, c ASC)");

        execute("INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 3, 5),
                   row("a", 2, 4));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2, 4),
                   row("a", 3, 5));

        // compound, mixed order
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b ASC, c DESC)");

        execute("INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 2, 4),
                   row("a", 3, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2, 4),
                   row("a", 3, 5));
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

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 > 0 AND c1 < 5 AND c2 = 1 AND v = 3 ALLOW FILTERING;"),
                       row(1, 1, 1, 3, 3));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 > 1 AND c1 < 5 AND c2 = 1 AND v = 3 ALLOW FILTERING;"));

            assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 > 1 AND c2 > 2 AND c3 > 2 AND v = 3 ALLOW FILTERING;"),
                       row(1, 3, 3, 3, 3));

            assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 > 1 AND c2 > 2 AND c3 = 3 AND v = 3 ALLOW FILTERING;"),
                       row(1, 3, 3, 3, 3));

            assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 IN(0,1,2) AND c2 > 1 AND c2 < 1 AND v = 3 ALLOW FILTERING;"));

            assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 IN(0,1,2) AND c2 = 1 AND v = 3 ALLOW FILTERING;"),
                       row(1, 1, 1, 3, 3));

            assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND  c1 IN(0,1,2) AND c2 = 1 AND v = 3"),
                       row(1, 1, 1, 3, 3));
        });
    }

    @Test
    public void testIndexQueryWithCompositePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (p1 int, p2 int, v int, PRIMARY KEY ((p1, p2)))");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE p1 = 1 AND v = 3");
        createIndex("CREATE INDEX ON %s(v)");

        execute("INSERT INTO %s(p1, p2, v) values (?, ?, ?)", 1, 1, 3);
        execute("INSERT INTO %s(p1, p2, v) values (?, ?, ?)", 1, 2, 3);
        execute("INSERT INTO %s(p1, p2, v) values (?, ?, ?)", 2, 1, 3);

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE p1 = 1 AND v = 3 ALLOW FILTERING"),
                       row(1, 2, 3),
                       row(1, 1, 3));
        });
    }

    @Test
    public void testEmptyRestrictionValue() throws Throwable
    {
        createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY ((pk), c))");
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                bytes("foo123"), bytes("1"), bytes("1"));
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                bytes("foo123"), bytes("2"), bytes("2"));

        beforeAndAfterFlush(() -> {

            assertInvalidMessage("Key may not be empty", "SELECT * FROM %s WHERE pk = text_as_blob('');");
            assertInvalidMessage("Key may not be empty", "SELECT * FROM %s WHERE pk IN (text_as_blob(''), text_as_blob('1'));");

            assertInvalidMessage("Key may not be empty",
                                 "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                 EMPTY_BYTE_BUFFER, bytes("2"), bytes("2"));

            // Test clustering columns restrictions
            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c = text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) = (text_as_blob(''));"));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c IN (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) IN ((text_as_blob('')), (text_as_blob('1')));"),
                       row(bytes("foo123"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) > (text_as_blob(''));"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) >= (text_as_blob(''));"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("2")));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c <= text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) <= (text_as_blob(''));"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c < text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) < (text_as_blob(''));"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('') AND c < text_as_blob('');"));
        });


        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4"));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c = text_as_blob('');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) = (text_as_blob(''));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c IN (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) IN ((text_as_blob('')), (text_as_blob('1')));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) > (text_as_blob(''));"),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) >= (text_as_blob(''));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c <= text_as_blob('');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) <= (text_as_blob(''));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c < text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) < (text_as_blob(''));"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('') AND c < text_as_blob('');"));
        });

        // Test restrictions on non-primary key value
        assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND v = text_as_blob('') ALLOW FILTERING;"));

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                bytes("foo123"), bytes("3"), EMPTY_BYTE_BUFFER);

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND v = text_as_blob('') ALLOW FILTERING;"),
                       row(bytes("foo123"), bytes("3"), EMPTY_BYTE_BUFFER));
        });
    }

    @Test
    public void testEmptyRestrictionValueWithMultipleClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))");
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("2"));

        beforeAndAfterFlush(() -> {

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 = text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) = (text_as_blob('1'), text_as_blob(''));"));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 IN (text_as_blob(''), text_as_blob('1')) AND c2 = text_as_blob('1');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 IN (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) IN ((text_as_blob(''), text_as_blob('1')), (text_as_blob('1'), text_as_blob('1')));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 > text_as_blob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 > text_as_blob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) > (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 >= text_as_blob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 <= text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) <= (text_as_blob('1'), text_as_blob(''));"));
        });

        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4"));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('') AND c2 = text_as_blob('1');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) = (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 IN (text_as_blob(''), text_as_blob('1')) AND c2 = text_as_blob('1');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) IN ((text_as_blob(''), text_as_blob('1')), (text_as_blob('1'), text_as_blob('1')));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) > (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) >= (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) <= (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) < (text_as_blob(''), text_as_blob('1'));"));
        });
    }

    @Test
    public void testEmptyRestrictionValueWithOrderBy() throws Throwable
    {
        for (String options : new String[]{ "",
                                            " WITH CLUSTERING ORDER BY (c DESC)" })
        {
            String orderingClause = options.contains("ORDER") ? "" : "ORDER BY c DESC";

            createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY ((pk), c))" + options);
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"),
                    bytes("1"),
                    bytes("1"));
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"),
                    bytes("2"),
                    bytes("2"));

            beforeAndAfterFlush(() -> {

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c < text_as_blob('')" + orderingClause));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c <= text_as_blob('')" + orderingClause));
            });

            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4"));

            beforeAndAfterFlush(() -> {

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c IN (text_as_blob(''), text_as_blob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c < text_as_blob('')" + orderingClause));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c <= text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));
            });
        }
    }

    @Test
    public void testEmptyRestrictionValueWithMultipleClusteringColumnsAndOrderBy() throws Throwable
    {
        for (String options : new String[]{ "",
                                            " WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)" })
        {
            String orderingClause = options.contains("ORDER") ? "" : "ORDER BY c1 DESC, c2 DESC";

            createTable("CREATE TABLE %s (pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))" + options);
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("2"));

            beforeAndAfterFlush(() -> {

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 > text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 > text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) > (text_as_blob(''), text_as_blob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 >= text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));
            });

            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                    bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4"));

            beforeAndAfterFlush(() -> {

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 IN (text_as_blob(''), text_as_blob('1')) AND c2 = text_as_blob('1')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) IN ((text_as_blob(''), text_as_blob('1')), (text_as_blob('1'), text_as_blob('1')))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) > (text_as_blob(''), text_as_blob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) >= (text_as_blob(''), text_as_blob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));
            });
        }
    }

    @Test
    public void testWithDistinctAndJsonAsColumnName() throws Throwable
    {
        createTable("CREATE TABLE %s (distinct int, json int, value int, PRIMARY KEY(distinct, json))");
        execute("INSERT INTO %s (distinct, json, value) VALUES (0, 0, 0)");

        assertRows(execute("SELECT distinct, json FROM %s"), row(0, 0));
        assertRows(execute("SELECT distinct distinct FROM %s"), row(0));
    }

    @Test
    public void testFilteringOnDurationColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, d duration)");
        execute("INSERT INTO %s (k, d) VALUES (0, 1s)");
        execute("INSERT INTO %s (k, d) VALUES (1, 2s)");
        execute("INSERT INTO %s (k, d) VALUES (2, 1s)");

        assertRows(execute("SELECT * FROM %s WHERE d=1s ALLOW FILTERING"),
                   row(0, Duration.from("1s")),
                   row(2, Duration.from("1s")));

        assertRows(execute("SELECT * FROM %s WHERE d IN (1s, 3s) ALLOW FILTERING"),
                   row(0, Duration.from("1s")),
                   row(2, Duration.from("1s")));

        assertInvalidMessage("Slice restrictions are not supported on duration columns",
                             "SELECT * FROM %s WHERE d > 1s ALLOW FILTERING");

        assertInvalidMessage("Slice restrictions are not supported on duration columns",
                             "SELECT * FROM %s WHERE d >= 1s ALLOW FILTERING");

        assertInvalidMessage("Slice restrictions are not supported on duration columns",
                             "SELECT * FROM %s WHERE d <= 1s ALLOW FILTERING");

        assertInvalidMessage("Slice restrictions are not supported on duration columns",
                             "SELECT * FROM %s WHERE d < 1s ALLOW FILTERING");
    }

    @Test
    public void testFilteringOnListContainingDurations() throws Throwable
    {
        for (Boolean frozen : new Boolean[]{Boolean.FALSE, Boolean.TRUE})
        {
            String listType = String.format(frozen ? "frozen<%s>" : "%s", "list<duration>");

            createTable("CREATE TABLE %s (k int PRIMARY KEY, l " + listType + ")");
            execute("INSERT INTO %s (k, l) VALUES (0, [1s, 2s])");
            execute("INSERT INTO %s (k, l) VALUES (1, [2s, 3s])");
            execute("INSERT INTO %s (k, l) VALUES (2, [1s, 3s])");

            if (frozen)
            {
                assertRows(execute("SELECT * FROM %s WHERE l = [1s, 2s] ALLOW FILTERING"),
                           row(0, list(Duration.from("1s"), Duration.from("2s"))));

                assertRows(execute("SELECT * FROM %s WHERE l IN ([1s, 2s], [2s, 3s]) ALLOW FILTERING"),
                           row(1, list(Duration.from("2s"), Duration.from("3s"))),
                           row(0, list(Duration.from("1s"), Duration.from("2s"))));
            }
            else
            {
                assertInvalidMessage("Collection column 'l' (list<duration>) cannot be restricted by a 'IN' relation",
                        "SELECT * FROM %s WHERE l IN ([1s, 2s], [2s, 3s]) ALLOW FILTERING");
            }

            assertInvalidMessage("Slice restrictions are not supported on collections containing durations",
                                 "SELECT * FROM %s WHERE l > [2s, 3s] ALLOW FILTERING");

            assertInvalidMessage("Slice restrictions are not supported on collections containing durations",
                                 "SELECT * FROM %s WHERE l >= [2s, 3s] ALLOW FILTERING");

            assertInvalidMessage("Slice restrictions are not supported on collections containing durations",
                                 "SELECT * FROM %s WHERE l <= [2s, 3s] ALLOW FILTERING");

            assertInvalidMessage("Slice restrictions are not supported on collections containing durations",
                                 "SELECT * FROM %s WHERE l < [2s, 3s] ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE l CONTAINS 1s ALLOW FILTERING"),
                       row(0, list(Duration.from("1s"), Duration.from("2s"))),
                       row(2, list(Duration.from("1s"), Duration.from("3s"))));
        }
    }

    @Test
    public void testFilteringOnMapContainingDurations() throws Throwable
    {
        for (Boolean frozen : new Boolean[]{Boolean.FALSE, Boolean.TRUE})
        {
            String mapType = String.format(frozen ? "frozen<%s>" : "%s", "map<int, duration>");

            createTable("CREATE TABLE %s (k int PRIMARY KEY, m " + mapType + ")");
            execute("INSERT INTO %s (k, m) VALUES (0, {1:1s, 2:2s})");
            execute("INSERT INTO %s (k, m) VALUES (1, {2:2s, 3:3s})");
            execute("INSERT INTO %s (k, m) VALUES (2, {1:1s, 3:3s})");

            if (frozen)
            {
                assertRows(execute("SELECT * FROM %s WHERE m = {1:1s, 2:2s} ALLOW FILTERING"),
                           row(0, map(1, Duration.from("1s"), 2, Duration.from("2s"))));

                assertRows(execute("SELECT * FROM %s WHERE m IN ({1:1s, 2:2s}, {1:1s, 3:3s}) ALLOW FILTERING"),
                        row(0, map(1, Duration.from("1s"), 2, Duration.from("2s"))),
                        row(2, map(1, Duration.from("1s"), 3, Duration.from("3s"))));
            }
            else
            {
                assertInvalidMessage("Collection column 'm' (map<int, duration>) cannot be restricted by a 'IN' relation",
                        "SELECT * FROM %s WHERE m IN ({1:1s, 2:2s}, {1:1s, 3:3s}) ALLOW FILTERING");
            }

            assertInvalidMessage("Slice restrictions are not supported on collections containing durations",
                    "SELECT * FROM %s WHERE m > {1:1s, 3:3s} ALLOW FILTERING");

            assertInvalidMessage("Slice restrictions are not supported on collections containing durations",
                    "SELECT * FROM %s WHERE m >= {1:1s, 3:3s} ALLOW FILTERING");

            assertInvalidMessage("Slice restrictions are not supported on collections containing durations",
                    "SELECT * FROM %s WHERE m <= {1:1s, 3:3s} ALLOW FILTERING");

            assertInvalidMessage("Slice restrictions are not supported on collections containing durations",
                    "SELECT * FROM %s WHERE m < {1:1s, 3:3s} ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE m CONTAINS 1s ALLOW FILTERING"),
                       row(0, map(1, Duration.from("1s"), 2, Duration.from("2s"))),
                       row(2, map(1, Duration.from("1s"), 3, Duration.from("3s"))));
        }
    }

    @Test
    public void testFilteringOnTupleContainingDurations() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t tuple<int, duration>)");
        execute("INSERT INTO %s (k, t) VALUES (0, (1, 2s))");
        execute("INSERT INTO %s (k, t) VALUES (1, (2, 3s))");
        execute("INSERT INTO %s (k, t) VALUES (2, (1, 3s))");

        assertRows(execute("SELECT * FROM %s WHERE t = (1, 2s) ALLOW FILTERING"),
                   row(0, tuple(1, Duration.from("2s"))));

        assertRows(execute("SELECT * FROM %s WHERE t IN ((1, 2s), (1, 3s)) ALLOW FILTERING"),
                   row(0, tuple(1, Duration.from("2s"))),
                   row(2, tuple(1, Duration.from("3s"))));

        assertInvalidMessage("Slice restrictions are not supported on tuples containing durations",
                "SELECT * FROM %s WHERE t > (1, 2s) ALLOW FILTERING");

        assertInvalidMessage("Slice restrictions are not supported on tuples containing durations",
                "SELECT * FROM %s WHERE t >= (1, 2s) ALLOW FILTERING");

        assertInvalidMessage("Slice restrictions are not supported on tuples containing durations",
                "SELECT * FROM %s WHERE t <= (1, 2s) ALLOW FILTERING");

        assertInvalidMessage("Slice restrictions are not supported on tuples containing durations",
                "SELECT * FROM %s WHERE t < (1, 2s) ALLOW FILTERING");
    }

    @Test
    public void testFilteringOnUdtContainingDurations() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (i int, d duration)");

        for (Boolean frozen : new Boolean[]{Boolean.FALSE, Boolean.TRUE})
        {
            udt = String.format(frozen ? "frozen<%s>" : "%s", udt);

            createTable("CREATE TABLE %s (k int PRIMARY KEY, u " + udt + ")");
            execute("INSERT INTO %s (k, u) VALUES (0, {i: 1, d:2s})");
            execute("INSERT INTO %s (k, u) VALUES (1, {i: 2, d:3s})");
            execute("INSERT INTO %s (k, u) VALUES (2, {i: 1, d:3s})");

            if (frozen)
            {
                assertRows(execute("SELECT * FROM %s WHERE u = {i: 1, d:2s} ALLOW FILTERING"),
                           row(0, userType("i", 1, "d", Duration.from("2s"))));

                assertRows(execute("SELECT * FROM %s WHERE u IN ({i: 2, d:3s}, {i: 1, d:3s}) ALLOW FILTERING"),
                        row(1, userType("i", 2, "d", Duration.from("3s"))),
                        row(2, userType("i", 1, "d", Duration.from("3s"))));
            }
            else
            {
                assertInvalidMessage("Non-frozen UDT column 'u' (" + udt + ") cannot be restricted by any relation",
                        "SELECT * FROM %s WHERE u = {i: 1, d:2s} ALLOW FILTERING");

                assertInvalidMessage("Non-frozen UDT column 'u' (" + udt + ") cannot be restricted by any relation",
                        "SELECT * FROM %s WHERE u IN ({i: 2, d:3s}, {i: 1, d:3s}) ALLOW FILTERING");
            }

            assertInvalidMessage("Slice restrictions are not supported on UDTs containing durations",
                    "SELECT * FROM %s WHERE u > {i: 1, d:3s} ALLOW FILTERING");

            assertInvalidMessage("Slice restrictions are not supported on UDTs containing durations",
                    "SELECT * FROM %s WHERE u >= {i: 1, d:3s} ALLOW FILTERING");

            assertInvalidMessage("Slice restrictions are not supported on UDTs containing durations",
                    "SELECT * FROM %s WHERE u <= {i: 1, d:3s} ALLOW FILTERING");

            assertInvalidMessage("Slice restrictions are not supported on UDTs containing durations",
                    "SELECT * FROM %s WHERE u < {i: 1, d:3s} ALLOW FILTERING");
        }
    }

    @Test
    public void testFilteringOnCollectionsWithNull() throws Throwable
    {
        createTable(" CREATE TABLE %s ( k int, v int, l list<int>, s set<text>, m map<text, int>, PRIMARY KEY (k, v))");

        createIndex("CREATE INDEX ON %s (v)");
        createIndex("CREATE INDEX ON %s (s)");
        createIndex("CREATE INDEX ON %s (m)");


        execute("INSERT INTO %s (k, v, l, s, m) VALUES (0, 0, [1, 2],    {'a'},      {'a' : 1})");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (0, 1, [3, 4],    {'b', 'c'}, {'a' : 1, 'b' : 2})");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (0, 2, [1],       {'a', 'c'}, {'c' : 3})");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (1, 0, [1, 2, 4], {},         {'b' : 1})");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (1, 1, [4, 5],    {'d'},      {'a' : 1, 'b' : 3})");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (1, 2, null,      null,       null)");

        beforeAndAfterFlush(() -> {
            // lists
            assertRows(execute("SELECT k, v FROM %s WHERE l CONTAINS 1 ALLOW FILTERING"), row(1, 0), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND l CONTAINS 1 ALLOW FILTERING"), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE l CONTAINS 2 ALLOW FILTERING"), row(1, 0), row(0, 0));
            assertEmpty(execute("SELECT k, v FROM %s WHERE l CONTAINS 6 ALLOW FILTERING"));

            // sets
            assertRows(execute("SELECT k, v FROM %s WHERE s CONTAINS 'a' ALLOW FILTERING" ), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND s CONTAINS 'a' ALLOW FILTERING"), row(0, 0), row(0, 2));
            assertRows(execute("SELECT k, v FROM %s WHERE s CONTAINS 'd' ALLOW FILTERING"), row(1, 1));
            assertEmpty(execute("SELECT k, v FROM %s  WHERE s CONTAINS 'e' ALLOW FILTERING"));

            // maps
            assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS 1 ALLOW FILTERING"), row(1, 0), row(1, 1), row(0, 0), row(0, 1));
            assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS 1 ALLOW FILTERING"), row(0, 0), row(0, 1));
            assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS 2 ALLOW FILTERING"), row(0, 1));
            assertEmpty(execute("SELECT k, v FROM %s  WHERE m CONTAINS 4 ALLOW FILTERING"));

            assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS KEY 'a' ALLOW FILTERING"), row(1, 1), row(0, 0), row(0, 1));
            assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS KEY 'a' ALLOW FILTERING"), row(0, 0), row(0, 1));
            assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS KEY 'c' ALLOW FILTERING"), row(0, 2));
        });
    }

    @Test
    public void testMixedTTLOnColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        execute("INSERT INTO %s (k) VALUES (2);");
        execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL 100;");
        execute("INSERT INTO %s (k, i) VALUES (3, 3) USING TTL 100;");
        assertRows(execute("SELECT k, i FROM %s "),
                   row(1, 1),
                   row(2, null),
                   row(3, 3));

        UntypedResultSet rs = execute("SELECT k, i, ttl(i) AS name_ttl FROM %s");
        assertEquals("name_ttl", rs.metadata().get(2).name.toString());
        int i = 0;
        for (UntypedResultSet.Row row : rs)
        {
            if ( i % 2 == 0) // Every odd row has a null i/ttl
                assertTrue(row.getInt("name_ttl") >= 90 && row.getInt("name_ttl") <= 100);
            else
                assertTrue(row.has("name_ttl") == false);

            i++;
        }
    }

    @Test
    public void testMixedTTLOnColumnsWide() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, i int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c) VALUES (2, 2);");
        execute("INSERT INTO %s (k, c, i) VALUES (1, 1, 1) USING TTL 100;");
        execute("INSERT INTO %s (k, c) VALUES (1, 2) ;");
        execute("INSERT INTO %s (k, c, i) VALUES (1, 3, 3) USING TTL 100;");
        execute("INSERT INTO %s (k, c, i) VALUES (3, 3, 3) USING TTL 100;");
        assertRows(execute("SELECT k, c, i FROM %s "),
                   row(1, 1, 1),
                   row(1, 2, null),
                   row(1, 3, 3),
                   row(2, 2, null),
                   row(3, 3, 3));

        UntypedResultSet rs = execute("SELECT k, c, i, ttl(i) AS name_ttl FROM %s");
        assertEquals("name_ttl", rs.metadata().get(3).name.toString());
        int i = 0;
        for (UntypedResultSet.Row row : rs)
        {
            if ( i % 2 == 0) // Every odd row has a null i/ttl
                assertTrue(row.getInt("name_ttl") >= 90 && row.getInt("name_ttl") <= 100);
            else
                assertTrue(row.has("name_ttl") == false);

            i++;
        }
    }

    @Test // CASSANDRA-14989
    public void testTokenFctAcceptsValidArguments() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))");
        execute("INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')");
        assertRowCount(execute("SELECT token(k1, k2) FROM %s"), 1);
    }

    @Test
    public void testTokenFctRejectsInvalidColumnName() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))");
        execute("INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')");
        assertInvalidMessage("Undefined column name ", "SELECT token(s1, k1) FROM %s");
    }

    @Test
    public void testTokenFctRejectsInvalidColumnType() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))");
        execute("INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')");
        assertInvalidMessage("Type error: k2 cannot be passed as argument 0 of function system.token of type uuid",
                             "SELECT token(k2, k1) FROM %s");
    }

    @Test
    public void testTokenFctRejectsInvalidColumnCount() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))");
        execute("INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')");
        assertInvalidMessage("Invalid number of arguments in call to function system.token: 2 required but 1 provided",
                             "SELECT token(k1) FROM %s");
    }

    @Test
    public void testCreatingUDFWithSameNameAsBuiltin_PrefersCompatibleArgs_SameKeyspace() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))");
        createFunctionOverload(KEYSPACE + ".token", "double",
                               "CREATE FUNCTION %s (val double) RETURNS null ON null INPUT RETURNS double LANGUAGE java AS 'return 10.0d;'");
        execute("INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')");
        assertRows(execute("SELECT token(10) FROM %s"), row(10.0d));
    }

    @Test
    public void testCreatingUDFWithSameNameAsBuiltin_FullyQualifiedFunctionNameWorks() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))");
        createFunctionOverload(KEYSPACE + ".token", "double",
                               "CREATE FUNCTION %s (val double) RETURNS null ON null INPUT RETURNS double LANGUAGE java AS 'return 10.0d;'");
        execute("INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')");
        assertRows(execute("SELECT " + KEYSPACE + ".token(10) FROM %s"), row(10.0d));
    }

    @Test
    public void testCreatingUDFWithSameNameAsBuiltin_PrefersCompatibleArgs() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))");
        createFunctionOverload(KEYSPACE + ".token", "double",
                               "CREATE FUNCTION %s (val double) RETURNS null ON null INPUT RETURNS double LANGUAGE java AS 'return 10.0d;'");
        execute("INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')");
        assertRowCount(execute("SELECT token(k1, k2) FROM %s"), 1);
    }

    @Test
    public void testCreatingUDFWithSameNameAsBuiltin_FullyQualifiedFunctionNameWorks_SystemKeyspace() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 uuid, k2 text, PRIMARY KEY ((k1, k2)))");
        createFunctionOverload(KEYSPACE + ".token", "double",
                               "CREATE FUNCTION %s (val double) RETURNS null ON null INPUT RETURNS double LANGUAGE java AS 'return 10.0d;'");
        execute("INSERT INTO %s (k1, k2) VALUES (uuid(), 'k2')");
        assertRowCount(execute("SELECT system.token(k1, k2) FROM %s"), 1);
    }

    @Test
    public void testQuotedMapTextData() throws Throwable
    {
        createTable("CREATE TABLE " + KEYSPACE + ".t1 (id int, data text, PRIMARY KEY (id))");
        createTable("CREATE TABLE " + KEYSPACE + ".t2 (id int, data map<int, text>, PRIMARY KEY (id))");

        execute("INSERT INTO " + KEYSPACE + ".t1 (id, data) VALUES (1, 'I''m newb')");
        execute("INSERT INTO " + KEYSPACE + ".t2 (id, data) VALUES (1, {1:'I''m newb'})");

        assertRows(execute("SELECT data FROM " + KEYSPACE + ".t1"), row("I'm newb"));
        assertRows(execute("SELECT data FROM " + KEYSPACE + ".t2"), row( map(1, "I'm newb")));
    }

    @Test
    public void testQuotedSimpleCollectionsData() throws Throwable
    {
        createTable("CREATE TABLE " + KEYSPACE + ".t3 (id int, set_data set<text>, list_data list<text>, tuple_data tuple<int, text>, PRIMARY KEY (id))");

        execute("INSERT INTO " + KEYSPACE + ".t3 (id, set_data, list_data, tuple_data) values(1, {'I''m newb'}, ['I''m newb'], (1, 'I''m newb'))");

        assertRows(execute("SELECT set_data FROM " + KEYSPACE + ".t3"), row(set("I'm newb")));
        assertRows(execute("SELECT list_data FROM " + KEYSPACE + ".t3"), row(list("I'm newb")));
        assertRows(execute("SELECT tuple_data FROM " + KEYSPACE + ".t3"), row(tuple(1, "I'm newb")));
    }

    @Test
    public void testQuotedUDTData() throws Throwable
    {
        createType("CREATE TYPE " + KEYSPACE + ".random (data text)");
        createTable("CREATE TABLE " + KEYSPACE + ".t4 (id int, udt_data frozen<random>, PRIMARY KEY (id))");

        execute("INSERT INTO " + KEYSPACE + ".t4 (id, udt_data) values(1, {data: 'I''m newb'})");

        assertRows(execute("SELECT udt_data FROM " + KEYSPACE + ".t4"), row(userType("random", "I'm newb")));
    }
}
