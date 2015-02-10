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
package org.apache.cassandra.cql3;

import java.util.Arrays;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SingleColumnRelationTest extends CQLTester
{
    @Test
    public void testInvalidCollectionEqualityRelation() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b set<int>, c list<int>, d map<int, int>)");
        createIndex("CREATE INDEX ON %s (b)");
        createIndex("CREATE INDEX ON %s (c)");
        createIndex("CREATE INDEX ON %s (d)");

        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND b=?", set(0));
        assertInvalidMessage("Collection column 'c' (list<int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND c=?", list(0));
        assertInvalidMessage("Collection column 'd' (map<int, int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND d=?", map(0, 0));
    }

    @Test
    public void testInvalidCollectionNonEQRelation() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b set<int>, c int)");
        createIndex("CREATE INDEX ON %s (c)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, {0}, 0)");

        // non-EQ operators
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '>' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b > ?", set(0));
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '>=' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b >= ?", set(0));
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '<' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b < ?", set(0));
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '<=' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b <= ?", set(0));
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a 'IN' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b IN (?)", set(0));
    }

    @Test
    public void testClusteringColumnRelations() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key(a, b, c))");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4);

        testSelectQueriesWithClusteringColumnRelations();
    }

    @Test
    public void testClusteringColumnRelationsWithCompactStorage() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key(a, b, c)) WITH COMPACT STORAGE;");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4);

        testSelectQueriesWithClusteringColumnRelations();
    }

    private void testSelectQueriesWithClusteringColumnRelations() throws Throwable
    {
        assertRows(execute("select * from %s where a in (?, ?)", "first", "second"),
                   row("first", 1, 5, 1),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3),
                   row("second", 4, 8, 4));

        assertRows(execute("select * from %s where a = ? and b = ? and c in (?, ?)", "first", 2, 6, 7),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and b in (?, ?) and c in (?, ?)", "first", 2, 3, 6, 7),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and b in (?, ?) and c in (?, ?)", "first", 3, 2, 7, 6),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?)", "first", 7, 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select c, d from %s where a = ? and c in (?, ?) and b in (?, ?)", "first", 7, 6, 3, 2),
                   row(6, 2),
                   row(7, 3));

        assertRows(execute("select c, d from %s where a = ? and c in (?, ?) and b in (?, ?, ?)", "first", 7, 6, 3, 2, 3),
                   row(6, 2),
                   row(7, 3));

        assertRows(execute("select * from %s where a = ? and b in (?, ?) and c = ?", "first", 3, 2, 7),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and b in ? and c in ?",
                           "first", Arrays.asList(3, 2), Arrays.asList(7, 6)),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertInvalidMessage("Invalid null value for IN restriction",
                             "select * from %s where a = ? and b in ? and c in ?", "first", null, Arrays.asList(7, 6));

        assertRows(execute("select * from %s where a = ? and c >= ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c > ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c <= ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and c < ? and b in (?, ?)", "first", 7, 3, 2),
                   row("first", 2, 6, 2));
//---
        assertRows(execute("select * from %s where a = ? and c >= ? and c <= ? and b in (?, ?)", "first", 6, 7, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c > ? and c <= ? and b in (?, ?)", "first", 6, 7, 3, 2),
                   row("first", 3, 7, 3));

        assertEmpty(execute("select * from %s where a = ? and c > ? and c < ? and b in (?, ?)", "first", 6, 7, 3, 2));

        assertInvalidMessage("Column \"c\" cannot be restricted by both an equality and an inequality relation",
                             "select * from %s where a = ? and c > ? and c = ? and b in (?, ?)", "first", 6, 7, 3, 2);

        assertInvalidMessage("c cannot be restricted by more than one relation if it includes an Equal",
                             "select * from %s where a = ? and c = ? and c > ?  and b in (?, ?)", "first", 6, 7, 3, 2);

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b DESC",
                           "first", 7, 6, 3, 2),
                   row("first", 3, 7, 3),
                   row("first", 2, 6, 2));

        assertInvalidMessage("More than one restriction was found for the start bound on b",
                             "select * from %s where a = ? and b > ? and b > ?", "first", 6, 3, 2);

        assertInvalidMessage("More than one restriction was found for the end bound on b",
                             "select * from %s where a = ? and b < ? and b <= ?", "first", 6, 3, 2);
    }

    @Test
    public void testPartitionKeyColumnRelations() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key((a, b), c))");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 1, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 2, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 3, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 4, 4, 4);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 1, 1, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 4, 4);

        assertRows(execute("select * from %s where a = ? and b = ?", "first", 2),
                   row("first", 2, 2, 2));

        assertRows(execute("select * from %s where a in (?, ?) and b in (?, ?)", "first", "second", 2, 3),
                   row("first", 2, 2, 2),
                   row("first", 3, 3, 3));

        assertRows(execute("select * from %s where a in (?, ?) and b = ?", "first", "second", 4),
                   row("first", 4, 4, 4),
                   row("second", 4, 4, 4));

        assertRows(execute("select * from %s where a = ? and b in (?, ?)", "first", 3, 4),
                   row("first", 3, 3, 3),
                   row("first", 4, 4, 4));

        assertRows(execute("select * from %s where a in (?, ?) and b in (?, ?)", "first", "second", 1, 4),
                   row("first", 1, 1, 1),
                   row("first", 4, 4, 4),
                   row("second", 1, 1, 1),
                   row("second", 4, 4, 4));

        assertInvalidMessage("Partition key parts: b must be restricted as other parts are",
                             "select * from %s where a in (?, ?)", "first", "second");
        assertInvalidMessage("Partition key parts: b must be restricted as other parts are",
                             "select * from %s where a = ?", "first");
        assertInvalidMessage("b cannot be restricted by more than one relation if it includes a IN",
                             "select * from %s where a = ? AND b IN (?, ?) AND b = ?", "first", 2, 2, 3);
        assertInvalidMessage("b cannot be restricted by more than one relation if it includes an Equal",
                             "select * from %s where a = ? AND b = ? AND b IN (?, ?)", "first", 2, 2, 3);
        assertInvalidMessage("a cannot be restricted by more than one relation if it includes a IN",
                             "select * from %s where a IN (?, ?) AND a = ? AND b = ?", "first", "second", "first", 3);
        assertInvalidMessage("a cannot be restricted by more than one relation if it includes an Equal",
                             "select * from %s where a = ? AND a IN (?, ?) AND b IN (?, ?)", "first", "second", "first", 2, 3);
    }

    @Test
    public void testClusteringColumnRelationsWithClusteringOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key(a, b, c)) WITH CLUSTERING ORDER BY (b DESC);");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4);

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b DESC",
                           "first", 7, 6, 3, 2),
                   row("first", 3, 7, 3),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b ASC",
                           "first", 7, 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));
    }

    @Test
    public void testAllowFilteringWithClusteringColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        execute("INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 1, 2, 1);
        execute("INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 1, 3, 2);
        execute("INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 2, 2, 3);

        // Don't require filtering, always allowed
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 1),
                   row(1, 2, 1),
                   row(1, 3, 2));

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND c > ?", 1, 2), row(1, 3, 2));

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND c = ?", 1, 2), row(1, 2, 1));

        assertRows(execute("SELECT * FROM %s WHERE k = ? ALLOW FILTERING", 1),
                   row(1, 2, 1),
                   row(1, 3, 2));

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND c > ? ALLOW FILTERING", 1, 2), row(1, 3, 2));

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND c = ? ALLOW FILTERING", 1, 2), row(1, 2, 1));

        // Require filtering, allowed only with ALLOW FILTERING
        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT * FROM %s WHERE c = ?", 2);
        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT * FROM %s WHERE c > ? AND c <= ?", 2, 4);

        assertRows(execute("SELECT * FROM %s WHERE c = ? ALLOW FILTERING", 2),
                   row(1, 2, 1),
                   row(2, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE c > ? AND c <= ? ALLOW FILTERING", 2, 4), row(1, 3, 2));
    }

    @Test
    public void testAllowFilteringWithIndexedColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");
        createIndex("CREATE INDEX ON %s(a)");

        execute("INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 1, 10, 100);
        execute("INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 2, 20, 200);
        execute("INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 3, 30, 300);
        execute("INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 4, 40, 400);

        // Don't require filtering, always allowed
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 1), row(1, 10, 100));
        assertRows(execute("SELECT * FROM %s WHERE a = ?", 20), row(2, 20, 200));
        assertRows(execute("SELECT * FROM %s WHERE k = ? ALLOW FILTERING", 1), row(1, 10, 100));
        assertRows(execute("SELECT * FROM %s WHERE a = ? ALLOW FILTERING", 20), row(2, 20, 200));

        assertInvalid("SELECT * FROM %s WHERE a = ? AND b = ?");
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ? ALLOW FILTERING", 20, 200), row(2, 20, 200));
    }

    @Test
    public void testIndexQueriesOnComplexPrimaryKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 int, pk1 int, ck0 int, ck1 int, ck2 int, value int, PRIMARY KEY ((pk0, pk1), ck0, ck1, ck2))");

        createIndex("CREATE INDEX ON %s (ck1)");
        createIndex("CREATE INDEX ON %s (ck2)");
        createIndex("CREATE INDEX ON %s (pk0)");
        createIndex("CREATE INDEX ON %s (ck0)");

        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 0, 1, 2, 3, 4, 5);
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 1, 2, 3, 4, 5, 0);
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 2, 3, 4, 5, 0, 1);
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 3, 4, 5, 0, 1, 2);
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 4, 5, 0, 1, 2, 3);
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 5, 0, 1, 2, 3, 4);

        assertRows(execute("SELECT value FROM %s WHERE pk0 = 2"), row(1));
        assertRows(execute("SELECT value FROM %s WHERE ck0 = 0"), row(3));
        assertRows(execute("SELECT value FROM %s WHERE pk0 = 3 AND pk1 = 4 AND ck1 = 0"), row(2));
        assertRows(execute("SELECT value FROM %s WHERE pk0 = 5 AND pk1 = 0 AND ck0 = 1 AND ck2 = 3 ALLOW FILTERING"), row(4));
    }

    @Test
    public void testIndexOnClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (id1 int, id2 int, author text, time bigint, v1 text, v2 text, PRIMARY KEY ((id1, id2), author, time))");
        createIndex("CREATE INDEX ON %s(time)");
        createIndex("CREATE INDEX ON %s(id2)");

        execute("INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 0, 'A', 'A')");
        execute("INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 1, 'B', 'B')");
        execute("INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 1, 'bob', 2, 'C', 'C')");
        execute("INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 0, 'tom', 0, 'D', 'D')");
        execute("INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 1, 'tom', 1, 'E', 'E')");

        assertRows(execute("SELECT v1 FROM %s WHERE time = 1"), row("B"), row("E"));

        assertRows(execute("SELECT v1 FROM %s WHERE id2 = 1"), row("C"), row("E"));

        assertRows(execute("SELECT v1 FROM %s WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 0"), row("A"));

        // Test for CASSANDRA-8206
        execute("UPDATE %s SET v2 = null WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 1");

        assertRows(execute("SELECT v1 FROM %s WHERE id2 = 0"), row("A"), row("B"), row("D"));

        assertRows(execute("SELECT v1 FROM %s WHERE time = 1"), row("B"), row("E"));

        assertInvalidMessage("IN restrictions are not supported on indexed columns",
                             "SELECT v1 FROM %s WHERE id2 = 0 and time IN (1, 2) ALLOW FILTERING");
    }

    @Test
    public void testCompositeIndexWithPrimaryKey() throws Throwable
    {
        createTable("CREATE TABLE %s (blog_id int, time1 int, time2 int, author text, content text, PRIMARY KEY (blog_id, time1, time2))");

        createIndex("CREATE INDEX ON %s(author)");

        String req = "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)";
        execute(req, 1, 0, 0, "foo", "bar1");
        execute(req, 1, 0, 1, "foo", "bar2");
        execute(req, 2, 1, 0, "foo", "baz");
        execute(req, 3, 0, 1, "gux", "qux");

        assertRows(execute("SELECT blog_id, content FROM %s WHERE author='foo'"),
                   row(1, "bar1"),
                   row(1, "bar2"),
                   row(2, "baz"));
        assertRows(execute("SELECT blog_id, content FROM %s WHERE time1 > 0 AND author='foo' ALLOW FILTERING"), row(2, "baz"));
        assertRows(execute("SELECT blog_id, content FROM %s WHERE time1 = 1 AND author='foo' ALLOW FILTERING"), row(2, "baz"));
        assertRows(execute("SELECT blog_id, content FROM %s WHERE time1 = 1 AND time2 = 0 AND author='foo' ALLOW FILTERING"),
                   row(2, "baz"));
        assertEmpty(execute("SELECT content FROM %s WHERE time1 = 1 AND time2 = 1 AND author='foo' ALLOW FILTERING"));
        assertEmpty(execute("SELECT content FROM %s WHERE time1 = 1 AND time2 > 0 AND author='foo' ALLOW FILTERING"));

        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT content FROM %s WHERE time2 >= 0 AND author='foo'");
    }

    @Test
    public void testRangeQueryOnIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, row int, setid int);");
        createIndex("CREATE INDEX ON %s (setid)");

        String q = "INSERT INTO %s (id, row, setid) VALUES (?, ?, ?);";
        execute(q, 0, 0, 0);
        execute(q, 1, 1, 0);
        execute(q, 2, 2, 0);
        execute(q, 3, 3, 0);

        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT * FROM %s WHERE setid = 0 AND row < 1;");
        assertRows(execute("SELECT * FROM %s WHERE setid = 0 AND row < 1 ALLOW FILTERING;"), row(0, 0, 0));
    }

    @Test
    public void testEmptyIN() throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))" + compactOption);

            for (int i = 0; i <= 2; i++)
                for (int j = 0; j <= 2; j++)
                    execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", i, j, i + j);

            assertEmpty(execute("SELECT v FROM %s WHERE k1 IN ()"));
            assertEmpty(execute("SELECT v FROM %s WHERE k1 = 0 AND k2 IN ()"));
        }
    }

    @Test
    public void testINWithDuplicateValue() throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))" + compactOption);
            execute("INSERT INTO %s (k1,  k2, v) VALUES (?, ?, ?)", 1, 1, 1);

            assertRows(execute("SELECT * FROM %s WHERE k1 IN (?, ?)", 1, 1),
                       row(1, 1, 1));

            assertRows(execute("SELECT * FROM %s WHERE k1 IN (?, ?) AND k2 IN (?, ?)", 1, 1, 1, 1),
                       row(1, 1, 1));

            assertRows(execute("SELECT * FROM %s WHERE k1 = ? AND k2 IN (?, ?)", 1, 1, 1),
                       row(1, 1, 1));
        }
    }

    @Test
    public void testLargeClusteringINValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        List<Integer> inValues = new ArrayList<>(10000);
        for (int i = 0; i < 10000; i++)
            inValues.add(i);
        assertRows(execute("SELECT * FROM %s WHERE k=? AND c IN ?", 0, inValues),
                row(0, 0, 0));
    }

    @Test
    public void testMultiplePartitionKeyWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, f int, PRIMARY KEY ((a, b), c, d, e))");
        createIndex("CREATE INDEX ON %s (c)");
        createIndex("CREATE INDEX ON %s (f)");

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 1, 1, 2);

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 0, 0, 3);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 1, 0, 4);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 1, 1, 5);

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 2, 0, 0, 5);

        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT * FROM %s WHERE a = ? AND c = ?", 0, 1);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c = ? ALLOW FILTERING", 0, 1),
                   row(0, 0, 1, 0, 0, 3),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5));

        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT * FROM %s WHERE a = ? AND c = ? AND d = ?", 0, 1, 1);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c = ? AND d = ? ALLOW FILTERING", 0, 1, 1),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5));

        assertInvalidMessage("Partition key parts: b must be restricted as other parts are",
                             "SELECT * FROM %s WHERE a = ? AND c IN (?) AND  d IN (?) ALLOW FILTERING", 0, 1, 1);

        assertInvalidMessage("Partition key parts: b must be restricted as other parts are",
                             "SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) ALLOW FILTERING", 0, 1, 1);

        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT * FROM %s WHERE a = ? AND c IN (?) AND f = ?", 0, 1, 5);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c IN (?) AND f = ? ALLOW FILTERING", 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5));

        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND f = ?", 0, 1, 2, 5);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND f = ? ALLOW FILTERING", 0, 1, 2, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));

        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT * FROM %s WHERE a = ? AND c IN (?) AND d IN (?) AND f = ?", 0, 1, 0, 3);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c IN (?) AND d IN (?) AND f = ? ALLOW FILTERING", 0, 1, 0, 3),
                   row(0, 0, 1, 0, 0, 3));

        assertInvalidMessage("Partition key parts: b must be restricted as other parts are",
                             "SELECT * FROM %s WHERE a = ? AND c >= ? ALLOW FILTERING", 0, 1);

        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT * FROM %s WHERE a = ? AND c >= ? AND f = ?", 0, 1, 5);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c >= ? AND f = ? ALLOW FILTERING", 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));

        assertInvalidMessage("Cannot execute this query as it might involve data filtering",
                             "SELECT * FROM %s WHERE a = ? AND c = ? AND d >= ? AND f = ?", 0, 1, 1, 5);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c = ? AND d >= ? AND f = ? ALLOW FILTERING", 0, 1, 1, 5),
                   row(0, 0, 1, 1, 1, 5));
    }
}
