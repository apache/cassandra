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

public class SingleColumnRelationTest extends CQLTester
{
    @Test
    public void testInvalidCollectionEqualityRelation() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b set<int>, c list<int>, d map<int, int>)");
        createIndex("CREATE INDEX ON %s (b)");
        createIndex("CREATE INDEX ON %s (c)");
        createIndex("CREATE INDEX ON %s (d)");

        assertInvalid("SELECT * FROM %s WHERE a = 0 AND b=?", set(0));
        assertInvalid("SELECT * FROM %s WHERE a = 0 AND c=?", list(0));
        assertInvalid("SELECT * FROM %s WHERE a = 0 AND d=?", map(0, 0));
    }

    @Test
    public void testInvalidCollectionNonEQRelation() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b set<int>, c int)");
        createIndex("CREATE INDEX ON %s (c)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, {0}, 0)");

        // non-EQ operators
        assertInvalid("SELECT * FROM %s WHERE c = 0 AND b > ?", set(0));
        assertInvalid("SELECT * FROM %s WHERE c = 0 AND b >= ?", set(0));
        assertInvalid("SELECT * FROM %s WHERE c = 0 AND b < ?", set(0));
        assertInvalid("SELECT * FROM %s WHERE c = 0 AND b <= ?", set(0));
        assertInvalid("SELECT * FROM %s WHERE c = 0 AND b IN (?)", set(0));
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

        assertInvalid("select * from %s where a = ? and b in ? and c in ?", "first", null, Arrays.asList(7, 6));

        assertRows(execute("select * from %s where a = ? and c >= ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c > ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c <= ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and c < ? and b in (?, ?)", "first", 7, 3, 2),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b DESC",
                           "first", 7, 6, 3, 2),
                   row("first", 3, 7, 3),
                   row("first", 2, 6, 2));
    }

    @Test
    public void testPartitionKeyColumnRelations() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key((a, b), c))");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 1, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 2, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 3, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 4, 4);

        assertInvalid("select * from %s where a in (?, ?)", "first", "second");
        assertInvalid("select * from %s where a in (?, ?) and b in (?, ?)", "first", "second", 2, 3);
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
}
