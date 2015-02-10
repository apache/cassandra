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
    public void testLargeClusteringINValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        List<Integer> inValues = new ArrayList<>(10000);
        for (int i = 0; i < 10000; i++)
            inValues.add(i);
        assertRows(execute("SELECT * FROM %s WHERE k=? AND c IN ?", 0, inValues),
                row(0, 0, 0)
        );
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

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c = ? ALLOW FILTERING", 0, 1),
                   row(0, 0, 1, 0, 0, 3),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c = ? AND d = ? ALLOW FILTERING", 0, 1, 1),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5));

        assertInvalidMessage("Partition key part b must be restricted since preceding part is",
                             "SELECT * FROM %s WHERE a = ? AND c >= ? ALLOW FILTERING", 0, 1);

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c >= ? AND f = ? ALLOW FILTERING", 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c = ? AND d >= ? AND f = ? ALLOW FILTERING", 0, 1, 1, 5),
                   row(0, 0, 1, 1, 1, 5));

        assertInvalidMessage("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING",
                             "SELECT * FROM %s WHERE a = ? AND d >= ? AND f = ?", 0, 1, 5);
    }
}
