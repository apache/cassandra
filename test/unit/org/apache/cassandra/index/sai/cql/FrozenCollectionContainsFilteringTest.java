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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

/**
 * CONTAINS / CONTAINS KEY restrictions on an unindexed frozen collection, post-filtered while an SAI index on another
 * column drives the query. See {@link FrozenCollectionUnindexedContainsTest} for the per-type matrix.
 */
public class FrozenCollectionContainsFilteringTest extends SAITester
{
    @Test
    public void testMultipleContainsOnSameColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c frozen<set<int>>, v int)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (k, c, v) VALUES (0, {1, 2}, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, {2, 3}, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (2, {1, 2, 3}, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (3, {1, 2, 3}, 2)");

        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS 2 AND c CONTAINS 3 ALLOW FILTERING"), row(1), row(2));
        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS 1 AND c CONTAINS 3 ALLOW FILTERING"), row(2));
        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS 4 ALLOW FILTERING"));
    }

    @Test
    public void testNullAndMissingCollection() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c frozen<list<int>>, v int)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (k, v) VALUES (0, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, [1, 2], 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (2, null, 1)");

        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS 2 ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS 3 ALLOW FILTERING"));
    }

    @Test
    public void testMapContainsKeyAndValueTogether() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c frozen<map<int, int>>, v int)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (k, c, v) VALUES (0, {1: 10}, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, {1: 20}, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (2, {2: 10}, 1)");

        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS KEY 1 AND c CONTAINS 10 ALLOW FILTERING"), row(0));
        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS KEY 2 ALLOW FILTERING"), row(2));
        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS 20 ALLOW FILTERING"), row(1));
    }

    @Test
    public void testDescendingClusteringColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c frozen<set<int>>, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (k, c, v) VALUES (0, {1}, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, {2}, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (2, {2}, 2)");

        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS 2 ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS 3 ALLOW FILTERING"));
    }

    @Test
    public void testDescendingMapClusteringColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c frozen<map<int, int>>, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (k, c, v) VALUES (0, {1: 10}, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, {2: 20}, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (2, {2: 20}, 2)");

        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS KEY 2 ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT k FROM %s WHERE v = 1 AND c CONTAINS 20 ALLOW FILTERING"), row(1));
    }
}
