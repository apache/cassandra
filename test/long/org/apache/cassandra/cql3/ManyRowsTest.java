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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

public class ManyRowsTest extends CQLTester
{
    /**
     * Migrated from cql_tests.py:TestCQL.large_count_test()
     */
    @Test
    public void testLargeCount() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k))");

        // We know we page at 10K, so test counting just before, at 10K, just after and
        // a bit after that.
        for (int k = 1; k < 10000; k++)
            execute("INSERT INTO %s (k) VALUES (?)", k);

        assertRows(execute("SELECT COUNT(*) FROM %s"), row(9999L));

        execute("INSERT INTO %s (k) VALUES (?)", 10000);

        assertRows(execute("SELECT COUNT(*) FROM %s"), row(10000L));

        execute("INSERT INTO %s (k) VALUES (?)", 10001);

        assertRows(execute("SELECT COUNT(*) FROM %s"), row(10001L));

        for (int k = 10002; k < 15001; k++)
            execute("INSERT INTO %s (k) VALUES (?)", k);

        assertRows(execute("SELECT COUNT(*) FROM %s"), row(15000L));
    }

    /**
     * Test for CASSANDRA-8410,
     * migrated from cql_tests.py:TestCQL.large_clustering_in_test()
     */
    @Test
    public void testLargeClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c) )");

        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 0, 0);

        // try to fetch one existing row and 9999 non-existing rows
        List<Integer> inValues = new ArrayList(10000);
        for (int i = 0; i < 10000; i++)
            inValues.add(i);

        assertRows(execute("SELECT * FROM %s WHERE k=? AND c IN ?", 0, inValues),
                   row(0, 0, 0));

        // insert approximately 1000 random rows between 0 and 10k
        Random rnd = new Random();
        Set<Integer> clusteringValues = new HashSet<>();
        for (int i = 0; i < 1000; i++)
            clusteringValues.add(rnd.nextInt(10000));

        clusteringValues.add(0);

        for (int i : clusteringValues) // TODO - this was done in parallel by dtests
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, i, i);

        assertRowCount(execute("SELECT * FROM %s WHERE k=? AND c IN ?", 0, inValues), clusteringValues.size());
    }
}
