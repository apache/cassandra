/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.cql;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;

public class PartitionRestrictedQueryTest extends SAITester
{
    private static final int LARGE_ROWS = 4096;
    private static final int LARGE_PARTITIONS = 64;
    private static final int LARGE_ROWS_PER_PARTITION = LARGE_ROWS / LARGE_PARTITIONS;
    private static final int LARGE_PARTITIONS_QUERIED = 16;

    private static final int SMALL_ROWS = 512;
    private static final int SMALL_PARTITIONS = 16;
    private static final int SMALL_ROWS_PER_PARTITION = SMALL_ROWS / SMALL_PARTITIONS;
    private static final int SMALL_PARTITIONS_QUERIED = 8;

    private static final String QUERY_TEMPLATE = "SELECT * FROM %s WHERE pk = %d AND value >= %d AND value < %d LIMIT %d";
    private static final String FILTERING_TEMPLATE = "SELECT * FROM %s WHERE pk = %d AND value >= %d AND value < %d LIMIT %d ALLOW FILTERING";

    private static final int DEFAULT_LIMIT = 10;

    private static final Random RANDOM = new Random(System.currentTimeMillis());

    private String largeTable;
    private String largeReferenceTable;
    private String smallTable;
    private String smallReferenceTable;

    @Before
    public void setup() throws Throwable
    {
        largeTable = createTable("CREATE TABLE %s (pk int, ck int, value int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = {'bkd_postings_skip' : 1, 'bkd_postings_min_leaves' : 2}");
        largeReferenceTable = createTable("CREATE TABLE %s (pk int, ck int, value int, PRIMARY KEY (pk, ck))");

        smallTable = createTable("CREATE TABLE %s (pk int, ck int, value int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = {'bkd_postings_skip' : 1, 'bkd_postings_min_leaves' : 2}");
        smallReferenceTable = createTable("CREATE TABLE %s (pk int, ck int, value int, PRIMARY KEY (pk, ck))");

        String template = "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)";

        // Stripe the values from 0 -> (LARGE_ROWS - 1) across LARGE_PARTITIONS partitions. This makes it
        // possible to easily query slices that span all partitions.
        for (int pk = 0; pk < LARGE_PARTITIONS; pk++)
        {
            for (int ck = 0; ck < LARGE_ROWS_PER_PARTITION; ck++)
            {
                int value = pk + (ck * LARGE_PARTITIONS);

                // Write to both the indexed and reference tables:
                execute(String.format(template, KEYSPACE + "." + largeTable), pk, ck, value);
                execute(String.format(template, KEYSPACE + "." + largeReferenceTable), pk, ck, value);
            }
        }

        // Stripe the values from 0 -> (SMALL_ROWS - 1) across SMALL_PARTITIONS partitions. This makes it
        // possible to easily query slices that span all partitions.
        for (int pk = 0; pk < SMALL_PARTITIONS; pk++)
        {
            for (int ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
            {
                int value = pk + (ck * SMALL_PARTITIONS);

                // Write to both the indexed and reference tables:
                execute(String.format(template, KEYSPACE + "." + smallTable), pk, ck, value);
                execute(String.format(template, KEYSPACE + "." + smallReferenceTable), pk, ck, value);
            }
        }
    }

    @Test
    public void shouldQueryLargeNumericRangeInSinglePartition() throws Throwable
    {
        for (Pair<Integer, Integer> scenario : buildScenarios(LARGE_ROWS))
        {
            for (int i = 0; i < LARGE_PARTITIONS_QUERIED; i++)
            {
                verifyPartition(largeTable, largeReferenceTable, scenario.right, scenario.left, LARGE_PARTITIONS, LARGE_ROWS_PER_PARTITION);
            }

            flush(KEYSPACE, largeTable);

            for (int i = 0; i < LARGE_PARTITIONS_QUERIED; i++)
            {
                verifyPartition(largeTable, largeReferenceTable, scenario.right, scenario.left, LARGE_PARTITIONS, LARGE_ROWS_PER_PARTITION);
            }
        }
    }

    @Test
    public void shouldQuerySmallNumericRangeInSinglePartition() throws Throwable
    {
        for (Pair<Integer, Integer> scenario : buildScenarios(SMALL_ROWS))
        {
            for (int i = 0; i < SMALL_PARTITIONS_QUERIED; i++)
            {
                verifyPartition(smallTable, smallReferenceTable, scenario.right, scenario.left, SMALL_PARTITIONS, SMALL_ROWS_PER_PARTITION);
            }

            flush(KEYSPACE, smallTable);

            for (int i = 0; i < SMALL_PARTITIONS_QUERIED; i++)
            {
                verifyPartition(smallTable, smallReferenceTable, scenario.right, scenario.left, SMALL_PARTITIONS, SMALL_ROWS_PER_PARTITION);
            }
        }
    }

    @Test
    public void testCount() throws Throwable
    {
        ResultSet indexedRows = executeNet(String.format("SELECT count(*) FROM %s WHERE pk = %d AND value >= %d", KEYSPACE + "." + smallTable, 0, SMALL_ROWS / 2));
        ResultSet filteredRows = executeNet(String.format("SELECT count(*) FROM %s WHERE pk = %d AND value >= %d ALLOW FILTERING", KEYSPACE + "." + smallReferenceTable, 0, SMALL_ROWS / 2));
        assertEquals(filteredRows.one().getLong(0), indexedRows.one().getLong(0));
    }

    @Test
    public void testSum() throws Throwable
    {
        ResultSet indexedRows = executeNet(String.format("SELECT sum(value) FROM %s WHERE pk = %d AND value >= %d", KEYSPACE + "." + smallTable, 0, SMALL_ROWS / 2));
        ResultSet filteredRows = executeNet(String.format("SELECT sum(value) FROM %s WHERE pk = %d AND value >= %d ALLOW FILTERING", KEYSPACE + "." + smallReferenceTable, 0, SMALL_ROWS / 2));
        assertEquals(filteredRows.one().getInt(0), indexedRows.one().getInt(0));
    }

    @Test
    public void testAverage() throws Throwable
    {
        ResultSet indexedRows = executeNet(String.format("SELECT avg(value) FROM %s WHERE pk = %d AND value >= %d", KEYSPACE + "." + smallTable, 0, SMALL_ROWS / 2));
        ResultSet filteredRows = executeNet(String.format("SELECT avg(value) FROM %s WHERE pk = %d AND value >= %d ALLOW FILTERING", KEYSPACE + "." + smallReferenceTable, 0, SMALL_ROWS / 2));
        assertEquals(filteredRows.one().getInt(0), indexedRows.one().getInt(0));
    }

    private void verifyPartition(String table, String referenceTable, int max, int min, int numPartitions, int rowsPerPartition) throws Throwable
    {
        int pk = RANDOM.nextInt(numPartitions);

        // Compare the index result w/ the result of an equivalent ALLOW FILTERING query:
        ResultSet indexedRows = executeNet(String.format(QUERY_TEMPLATE, KEYSPACE + "." + table, pk, min, max, DEFAULT_LIMIT));
        ResultSet filteredRows = executeNet(String.format(FILTERING_TEMPLATE, KEYSPACE + "." + referenceTable, pk, min, max, DEFAULT_LIMIT));
        verifyRowValues(filteredRows.all(), indexedRows.all());

        // Then do the same thing with a DEFAULT_LIMIT high enough to exhaust the partition:
        indexedRows = executeNet(String.format(QUERY_TEMPLATE, KEYSPACE + "." + table, pk, min, max, rowsPerPartition + 1));
        filteredRows = executeNet(String.format(FILTERING_TEMPLATE, KEYSPACE + "." + referenceTable, pk, min, max, rowsPerPartition + 1));
        verifyRowValues(filteredRows.all(), indexedRows.all());
    }

    private void verifyRowValues(List<Row> expected, List<Row> actual)
    {
        assertEquals(expected.size(), actual.size());

        for (int i = 0; i < expected.size(); i++)
        {
            assertEquals(expected.get(i).getInt("pk"), actual.get(i).getInt("pk"));
            assertEquals(expected.get(i).getInt("ck"), actual.get(i).getInt("ck"));
            assertEquals(expected.get(i).getInt("value"), actual.get(i).getInt("value"));
        }
    }

    private List<Pair<Integer, Integer>> buildScenarios(int numRows)
    {

        List<Pair<Integer, Integer>> scenarios = new LinkedList<>();

        scenarios.add(Pair.create(numRows / 16, numRows));
        scenarios.add(Pair.create(numRows / 8, numRows));
        scenarios.add(Pair.create(numRows / 4, numRows));
        scenarios.add(Pair.create(numRows / 2, numRows));

        scenarios.add(Pair.create(0, numRows / 16));
        scenarios.add(Pair.create(0, numRows / 8));
        scenarios.add(Pair.create(0, numRows / 4));
        scenarios.add(Pair.create(0, numRows / 2));

        scenarios.add(Pair.create(0, numRows));

        return scenarios;
    }
}
