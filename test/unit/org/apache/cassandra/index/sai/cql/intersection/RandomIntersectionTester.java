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

package org.apache.cassandra.index.sai.cql.intersection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;

public abstract class RandomIntersectionTester extends SAIRandomizedTester
{
    private static final Object[][] EMPTY_ROWS = new Object[][]{};

    protected enum Mode { REGULAR, STATIC, REGULAR_STATIC, TWO_REGULAR_ONE_STATIC }

    @Parameterized.Parameter
    public String testName;

    @Parameterized.Parameter(1)
    public boolean partitionRestricted;

    @Parameterized.Parameter(2)
    public boolean largePartition;

    @Parameterized.Parameter(3)
    public boolean v1Cardinality;

    @Parameterized.Parameter(4)
    public boolean v2Cardinality;

    @Parameterized.Parameter(5)
    public Mode mode;

    private int numPartitions;

    @Before
    public void createTableAndIndexes()
    {
        CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(3);

        createTable("CREATE TABLE %s (pk int, ck int, v1 int, v2 int, s1 int static, s2 int static, PRIMARY KEY(pk, ck))");
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v2) USING 'sai'");
        createIndex("CREATE INDEX ON %s(s1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(s2) USING 'sai'");

        numPartitions = nextInt(15000, 100000);
    }

    protected void runRestrictedQueries() throws Throwable
    {
        Map<Integer, List<TestRow>> testRowMap = buildAndLoadTestRows();

        beforeAndAfterFlush(() -> {
            for (int queryCount = 0; queryCount < nextInt(10, 100); queryCount++)
            {
                int pk = testRowMap.keySet().stream().skip(nextInt(0, testRowMap.size())).findFirst().orElseThrow();
                int v1 = nextV1();
                int v2 = nextV2();

                Predicate<TestRow> predicate = null;

                if (mode == Mode.REGULAR)
                    predicate = row -> row.v1 > v1 && row.v2 > v2;
                else if (mode == Mode.STATIC)
                    predicate = row -> row.s1 > v1 && row.s2 > v2;
                else if (mode == Mode.REGULAR_STATIC)
                    predicate = row -> row.v1 > v1 && row.s2 > v2;
                else if (mode == Mode.TWO_REGULAR_ONE_STATIC)
                    predicate = row -> row.v1 > v1 && row.v2 > v2 && row.s2 > v2;

                assert predicate != null : "Predicate should be assigned!";

                List<Object[]> expected = testRowMap.get(pk)
                                                    .stream()
                                                    .sorted(Comparator.comparingInt(o -> o.ck))
                                                    .filter(predicate)
                                                    .map(row -> row(row.pk, row.ck))
                                                    .collect(Collectors.toList());

                UntypedResultSet result = null;

                if (mode == Mode.REGULAR)
                    result = execute("SELECT pk, ck FROM %s WHERE pk = ? AND v1 > ? AND v2 > ?", pk, v1, v2);
                else if (mode == Mode.STATIC)
                    result = execute("SELECT pk, ck FROM %s WHERE pk = ? AND s1 > ? AND s2 > ?", pk, v1, v2);
                else if (mode == Mode.REGULAR_STATIC)
                    result = execute("SELECT pk, ck FROM %s WHERE pk = ? AND v1 > ? AND s2 > ?", pk, v1, v2);
                else if (mode == Mode.TWO_REGULAR_ONE_STATIC)
                    result = execute("SELECT pk, ck FROM %s WHERE pk = ? AND v1 > ? AND v2 > ? AND s2 > ?", pk, v1, v2, v2);

                assertRows(result, expected.toArray(EMPTY_ROWS));
            }
        });
    }

    protected void runUnrestrictedQueries() throws Throwable
    {
        Map<Integer, List<TestRow>> testRowMap = buildAndLoadTestRows();

        beforeAndAfterFlush(() -> {
            for (int queryCount = 0; queryCount < nextInt(10, 100); queryCount++)
            {
                int v1 = nextV1();
                int v2 = nextV2();

                Predicate<TestRow> predicate = null;
                
                if (mode == Mode.REGULAR)
                    predicate = row -> row.v1 == v1 && row.v2 > v2;
                else if (mode == Mode.STATIC)
                    predicate = row -> row.s1 > v1 && row.s2 > v2;
                else if (mode == Mode.REGULAR_STATIC)
                    predicate = row -> row.v1 == v1 && row.s2 > v2;
                else if (mode == Mode.TWO_REGULAR_ONE_STATIC)
                    predicate = row -> row.v1 == v1 && row.v2 > v2 && row.s2 > v2;
                
                assert predicate != null : "Predicate should be assigned!";
                
                List<Object[]> expected = testRowMap.values()
                                                    .stream()
                                                    .flatMap(Collection::stream)
                                                    .filter(predicate)
                                                    .map(row -> row(row.pk, row.ck))
                                                    .collect(Collectors.toList());

                UntypedResultSet result = null;
                
                if (mode == Mode.REGULAR)
                    result = execute("SELECT pk, ck FROM %s WHERE v1 = ? AND v2 > ?", v1, v2);
                else if (mode == Mode.STATIC)
                    result = execute("SELECT pk, ck FROM %s WHERE s1 > ? AND s2 > ?", v1, v2);
                else if (mode == Mode.REGULAR_STATIC)
                    result = execute("SELECT pk, ck FROM %s WHERE v1 = ? AND s2 > ?", v1, v2);
                else if (mode == Mode.TWO_REGULAR_ONE_STATIC)
                    result = execute("SELECT pk, ck FROM %s WHERE v1 = ? AND v2 > ? AND s2 > ?", v1, v2, v2);

                assertRowsIgnoringOrder(result, expected.toArray(EMPTY_ROWS));
            }
        });
    }

    private Map<Integer, List<TestRow>> buildAndLoadTestRows()
    {
        Map<Integer, List<TestRow>> testRowMap = new HashMap<>();

        int clusterSize = nextPartitionSize();
        int partition = nextInt(0, numPartitions);
        int s1 = nextV1();
        int s2 = nextV2();
        List<TestRow> rowList = new ArrayList<>(clusterSize);
        testRowMap.put(partition, rowList);
        int clusterCount = 0;

        for (int index = 0; index < numPartitions; index++)
        {
            TestRow row = new TestRow(partition, nextInt(10, numPartitions), nextV1(), nextV2(), s1, s2);
            while (rowList.contains(row))
                row = new TestRow(partition, nextInt(10, numPartitions), nextV1(), nextV2(), s1, s2);

            rowList.add(row);
            clusterCount++;

            if (clusterCount == clusterSize)
            {
                clusterCount = 0;
                clusterSize = nextPartitionSize();
                partition = nextInt(0, numPartitions);
                while (testRowMap.containsKey(partition))
                    partition = nextInt(0, numPartitions);
                rowList = new ArrayList<>(clusterSize);
                testRowMap.put(partition, rowList);
            }
        }
       
        testRowMap.values().stream().flatMap(Collection::stream).forEach(row -> {
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", row.pk, row.ck, row.v1, row.v2);
            execute("INSERT INTO %s (pk, s1, s2) VALUES (?, ?, ?)", row.pk, row.s1, row.s2);
        });

        return testRowMap;
    }

    private int nextPartitionSize()
    {
        return largePartition ? nextInt(1024, 4096) : nextInt(1, 64);
    }

    private int nextV1()
    {
        return v1Cardinality ? nextInt(10, numPartitions / 10) : nextInt(10, numPartitions / 1000);
    }

    private int nextV2()
    {
        return v2Cardinality ? nextInt(10, numPartitions / 10) : nextInt(10, numPartitions / 1000);
    }

    private static class TestRow implements Comparable<TestRow>
    {
        final int pk;
        final int ck;
        final int v1;
        final int v2;
        final int s1;
        final int s2;

        TestRow(int pk, int ck, int v1, int v2, int s1, int s2)
        {
            this.pk = pk;
            this.ck = ck;
            this.v1 = v1;
            this.v2 = v2;
            this.s1 = s1;
            this.s2 = s2;
        }

        @Override
        public int compareTo(TestRow other)
        {
            int cmp = Integer.compare(pk, other.pk);
            if (cmp != 0)
                return cmp;
            return Integer.compare(ck, other.ck);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof TestRow)
                return compareTo((TestRow) obj) == 0;

            return false;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(pk, ck);
        }
    }
}
