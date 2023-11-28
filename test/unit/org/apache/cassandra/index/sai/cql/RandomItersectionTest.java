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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;

public class RandomItersectionTest extends SAIRandomizedTester
{
    private int numRows;

    @Before
    public void createTableAndIndexes()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v1 int, v2 int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v2) USING 'sai'");

        numRows = nextInt(50000, 200000);
    }

    @Test
    public void smallPartitionPartitionRestrictedTest()
    {
        Map<Integer, List<TestRow>> testRowMap = buildAndLoadTestRows(false);

        for (int queryCount = 0; queryCount < nextInt(10, 100); queryCount++)
        {
            int pk = testRowMap.keySet().stream().skip(nextInt(0, testRowMap.size())).findFirst().get();
            int v1 = nextInt(10, numRows/10);
            int v2 = nextInt(10, numRows/50);

            List<Object[]> expected = testRowMap.get(pk)
                                                .stream()
                                                .sorted(Comparator.comparingInt(o -> o.ck))
                                                .filter(row -> row.v1 > v1 && row.v2 > v2)
                                                .map(row -> row(row.ck))
                                                .collect(Collectors.toList());

            assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = ? AND v1 > ? AND v2 > ?", pk, v1, v2), expected.toArray(new Object[][]{}));
        }
    }

    @Test
    public void smallPartitionUnrestrictedTest()
    {
        Map<Integer, List<TestRow>> testRowMap = buildAndLoadTestRows(false);

        for (int queryCount = 0; queryCount < nextInt(10, 100); queryCount++)
        {
            int v1 = nextInt(10, numRows/10);
            int v2 = nextInt(10, numRows/50);

            List<Object[]> expected = testRowMap.values()
                                                .stream()
                                                .flatMap(list -> list.stream())
                                                .filter(row -> row.v1 == v1 && row.v2 == v2)
                                                .map(row -> row(row.ck))
                                                .collect(Collectors.toList());

            assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE v1 = ? AND v2 = ?", v1, v2), expected.toArray(new Object[][]{}));
        }
    }

    @Test
    public void largePartitionPartitionRestrictedTest()
    {
        Map<Integer, List<TestRow>> testRowMap = buildAndLoadTestRows(true);

        for (int queryCount = 0; queryCount < nextInt(10, 100); queryCount++)
        {
            int pk = testRowMap.keySet().stream().skip(nextInt(0, testRowMap.size())).findFirst().get();
            int v1 = nextInt(10, numRows/10);
            int v2 = nextInt(10, numRows/50);

            List<Object[]> expected = testRowMap.get(pk)
                                                .stream()
                                                .sorted(Comparator.comparingInt(o -> o.ck))
                                                .filter(row -> row.v1 > v1 && row.v2 > v2)
                                                .map(row -> row(row.ck))
                                                .collect(Collectors.toList());

            assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = ? AND v1 > ? AND v2 > ?", pk, v1, v2), expected.toArray(new Object[][]{}));
        }
    }

    @Test
    public void largePartitionUnrestrictedTest()
    {
        Map<Integer, List<TestRow>> testRowMap = buildAndLoadTestRows(true);

        for (int queryCount = 0; queryCount < nextInt(10, 100); queryCount++)
        {
            int v1 = nextInt(10, numRows/10);
            int v2 = nextInt(10, numRows/50);

            List<Object[]> expected = testRowMap.values()
                                                .stream()
                                                .flatMap(list -> list.stream())
                                                .filter(row -> row.v1 == v1 && row.v2 == v2)
                                                .map(row -> row(row.ck))
                                                .collect(Collectors.toList());

            assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE v1 = ? AND v2 = ?", v1, v2), expected.toArray(new Object[][]{}));
        }
    }

    private Map<Integer, List<TestRow>> buildAndLoadTestRows(boolean largeClusters)
    {
        Map<Integer, List<TestRow>> testRowMap = new HashMap<>();

        int clusterSize = largeClusters ? nextInt(500, 5000) : nextInt(10, 100);
        int partition = nextInt(0, numRows);
        while (testRowMap.containsKey(partition))
            partition = nextInt(0, numRows);
        List<TestRow> rowList = new ArrayList<>(clusterSize);
        testRowMap.put(partition, rowList);
        int clusterCount = 0;
        for (int index = 0; index < numRows; index++)
        {
            TestRow row = new TestRow(partition, nextInt(10, numRows), nextInt(10, numRows/10), nextInt(10, numRows/50));
            while (rowList.contains(row))
                row = new TestRow(partition, nextInt(10, numRows), nextInt(10, numRows/10), nextInt(10, numRows/50));

            rowList.add(row);
            clusterCount++;
            if (clusterCount == clusterSize)
            {
                clusterCount = 0;
                clusterSize = largeClusters ? nextInt(500, 5000) : nextInt(10, 100);
                partition = nextInt(0, numRows);
                while (testRowMap.containsKey(partition))
                    partition = nextInt(0, numRows);
                rowList = new ArrayList<>(clusterSize);
                testRowMap.put(partition, rowList);
            }
        }
        testRowMap.values().stream().flatMap(val -> val.stream()).forEach(row -> execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)",
                                                                                         row.pk, row.ck, row.v1, row.v2));
        flush();
        return testRowMap;
    }

    private static class TestRow implements Comparable<TestRow>
    {
        final int pk;
        final int ck;
        final int v1;
        final int v2;

        TestRow(int pk, int ck, int v1, int v2)
        {
            this.pk = pk;
            this.ck = ck;
            this.v1 = v1;
            this.v2 = v2;
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
