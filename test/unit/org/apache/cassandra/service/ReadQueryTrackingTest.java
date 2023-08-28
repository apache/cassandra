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

package org.apache.cassandra.service;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.CQLTester;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test for basic read queries tracking with different kinds of data models
 * <p>
 * In this test we're verifying the counting of read-related events in happy-path scenarios,
 * with a couple of different data models.
 * <p>
 * Test scenarios are a combination of the following dimensions:
 * 1. whether there are regular columns in the table
 * 2. whether there is a static row in the table
 * 3. whether there is a clustering column in the table
 * 4. whether the query has a filter that requires scanning
 * <p>
 * The actual scenarios are more-or-less a cartesian product of the above dimensions (excluding
 * those that don't make sense - like a static row without clustering columns).
 * <p>
 * Each test case bears expectation wrt the number of reads executed, the number of rows read,
 * the number of partitions read.
 * <p>
 * This test does NOT test the correctness of the number of executed replica plans
 *
 * @see QueryInfoTrackerTest for another suite of tests for read queries tracking
 *
 */
@RunWith(Parameterized.class)
public class ReadQueryTrackingTest extends CQLTester
{
    // schema properties:
    private static final String SEVEN_REGULAR_VALUES = "regular column with seven distinct values";
    private static final String NO_REGULAR_COLUMNS = "no regular columns";
    private static final String STATIC_ROW = "static row";
    private static final String NO_STATIC_ROW = "no static row";
    private static final String FIVE_ROWS_PER_PARTITION = "clustering columns with five rows per partition";
    private static final String NO_CLUSTERING_COLUMNS = "no clustering columns";

    // query properties:
    private static final String FILTERING_FOR_TWO_PARTITIONS = "range read with allow filtering yielding two partitions";
    private static final String READ_THREE_PARTITIONS_AND_TWO_ROWS_WITHIN_EACH = "multi partition read yielding three partitions and two rows within each";
    private static final String SINGLE_PARTITION_READ = "no filtering";
    private volatile QueryInfoTrackerTest.TestQueryInfoTracker tracker;
    private volatile Session session;

    @Parameters(name = "{index}: {0}")
    public static Collection<Object[]> testScenarios() {
        return Arrays.asList(
            scenario(
                SEVEN_REGULAR_VALUES, NO_STATIC_ROW, NO_CLUSTERING_COLUMNS, SINGLE_PARTITION_READ,
                expect(reads(1), rows(1), partitions(1))),
            scenario(
                SEVEN_REGULAR_VALUES, NO_STATIC_ROW, NO_CLUSTERING_COLUMNS, FILTERING_FOR_TWO_PARTITIONS,
                expect(reads(1), rows(2), partitions(2))),
            scenario(
                SEVEN_REGULAR_VALUES, NO_STATIC_ROW, FIVE_ROWS_PER_PARTITION, SINGLE_PARTITION_READ,
                expect(reads(1), rows(5), partitions(1))),
            scenario(
                SEVEN_REGULAR_VALUES, NO_STATIC_ROW, FIVE_ROWS_PER_PARTITION, FILTERING_FOR_TWO_PARTITIONS,
                expect(reads(1), rows(2 * 5), partitions(2))),
            scenario(
                SEVEN_REGULAR_VALUES, NO_STATIC_ROW, FIVE_ROWS_PER_PARTITION, READ_THREE_PARTITIONS_AND_TWO_ROWS_WITHIN_EACH,
                expect(reads(3), rows(3 * 2), partitions(3))),
            scenario(
                SEVEN_REGULAR_VALUES, STATIC_ROW, FIVE_ROWS_PER_PARTITION, SINGLE_PARTITION_READ,
                expect(reads(1), rows(1 + 5), partitions(1))),
            scenario(
                SEVEN_REGULAR_VALUES, STATIC_ROW, FIVE_ROWS_PER_PARTITION, FILTERING_FOR_TWO_PARTITIONS,
                expect(reads(1), rows(2 * (1 + 5)), partitions(2))),
            scenario(
                SEVEN_REGULAR_VALUES, STATIC_ROW, FIVE_ROWS_PER_PARTITION, READ_THREE_PARTITIONS_AND_TWO_ROWS_WITHIN_EACH,
                expect(reads(3), rows(3 * (1 + 2)), partitions(3))),
            scenario(
                NO_REGULAR_COLUMNS, STATIC_ROW, FIVE_ROWS_PER_PARTITION, SINGLE_PARTITION_READ,
                expect(reads(1), rows(1 + 5), partitions(1))),
            scenario(
                NO_REGULAR_COLUMNS, STATIC_ROW, FIVE_ROWS_PER_PARTITION, FILTERING_FOR_TWO_PARTITIONS,
                expect(reads(1), rows(2 * (1 + 5)), partitions(2))),
            scenario(
                NO_REGULAR_COLUMNS, STATIC_ROW, FIVE_ROWS_PER_PARTITION, READ_THREE_PARTITIONS_AND_TWO_ROWS_WITHIN_EACH,
                expect(reads(3), rows(3 * (1 + 2)), partitions(3))),
            scenario(
                NO_REGULAR_COLUMNS, NO_STATIC_ROW, FIVE_ROWS_PER_PARTITION, SINGLE_PARTITION_READ,
                expect(reads(1), rows(5), partitions(1))),
            scenario(
                NO_REGULAR_COLUMNS, NO_STATIC_ROW, FIVE_ROWS_PER_PARTITION, FILTERING_FOR_TWO_PARTITIONS,
                expect(reads(1), rows(2 * 5), partitions(2))),
            scenario(
                NO_REGULAR_COLUMNS, NO_STATIC_ROW, FIVE_ROWS_PER_PARTITION, READ_THREE_PARTITIONS_AND_TWO_ROWS_WITHIN_EACH,
                expect(reads(3), rows(2 * 3), partitions(3))));
    };

    @Parameterized.Parameter
    public ReadQueryTrackingTestScenario scenario;

    @Before
    public void setupTest()
    {
        requireNetwork();
        session = sessionNet();
    }

    @Test
    public void testReadQueryTracing() {
        String table = KEYSPACE + "." + scenario.toString().replace(", ", "_").replace(" ", "_").toLowerCase();

        String regularRowsColumn = scenario.rows.equals(SEVEN_REGULAR_VALUES) ? ", v" : "";
        String clusteringColumn = scenario.clustering.equals(FIVE_ROWS_PER_PARTITION) ? ", c" : "";
        String staticColumn = scenario.staticRow.equals(STATIC_ROW) ? ", sv" : "";

        session.execute(format("CREATE TABLE " + table + "(k int %s %s %s, PRIMARY KEY (k %s))",
                               clusteringColumn.isEmpty() ? "" : clusteringColumn + " int",
                               staticColumn.isEmpty() ? "" : staticColumn + " int static",
                               regularRowsColumn.isEmpty() ? "" : regularRowsColumn + " int",
                               clusteringColumn));

        // seven distinct values for regular columns; we'll have the same for partition key
        for (int k = 0; k < 7; k++)
        {
            // five distinct values for clustering columns
            for (int c = 0; c < 5; c++)
            {
                session.execute(format("INSERT INTO " + table + "(k %s %s %s) values (%d %s %s %s)",
                                       clusteringColumn,
                                       staticColumn,
                                       regularRowsColumn,
                                       k,
                                       clusteringColumn.isEmpty() ? "" : ", " + c,
                                       staticColumn.isEmpty() ? "" : ", " + k * 77,
                                       regularRowsColumn.isEmpty() ? "" : ", " + k)
                                );
            }
        }

        dumpTable(table);

        tracker = new QueryInfoTrackerTest.TestQueryInfoTracker(KEYSPACE);
        StorageProxy.instance.registerQueryTracker(tracker);
        assertEquals(0, tracker.reads.get());

        // now, let's issue a query
        if (scenario.filtering.equals(SINGLE_PARTITION_READ))
            session.execute("SELECT * FROM " + table + " WHERE k = ?", 4);
        else if (scenario.filtering.equals(FILTERING_FOR_TWO_PARTITIONS))
            session.execute("SELECT * FROM " + table + " WHERE k < ? ALLOW FILTERING", 2);
        else if (scenario.filtering.equals(READ_THREE_PARTITIONS_AND_TWO_ROWS_WITHIN_EACH))
            session.execute("SELECT * FROM " + table + " WHERE k IN (1, 2, 3) AND c IN (3, 1)");
        else
            fail("Unknown filtering scenario: " + scenario.filtering);

        // verify expectations
        if (scenario.filtering.equals(SINGLE_PARTITION_READ))
        {
            // single partition read
            assertEquals(scenario.expectation.reads, tracker.reads.get());
        }
        else if (scenario.filtering.equals(FILTERING_FOR_TWO_PARTITIONS))
        {
            // range read
            assertEquals(scenario.expectation.reads, tracker.rangeReads.get());
        }
        else if (scenario.filtering.equals(READ_THREE_PARTITIONS_AND_TWO_ROWS_WITHIN_EACH))
        {
            // multi-partition read
            assertEquals(scenario.expectation.reads, tracker.reads.get());
        }
        else fail("Unknown filtering scenario: " + scenario.filtering);

        assertEquals(scenario.expectation.rows, tracker.readRows.get());
        assertEquals(scenario.expectation.partitions, tracker.readPartitions.get());
        assertEquals(scenario.expectation.reads, tracker.replicaPlans.get());
    }

    private void dumpTable(String table)
    {
        ResultSet contents = session.execute("SELECT * FROM " + table);
        for(Row row: contents.all())
        {
            StringBuilder rowString = new StringBuilder("| ");
            for (int columnIdx = 0; columnIdx < row.getColumnDefinitions().size(); columnIdx++)
            {
                String columnName = row.getColumnDefinitions().getName(columnIdx);
                Object value = row.getObject(columnIdx);
                rowString.append(columnName).append(": ").append(value).append(" | ");
            }
            logger.debug("{}", rowString);
        }
    }

    // boilerplate (thank you, copilot)
    private static class ReadQueryTrackingTestScenario
    {
        final String rows;
        final String staticRow;
        final String clustering;
        final String filtering;
        final Expectation expectation;

        private ReadQueryTrackingTestScenario(String rows, String staticRow, String clustering, String filtering, Expectation expectation)
        {
            this.rows = rows;
            this.staticRow = staticRow;
            this.clustering = clustering;
            this.filtering = filtering;
            this.expectation = expectation;
        }

        @Override
        public String toString()
        {
            return rows +
                ", " +
                staticRow +
                ", " +
                clustering +
                ", " +
                filtering;
        }
    }

    private static class Expectation
    {
        final int reads;
        final int rows;
        final int partitions;

        private Expectation(int reads, int rows, int partitions)
        {
            this.reads = reads;
            this.rows = rows;
            this.partitions = partitions;
        }
    }

    static int reads(int reads)
    {
        return reads;
    }

    static int rows(int rows)
    {
        return rows;
    }

    static int partitions(int partitions)
    {
        return partitions;
    }

    static Expectation expect(int reads, int rows, int partitions)
    {
        return new Expectation(reads, rows, partitions);
    }

    static Object[] scenario(String rows, String staticRow, String clustering, String filtering, Expectation expectation)
    {
        return new Object[] { new ReadQueryTrackingTestScenario(rows, staticRow, clustering, filtering, expectation) };
    }
}
