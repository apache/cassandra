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
package org.apache.cassandra.cql3.validation.miscellaneous;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.ClearableHistogram;

/**
 * Tests for checking how many sstables we access during cql queries.
 */
public class SSTablesIteratedTest extends CQLTester
{
    private void executeAndCheck(String query, int numSSTables, Object[]... rows) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore(KEYSPACE_PER_TEST);

        ((ClearableHistogram) cfs.metric.sstablesPerReadHistogram.cf).clear(); // resets counts

        assertRows(execute(query), rows);

        long numSSTablesIterated = cfs.metric.sstablesPerReadHistogram.cf.getSnapshot().getMax(); // max sstables read
        assertEquals(String.format("Expected %d sstables iterated but got %d instead, with %d live sstables",
                                   numSSTables, numSSTablesIterated, cfs.getLiveSSTables().size()),
                     numSSTables,
                     numSSTablesIterated);
    }

    @Override
    protected String createTable(String query)
    {
        String ret = super.createTable(KEYSPACE_PER_TEST, query);
        disableCompaction(KEYSPACE_PER_TEST);
        return ret;
    }

    @Override
    protected UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        return executeFormattedQuery(formatQuery(KEYSPACE_PER_TEST, query), values);
    }

    @Override
    public void flush()
    {
        super.flush(KEYSPACE_PER_TEST);
    }

    @Test
    public void testSinglePartitionQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v text, PRIMARY KEY (pk, c))");

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 1, 40, "41");
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 2, 10, "12");
        flush();

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 1, 10, "11");
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 3, 30, "33");
        flush();

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 1, 20, "21");
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 2, 40, "42");
        execute("UPDATE %s SET v = '12' WHERE pk = 2 AND c = 10");
        flush();

        // Test with all the table being merged
        executeAndCheck("SELECT * FROM %s WHERE pk = 1", 3,
                        row(1, 10, "11"),
                        row(1, 20, "21"),
                        row(1, 40, "41"));

        // Test with only 2 of the 3 SSTables being merged
        executeAndCheck("SELECT * FROM %s WHERE pk = 2", 2,
                        row(2, 10, "12"),
                        row(2, 40, "42"));

        executeAndCheck("SELECT * FROM %s WHERE pk = 2 ORDER BY c DESC", 2,
                        row(2, 40, "42"),
                        row(2, 10, "12"));

        // Test with only 2 of the 3 SSTables being merged and a Slice filter
        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c > 20", 2,
                        row(2, 40, "42"));

        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c > 20 ORDER BY c DESC", 2,
                        row(2, 40, "42"));

        // Test with only 2 of the 3 SSTables being merged and a Name filter
        // This test checks the SinglePartitionReadCommand::queryMemtableAndSSTablesInTimestampOrder which is only
        // used for ClusteringIndexNamesFilter when there are no multi-cell columns
        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 10", 2,
                        row(2, 10, "12"));

        // For partition range queries the metric must not be updated. The reason being that range queries simply
        // scan all the SSTables containing data within the partition range. Due to that they might pollute the metric
        // and give a wrong view of the system.
        executeAndCheck("SELECT * FROM %s", 0,
                        row(1, 10, "11"),
                        row(1, 20, "21"),
                        row(1, 40, "41"),
                        row(2, 10, "12"),
                        row(2, 40, "42"),
                        row(3, 30, "33"));

        executeAndCheck("SELECT * FROM %s WHERE token(pk) = token(1)", 0,
                        row(1, 10, "11"),
                        row(1, 20, "21"),
                        row(1, 40, "41"));

        assertInvalidMessage("ORDER BY is only supported when the partition key is restricted by an EQ or an IN",
                             "SELECT * FROM %s WHERE token(pk) = token(1) ORDER BY C DESC");
    }
}