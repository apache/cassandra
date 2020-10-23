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

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.ClearableHistogram;

import static org.junit.Assert.assertEquals;

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

        // Test with only 1 of the 3 SSTables being merged and a Name filter
        // This test checks the SinglePartitionReadCommand::queryMemtableAndSSTablesInTimestampOrder which is only
        // used for ClusteringIndexNamesFilter when there are no multi-cell columns
        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 10", 1,
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

    @Test
    public void testNonCompactTableRowDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, '1')");
        flush();

        execute("DELETE FROM %s WHERE pk = 1 AND ck = 1");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 2);
    }

    @Test
    public void testNonCompactTableRangeDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        flush();

        execute("DELETE FROM %s WHERE a=? AND b=?", 1, 1);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 2);
    }

    @Test
    public void testNonCompactTableCellsDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v1 text, v2 text, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, 1, '1', '1')");
        flush();

        execute("DELETE v1 FROM %s WHERE pk = 1 AND ck = 1");
        execute("DELETE v2 FROM %s WHERE pk = 1 AND ck = 1");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 2, row(1, 1, null, null));
    }

    @Test
    public void testCompactTableSkipping() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, '1') USING TIMESTAMP 1000000");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 50, '2') USING TIMESTAMP 1000001");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 100, '3') USING TIMESTAMP 1000002");
        flush();

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, '4') USING TIMESTAMP 2000000");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 51, '5') USING TIMESTAMP 2000001");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 101, '6') USING TIMESTAMP 2000002");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 51", 1, row(1, 51, "5"));

        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 51", 1, row(1, 51, "5"));
    }

    @Test
    public void testCompactTableSkippingPkOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, ck) VALUES (1, 1) USING TIMESTAMP 1000000");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 50) USING TIMESTAMP 1000001");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 100) USING TIMESTAMP 1000002");
        flush();

        execute("INSERT INTO %s (pk, ck) VALUES (1, 2) USING TIMESTAMP 2000000");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 51) USING TIMESTAMP 2000001");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 101) USING TIMESTAMP 2000002");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 51", 1, row(1, 51));

        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 51", 1, row(1, 51));
    }

    @Test
    public void testCompactTableCellDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, '1')");
        flush();

        execute("DELETE v FROM %s WHERE pk = 1 AND ck = 1");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 1);

        // Dropping compact storage forces us to hit an extra SSTable, since we can't rely on the isDense flag
        // to determine that a row with a complete set of column deletes is complete.
        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 2);
    }

    @Test
    public void testCompactTableRowDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, '1')");
        flush();

        execute("DELETE FROM %s WHERE pk = 1 AND ck = 1");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 1);

        // Dropping compact storage forces us to hit an extra SSTable, since we can't rely on the isDense flag
        // to determine that a row with a complete set of column deletes is complete.
        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 2);
    }

    @Test
    public void testCompactTableRangeDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 1, 1);
        flush();

        execute("DELETE FROM %s WHERE a=? AND b=?", 1, 1);
        flush();

        // Even with a compact table, we can't short-circuit for a range deletion rather than a cell tombstone.
        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 2);

        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 2);
    }

    @Test
    public void testCompactTableRangeOverRowDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 1, 1);
        flush();

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 1, 1, 1);
        flush();

        execute("DELETE FROM %s WHERE a=? AND b=?", 1, 1);
        flush();

        // The range delete will subsume the row delete, and the latter will not factor into skipping decisions.
        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 3);

        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 3);
    }

    @Test
    public void testCompactTableRowOverRangeDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 1, 1);
        flush();

        execute("DELETE FROM %s WHERE a=? AND b=?", 1, 1);
        flush();

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 1, 1, 1);
        flush();

        // The row delete provides a tombstone, which is enough information to short-circuit after the first SSTable.
        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 1);

        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 3);
    }

    @Test
    public void testCompactTableCellUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, '1')");
        flush();

        execute("UPDATE %s SET v = '2' WHERE pk = 1 AND ck = 1");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 1, row(1, 1, "2"));

        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 1, row(1, 1, "2"));
    }

    @Test
    public void testCompactTableDeleteOverlappingSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, ck) VALUES (1, 51) USING TIMESTAMP 1000002");
        flush();
        execute("DELETE FROM %s WHERE pk = 1 AND ck = 51");
        flush();

        execute("INSERT INTO %s (pk, ck) VALUES (1, 51) USING TIMESTAMP 1000001");
        execute("INSERT INTO %s (pk, ck) VALUES (2, 51)");
        flush();

        // If it weren't for the write to pk = 2, ck = 51, we could skip the third SSTable too and hit only one here.
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 51", 2);

        // Dropping compact storage forces us to hit an extra SSTable, since we can't rely on the isDense flag
        // to determine that a row with a complete set of column deletes is complete.
        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 51", 3);
    }
}
