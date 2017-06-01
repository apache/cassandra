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
import org.apache.cassandra.config.DatabaseDescriptor;
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

    @Test
    public void testSSTablesOnlyASC() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col ASC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 2, row(1, 10, "10"), row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 3, row(1, 10, "10"), row(1, 20, "20"), row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 3, row(1, 10, "10"), row(1, 20, "20"), row(1, 30, "30"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 1, row(1, 10, "10"));
    }

    @Test
    public void testMixedMemtableSStablesASC() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col ASC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 0, row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 1, row(1, 10, "10"), row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 2, row(1, 10, "10"), row(1, 20, "20"), row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, row(1, 10, "10"), row(1, 20, "20"), row(1, 30, "30"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 0, row(1, 10, "10"));
    }

    @Test
    public void testOverlappingSStablesASC() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col ASC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 2, row(1, 10, "10"), row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 2, row(1, 10, "10"), row(1, 20, "20"), row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, row(1, 10, "10"), row(1, 20, "20"), row(1, 30, "30"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 1, row(1, 10, "10"));
    }

    @Test
    public void testSSTablesOnlyDESC() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 2, row(1, 30, "30"), row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 3, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 3, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 1, row(1, 30, "30"));
    }

    @Test
    public void testMixedMemtableSStablesDESC() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 0, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 1, row(1, 30, "30"), row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 2, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 0, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 0, row(1, 30, "30"));
    }

    @Test
    public void testOverlappingSStablesDESC() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 2, row(1, 30, "30"), row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 2, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, row(1, 30, "30"), row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1, row(1, 30, "30"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 1, row(1, 30, "30"));
    }

    @Test
    public void testDeletionOnDifferentSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        flush();

        execute("DELETE FROM %s WHERE id=1 and col=30");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 3, row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 4, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 4, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 4, row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 2);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 3, row(1, 20, "20"));
    }

    @Test
    public void testDeletionOnSameSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        execute("DELETE FROM %s WHERE id=1 and col=30");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 2, row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 3, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 3, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 3, row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 1);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 2, row(1, 20, "20"));
    }

    @Test
    public void testDeletionOnMemTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        execute("DELETE FROM %s WHERE id=1 and col=30");

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 20, "20"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 2, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 3", 2, row(1, 20, "20"), row(1, 10, "10"));
        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, row(1, 20, "20"), row(1, 10, "10"));

        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 25 LIMIT 1", 0);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col < 40 LIMIT 1", 1, row(1, 20, "20"));
    }

    @Test
    public void testDeletionOnIndexedSSTableDESC() throws Throwable
    {
        testDeletionOnIndexedSSTableDESC(true);
        testDeletionOnIndexedSSTableDESC(false);
    }

    private void testDeletionOnIndexedSSTableDESC(boolean deleteWithRange) throws Throwable
    {
        // reduce the column index size so that columns get indexed during flush
        DatabaseDescriptor.setColumnIndexSize(1);

        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        for (int i = 1; i <= 1000; i++)
        {
            execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, i, Integer.toString(i));
        }
        flush();

        Object[][] allRows = new Object[1000][];
        for (int i = 1001; i <= 2000; i++)
        {
            execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, i, Integer.toString(i));
            allRows[2000 - i] = row(1, i, Integer.toString(i));
        }

        if (deleteWithRange)
        {
            execute("DELETE FROM %s WHERE id=1 and col <= ?", 1000);
        }
        else
        {
            for (int i = 1; i <= 1000; i++)
                execute("DELETE FROM %s WHERE id=1 and col = ?", i);
        }
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 2000, "2000"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 1, row(1, 2000, "2000"), row(1, 1999, "1999"));

        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, allRows);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 1000 LIMIT 1", 1, row(1, 2000, "2000"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 2000 LIMIT 1", 1, row(1, 2000, "2000"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 1000", 1, allRows);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 2000", 2, allRows);
    }

    @Test
    public void testDeletionOnIndexedSSTableASC() throws Throwable
    {
        testDeletionOnIndexedSSTableASC(true);
        testDeletionOnIndexedSSTableASC(false);
    }

    private void testDeletionOnIndexedSSTableASC(boolean deleteWithRange) throws Throwable
    {
        // reduce the column index size so that columns get indexed during flush
        DatabaseDescriptor.setColumnIndexSize(1);

        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col ASC)");

        for (int i = 1; i <= 1000; i++)
        {
            execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, i, Integer.toString(i));
        }
        flush();

        Object[][] allRows = new Object[1000][];
        for (int i = 1001; i <= 2000; i++)
        {
            execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, i, Integer.toString(i));
            allRows[i - 1001] = row(1, i, Integer.toString(i));
        }
        flush();

        if (deleteWithRange)
        {
            execute("DELETE FROM %s WHERE id =1 and col <= ?", 1000);
        }
        else
        {
            for (int i = 1; i <= 1000; i++)
                execute("DELETE FROM %s WHERE id=1 and col = ?", i);
        }
        flush();

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 3, row(1, 1001, "1001"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 3, row(1, 1001, "1001"), row(1, 1002, "1002"));

        executeAndCheck("SELECT * FROM %s WHERE id=1", 3, allRows);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 1000 LIMIT 1", 2, row(1, 1001, "1001"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 2000 LIMIT 1", 3, row(1, 1001, "1001"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 1000", 2, allRows);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 2000", 3, allRows);
    }

    @Test
    public void testDeletionOnOverlappingIndexedSSTable() throws Throwable
    {
        testDeletionOnOverlappingIndexedSSTable(true);
        testDeletionOnOverlappingIndexedSSTable(false);
    }

    private void testDeletionOnOverlappingIndexedSSTable(boolean deleteWithRange) throws Throwable
    {
        // reduce the column index size so that columns get indexed during flush
        DatabaseDescriptor.setColumnIndexSize(1);

        createTable("CREATE TABLE %s (id int, col int, val1 text, val2 text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col ASC)");

        for (int i = 1; i <= 500; i++)
        {
            if (i % 2 == 0)
                execute("INSERT INTO %s (id, col, val1) VALUES (?, ?, ?)", 1, i, Integer.toString(i));
            else
                execute("INSERT INTO %s (id, col, val1, val2) VALUES (?, ?, ?, ?)", 1, i, Integer.toString(i), Integer.toString(i));
        }

        for (int i = 1001; i <= 1500; i++)
        {
            if (i % 2 == 0)
                execute("INSERT INTO %s (id, col, val1) VALUES (?, ?, ?)", 1, i, Integer.toString(i));
            else
                execute("INSERT INTO %s (id, col, val1, val2) VALUES (?, ?, ?, ?)", 1, i, Integer.toString(i), Integer.toString(i));
        }

        flush();

        for (int i = 501; i <= 1000; i++)
        {
            if (i % 2 == 0)
                execute("INSERT INTO %s (id, col, val1) VALUES (?, ?, ?)", 1, i, Integer.toString(i));
            else
                execute("INSERT INTO %s (id, col, val1, val2) VALUES (?, ?, ?, ?)", 1, i, Integer.toString(i), Integer.toString(i));
        }

        for (int i = 1501; i <= 2000; i++)
        {
            if (i % 2 == 0)
                execute("INSERT INTO %s (id, col, val1) VALUES (?, ?, ?)", 1, i, Integer.toString(i));
            else
                execute("INSERT INTO %s (id, col, val1, val2) VALUES (?, ?, ?, ?)", 1, i, Integer.toString(i), Integer.toString(i));
        }

        if (deleteWithRange)
        {
            execute("DELETE FROM %s WHERE id=1 and col > ? and col <= ?", 250, 750);
        }
        else
        {
            for (int i = 251; i <= 750; i++)
                execute("DELETE FROM %s WHERE id=1 and col = ?", i);
        }

        flush();

        Object[][] allRows = new Object[1500][]; // non deleted rows
        for (int i = 1; i <= 2000; i++)
        {
            if (i > 250 && i <= 750)
                continue; // skip deleted records

            int idx = (i <= 250 ? i - 1 : i - 501);

            if (i % 2 == 0)
                allRows[idx] = row(1, i, Integer.toString(i), null);
            else
                allRows[idx] = row(1, i, Integer.toString(i), Integer.toString(i));
        }

        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 2, row(1, 1, "1", "1"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 2, row(1, 1, "1", "1"), row(1, 2, "2", null));

        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, allRows);
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 1000 LIMIT 1", 2, row(1, 1001, "1001", "1001"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 2000 LIMIT 1", 2, row(1, 1, "1", "1"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 500 LIMIT 1", 2, row(1, 751, "751", "751"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 500 LIMIT 1", 2, row(1, 1, "1", "1"));
    }

    @Test
    public void testMultiplePartitionsDESC() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, col int, val text, PRIMARY KEY (id, col)) WITH CLUSTERING ORDER BY (col DESC)");

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 10, "10");
        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 2, 10, "10");
        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 3, 10, "10");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 20, "20");
        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 2, 20, "20");
        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 3, 20, "20");
        flush();

        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 1, 30, "30");
        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 2, 30, "30");
        execute("INSERT INTO %s (id, col, val) VALUES (?, ?, ?)", 3, 30, "30");
        flush();

        for (int i = 1; i <= 3; i++)
        {
            String base = "SELECT * FROM %s ";

            executeAndCheck(base + String.format("WHERE id=%d LIMIT 1", i), 1, row(i, 30, "30"));
            executeAndCheck(base + String.format("WHERE id=%d LIMIT 2", i), 2, row(i, 30, "30"), row(i, 20, "20"));
            executeAndCheck(base + String.format("WHERE id=%d LIMIT 3", i), 3, row(i, 30, "30"), row(i, 20, "20"), row(i, 10, "10"));
            executeAndCheck(base + String.format("WHERE id=%d", i), 3, row(i, 30, "30"), row(i, 20, "20"), row(i, 10, "10"));

            executeAndCheck(base + String.format("WHERE id=%d AND col > 25 LIMIT 1", i), 1, row(i, 30, "30"));
            executeAndCheck(base + String.format("WHERE id=%d AND col < 40 LIMIT 1", i), 1, row(i, 30, "30"));
        }
    }
}
