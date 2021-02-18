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

import org.apache.cassandra.config.DatabaseDescriptor;
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
    public String createTable(String query)
    {
        String ret = super.createTable(KEYSPACE_PER_TEST, query);
        disableCompaction(KEYSPACE_PER_TEST);
        return ret;
    }

    @Override
    public UntypedResultSet execute(String query, Object... values) throws Throwable
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

        // The code has to read the 1st and 3rd sstables to see that everything before the 2nd sstable is deleted, and
        // overall has to read the 3 sstables
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 3, row(1, 1001, "1001"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 3, row(1, 1001, "1001"), row(1, 1002, "1002"));

        executeAndCheck("SELECT * FROM %s WHERE id=1", 3, allRows);

        // The 1st and 3rd sstables have data only up to 1000, so they will be skipped
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 1000 LIMIT 1", 1, row(1, 1001, "1001"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 1000", 1, allRows);

        // The condition makes no difference to the code, and all 3 sstables have to read
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 2000 LIMIT 1", 3, row(1, 1001, "1001"));
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

        // The 500th first rows are in the first sstable (and there is no partition deletion/static row), so the 'lower
        // bound' optimization will kick in and we'll only read the 1st sstable.
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 1", 1, row(1, 1, "1", "1"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 LIMIT 2", 1, row(1, 1, "1", "1"), row(1, 2, "2", null));

        // Getting everything obviously requires reading both sstables
        executeAndCheck("SELECT * FROM %s WHERE id=1", 2, allRows);

        // The 'lower bound' optimization don't help us because while the row to fetch is in the 1st sstable, the lower
        // bound for the 2nd sstable is 501, which is lower than 1000.
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 1000 LIMIT 1", 2, row(1, 1001, "1001", "1001"));

        // Somewhat similar to the previous one: the row is in th 2nd sstable in this case, but as the lower bound for
        // the first sstable is 1, this doesn't help.
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col > 500 LIMIT 1", 2, row(1, 751, "751", "751"));

        // The 'col <= ?' condition in both queries doesn't impact the read path, which can still make use of the lower
        // bound optimization and read only the first sstable.
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 2000 LIMIT 1", 1, row(1, 1, "1", "1"));
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 500 LIMIT 1", 1, row(1, 1, "1", "1"));

        // Making sure the 'lower bound' optimization also work in reverse queries (in which it's more of a 'upper
        // bound' optimization).
        executeAndCheck("SELECT * FROM %s WHERE id=1 AND col <= 2000 ORDER BY col DESC LIMIT 1", 1, row(1, 2000, "2000", null));
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

        // Dropping CS exposes a previously hidden/implicit field, so take that into account.
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 51", 1, row(1, 51, null));
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
