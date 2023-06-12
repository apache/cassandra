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

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
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

    private void executeAndCheckRangeQuery(String query, int numSSTables, Object[]... rows) throws Throwable
    {
        logger.info("Executing query: {} with parameters: {}", query, Arrays.toString(rows));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore(KEYSPACE_PER_TEST);

        ((ClearableHistogram) cfs.metric.sstablesPerRangeReadHistogram.cf).clear(); // resets counts

        assertRows(execute(query), rows);

        long numSSTablesIterated = cfs.metric.sstablesPerRangeReadHistogram.cf.getSnapshot().getMax(); // max sstables read
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
    protected UntypedResultSet execute(String query, Object... values)
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

        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c > 20", 2,
                        row(2, 40, "42"));

        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c > 20 ORDER BY c DESC", 2,
                        row(2, 40, "42"));

        // Test with only 1 of the 3 SSTables being merged and a Name filter
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
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);

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
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);

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
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);

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

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 1);
    }

    @Test
    public void testNonCompactTableRangeDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        flush();

        execute("DELETE FROM %s WHERE a=? AND b=?", 1, 1);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 1);
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
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 51", 2, row(1, 51, "5"));
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

        // The fact that non-compact table insert do not have primary key liveness force us to hit an extra sstable
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 51", 2, row(1, 51, null));
    }

    @Test
    public void testNonCompactTableSkippingPkOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck) VALUES (1, 1) USING TIMESTAMP 1000000");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 50) USING TIMESTAMP 1000001");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 100) USING TIMESTAMP 1000002");
        flush();

        execute("INSERT INTO %s (pk, ck) VALUES (1, 2) USING TIMESTAMP 2000000");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 51) USING TIMESTAMP 2000001");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 101) USING TIMESTAMP 2000002");
        flush();

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

        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 1);

        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 1);
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

        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 1);

        execute("ALTER TABLE %s DROP COMPACT STORAGE");
        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 1);
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

        // For non static compact tables, row deletions are done through deletion of the compact column (d in this case).
        // Once converted into non compact table as the row does not have a primary key liveness the code does not
        // have enough to allow the logic to stop at the first SSTable and has to read the second one where it find
        // the range deletion.
        executeAndCheck("SELECT * FROM %s WHERE a=1 AND b=1 AND c=1", 2);
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
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 2, row(1, 1, "2"));
    }

    @Test
    public void testNonCompactTableCellUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, '1')");
        flush();

        execute("UPDATE %s SET v = '2' WHERE pk = 1 AND ck = 1");
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND ck = 1", 2, row(1, 1, "2"));
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

    @Test
    public void testNonCompactTableWithClusteringColumnAndMultipleRegularColumnsAndNullColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v1 int, v2 int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 1001", 1, 2, 1);
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 1002", 1, 3, 1);
        flush();
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 2001", 1, 2, 2);
        execute("UPDATE %s USING TIMESTAMP 2002 SET v1 = ? WHERE pk = ? AND c = ?", 2, 1, 3);
        flush();
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 3000", 1, 1, 3);
        execute("UPDATE %s USING TIMESTAMP 3001 SET v1 = ? WHERE pk = ? AND c = ?", 3, 1, 2);
        execute("UPDATE %s USING TIMESTAMP 3002 SET v1 = ? WHERE pk = ? AND c = ?", 3, 1, 3);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 3, row(1, 1, 3, null));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 2", 3, row(1, 2, 3, null));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 3", 3, row(1, 3, 3, null));

        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 1", 1, row(1, 3));
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 2", 2, row(2, 3));
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 3", 3, row(3, 3));

        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1 AND c = 1", 1, row(3));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1 AND c = 2", 2, row(3));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1 AND c = 3", 3, row(3));

        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1 AND c = 1", 3, row(3, (Integer) null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1 AND c = 2", 3, row(3, (Integer) null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1 AND c = 3", 3, row(3, (Integer) null));

        executeAndCheck("SELECT v2 FROM %s WHERE pk = 1 AND c = 1", 3, row((Integer) null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 1 AND c = 2", 3, row((Integer) null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 1 AND c = 3", 3, row((Integer) null));
    }

    @Test
    public void testNonCompactTableWithClusteringColumnAndMultipleRegularColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v1 int, v2 int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1001", 1, 2, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1002", 1, 3, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 2001", 1, 2, 2);
        execute("UPDATE %s  USING TIMESTAMP 2002 SET v1 = ? WHERE pk = ? AND c = ?", 2, 1, 3);
        flush();
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 3000", 1, 1, 3);
        execute("UPDATE %s USING TIMESTAMP 3001 SET v1 = ? WHERE pk = ? AND c = ?", 3, 1, 2);
        execute("UPDATE %s USING TIMESTAMP 3002 SET v1 = ? WHERE pk = ? AND c = ?", 3, 1, 3);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 3, row(1, 1, 3, 1));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 2", 3, row(1, 2, 3, 1));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 3", 3, row(1, 3, 3, 1));

        // As we have the primary key liveness and all the queried columns in the first SSTable we can stop at this point
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 1", 1, row(1, 3));
        // As we have the primary key liveness and all the queried columns in the second SSTable we can stop at this point
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 2", 2, row(2, 3));

        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1 AND c = 1", 1, row(3));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1 AND c = 2", 2, row(3));

        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1 AND c = 1", 3, row(3, 1));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1 AND c = 2", 3, row(3, 1));

        executeAndCheck("SELECT v2 FROM %s WHERE pk = 1 AND c = 1", 3, row(1));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 1 AND c = 2", 3, row(1));
    }

    @Test
    public void testNonCompactTableWithStaticColumnValueMissing() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1001", 2, 2, 1, 1);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1001", 3, 3, 1);
        flush();
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP 2002", 3, 2);
        flush();
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 3000", 1, 1, 3);
        execute("UPDATE %s  USING TIMESTAMP 3001 SET v = ? WHERE pk = ? AND c = ?", 3, 2, 1);
        execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP 3002", 3, 3);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 3, row(1, 1, null, 3));
        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 2, row(2, 1, 1, 3));
        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 3", 3, row(3, 3, 3, 1));
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 1 AND c = 1", 3, row(null, 3));
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 2 AND c = 1", 2, row(1, 3));
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 3 AND c = 3", 3, row(3, 1));

        // As we have the primary key liveness and all the queried columns in the first SSTable we can stop at this point
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 1, row(3));
        executeAndCheck("SELECT v FROM %s WHERE pk = 2 AND c = 1", 2, row(3));
        executeAndCheck("SELECT v FROM %s WHERE pk = 3 AND c = 3", 1, row(1));
        executeAndCheck("SELECT s FROM %s WHERE pk = 1", 3, row((Integer) null));
        executeAndCheck("SELECT s FROM %s WHERE pk = 2", 2, row(1), row(1));
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 2", 2, row(1));
        executeAndCheck("SELECT s FROM %s WHERE pk = 3", 3, row(3));
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 3", 3, row(3));
    }

    @Test
    public void testNonCompactTableWithClusteringColumnAndNullColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c) VALUES (?, ?) USING TIMESTAMP 1000", 1, 1);
        execute("INSERT INTO %s (pk, c) VALUES (?, ?) USING TIMESTAMP 1001", 1, 2);
        execute("INSERT INTO %s (pk, c) VALUES (?, ?) USING TIMESTAMP 1001", 1, 3);
        flush();
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        execute("INSERT INTO %s (pk, c) VALUES (?, ?) USING TIMESTAMP 2001", 1, 2);
        execute("UPDATE %s USING TIMESTAMP 2002 SET v = ? WHERE pk = ? AND c = ?", 2, 1, 3);
        flush();
        execute("INSERT INTO %s (pk, c) VALUES (?, ?) USING TIMESTAMP 3000", 1, 1);
        execute("INSERT INTO %s (pk, c) VALUES (?, ?) USING TIMESTAMP 3001", 1, 2);
        execute("INSERT INTO %s (pk, c) VALUES (?, ?) USING TIMESTAMP 3002", 1, 3);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 2, row(1, 1, 2));
        executeAndCheck("SELECT c, v FROM %s WHERE pk = 1 AND c = 1", 2, row(1, 2));
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 2, row(2));

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 2", 3, row(1, 2, (Integer) null));
        executeAndCheck("SELECT c, v FROM %s WHERE pk = 1 AND c = 2", 3, row(2, (Integer) null));
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 2", 3, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 3", 2, row(1, 3, 2));
        executeAndCheck("SELECT c, v FROM %s WHERE pk = 1 AND c = 3", 2, row(3, 2));
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 3", 2, row(2));
    }

    @Test
    public void testNonCompactTableWithMulticellColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v1 int, v2 int, s set<int>, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v1, s) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, set(1, 2));
        execute("INSERT INTO %s (pk, c, v1, s) VALUES (?, ?, ?, ?) USING TIMESTAMP 1001", 1, 2, 1, set(1, 2));
        flush();
        execute("INSERT INTO %s (pk, c, v1, s) VALUES (?, ?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2, set(2, 3));
        execute("UPDATE %s USING TIMESTAMP 2001 SET v1 = ?, s = ? WHERE pk = ? AND c = ?", 2, set(2, 3), 1, 2);
        flush();
        execute("INSERT INTO %s (pk, c, v1, s) VALUES (?, ?, ?, ?) USING TIMESTAMP 3000", 1, 1, 3, set(3, 4));
        execute("UPDATE %s USING TIMESTAMP 3001 SET v1 = ?, s = ? WHERE pk = ? AND c = ?", 3, set(3, 4), 1, 2);
        flush();

        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 1", 1, row(1, 3));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1 AND c = 1", 1, row(3));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 3, row(1, 1, set(3, 4), 3, null));
        executeAndCheck("SELECT c, s FROM %s WHERE pk = 1 AND c = 1", 3, row(1, set(3, 4)));

        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 2", 3, row(2, 3));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1 AND c = 2", 3, row(3));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 2", 3, row(1, 2, set(3, 4), 3, null));
        executeAndCheck("SELECT c, s FROM %s WHERE pk = 1 AND c = 2", 3, row(2, set(3, 4)));
    }

    @Test
    public void testCompactAndNonCompactTableWithCounter() throws Throwable
    {
        for (String with : new String[]{ "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (pk int, c int, count counter, PRIMARY KEY(pk, c))" + with);

            execute("UPDATE %s SET count = count + 1 WHERE pk = 1 AND c = 1");
            flush();
            execute("UPDATE %s SET count = count + 1 WHERE pk = 1 AND c = 1");
            flush();
            execute("UPDATE %s SET count = count + 1 WHERE pk = 1 AND c = 1");
            flush();

            executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 3, row(1, 1, 3L));
            executeAndCheck("SELECT pk, c FROM %s WHERE pk = 1 AND c = 1", 3, row(1, 1));
            executeAndCheck("SELECT count FROM %s WHERE pk = 1 AND c = 1", 3, row(3L));
        }
    }

    @Test
    public void testNonCompactTableWithStaticColumnValueMissingAndMulticellColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v set<int>, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, set(1));
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1001", 2, 2, 1, set(1));
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1001", 3, 3, set(1));
        flush();
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, set(2));
        execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP 2002", 3, 2);
        flush();
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 3000", 1, 1, set(3));
        execute("UPDATE %s  USING TIMESTAMP 3001 SET v = ? WHERE pk = ? AND c = ?", set(3), 2, 1);
        execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP 3002", 3, 3);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 3, row(1, 1, null, set(3)));
        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 2, row(2, 1, 1, set(3)));
        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 3", 3, row(3, 3, 3, set(1)));
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 1 AND c = 1", 3, row(null, set(3)));
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 2 AND c = 1", 2, row(1, set(3)));
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 3 AND c = 3", 3, row(3, set(1)));
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 3, row(set(3)));
        executeAndCheck("SELECT v FROM %s WHERE pk = 2 AND c = 1", 2, row(set(3)));
        executeAndCheck("SELECT v FROM %s WHERE pk = 3 AND c = 3", 1, row(set(1)));
        executeAndCheck("SELECT s FROM %s WHERE pk = 1", 3, row((Integer) null));
        executeAndCheck("SELECT s FROM %s WHERE pk = 2", 2, row(1), row(1));
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 2", 2, row(1));
        executeAndCheck("SELECT s FROM %s WHERE pk = 3", 3, row(3));
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 3", 3, row(3));
    }

    @Test
    public void testNonCompactTableWithClusteringColumnAndMultipleRegularColumnsAndPartitionTombstones() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v1 int, v2 int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE pk = ?", 2);
        execute("UPDATE %s USING TIMESTAMP 2002 SET v1 = ? WHERE pk = ? AND c = ?", 2, 3, 1);
        execute("DELETE FROM %s USING TIMESTAMP 2003 WHERE pk = ?", 4);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ?", 1);
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 3001", 2, 1, 3);
        execute("DELETE FROM %s USING TIMESTAMP 3002 WHERE pk = ?", 3);
        execute("UPDATE %s USING TIMESTAMP 3003 SET v1 = ? WHERE pk = ? AND c = ?", 3, 4, 1);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 1);
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 1", 1);
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 2, row(2, 1, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2 AND c = 1", 2, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 2 AND c = 1", 2, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 1", 1);
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 3 AND c = 1", 1);
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 3 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 4 AND c = 1", 2, row(4, 1, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4 AND c = 1", 2, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 4 AND c = 1", 2, row((Integer) null));
    }

    @Test
    public void testCompactAndNonCompactTableWithPartitionTombstones() throws Throwable
    {
        for (Boolean compact : new Boolean[]{ Boolean.FALSE, Boolean.TRUE })
        {
            String with = compact ? " WITH COMPACT STORAGE" : "";
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int)" + with);

            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1);
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1);
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1);
            flush();
            execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 2000", 1, 2);
            execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE pk = ?", 2);
            execute("UPDATE %s USING TIMESTAMP 2002 SET v1 = ? WHERE pk = ?", 2, 3);
            execute("DELETE FROM %s USING TIMESTAMP 2003 WHERE pk = ?", 4);
            flush();
            execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ?", 1);
            execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 3001", 2, 3);
            execute("DELETE FROM %s USING TIMESTAMP 3002 WHERE pk = ?", 3);
            execute("UPDATE %s USING TIMESTAMP 3003 SET v1 = ? WHERE pk = ?", 3, 4);
            flush();

            executeAndCheck("SELECT * FROM %s WHERE pk = 1", 1);
            executeAndCheck("SELECT pk, v1 FROM %s WHERE pk = 1", 1);
            executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1", 1);

            executeAndCheck("SELECT * FROM %s WHERE pk = 2", 2, row(2, 3, null));
            executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2", 2, row(3, null));
            executeAndCheck("SELECT v2 FROM %s WHERE pk = 2", 2, row((Integer) null));

            executeAndCheck("SELECT * FROM %s WHERE pk = 3", 1);
            executeAndCheck("SELECT pk, v1 FROM %s WHERE pk = 3", 1);
            executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 3", 1);

            executeAndCheck("SELECT * FROM %s WHERE pk = 4", 2, row(4, 3, null));
            executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4", 2, row(3, null));
            executeAndCheck("SELECT v2 FROM %s WHERE pk = 4", 2, row((Integer) null));

            if (compact)
            {
                execute("ALTER TABLE %s DROP COMPACT STORAGE");
                executeAndCheck("SELECT * FROM %s WHERE pk = 1", 1);
                executeAndCheck("SELECT pk, v1 FROM %s WHERE pk = 1", 1);
                executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1", 1);

                assertColumnNames(execute("SELECT * FROM %s WHERE pk = 1"), "pk", "column1", "v1", "v2", "value");
                executeAndCheck("SELECT * FROM %s WHERE pk = 2", 2, row(2, null, 3, null, null));
                executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2", 2, row(3, null));
                executeAndCheck("SELECT v2 FROM %s WHERE pk = 2", 2, row((Integer) null));

                executeAndCheck("SELECT * FROM %s WHERE pk = 3", 1);
                executeAndCheck("SELECT pk, v1 FROM %s WHERE pk = 3", 1);
                executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 3", 1);

                executeAndCheck("SELECT * FROM %s WHERE pk = 4", 2, row(4, null, 3, null, null));
                executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4", 2, row(3, null));
                executeAndCheck("SELECT v2 FROM %s WHERE pk = 4", 2, row((Integer) null));
            }
        }
    }

    @Test
    public void testNonCompactTableWithStaticColumnAndPartitionTombstones() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, c, s) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE pk = ?", 2);
        execute("UPDATE %s USING TIMESTAMP 2002 SET s = ? WHERE pk = ?", 2, 3);
        execute("DELETE FROM %s USING TIMESTAMP 2003 WHERE pk = ?", 4);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ?", 1);
        execute("INSERT INTO %s (pk, c, s) VALUES (?, ?, ?) USING TIMESTAMP 3001", 2, 1, 3);
        execute("DELETE FROM %s USING TIMESTAMP 3002 WHERE pk = ?", 3);
        execute("UPDATE %s USING TIMESTAMP 3003 SET s = ? WHERE pk = ?", 3, 4);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 1);
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 1 AND c = 1", 1);
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 1", 1);
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 2, row(2, 1, 3, null));
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 2 AND c = 1", 2, row(3, null));
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 2", 2, row(3));
        executeAndCheck("SELECT v FROM %s WHERE pk = 2 AND c = 1", 2, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 1", 1);
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 3 AND c = 1", 1);
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 3", 1);
        executeAndCheck("SELECT v FROM %s WHERE pk = 3 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 4 AND c = 1", 2);
        executeAndCheck("SELECT s, v FROM %s WHERE pk = 4 AND c = 1", 2);
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 4", 2, row(3));
        executeAndCheck("SELECT v FROM %s WHERE pk = 4 AND c = 1", 2);
    }

    @Test
    public void testNonCompactTableWithClusteringColumnAndMultipleRegularColumnsAndRowDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v1 int, v2 int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE pk = ? AND c = ? ", 2, 1);
        execute("UPDATE %s USING TIMESTAMP 2002 SET v1 = ? WHERE pk = ? AND c = ?", 2, 3, 1);
        execute("DELETE FROM %s USING TIMESTAMP 2003 WHERE pk = ? AND c = ? ", 4, 1);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND c = ?", 1, 1);
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 3001", 2, 1, 3);
        execute("DELETE FROM %s USING TIMESTAMP 3002 WHERE pk = ? AND c = ?", 3, 1);
        execute("UPDATE %s USING TIMESTAMP 3003 SET v1 = ? WHERE pk = ? AND c = ?", 3, 4, 1);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 1);
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 1", 1);
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 2, row(2, 1, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2 AND c = 1", 2, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 2 AND c = 1", 2, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 1", 1);
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 3 AND c = 1", 1);
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 3 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 4 AND c = 1", 2, row(4, 1, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4 AND c = 1", 2, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 4 AND c = 1", 2, row((Integer) null));
    }

    @Test
    public void testNonCompactTableWithClusteringColumnAndMultipleRegularColumnsAndRowRangeDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v1 int, v2 int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE pk = ? AND c >= ? ", 2, 1);
        execute("UPDATE %s USING TIMESTAMP 2002 SET v1 = ? WHERE pk = ? AND c = ?", 2, 3, 1);
        execute("DELETE FROM %s USING TIMESTAMP 2003 WHERE pk = ? AND c >= ? ", 4, 1);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND c <= ?", 1, 1);
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 3001", 2, 1, 3);
        execute("DELETE FROM %s USING TIMESTAMP 3002 WHERE pk = ? AND c <= ?", 3, 1);
        execute("UPDATE %s USING TIMESTAMP 3003 SET v1 = ? WHERE pk = ? AND c = ?", 3, 4, 1);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 1);
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 1", 1);
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 2, row(2, 1, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2 AND c = 1", 2, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 2 AND c = 1", 2, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 1", 1);
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 3 AND c = 1", 1);
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 3 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 4 AND c = 1", 2, row(4, 1, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4 AND c = 1", 2, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 4 AND c = 1", 2, row((Integer) null));
    }

    @Test
    public void testNonCompactTableWithClusteringColumnAndMultipleRegularColumnsAndColumnDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v1 int, v2 int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        execute("DELETE v2 FROM %s USING TIMESTAMP 2001 WHERE pk = ? AND c = ?", 2, 1);
        execute("UPDATE %s USING TIMESTAMP 2002 SET v1 = ? WHERE pk = ? AND c = ?", 2, 3, 1);
        execute("DELETE v2 FROM %s USING TIMESTAMP 2003 WHERE pk = ? AND c = ?", 4, 1);
        flush();
        execute("DELETE v1 FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND c = ?", 1, 1);
        execute("INSERT INTO %s (pk, c, v1) VALUES (?, ?, ?) USING TIMESTAMP 3001", 2, 1, 3);
        execute("DELETE v1 FROM %s USING TIMESTAMP 3002 WHERE pk = ? AND c = ?", 3, 1);
        execute("UPDATE %s USING TIMESTAMP 3003 SET v1 = ? WHERE pk = ? AND c = ?", 3, 4, 1);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 3, row(1, 1, null, 1));
        // As we have the primary key liveness and all the queried columns in the second SSTable we can stop at this point
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 1 AND c = 1", 2, row(1, null));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1 AND c = 1", 2, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 2, row(2, 1, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2 AND c = 1", 2, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 2 AND c = 1", 2, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 1", 3, row(3, 1, null, 1));
        executeAndCheck("SELECT c, v1 FROM %s WHERE pk = 3 AND c = 1", 3, row(1, null));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 3 AND c = 1", 3, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 4 AND c = 1", 3, row(4, 1, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4 AND c = 1", 3, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 4 AND c = 1", 3, row((Integer) null));
    }

    @Test
    public void testNonCompactTableWithClusteringColumnAndColumnDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        flush();
        execute("DELETE v FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND c = ?", 1, 1);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 2, row(1, 1, null));
        executeAndCheck("SELECT c, v FROM %s WHERE pk = 1 AND c = 1", 2, row(1, null));
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 2, row((Integer) null));
    }

    @Test
    public void testCompactTableWithClusteringColumnAndColumnDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v int, PRIMARY KEY(pk, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        flush();
        execute("DELETE v FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND c = ?", 1, 1);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 1);
        executeAndCheck("SELECT c, v FROM %s WHERE pk = 1 AND c = 1", 1);
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 1);

        execute("ALTER TABLE %s DROP COMPACT STORAGE");

        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 2);
        executeAndCheck("SELECT c, v FROM %s WHERE pk = 1 AND c = 1", 2);
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 2);
    }

    @Test
    public void testNonCompactTableWithMultipleRegularColumnsAndColumnDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int)");

        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1);
        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1);
        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 2000", 1, 2);
        execute("DELETE v2 FROM %s USING TIMESTAMP 2001 WHERE pk = ?", 2);
        execute("UPDATE %s USING TIMESTAMP 2003 SET v1 = ? WHERE pk = ?", 2, 3);
        execute("DELETE v2 FROM %s USING TIMESTAMP 2004 WHERE pk = ?", 4);
        flush();
        execute("DELETE v1 FROM %s USING TIMESTAMP 3000 WHERE pk = ?", 1);
        execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 3001", 2, 3);
        execute("DELETE v1 FROM %s USING TIMESTAMP 3002 WHERE pk = ?", 3);
        execute("UPDATE %s USING TIMESTAMP 3004 SET v1 = ? WHERE pk = ?", 3, 4);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1", 3, row(1, null, 1));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1", 3, row((Integer) null, 1));
        // As the primary key liveness is found on the second SSTable, we can stop there as it it enough to ensure
        // that we know that the row exist
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1", 2, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 2", 2, row(2, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2", 2, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 2", 2, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 3", 3, row(3, null, 1));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 3", 3, row((Integer) null, 1));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 3", 3, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 4", 3, row(4, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4", 3, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 4", 3, row((Integer) null));
    }

    @Test
    public void testCompactTableWithMultipleRegularColumnsAndColumnDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1);
        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1);
        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 2000", 1, 2);
        execute("DELETE v2 FROM %s USING TIMESTAMP 2001 WHERE pk = ?", 2);
        execute("UPDATE %s USING TIMESTAMP 2003 SET v1 = ? WHERE pk = ?", 2, 3);
        execute("DELETE v2 FROM %s USING TIMESTAMP 2004 WHERE pk = ?", 4);
        flush();
        execute("DELETE v1 FROM %s USING TIMESTAMP 3000 WHERE pk = ?", 1);
        execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 3001", 2, 3);
        execute("DELETE v1 FROM %s USING TIMESTAMP 3002 WHERE pk = ?", 3);
        execute("UPDATE %s USING TIMESTAMP 3004 SET v1 = ? WHERE pk = ?", 3, 4);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1", 3, row(1, null, 1));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1", 3, row((Integer) null, 1));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1", 3, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 2", 2, row(2, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2", 2, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 2", 2, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 3", 3, row(3, null, 1));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 3", 3, row((Integer) null, 1));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 3", 3, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 4", 2, row(4, 3, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4", 2, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 4", 2, row((Integer) null));

        execute("ALTER TABLE %s DROP COMPACT STORAGE");

        assertColumnNames(execute("SELECT * FROM %s WHERE pk = 1"), "pk", "column1", "v1", "v2", "value");
        executeAndCheck("SELECT * FROM %s WHERE pk = 1", 3, row(1, null, null, 1, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 1", 3, row((Integer) null, 1));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1", 3, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 2", 3, row(2, null, 3, null, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 2", 3, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 2", 3, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 3", 3, row(3, null, null, 1, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 3", 3, row((Integer) null, 1));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 3", 3, row((Integer) null));

        executeAndCheck("SELECT * FROM %s WHERE pk = 4", 3, row(4, null, 3, null, null));
        executeAndCheck("SELECT v1, v2 FROM %s WHERE pk = 4", 3, row(3, null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 4", 3, row((Integer) null));
    }

    @Test
    public void testNonCompactTableWithAlterTableStatement() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v1 int, PRIMARY KEY(pk, ck))");

        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        flush();
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 3000", 1, 1, 3);
        flush();

        executeAndCheck("SELECT pk, ck, v1 FROM %s WHERE pk = 1 AND ck = 1", 1, row(1, 1, 3));

        execute("ALTER TABLE %s ADD v2 int");

        executeAndCheck("SELECT pk, ck, v1 FROM %s WHERE pk = 1 AND ck = 1", 1, row(1, 1, 3));

        execute("ALTER TABLE %s ADD s int static");

        executeAndCheck("SELECT pk, ck, v1 FROM %s WHERE pk = 1 AND ck = 1", 1, row(1, 1, 3));
    }

    @Test
    public void testNonCompactTableWithAlterTableStatementAndStaticColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s1 int static, v1 int, PRIMARY KEY(pk, ck))");

        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1, 1, 2);
        flush();
        execute("INSERT INTO %s (pk, s1) VALUES (?, ?) USING TIMESTAMP 3000", 1, 3);
        flush();

        executeAndCheck("SELECT pk, s1 FROM %s WHERE pk = 1", 3, row(1, 3));
        executeAndCheck("SELECT DISTINCT pk, s1 FROM %s WHERE pk = 1", 3, row(1, 3));
        executeAndCheck("SELECT s1 FROM %s WHERE pk = 1", 3, row(3));

        execute("ALTER TABLE %s ADD s2 int static");

        executeAndCheck("SELECT pk, s1 FROM %s WHERE pk = 1", 3, row(1, 3));
        executeAndCheck("SELECT DISTINCT pk, s1 FROM %s WHERE pk = 1", 3, row(1, 3));
        executeAndCheck("SELECT s1 FROM %s WHERE pk = 1", 3, row(3));
    }

    @Test
    public void testNonCompactTableWithStaticColumnAndRowDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP 1001", 2, 2);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1);
        flush();
        execute("UPDATE %s USING TIMESTAMP 2000 SET v = ? WHERE pk = ? AND c = ?", 2, 1, 1);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 2001", 2, 1, 2);
        execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE pk = ? AND c = ?", 3, 1);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND c = ?", 1, 1);
        execute("DELETE FROM %s USING TIMESTAMP 3001 WHERE pk = ? AND c = ?", 2, 1);
        execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP 3002", 3, 3);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1", 3, row(1, null, 1, null));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 3);
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 2", 3, row(2, null, 2, null));
        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 3);
        executeAndCheck("SELECT v FROM %s WHERE pk = 2 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 3", 3, row(3, null, 3, null));
        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 1", 2);
        executeAndCheck("SELECT v FROM %s WHERE pk = 3 AND c = 1", 2);
    }

    @Test
    public void testNonCompactTableWithStaticColumnAndRangeDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP 1001", 2, 2);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1);
        flush();
        execute("UPDATE %s USING TIMESTAMP 2000 SET v = ? WHERE pk = ? AND c = ?", 2, 1, 1);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 2001", 2, 1, 2);
        execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE pk = ? AND c >= ?", 3, 0);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = ? AND c > ?", 1, 0);
        execute("DELETE FROM %s USING TIMESTAMP 3001 WHERE pk = ? AND c > ?", 2, 0);
        execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP 3002", 3, 3);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1", 3, row(1, null, 1, null));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 3);
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 2", 3, row(2, null, 2, null));
        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 3);
        executeAndCheck("SELECT v FROM %s WHERE pk = 2 AND c = 1", 1);

        executeAndCheck("SELECT * FROM %s WHERE pk = 3", 3, row(3, null, 3, null));
        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 1", 2);
        executeAndCheck("SELECT v FROM %s WHERE pk = 3 AND c = 1", 2);
    }

    @Test
    public void testNonCompactTableWithStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1);
        flush();
        execute("UPDATE %s USING TIMESTAMP 2000 SET v = ? WHERE pk = ? AND c = ?", 2, 1, 1);
        execute("UPDATE %s USING TIMESTAMP 2001 SET v = ? WHERE pk = ? AND c = ?", 2, 2, 1);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 2002", 3, 1, 2);
        flush();
        execute("UPDATE %s USING TIMESTAMP 3000 SET s = ? WHERE pk = ?", 2, 1);
        execute("INSERT INTO %s (pk, s) VALUES (?, ?) USING TIMESTAMP 3001", 2, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?) USING TIMESTAMP 3002", 3, 1, 1, 3);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1", 3, row(1, 1, 2, 2));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 3, row(1, 1, 2, 2));
        executeAndCheck("SELECT v FROM %s WHERE pk = 1 AND c = 1", 3, row(2));
        executeAndCheck("SELECT s FROM %s WHERE pk = 1", 3, row(2));
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 1", 3, row(2));

        executeAndCheck("SELECT * FROM %s WHERE pk = 2", 3, row(2, 1, 1, 2));
        executeAndCheck("SELECT * FROM %s WHERE pk = 2 AND c = 1", 3, row(2, 1, 1, 2));
        executeAndCheck("SELECT v FROM %s WHERE pk = 2 AND c = 1", 3, row(2));
        executeAndCheck("SELECT s FROM %s WHERE pk = 2", 3, row(1));
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 2", 3, row(1));

        executeAndCheck("SELECT * FROM %s WHERE pk = 3", 3, row(3, 1, 1, 3));
        executeAndCheck("SELECT * FROM %s WHERE pk = 3 AND c = 1", 1, row(3, 1, 1, 3));
        executeAndCheck("SELECT v FROM %s WHERE pk = 3 AND c = 1", 1, row(3));
        executeAndCheck("SELECT s FROM %s WHERE pk = 3", 3, row(1));
        executeAndCheck("SELECT DISTINCT s FROM %s WHERE pk = 3", 3, row(1));
    }

    @Test
    public void testCompactStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?) USING TIMESTAMP 1000", 1, 1);
        execute("INSERT INTO %s (pk, v) VALUES (?, ?) USING TIMESTAMP 1001", 2, 1);
        execute("INSERT INTO %s (pk, v) VALUES (?, ?) USING TIMESTAMP 1002", 3, 1);
        execute("INSERT INTO %s (pk, v) VALUES (?, ?) USING TIMESTAMP 1003", 4, 1);
        flush();
        execute("INSERT INTO %s (pk, v) VALUES (?, ?) USING TIMESTAMP 2000", 1, 2);
        execute("UPDATE %s USING TIMESTAMP 2001 SET v = ? WHERE pk = ?", 2, 2);
        execute("DELETE FROM %s USING TIMESTAMP 2002 WHERE pk = ?", 3);
        execute("DELETE v FROM %s USING TIMESTAMP 2003 WHERE pk = ?", 4);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1", 1, row(1, 2));
        executeAndCheck("SELECT v FROM %s WHERE pk = 1", 1, row(2));
        executeAndCheck("SELECT * FROM %s WHERE pk = 2", 1, row(2, 2));
        executeAndCheck("SELECT v FROM %s WHERE pk = 2", 1, row(2));
        executeAndCheck("SELECT * FROM %s WHERE pk = 3", 1);
        executeAndCheck("SELECT v FROM %s WHERE pk = 3", 1);
        executeAndCheck("SELECT * FROM %s WHERE pk = 4", 1);
        executeAndCheck("SELECT v FROM %s WHERE pk = 4", 1);
    }

    @Test
    public void testNonCompositeCompactTableWithMultipleRegularColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1001", 2, 1, 1);
        execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP 1002", 3, 1, 1);
        execute("INSERT INTO %s (pk, v1, V2) VALUES (?, ?, ?) USING TIMESTAMP 1003", 4, 1, 1);
        execute("INSERT INTO %s (pk, v1, V2) VALUES (?, ?, ?) USING TIMESTAMP 1004", 5, 1, 1);
        flush();
        execute("INSERT INTO %s (pk, v1) VALUES (?, ?) USING TIMESTAMP 2000", 1, 2);
        execute("UPDATE %s USING TIMESTAMP 2001 SET v1 = ? WHERE pk = ?", 2, 2);
        execute("DELETE FROM %s USING TIMESTAMP 2002 WHERE pk = ?", 3);
        execute("DELETE v1 FROM %s USING TIMESTAMP 2003 WHERE pk = ?", 4);
        execute("DELETE v1, v2 FROM %s USING TIMESTAMP 2004 WHERE pk = ?", 5);
        flush();

        executeAndCheck("SELECT * FROM %s WHERE pk = 1", 2, row(1, 2, 1));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 1", 2, row(2));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 1", 2, row(1));
        executeAndCheck("SELECT * FROM %s WHERE pk = 2", 2, row(2, 2, 1));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 2", 2, row(2));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 2", 2, row(1));
        executeAndCheck("SELECT * FROM %s WHERE pk = 3", 1);
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 3", 1);
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 3", 1);
        executeAndCheck("SELECT * FROM %s WHERE pk = 4", 2, row(4, null, 1));
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 4", 2, row((Integer) null));
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 4", 2, row(1));
        executeAndCheck("SELECT * FROM %s WHERE pk = 5", 1);
        executeAndCheck("SELECT v1 FROM %s WHERE pk = 5", 1);
        executeAndCheck("SELECT v2 FROM %s WHERE pk = 5", 1);
    }

    @Test
    public void testSkippingBySliceInSinglePartitionReads() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 2, 2);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 3, 3);
        flush();
        assertEquals(1, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 2, 4);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 3, 5);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 4, 6);
        flush();
        assertEquals(2, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 3, 7);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 4, 8);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 5, 9);
        flush();
        assertEquals(3, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 4, 10);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 5, 11);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 6, 12);
        flush();
        assertEquals(4, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        // point query - test whether sstables are skipped due to not covering the requested slice
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 0", 0);
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 1", 1, row(1, 1, 1));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 2", 2, row(1, 2, 4));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 3", 3, row(1, 3, 7));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 4", 3, row(1, 4, 10));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 5", 2, row(1, 5, 11));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 6", 1, row(1, 6, 12));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c = 7", 0);

        // range query - test whether sstables are skipped due to not covering the requeste slice
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c > -10 AND c <= 0", 0);
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c > -10 AND c < 1", 0);
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c > -10 AND c <= 1", 1, row(1, 1, 1));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c > 1 AND c < 3", 2, row(1, 2, 4));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c >= 6", 1, row(1, 6, 12));
        executeAndCheck("SELECT * FROM %s WHERE pk = 1 AND c > 6", 0);
    }

    @Test
    public void testSkippingBySliceInPartitionRangeReads() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 2, 2);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 3, 3);
        flush();
        assertEquals(1, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 2, 4);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 3, 5);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 4, 6);
        flush();
        assertEquals(2, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 3, 7);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 4, 8);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 5, 9);
        flush();
        assertEquals(3, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 4, 10);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 5, 11);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1, 6, 12);
        flush();
        assertEquals(4, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        // point query - test whether sstables are skipped due to not covering the requested slice
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c = 0 ALLOW FILTERING", 0);
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c = 1 ALLOW FILTERING", 1, row(1, 1, 1));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c = 2 ALLOW FILTERING", 2, row(1, 2, 4));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c = 3 ALLOW FILTERING", 3, row(1, 3, 7));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c = 4 ALLOW FILTERING", 3, row(1, 4, 10));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c = 5 ALLOW FILTERING", 2, row(1, 5, 11));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c = 6 ALLOW FILTERING", 1, row(1, 6, 12));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", 0);

        // range query - test whether sstables are skipped due to not covering the requeste slice
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c > -10 AND c <= 0 ALLOW FILTERING", 0);
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c > -10 AND c < 1 ALLOW FILTERING", 0);
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c > -10 AND c <= 1 ALLOW FILTERING", 1, row(1, 1, 1));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c > 1 AND c < 3 ALLOW FILTERING", 2, row(1, 2, 4));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c >= 6 ALLOW FILTERING", 1, row(1, 6, 12));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE c > 6 ALLOW FILTERING", 0);
    }

    @Test
    public void testSkippingByKeyRangeInPartitionRangeReads() throws Throwable
    {
        DecoratedKey[] keys = new DecoratedKey[8];
        for (int i = 0; i < keys.length; i++)
            keys[i] = DatabaseDescriptor.getPartitioner().decorateKey(Int32Type.instance.decompose(i));
        Arrays.sort(keys);

        int[] k = new int[keys.length];
        String[] t = new String[keys.length];
        for (int i = 0; i < keys.length; i++)
        {
            DecoratedKey key = keys[i];
            k[i] = Int32Type.instance.compose(key.getKey());
            t[i] = key.getToken().getTokenValue().toString();
        }

        createTable("CREATE TABLE %s (pk int, c int, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[1], 1, 1);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[2], 2, 2);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[3], 3, 3);
        flush();
        assertEquals(1, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[2], 2, 4);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[3], 3, 5);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[4], 4, 6);
        flush();
        assertEquals(2, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[3], 3, 7);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[4], 4, 8);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[5], 5, 9);
        flush();
        assertEquals(3, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[4], 4, 10);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[5], 5, 11);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", k[6], 6, 12);
        flush();
        assertEquals(4, getCurrentColumnFamilyStore(KEYSPACE_PER_TEST).getLiveSSTables().size());

        // range query - test whether sstables are skipped due to not covering the requested slice
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN (pk) <= " + t[0], 0);
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN (pk) < " + t[1], 0);
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN(pk) >= " + t[1] + " AND TOKEN (pk) < " + t[2], 1, row(k[1], 1, 1));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN(pk) >= " + t[2] + " AND TOKEN (pk) < " + t[3], 2, row(k[2], 2, 4));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN(pk) >= " + t[3] + " AND TOKEN (pk) < " + t[4], 3, row(k[3], 3, 7));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN(pk) >= " + t[3] + " AND TOKEN (pk) <= " + t[4], 4, row(k[3], 3, 7), row(k[4], 4, 10));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN(pk) >= " + t[4] + " AND TOKEN (pk) < " + t[5], 3, row(k[4], 4, 10));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN(pk) >= " + t[5] + " AND TOKEN (pk) < " + t[6], 2, row(k[5], 5, 11));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN(pk) >= " + t[6] + " AND TOKEN (pk) < " + t[7], 1, row(k[6], 6, 12));
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN(pk) > " + t[6], 0);
        executeAndCheckRangeQuery("SELECT * FROM %s WHERE TOKEN(pk) >= " + t[7], 0);
    }
}