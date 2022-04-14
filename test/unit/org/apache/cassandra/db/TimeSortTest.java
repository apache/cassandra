/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.util.Iterator;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;

public class TimeSortTest extends CQLTester
{
    @Test
    public void testMixedSources() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 0, 100, 0, 100L);
        Util.flush(cfs);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 0, 0, 1, 0L);

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b >= ? LIMIT 1000", 0, 10), row(0, 100, 0));
    }

    @Test
    public void testTimeSort() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        for (int i = 900; i < 1000; ++i)
            for (int j = 0; j < 8; ++j)
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", i, j * 2, 0, (long)j * 2);

        validateTimeSort();
        Util.flush(cfs);
        validateTimeSort();

        // interleave some new data to test memtable + sstable
        DecoratedKey key = Util.dk("900");
        for (int j = 0; j < 4; ++j)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 900, j * 2 + 1, 1, (long)j * 2 + 1);

        // and some overwrites
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 900, 0, 2, 100L);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 900, 10, 2, 100L);

        // verify
        UntypedResultSet results = execute("SELECT * FROM %s WHERE a = ? AND b >= ? LIMIT 1000", 900, 0);
        assertEquals(12, results.size());
        Iterator<UntypedResultSet.Row> iter = results.iterator();
        for (int j = 0; j < 8; j++)
        {
            UntypedResultSet.Row row = iter.next();
            assertEquals(j, row.getInt("b"));
        }

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b IN (?, ?)", 900, 0, 10),
                row(900, 0, 2),
                row(900, 10, 2));
    }

    private void validateTimeSort() throws Throwable
    {
        for (int i = 900; i < 1000; ++i)
        {
            for (int j = 0; j < 8; j += 3)
            {
                UntypedResultSet results = execute("SELECT writetime(c) AS wt FROM %s WHERE a = ? AND b >= ? LIMIT 1000", i, j * 2);
                assertEquals(8 - j, results.size());
                int k = j;
                for (UntypedResultSet.Row row : results)
                    assertEquals((k++) * 2, row.getLong("wt"));
            }
        }
    }
}
