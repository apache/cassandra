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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class RowIterationTest extends CQLTester
{
    @Test
    public void testRowIteration() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", i, 0, i, i, (long)i);
        Util.flush(cfs);
        assertEquals(10, execute("SELECT * FROM %s").size());
    }

    @Test
    public void testRowIterationDeletionTime() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        execute("INSERT INTO %s (a, b) VALUES (?, ?) USING TIMESTAMP ?", 0, 0, 0L);
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE a = ?", 0L, 0);

        Util.flush(cfs);

        // Delete row in second sstable with higher timestamp
        execute("INSERT INTO %s (a, b) VALUES (?, ?) USING TIMESTAMP ?", 0, 0, 1L);
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE a = ?", 1L, 0);

        long localDeletionTime = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs).build()).partitionLevelDeletion().localDeletionTime();

        Util.flush(cfs);

        DeletionTime dt = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs).build()).partitionLevelDeletion();
        assertEquals(1L, dt.markedForDeleteAt());
        assertEquals(localDeletionTime, dt.localDeletionTime());
    }

    @Test
    public void testRowIterationDeletion() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        // Delete a row in first sstable
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE a = ?", 0L, 0);
        Util.flush(cfs);

        assertFalse(Util.getOnlyPartitionUnfiltered(Util.cmd(cfs).build()).isEmpty());
    }
}
