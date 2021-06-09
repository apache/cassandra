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

package org.apache.cassandra.cql3;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;

@RunWith(Parameterized.class)
public class MemtableQuickTest extends CQLTester
{
    static String keyspace;
    String table;
    ColumnFamilyStore cfs;

    int partitions = 50_000;
    int rowsPerPartition = 4;

    int deletedPartitionsStart = 20_000;
    int deletedPartitionsEnd = deletedPartitionsStart + 10_000;

    int deletedRowsStart = 40_000;
    int deletedRowsEnd = deletedRowsStart + 5_000;

    @Parameterized.Parameter()
    public String memtableClass;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of("SkipListMemtable",
                                "TrieMemtable",
                                "PersistentMemoryMemtable");
    }

    @BeforeClass
    public static void setUp()
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        CQLTester.disablePreparedReuseForTest();
        System.err.println("setupClass done.");
    }

    @Test
    public void testMemtable() throws Throwable
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))" +
                                      " with compression = {'enabled': false}" +
                                      " and memtable = { 'class': '" + memtableClass + "'}");
        execute("use " + keyspace + ';');

        String writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        cfs.forceBlockingFlush(UNIT_TESTS);

        long i;
        long limit = partitions;
        System.out.println("Writing " + partitions + " partitions of " + rowsPerPartition + " rows");
        for (i = 0; i < limit; ++i)
        {
            for (long j = 0; j < rowsPerPartition; ++j)
                execute(writeStatement, i, j, i + j);
        }

        System.out.println("Deleting partitions between " + deletedPartitionsStart + " and " + deletedPartitionsEnd);
        for (i = deletedPartitionsStart; i < deletedPartitionsEnd; ++i)
        {
            // no partition exists, but we will create a tombstone
            execute("DELETE FROM " + table + " WHERE userid = ?", i);
        }

        System.out.println("Deleting rows between " + deletedRowsStart + " and " + deletedRowsEnd);
        for (i = deletedRowsStart; i < deletedRowsEnd; ++i)
        {
            // no row exists, but we will create a tombstone (and partition)
            execute("DELETE FROM " + table + " WHERE userid = ? AND picid = ?", i, 0L);
        }

        System.out.println("Reading " + partitions + " partitions");
        for (i = 0; i < limit; ++i)
        {
            UntypedResultSet result = execute("SELECT * FROM " + table + " WHERE userid = ?", i);
            if (i >= deletedPartitionsStart && i < deletedPartitionsEnd)
                assertEmpty(result);
            else
            {
                int start = 0;
                if (i >= deletedRowsStart && i < deletedRowsEnd)
                    start = 1;
                Object[][] rows = new Object[rowsPerPartition - start][];
                for (long j = start; j < rowsPerPartition; ++j)
                    rows[(int) (j - start)] = row(i, j, i + j);
                assertRows(result, rows);
            }
        }


        int deletedPartitions = deletedPartitionsEnd - deletedPartitionsStart;
        int deletedRows = deletedRowsEnd - deletedRowsStart;
        System.out.println("Selecting *");
        UntypedResultSet result = execute("SELECT * FROM " + table);
        assertRowCount(result, rowsPerPartition * (partitions - deletedPartitions) - deletedRows);

        cfs.forceBlockingFlush(UNIT_TESTS);

        System.out.println("Selecting *");
        result = execute("SELECT * FROM " + table);
        assertRowCount(result, rowsPerPartition * (partitions - deletedPartitions) - deletedRows);
    }
}