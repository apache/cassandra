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

package org.apache.cassandra.db.compaction.simple;


import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.utils.TestHelper.verifyAndPrint;

public class CompactionSimpleValueMergeTest extends SimpleCompactionTest
{
    @Test
    public void testStaticRowCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        String writeStatement1 = "INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?)";
        String writeStatement2 = "INSERT INTO " + table + "(pk,ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?)";
        int prefix = 0;
        // Writes, 3 rows in each partition
        for (int i = 0; i < 4; i++)
        {
            execute(writeStatement1,
                    Long.valueOf(i), //pk
                    Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                    Long.valueOf(1), Integer.valueOf(1),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i));//c1,c2
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(2), Integer.valueOf(2),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i));//c1,c2
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(3), Integer.valueOf(3),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i));//c1,c2
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        prefix = 10;
        // Writes, 3 rows in each partition
        for (int i = 0; i < 4; i++)
        {
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(1), Integer.valueOf(1),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i));//c1,c2
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(2), Integer.valueOf(2),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i));//c1,c2
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(3), Integer.valueOf(3),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i));//c1,c2
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        prefix = 20;
        // Writes, 3 rows in each partition
        for (int i = 0; i < 4; i++)
        {
            execute(writeStatement1,
                    Long.valueOf(i), //pk
                    Long.valueOf(311), Integer.valueOf(322),//sc1,sc2
                    Long.valueOf(1), Integer.valueOf(1),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i));//c1,c2
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(2), Integer.valueOf(2),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i));//c1,c2
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(3), Integer.valueOf(3),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i));//c1,c2
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        List<Object[]> rows = new ArrayList<>();
        int[] pks = {2,3,0,1};
        for (int pk : pks)
        {
            rows.add(new Object[]{
            Long.valueOf(pk), //pk
            Long.valueOf(311), Integer.valueOf(322),//sc1,sc2
            Long.valueOf(1), Integer.valueOf(1),//ck1,ck2
            Long.valueOf(prefix+pk), Integer.valueOf(prefix+pk)});//c1,c2
            rows.add(new Object[]{
            Long.valueOf(pk), //pk
            Long.valueOf(311), Integer.valueOf(322),//sc1,sc2
            Long.valueOf(2), Integer.valueOf(2),//ck1,ck2
            Long.valueOf(prefix+pk), Integer.valueOf(prefix+pk)});//c1,c2
            rows.add(new Object[]{
            Long.valueOf(pk), //pk
            Long.valueOf(311), Integer.valueOf(322),//sc1,sc2
            Long.valueOf(3), Integer.valueOf(3),//ck1,ck2
            Long.valueOf(prefix+pk), Integer.valueOf(prefix+pk)});//c1,c2
        }
        UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);
        assertRows(result,
                   rows);
    }

    @Test
    public void testCompaction1TableNoPartitionOverlap() throws Throwable
    {
        genetrateCompactAndVerify(1, 10, false, false, false);
    }

    @Test
    public void testCompaction2TableNoPartitionOverlap() throws Throwable
    {
        genetrateCompactAndVerify(2, 10, false, false, false);
    }

    @Test
    public void testCompaction3TableNoPartitionOverlap() throws Throwable
    {
        genetrateCompactAndVerify(3, 10, false, false, false);
    }

    @Test
    public void testCompaction2TableWithPartitionOverlap() throws Throwable
    {
        genetrateCompactAndVerify(2, 10, true, false, false);
    }

    @Test
    public void testCompaction3TableWithPartitionOverlap() throws Throwable
    {
        genetrateCompactAndVerify(3, 10, true, false, false);
    }
    @Test
    public void testCompaction2TableWithRowOverlap() throws Throwable
    {
        genetrateCompactAndVerify(2, 10, true, true, false);
    }

    @Test
    public void testCompaction3TableWithRowOverlap() throws Throwable
    {
        genetrateCompactAndVerify(3, 10, true, true, false);
    }

    protected void genetrateCompactAndVerify(int sstableCount, int partitionCount, boolean pOverlap, boolean rOverlap, boolean cOverlap) throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        String writeStatement = "INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?)";
        for (int j = 0; j < sstableCount; j++)
        {
            for (int i = 0; i < partitionCount; i++)
                execute(writeStatement,
                        (Long.valueOf(pOverlap ? 0 : j * partitionCount) + i), //pk
                        Long.valueOf(j * partitionCount + i), Integer.valueOf(j * partitionCount + i),//sc1,sc2
                        Long.valueOf((rOverlap ? 0 : j * partitionCount) + i), Integer.valueOf((rOverlap ? 0 : j * partitionCount) + i),//ck1,ck2
                        Long.valueOf(j * partitionCount + i), Integer.valueOf(j * partitionCount + i));//c1,c2

            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        List<Object[]> rows = new ArrayList<>();

        if (pOverlap && rOverlap)
        {
            int prefix = (sstableCount - 1) * partitionCount;
            int[] pks = {2,3,7,9,4,0,8,5,6,1};
            for (int pk : pks)
            {
                rows.add(new Object[]{ Long.valueOf(pk), //pk
                                       Long.valueOf(prefix + pk), Integer.valueOf(prefix + pk),//sc1,sc2
                                       Long.valueOf(pk), Integer.valueOf(pk),//ck1,ck2
                                       Long.valueOf(prefix + pk), Integer.valueOf(prefix + pk) });//c1,c2
            }
            UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);
            assertRows(result,
                       rows);
        }

        if (pOverlap && !rOverlap)
        {
            int prefix = (sstableCount - 1) * partitionCount;
            int[] pks = {2,3,7,9,4,0,8,5,6,1};
            for (int pk : pks)
            {
                for (int j = 0; j < sstableCount; j++)
                {
                    rows.add(new Object[]{ Long.valueOf(pk), //pk
                                           Long.valueOf(prefix + pk), Integer.valueOf(prefix + pk),//sc1,sc2
                                           Long.valueOf(j * partitionCount + pk), Integer.valueOf(j * partitionCount + pk),//ck1,ck2
                                           Long.valueOf(j * partitionCount + pk), Integer.valueOf(j * partitionCount + pk) });//c1,c2
                }
            }
            UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);
            assertRows(result,
                       rows);
        }

        if (!pOverlap && !rOverlap)
        {
            int[][] pks = {{2,3,7,9,4,0,8,5,6,1},
                           {19,2,3,16,12,13,7,15,9,4,10,0,11,14,8,5,6,1,18,17},
                           {19,2,24,3,16,25,12,20,13,7,26,15,23,9,27,21,4,10,28,0,11,14,8,5,22,6,1,18,17,29}};
            for (int pk : pks[sstableCount - 1])
            {
                rows.add(new Object[]{ Long.valueOf(pk), //pk
                                       Long.valueOf(pk), Integer.valueOf(pk),//sc1,sc2
                                       Long.valueOf(pk), Integer.valueOf(pk),//ck1,ck2
                                       Long.valueOf(pk), Integer.valueOf(pk) });//c1,c2

            }
            UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);
            assertRows(result,
                       rows);
        }
    }
}
