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


import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.utils.TestHelper.verifyAndPrint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({ "UnnecessaryBoxing", "SingleCharacterStringConcatenation" })
public class CompactionColumnTest extends SimpleCompactionTest
{
    @Test
    public void testColumn1DeleteCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Delete cell
        execute("DELETE c1 FROM " + table + "  using timestamp 1 WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected: {"table kind":"REGULAR","partition":{"key":["0"],"position":11},"rows":[{"type":"row","position":11,"clustering":[0,0],"cells":[{"name":"c1","deletion_info":{"local_delete_time":"2025-03-12T11:32:18Z"},"tstamp":"1970-01-01T00:00:00.000001Z"}]}]}
        UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);
        assertRows(result);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(partition.staticRow().isEmpty());
        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row)row).deletion().time().isLive());
        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        Cell<?> cell = cells.next();
        assertEquals(1, cell.timestamp());
        assertTrue(cell.isTombstone());
    }

    @Test
    public void testColumnCompactionIntoSingleRow() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, c3 bigint, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Write
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c3)VALUES(?, ?,?, ?,?, ?)  using timestamp 1",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(0), Integer.valueOf(0),//ck1,ck2
                Long.valueOf(3));//c3

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Write
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c2)VALUES(?, ?,?, ?,?, ?)  using timestamp 2",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(0), Integer.valueOf(0),//ck1,ck2
                Integer.valueOf(2));//c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Write
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1)VALUES(?, ?,?, ?,?, ?)  using timestamp 3",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(0), Integer.valueOf(0),//ck1,ck2
                Long.valueOf(1));//c1

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());
        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row)row).deletion().time().isLive());
        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        Cell<?> cell = cells.next();
        cell = cells.next();
        cell = cells.next();
        assertTrue(!partition.hasNext());

    }

    @Test
    public void testPartialColumnsCompaction64Columns() throws Throwable
    {
        int columnCount = 64;
        testPartialColoumnsCompaction(columnCount);
    }

    @Test
    public void testPartialColumnsCompactionOver64Columns() throws Throwable
    {
        int columnCount = 68;
        testPartialColoumnsCompaction(columnCount);
    }

    private void testPartialColoumnsCompaction(int columnCount) throws IOException
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String createTable = "CREATE TABLE %s ( pk bigint, ck1 bigint";
        for (int i = 0; i < columnCount; i++) createTable += ", c" + i + " bigint";
        createTable += ", PRIMARY KEY(pk, ck1))";

        String table = createTable(keyspace, createTable);
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        // one row has all the columns
        String insertAll = "INSERT INTO " + table + "(pk,ck1";
        for (int i = 0; i < columnCount; i++) insertAll += ", c" + i;
        insertAll += ")VALUES(?,?";
        for (int i = 0; i < columnCount; i++) insertAll += ",?";
        insertAll += ")  using timestamp 1";
        Long[] values = new Long[2 + columnCount];
        Arrays.fill(values, Long.valueOf(0));
        execute(insertAll, (Object[]) values);

        String insertEven = "INSERT INTO " + table + "(pk,ck1";
        for (int i = 0; i < columnCount; i+=2) insertEven += ", c" + i;
        insertEven += ")VALUES(?,?";
        for (int i = 0; i < columnCount; i+=2) insertEven += ",?";
        insertEven += ")  using timestamp 2";
        values = new Long[2 + columnCount / 2];
        Arrays.fill(values, Long.valueOf(1));
        execute(insertEven, (Object[]) values);

        String insertOdd = "INSERT INTO " + table + "(pk,ck1";
        for (int i = 1; i < columnCount; i+=2) insertOdd += ", c" + i;
        insertOdd += ")VALUES(?,?";
        for (int i = 1; i < columnCount; i+=2) insertOdd += ",?";
        insertOdd += ")  using timestamp 3";
        values = new Long[2 + columnCount / 2];
        Arrays.fill(values, Long.valueOf(2));
        execute(insertOdd, (Object[]) values);

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        ISSTableScanner partitions = sstable.getScanner();

        for (int i=0;i<3;i++)
        {
            UnfilteredRowIterator partition = partitions.next();
            assertTrue(partition.hasNext());
            assertTrue(partition.partitionLevelDeletion().isLive());
            assertTrue(partition.staticRow().isEmpty());
            Unfiltered row = partition.next();
            assertTrue(row.isRow());
            assertTrue(((Row) row).deletion().time().isLive());
            long timestamp = ((Row) row).primaryKeyLivenessInfo().timestamp();
            Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
            if (timestamp == 1)
            {
                for (int colIndex=0;colIndex<columnCount;colIndex++)
                {
                    Cell<?> cell = cells.next();
                    assertTrue(cell.valueSize()!=0);
                }
            }
            else if (timestamp == 2)
            {
                for (int colIndex=0;colIndex<columnCount/2;colIndex++)
                {
                    Cell<?> cell = cells.next();
                    assertTrue(cell.valueSize()!=0);
                    String columnName = cell.column().name.toString();
                    int cellColIndex = Integer.parseInt(columnName.substring(1));
                    assertEquals("Unexpected position:" + cellColIndex, 0, cellColIndex % 2);
                }
            }
            else if (timestamp == 3)
            {
                for (int colIndex = 0; colIndex<columnCount/2; colIndex++)
                {
                    Cell<?> cell = cells.next();
                    assertTrue(cell.valueSize()!=0);
                    String columnName = cell.column().name.toString();
                    int cellColIndex = Integer.parseInt(columnName.substring(1));
                    assertEquals("Unexpected position:" + cellColIndex, 1, cellColIndex % 2);
                }
            }
            else {
                fail();
            }
            assertTrue(!cells.hasNext());
            assertTrue(!partition.hasNext());
        }
        assertTrue(!partitions.hasNext());
    }

    @Test
    public void testPartialColumnsCompactionUnder64Columns() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, c3 bigint, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Write 1,1,c1
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1)VALUES(?, ?,?, ?,?, ?)  using timestamp 1",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(1), Integer.valueOf(1),//ck1,ck2
                Long.valueOf(1));//c1

        // Write 2,2,c2
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c2)VALUES(?, ?,?, ?,?, ?)  using timestamp 2",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(2), Integer.valueOf(2),//ck1,ck2
                Integer.valueOf(2));//c2

        // Write 3,3,c3
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c3)VALUES(?, ?,?, ?,?, ?)  using timestamp 3",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(3), Integer.valueOf(3),//ck1,ck2
                Long.valueOf(3));//c3
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Write 1,1,c3
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c3)VALUES(?, ?,?, ?,?, ?)  using timestamp 4",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(1), Integer.valueOf(1),//ck1,ck2
                Long.valueOf(1));//c3

        // Write 2,2,c1
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1)VALUES(?, ?,?, ?,?, ?)  using timestamp 5",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(2), Integer.valueOf(2),//ck1,ck2
                Long.valueOf(2));//c1

        // Write 3,3,c2
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c2)VALUES(?, ?,?, ?,?, ?)  using timestamp 6",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(3), Integer.valueOf(3),//ck1,ck2
                Integer.valueOf(3));//c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        // We expect:
        // 1,1,c1=1,c3=1
        // 2,2,c1=2,c2=2
        // 3,3,c2=3,c3=3
        // {"table kind":"REGULAR","partition":{"key":["0"],"position":31},
        //      "rows":[
        //          {"type":"static_block","position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000006Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000006Z"}]},
        //          {"type":"row","position":31,"clustering":[1,1],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000004Z"},
        //              "cells":[{"name":"c1","value":1},{"name":"c3","value":1}]},
        //          {"type":"row","position":67,"clustering":[2,2],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000005Z"},
        //              "cells":[{"name":"c1","value":2},{"name":"c2","value":2}]},
        //         {"type":"row","position":99,"clustering":[3,3],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000006Z"},
        //              "cells":[{"name":"c2","value":3},{"name":"c3","value":3}]}]}
        verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());
        for (int i=0;i<3;i++)
        {
            Unfiltered row = partition.next();
            assertTrue(row.isRow());
            assertTrue(((Row) row).deletion().time().isLive());
            Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
            Cell<?> cell = cells.next();
            cell = cells.next();
            assertTrue(!cells.hasNext());
        }
        assertTrue(!partition.hasNext());
    }

    @Test
    public void testWriteRowAndDeleteAllColumnsCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Write
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?)  using timestamp 1",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(0), Integer.valueOf(0),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete cells
        execute("DELETE c1, c2 FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected:{"table kind":"REGULAR","partition":{"key":["0"],"position":31},"rows":[{"type":"static_block","position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000001Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000001Z"}]},{"type":"row","position":31,"clustering":[0,0],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000001Z"},"cells":[{"name":"c1","deletion_info":{"local_delete_time":"2025-01-25T08:48:55Z"},"tstamp":"1970-01-01T00:00:00.000002Z"},{"name":"c2","deletion_info":{"local_delete_time":"2025-01-25T08:48:55Z"},"tstamp":"1970-01-01T00:00:00.000002Z"}]}]}
        //          {"table kind":"REGULAR","partition":{"key":["0"],"position":31},"rows":[{"type":"static_block","position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000001Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000001Z"}]},{"type":"row","position":31,"clustering":[0,0],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000001Z"},"cells":[{"name":"c1","deletion_info":{"local_delete_time":"2025-01-25T08:49:54Z"},"tstamp":"1970-01-01T00:00:00.000002Z"},{"name":"c2","deletion_info":{"local_delete_time":"2025-01-25T08:49:54Z"},"tstamp":"1970-01-01T00:00:00.000002Z"}]}]}
        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row)row).deletion().time().isLive());

        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        Cell<?> cell = cells.next();
        assertEquals(2, cell.timestamp());
        assertTrue(cell.isTombstone());
        cell = cells.next();
        assertEquals(2, cell.timestamp());
    }

    @Test
    public void testWriteRowAndDeleteOneColumnCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Write
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?)  using timestamp 1",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(0), Integer.valueOf(0),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete cells
        execute("DELETE c1 FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row)row).deletion().time().isLive());

        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        Cell<?> cell = cells.next();
        assertEquals(2, cell.timestamp());
        assertTrue(cell.isTombstone());

        cell = cells.next();
        assertTrue(!cell.isTombstone());
    }

    @Test
    public void testWriteRowAndDeleteOneColumnViaTTLCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', " +
                                         "'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 " +
                                             "bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Write
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?)  using timestamp 1",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(0), Integer.valueOf(0),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // set column TTL
        execute("UPDATE " + table + " using TTL 1 SET c1 = ? WHERE pk = ? AND ck1 = ? AND ck2 = ?",
                Long.valueOf(2), // c1
                Long.valueOf(0), //pk
                Long.valueOf(0), Integer.valueOf(0));//ck1,ck2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        Thread.sleep(2000);
        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row) row).deletion().time().isLive());

        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        Cell<?> cell = cells.next();
        // The cell is converted to a tombstone, and the expiration time becomes both the TS and the LDT
        assertEquals(cell.localDeletionTime(), cell.timestamp()/1000000);
        assertTrue(cell.isTombstone());

        assertTrue(cells.hasNext());
        cell = cells.next();
        assertTrue(!cell.isTombstone());
        assertTrue(!cells.hasNext());
    }

    @Test
    public void testWriteRowAndDeleteOneStaticColumnCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Write
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?)  using timestamp 1",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(0), Integer.valueOf(0),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete cells
        execute("DELETE sc1 FROM " + table + "  using timestamp 2 WHERE pk = ?;",
                Long.valueOf(0) //pk
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected:{"table kind":"REGULAR","partition":{"key":["0"],"position":31},"rows":[{"type":"static_block","position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000001Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000001Z"}]},{"type":"row","position":31,"clustering":[0,0],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000001Z"},"cells":[{"name":"c1","deletion_info":{"local_delete_time":"2025-01-25T08:48:55Z"},"tstamp":"1970-01-01T00:00:00.000002Z"},{"name":"c2","deletion_info":{"local_delete_time":"2025-01-25T08:48:55Z"},"tstamp":"1970-01-01T00:00:00.000002Z"}]}]}
        //          {"table kind":"REGULAR","partition":{"key":["0"],"position":31},"rows":[{"type":"static_block","position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000001Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000001Z"}]},{"type":"row","position":31,"clustering":[0,0],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000001Z"},"cells":[{"name":"c1","deletion_info":{"local_delete_time":"2025-01-25T08:49:54Z"},"tstamp":"1970-01-01T00:00:00.000002Z"},{"name":"c2","deletion_info":{"local_delete_time":"2025-01-25T08:49:54Z"},"tstamp":"1970-01-01T00:00:00.000002Z"}]}]}
        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        Row staticRow = partition.staticRow();
        assertTrue(!staticRow.isEmpty());
        Iterator<Cell<?>> staticCells = staticRow.cells().iterator();
        Cell<?> cell = staticCells.next();
        assertTrue(cell.isTombstone());
        cell = staticCells.next();
        assertTrue(!cell.isTombstone());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row)row).deletion().time().isLive());

        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        cell = cells.next();
        assertTrue(!cell.isTombstone());
        cell = cells.next();
        assertTrue(!cell.isTombstone());
    }
}
