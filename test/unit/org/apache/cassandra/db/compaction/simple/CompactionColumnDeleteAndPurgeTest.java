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


import java.util.Iterator;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.TestHelper;

import static org.junit.Assert.assertTrue;

@SuppressWarnings({ "UnnecessaryBoxing", "SingleCharacterStringConcatenation" })
public class CompactionColumnDeleteAndPurgeTest extends SimpleCompactionTest
{
    @Test
    public void testColumn1DeleteCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', " +
                                         "'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 " +
                                             "bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with " +
                                             "gc_grace_seconds=0");
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

        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        assertTrue(cfs.getLiveSSTables().isEmpty());
    }

    @Test
    public void testWriteRowAndDeleteAllColumnsCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', " +
                                         "'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 " +
                                             "bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with " +
                                             "gc_grace_seconds=0");
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

        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        TestHelper.verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row) row).deletion().time().isLive());

        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        assertTrue(!cells.hasNext());
    }

    @Test
    public void testWriteRowAndDeleteOneColumnCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', " +
                                         "'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 " +
                                             "bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with " +
                                             "gc_grace_seconds=0");
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

        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        TestHelper.verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row) row).deletion().time().isLive());

        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        Cell<?> cell = cells.next();
        assertTrue(!cell.isTombstone());
        assertTrue(!cells.hasNext());
    }

    @Test
    public void testWriteRowAndDeleteOneColumnViaTTLCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', " +
                                         "'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 " +
                                             "bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with " +
                                             "gc_grace_seconds=0");
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
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        TestHelper.verifyAndPrint(cfs, sstable);
        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row) row).deletion().time().isLive());

        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        Cell<?> cell = cells.next();
        assertTrue(!cell.isTombstone());
        assertTrue(!cells.hasNext());
    }

    @Test
    public void testWriteRowAndDeleteOneStaticColumnCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', " +
                                         "'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 " +
                                             "bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with " +
                                             "gc_grace_seconds=0");
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

        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        TestHelper.verifyAndPrint(cfs, sstable);
        // Expected:{"table kind":"REGULAR","partition":{"key":["0"],"position":31},"rows":[{"type":"static_block",
        // "position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000001Z"},{"name":"sc2",
        // "value":222,"tstamp":"1970-01-01T00:00:00.000001Z"}]},{"type":"row","position":31,"clustering":[0,0],
        // "liveness_info":{"tstamp":"1970-01-01T00:00:00.000001Z"},"cells":[{"name":"c1",
        // "deletion_info":{"local_delete_time":"2025-01-25T08:48:55Z"},"tstamp":"1970-01-01T00:00:00.000002Z"},
        // {"name":"c2","deletion_info":{"local_delete_time":"2025-01-25T08:48:55Z"},"tstamp":"1970-01-01T00:00:00
        // .000002Z"}]}]}
        //          {"table kind":"REGULAR","partition":{"key":["0"],"position":31},"rows":[{"type":"static_block",
        //          "position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000001Z"},
        //          {"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000001Z"}]},{"type":"row","position":31,
        //          "clustering":[0,0],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000001Z"},
        //          "cells":[{"name":"c1","deletion_info":{"local_delete_time":"2025-01-25T08:49:54Z"},
        //          "tstamp":"1970-01-01T00:00:00.000002Z"},{"name":"c2",
        //          "deletion_info":{"local_delete_time":"2025-01-25T08:49:54Z"},"tstamp":"1970-01-01T00:00:00
        //          .000002Z"}]}]}
        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        Row staticRow = partition.staticRow();
        assertTrue(!staticRow.isEmpty());
        Iterator<Cell<?>> staticCells = staticRow.cells().iterator();
        Cell<?> cell = staticCells.next();
        assertTrue(!cell.isTombstone());
        assertTrue(!staticCells.hasNext());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row) row).deletion().time().isLive());

        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        cell = cells.next();
        assertTrue(!cell.isTombstone());
        cell = cells.next();
        assertTrue(!cell.isTombstone());
    }
}
