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

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.utils.TestHelper.verifyAndPrint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({ "UnnecessaryBoxing", "SingleCharacterStringConcatenation" })
public class CompactionDeleteRowTest extends SimpleCompactionTest
{
    @Test
    public void testRow1DeleteCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected: {"table kind":"REGULAR","partition":{"key":["0"],"position":27,"deletion_info":{"marked_deleted":"2025-01-14T11:20:37.220Z","local_delete_time":"2025-01-14T11:20:37Z"}},"rows":[]}
        UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);
        assertRows(result);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(partition.staticRow().isEmpty());
        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(!((Row)row).deletion().time().isLive());
        assertEquals(2, ((Row)row).deletion().time().markedForDeleteAt());
    }

    @Test
    public void testRow1WriteAndDeleteCompaction() throws Throwable
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
                Long.valueOf(11), Integer.valueOf(21),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                Long.valueOf(0), //pk
                Long.valueOf(11), //ck1
                Integer.valueOf(21)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected: {"table kind":"REGULAR","partition":{"key":["0"],"position":27,"deletion_info":{"marked_deleted":"2025-01-14T11:20:37.220Z","local_delete_time":"2025-01-14T11:20:37Z"}},"rows":[]}
        UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());
        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(!((Row)row).deletion().time().isLive());
        assertEquals(2, ((Row)row).deletion().time().markedForDeleteAt());

        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        assertTrue(!cells.hasNext());
    }

    @Test
    public void testRow1WriteAndDeleteViaTTLCompaction() throws Throwable
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
                Long.valueOf(11), Integer.valueOf(21),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // set column TTL
        execute("INSERT INTO " + table + "(pk, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?) using TTL 1",
                Long.valueOf(0), //pk
                Long.valueOf(11), Integer.valueOf(21),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        Thread.sleep(2000);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        // {"table kind":"REGULAR",
        // "partition":{"key":["0"],"position":31},
        // "rows":[
        //      {"type":"static_block","position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000001Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000001Z"}]},
        //      {"type":"row","position":31,"clustering":[11,21],
        //          "liveness_info":{"tstamp":"2025-03-12T09:01:59.127Z","ttl":1,"expires_at":"2025-03-12T09:02:00Z","expired":true},
        //          "cells":[{"name":"c1","deletion_info":{"local_delete_time":"2025-03-12T09:01:59Z"}},{"name":"c2","deletion_info":{"local_delete_time":"2025-03-12T09:01:59Z"}}]}]}

        // {"table kind":"REGULAR","partition":{"key":["0"],"position":31},
        // "rows":[
        //      {"type":"static_block","position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000001Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000001Z"}]},
        //      {"type":"row","position":31,"clustering":[11,21],
        //      "liveness_info":{"tstamp":"2025-03-12T09:47:44.760Z","ttl":1,"expires_at":"2025-03-12T09:47:45Z","expired":true},
        //      "cells":[{"name":"c1","value":""},{"name":"c2","value":""}]}]}
        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());
        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row)row).deletion().time().isLive()); // expired rows are not transformed into tombstones
        LivenessInfo livenessInfo = ((Row) row).primaryKeyLivenessInfo();
        assertEquals(1, livenessInfo.ttl()); // TTL as set
        assertEquals(livenessInfo.localExpirationTime()-1, livenessInfo.timestamp()/1000000);

        // TTL expiry for the row turns the cells into tombstones
        Iterator<Cell<?>> cells = ((Row) row).cells().iterator();
        Cell<?> cell = cells.next();
        assertEquals(cell.localDeletionTime(), cell.timestamp()/1000000);
        assertTrue(cell.isTombstone());
        assertTrue(cells.hasNext());
        cell = cells.next();
        assertEquals(cell.localDeletionTime(), cell.timestamp()/1000000);
        assertTrue(cell.isTombstone());
        assertTrue(!cells.hasNext());
    }

    @Test
    public void testRow1WriteAndRowDeleteAndPKDeleteCompaction() throws Throwable
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
                Long.valueOf(11), Integer.valueOf(21),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                Long.valueOf(0), //pk
                Long.valueOf(11), //ck1
                Integer.valueOf(21)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 3 WHERE pk = ?;",
                Long.valueOf(0) //pk
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected: {"table kind":"REGULAR","partition":{"key":["0"],"position":27,"deletion_info":{"marked_deleted":"2025-01-14T11:20:37.220Z","local_delete_time":"2025-01-14T11:20:37Z"}},"rows":[]}
        UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(!partition.hasNext());
        assertTrue(!partition.partitionLevelDeletion().isLive());
        assertEquals(3, partition.partitionLevelDeletion().markedForDeleteAt());
        assertTrue(partition.staticRow().isEmpty());
    }

    @Test
    public void testRow1WriteAndPKDeleteAndRowDeleteCompaction() throws Throwable
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
                Long.valueOf(11), Integer.valueOf(21),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ?;",
                Long.valueOf(0) //pk
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 3 WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                Long.valueOf(0), //pk
                Long.valueOf(11), //ck1
                Integer.valueOf(21)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(!partition.partitionLevelDeletion().isLive());
        assertEquals(2, partition.partitionLevelDeletion().markedForDeleteAt());
        assertTrue(partition.staticRow().isEmpty());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(!((Row)row).deletion().time().isLive());
        assertEquals(3, ((Row)row).deletion().time().markedForDeleteAt());
        assertTrue(((Row)row).columnData().isEmpty());
    }

    @Test
    public void testRow2WriteDeleteWriteCompaction() throws Throwable
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
                Long.valueOf(11), Integer.valueOf(21),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                Long.valueOf(0), //pk
                Long.valueOf(11), //ck1
                Integer.valueOf(21)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Write
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?)  using timestamp 3",
                Long.valueOf(0), //pk
                Long.valueOf(112), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(11), Integer.valueOf(21),//ck1,ck2
                Long.valueOf(12), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(!((Row)row).deletion().time().isLive());
        assertEquals(2, ((Row)row).deletion().time().markedForDeleteAt());
        assertTrue(!row.isEmpty());
    }

    @Test
    public void testRowDeleteCompactionInterleaving() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        String writeStatement1 = "INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?) using timestamp ?";
        String writeStatement2 = "INSERT INTO " + table + "(pk,ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?) using timestamp ?";
        int prefix = 0;
        long timestamp = 0;
        // Writes, 3 rows in each partition
        for (int i = 0; i < 4; i++)
        {
            execute(writeStatement1,
                    Long.valueOf(i), //pk
                    Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                    Long.valueOf(1), Integer.valueOf(1),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i),//c1,c2
                    timestamp++);
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(2), Integer.valueOf(2),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i),//c1,c2
                    timestamp++);
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(3), Integer.valueOf(3),//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(prefix+i),//c1,c2
                    timestamp++);
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // delete every other partition
        for (int i = 0; i < 4; i+=2)
        {
            execute("DELETE FROM " + table + " using timestamp ? WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                    Long.valueOf(timestamp + i), // timestamp
                    Long.valueOf(i), //pk
                    Long.valueOf(2), //ck1
                    Integer.valueOf(2)  //ck2
            );
        }

        // delete a partition + row that we don't have
        execute("DELETE FROM " + table + " using timestamp ? WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                Long.valueOf(timestamp + 5), // timestamp
                Long.valueOf(5), //pk
                Long.valueOf(11), //ck1
                Integer.valueOf(21)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        try(ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext()) {
                UnfilteredRowIterator partition = scanner.next();
                long pk = partition.partitionKey().getKey().getLong();
                if (pk == 5) {
                    assertTrue(partition.hasNext());
                    assertTrue(partition.partitionLevelDeletion().isLive());
                    assertTrue(partition.staticRow().isEmpty());
                    Unfiltered row = partition.next();
                    assertTrue("pk="+pk,!row.isEmpty());
                    assertTrue("pk="+pk,row.isRow());
                    assertTrue("pk="+pk,!((Row)row).deletion().time().isLive());
                    assertEquals("pk="+pk,pk + timestamp, ((Row)row).deletion().time().markedForDeleteAt());

                }
                else if (pk % 2 == 0)
                {
                    assertTrue("pk="+pk,partition.hasNext());
                    assertTrue("pk="+pk,partition.partitionLevelDeletion().isLive());
                    assertTrue("pk="+pk,!partition.staticRow().isEmpty());
                    Unfiltered row = partition.next();
                    assertTrue("pk=" + pk, !row.isEmpty());
                    row = partition.next();
                    assertTrue("pk="+pk,!row.isEmpty());
                    assertTrue("pk="+pk,row.isRow());
                    assertTrue("pk="+pk,!((Row)row).deletion().time().isLive());
                    assertEquals("pk="+pk,pk + timestamp, ((Row)row).deletion().time().markedForDeleteAt());

                    row = partition.next();
                    assertTrue("pk="+pk,!row.isEmpty());
                    assertTrue("pk="+pk,!partition.hasNext());
                }
                else
                {
                    assertTrue("pk="+pk,partition.hasNext());
                    assertTrue("pk="+pk,partition.partitionLevelDeletion().isLive());
                    assertTrue("pk="+pk,!partition.staticRow().isEmpty());
                    assertTrue("pk="+pk,!partition.next().isEmpty());
                    assertTrue("pk="+pk,!partition.next().isEmpty());
                    assertTrue("pk="+pk,!partition.next().isEmpty());
                    assertTrue("pk="+pk,!partition.hasNext());
                }
            }
        }
    }
}
