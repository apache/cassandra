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


import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.utils.TestHelper.verifyAndPrint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompactionDeletePKTest extends SimpleCompactionTest
{
    @Test
    public void testPK1DeleteCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ?;",
                Long.valueOf(0) //pk
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected: {"table kind":"REGULAR","partition":{"key":["0"],"position":27,"deletion_info":{"marked_deleted":"2025-01-14T11:20:37.220Z","local_delete_time":"2025-01-14T11:20:37Z"}},"rows":[]}
        UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);
        assertRows(result);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(!partition.hasNext());
        assertTrue(!partition.partitionLevelDeletion().isLive());
        assertTrue(partition.staticRow().isEmpty());
        assertEquals(2, partition.partitionLevelDeletion().markedForDeleteAt());
    }

    @Test
    public void testPK1WriteAndDeleteCompaction() throws Throwable
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

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected: {"table kind":"REGULAR","partition":{"key":["0"],"position":27,"deletion_info":{"marked_deleted":"2025-01-14T11:20:37.220Z","local_delete_time":"2025-01-14T11:20:37Z"}},"rows":[]}
        UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);
        assertRows(result);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(!partition.hasNext());
        assertTrue(!partition.partitionLevelDeletion().isLive());
        assertTrue(partition.staticRow().isEmpty());
        assertEquals(2, partition.partitionLevelDeletion().markedForDeleteAt());
    }

    @Test
    public void testPK2WriteAndDeleteCompactionTwice() throws Throwable
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
        // Write
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?)  using timestamp 3",
                Long.valueOf(0), //pk
                Long.valueOf(112), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(12), Integer.valueOf(21),//ck1,ck2
                Long.valueOf(12), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 4 WHERE pk = ?;",
                Long.valueOf(0) //pk
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        // Expected: {"table kind":"REGULAR","partition":{"key":["0"],"position":27,"deletion_info":{"marked_deleted":"2025-01-14T11:20:37.220Z","local_delete_time":"2025-01-14T11:20:37Z"}},"rows":[]}
        UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);
        assertRows(result);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(!partition.hasNext());
        assertTrue(!partition.partitionLevelDeletion().isLive());
        assertTrue(partition.staticRow().isEmpty());
        assertEquals(4, partition.partitionLevelDeletion().markedForDeleteAt());
    }

    @Test
    public void testPK3DeleteAndWriteCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Delete
        execute("DELETE FROM " + table + " using timestamp 1 WHERE pk = ?;",
                Long.valueOf(0) //pk
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Write
        execute("INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?) using timestamp 2",
                Long.valueOf(0), //pk
                Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                Long.valueOf(11), Integer.valueOf(21),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(!partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());
        assertTrue(!partition.next().isEmpty());
        assertEquals(1, partition.partitionLevelDeletion().markedForDeleteAt());
    }

    @Test
    public void testPKDeleteCompactionInterleaving() throws Throwable
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
            execute("DELETE FROM " + table + " using timestamp ? WHERE pk = ?;",
                    Long.valueOf(timestamp + i), // timestamp
                    Long.valueOf(i) //pk
            );
        }

        // delete a partition that we don't have
        execute("DELETE FROM " + table + " using timestamp ? WHERE pk = ?;",
                Long.valueOf(timestamp + 5), // timestamp
                Long.valueOf(5) //pk
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        majorCompact(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        try(ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext()) {
                UnfilteredRowIterator partition = scanner.next();
                long pk = partition.partitionKey().getKey().getLong();
                if (pk == 5) {
                    assertTrue(!partition.hasNext());
                    assertTrue(!partition.partitionLevelDeletion().isLive());
                    assertTrue(partition.staticRow().isEmpty());
                    assertEquals(pk + timestamp, partition.partitionLevelDeletion().markedForDeleteAt());
                }
                else if (pk % 2 == 0)
                {
                    assertTrue("pk="+pk, !partition.hasNext());
                    assertTrue("pk="+pk,!partition.partitionLevelDeletion().isLive());
                    assertTrue("pk="+pk,partition.staticRow().isEmpty());
                    assertEquals(pk + timestamp, partition.partitionLevelDeletion().markedForDeleteAt());
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
