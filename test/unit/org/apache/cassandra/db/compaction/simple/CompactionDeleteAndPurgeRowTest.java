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
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.utils.TestHelper.verifyAndPrint;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({ "UnnecessaryBoxing", "SingleCharacterStringConcatenation" })
public class CompactionDeleteAndPurgeRowTest extends SimpleCompactionTest
{
    @Test
    public void testRow1DeleteCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with gc_grace_seconds=0");
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
        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        assertTrue(cfs.getLiveSSTables().isEmpty());
    }

    @Test
    public void testRow1WriteAndDeleteCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with gc_grace_seconds=0");
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
                Long.valueOf(11),Integer.valueOf(21)  //ck1,ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        Thread.sleep(1000);

        cfs.forceMajorCompaction();

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());
        assertTrue(!partition.hasNext());
    }

    @Test
    public void testRow1WriteAndDeleteViaTTLCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with gc_grace_seconds=0");
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

        // set row TTL
        execute("INSERT INTO " + table + "(pk, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?) using TTL 1",
                Long.valueOf(0), //pk
                Long.valueOf(11), Integer.valueOf(21),//ck1,ck2
                Long.valueOf(1), Integer.valueOf(2));//c1,c2

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        Thread.sleep(2000);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());
        assertTrue(!partition.hasNext());
    }

    @Test
    public void testRow1WriteAndRowDeleteAndPKDeleteCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with gc_grace_seconds=0");
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
        Thread.sleep(1000);
        cfs.forceMajorCompaction();

        assertTrue(cfs.getLiveSSTables().isEmpty());
    }

    @Test
    public void testRow1WriteAndPKDeleteAndRowDeleteCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with gc_grace_seconds=0");
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

        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        assertTrue(cfs.getLiveSSTables().isEmpty());
    }

    @Test
    public void testRow2WriteDeleteWriteCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with gc_grace_seconds=0");
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
        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());

        Unfiltered row = partition.next();
        assertTrue(row.isRow());
        assertTrue(((Row)row).deletion().time().isLive());
        assertTrue(!row.isEmpty());
    }

    @Test
    public void testRowDeleteCompactionInterleaving() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with gc_grace_seconds=0");
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

        // delete rows [0,2,2] and [2,2,2]
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
        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        try(ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext()) {
                UnfilteredRowIterator partition = scanner.next();
                long pk = partition.partitionKey().getKey().getLong();
                if (pk == 5) {
                    fail();
                }
                else if (pk % 2 == 0)
                {
                    assertTrue("pk="+pk,partition.hasNext());
                    assertTrue("pk="+pk,partition.partitionLevelDeletion().isLive());
                    assertTrue("pk="+pk,!partition.staticRow().isEmpty());
                    // only have 2 live rows
                    Unfiltered row = partition.next();
                    assertTrue("pk=" + pk, !row.isEmpty());
                    assertTrue("pk="+pk,((Row)row).deletion().time().isLive());

                    row = partition.next();
                    assertTrue("pk=" + pk, !row.isEmpty());
                    assertTrue("pk="+pk,((Row)row).deletion().time().isLive());

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

    @Test
    public void testLargeRowDeleteCompactionInterleaving() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 blob, PRIMARY KEY(pk, ck1, ck2)) with gc_grace_seconds=0");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        String writeStatement1 = "INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?) using timestamp ?";
        String writeStatement2 = "INSERT INTO " + table + "(pk,ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?) using timestamp ?";
        int prefix = 0;
        long timestamp = 0;
        // Writes, 3 rows in each partition
        byte[] blob = new byte[DatabaseDescriptor.getColumnIndexCacheSize()*2];
        ByteBuffer byteBuffer = ByteBuffer.wrap(blob);
        ThreadLocalRandom.current().nextBytes(blob);
        for (int i = 0; i < 4; i++)
        {
            execute(writeStatement1,
                    Long.valueOf(i), //pk
                    Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                    Long.valueOf(1), Integer.valueOf(1),//ck1,ck2
                    Long.valueOf(prefix+i), byteBuffer,//c1,c2
                    timestamp++);
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(2), Integer.valueOf(2),//ck1,ck2
                    Long.valueOf(prefix+i), byteBuffer,//c1,c2
                    timestamp++);
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(3), Integer.valueOf(3),//ck1,ck2
                    Long.valueOf(prefix+i), byteBuffer,//c1,c2
                    timestamp++);
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // delete rows [0,2,2] and [2,2,2]
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

        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        try(ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext()) {
                UnfilteredRowIterator partition = scanner.next();
                long pk = partition.partitionKey().getKey().getLong();
                if (pk == 5) {
                    fail();
                }
                else if (pk % 2 == 0)
                {
                    assertTrue("pk="+pk,partition.hasNext());
                    assertTrue("pk="+pk,partition.partitionLevelDeletion().isLive());
                    assertTrue("pk="+pk,!partition.staticRow().isEmpty());
                    // only have 2 live rows
                    Unfiltered row = partition.next();
                    assertTrue("pk=" + pk, !row.isEmpty());
                    assertTrue("pk="+pk,((Row)row).deletion().time().isLive());

                    row = partition.next();
                    assertTrue("pk=" + pk, !row.isEmpty());
                    assertTrue("pk="+pk,((Row)row).deletion().time().isLive());

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
        try (KeyReader it = sstable.keyReader())
        {
            ByteBuffer last = it.key();
            while (it.advance()) last = it.key(); // no-op, just check if index is readable
            if (!Objects.equals(last, sstable.getLast().getKey()))
                throw new CorruptSSTableException(new IOException("Failed to read partition index"), it.toString());
        }
    }

    @Test
    public void testLargeCKRowDeleteCompactionInterleaving() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 blob, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with gc_grace_seconds=0");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        String writeStatement1 = "INSERT INTO " + table + "(pk,sc1,sc2, ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?, ?,?) using timestamp ?";
        String writeStatement2 = "INSERT INTO " + table + "(pk,ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?) using timestamp ?";
        int prefix = 0;
        long timestamp = 0;
        // Writes, 3 rows in each partition
        byte[] blob = new byte[DatabaseDescriptor.getColumnIndexCacheSize()*2];
        ByteBuffer byteBuffer = ByteBuffer.wrap(blob);
        ThreadLocalRandom.current().nextBytes(blob);
        for (int i = 0; i < 4; i++)
        {
            execute(writeStatement1,
                    Long.valueOf(i), //pk
                    Long.valueOf(111), Integer.valueOf(222),//sc1,sc2
                    Long.valueOf(1), byteBuffer,//ck1,ck2
                    Long.valueOf(prefix+i),  Integer.valueOf(1),//c1,c2
                    timestamp++);
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(2), byteBuffer,//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(1),//c1,c2
                    timestamp++);
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(3), byteBuffer, //ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(1), //c1,c2
                    timestamp++);
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // delete rows [0,2,bb] and [2,2,bb]
        for (int i = 0; i < 4; i+=2)
        {
            execute("DELETE FROM " + table + " using timestamp ? WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                    Long.valueOf(timestamp + i), // timestamp
                    Long.valueOf(i), //pk
                    Long.valueOf(2), //ck1
                    byteBuffer  //ck2
            );
        }

        // delete a partition + row that we don't have
        execute("DELETE FROM " + table + " using timestamp ? WHERE pk = ? AND ck1 = ? AND ck2 = ?;",
                Long.valueOf(timestamp + 5), // timestamp
                Long.valueOf(5), //pk
                Long.valueOf(11), //ck1
                byteBuffer  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        try(ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext()) {
                UnfilteredRowIterator partition = scanner.next();
                long pk = partition.partitionKey().getKey().getLong();
                if (pk == 5) {
                    fail();
                }
                else if (pk % 2 == 0)
                {
                    assertTrue("pk="+pk,partition.hasNext());
                    assertTrue("pk="+pk,partition.partitionLevelDeletion().isLive());
                    assertTrue("pk="+pk,!partition.staticRow().isEmpty());
                    // only have 2 live rows
                    Unfiltered row = partition.next();
                    assertTrue("pk=" + pk, !row.isEmpty());
                    assertTrue("pk="+pk,((Row)row).deletion().time().isLive());

                    row = partition.next();
                    assertTrue("pk=" + pk, !row.isEmpty());
                    assertTrue("pk="+pk,((Row)row).deletion().time().isLive());

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
        try (KeyReader it = sstable.keyReader())
        {
            ByteBuffer last = it.key();
            while (it.advance()) last = it.key(); // no-op, just check if index is readable
            if (!Objects.equals(last, sstable.getLast().getKey()))
                throw new CorruptSSTableException(new IOException("Failed to read partition index"), it.toString());
        }
    }

    @Test
    public void testSingleLargeCKRow() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, ck1 bigint, ck2 blob, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2)) with gc_grace_seconds=0");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

         String writeStatement2 = "INSERT INTO " + table + "(pk,ck1,ck2, c1,c2)VALUES(?, ?,?, ?,?) using timestamp ?";
        int prefix = 0;
        long timestamp = 0;
        // Writes, 3 rows in each partition
        byte[] blob = new byte[DatabaseDescriptor.getColumnIndexCacheSize()*2];
        ByteBuffer byteBuffer = ByteBuffer.wrap(blob);
        ThreadLocalRandom.current().nextBytes(blob);
        for (int i = 0; i < 1; i++)
        {
            execute(writeStatement2,
                    Long.valueOf(i), //pk
                    Long.valueOf(2), byteBuffer,//ck1,ck2
                    Long.valueOf(prefix+i), Integer.valueOf(1),//c1,c2
                    timestamp++);
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        Thread.sleep(1000);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
    }
}
