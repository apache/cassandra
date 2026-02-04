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
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.utils.TestHelper.verifyAndPrint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({ "UnnecessaryBoxing", "SingleCharacterStringConcatenation" })
public class CompactionDeleteRowRangeTest extends SimpleCompactionTest
{
    @Test
    public void testRow1DeleteRangeCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 < ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected: {"table kind":"REGULAR",
        //            "partition":{"key":["0"],"position":11},
        //            "rows": [
        //              {"type":"range_tombstone_bound","start":{"type":"inclusive","clustering":[0,"*"],
        //                                              "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //                                              "local_delete_time":"2025-04-18T10:29:02Z"}}},
        //              {"type":"range_tombstone_bound","end":{"type":"exclusive","clustering":[0,0],
        //                                              "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //                                              "local_delete_time":"2025-04-18T10:29:02Z"}}}]}
        UntypedResultSet result = execute("SELECT pk,sc1,sc2, ck1,ck2, c1,c2 FROM " + table);
        assertRows(result);

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(partition.staticRow().isEmpty());

        Unfiltered tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneBoundMarker)tombstoneMarker).openIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());

        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).closeIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());
        assertFalse(partition.hasNext());
    }

    @Test
    public void test2DeleteRangeWithExclusiveMatchingBoundCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 < ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 3 WHERE pk = ? AND ck1 = ? AND ck2 > ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected: {"table kind":"REGULAR",
        //              "partition":{"key":["0"],"position":11},"rows":[
        //              {"type":"range_tombstone_bound","start":{"type":"inclusive","clustering":[0,"*"],
        //                      "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //                                       "local_delete_time":"2025-05-17T09:05:05Z"}}},
        //              {"type":"range_tombstone_bound","end":{"type":"exclusive","clustering":[0,0],
        //                      "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //                                       "local_delete_time":"2025-05-17T09:05:05Z"}}},
        //              {"type":"range_tombstone_bound","start":{"type":"exclusive","clustering":[0,0],
        //                      "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //                                       "local_delete_time":"2025-05-17T09:05:05Z"}}},
        //              {"type":"range_tombstone_bound","end":{"type":"inclusive","clustering":[0,"*"],
        //                      "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //                      "local_delete_time":"2025-05-17T09:05:05Z"}}}]}

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(partition.staticRow().isEmpty());
        Unfiltered tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneBoundMarker)tombstoneMarker).openIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());
        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).closeIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());

        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(!((RangeTombstoneBoundMarker)tombstoneMarker).openIsInclusive(false));
        assertEquals(3, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());

        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneMarker)tombstoneMarker).closeIsInclusive(false));
        assertEquals(3, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());
        assertFalse(partition.hasNext());
    }

    @Test
    public void test2DeleteRangeWithInclusiveMatchingBoundCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 <= ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 3 WHERE pk = ? AND ck1 = ? AND ck2 >= ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected:  {"table kind":"REGULAR","partition":{"key":["0"],"position":11},"rows":[
        // 	{"type":"range_tombstone_bound",
        //	     "start":{"type":"inclusive","clustering":[0,"*"],
        //		 		  "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //				  				   "local_delete_time":"2025-05-17T09:12:27Z"}}},
        //	{"type":"range_tombstone_boundary",
        //		"start":{"type":"inclusive","clustering":[0,0],
        //				 "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000003Z",
        //				 				  "local_delete_time":"2025-05-17T09:12:27Z"}},
        //		"end":{"type":"exclusive","clustering":[0,0],
        //				"deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //				                 "local_delete_time":"2025-05-17T09:12:27Z"}}},
        //	{"type":"range_tombstone_bound",
        //		"end":{"type":"inclusive","clustering":[0,"*"],
        //			   "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000003Z",
        //			   					"local_delete_time":"2025-05-17T09:12:27Z"}}}]}

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(partition.staticRow().isEmpty());
        Unfiltered tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneBoundMarker)tombstoneMarker).openIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());
        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneMarker)tombstoneMarker).openIsInclusive(false));
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).closeIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundaryMarker)tombstoneMarker).endDeletionTime().markedForDeleteAt());
        assertEquals(3, ((RangeTombstoneBoundaryMarker)tombstoneMarker).startDeletionTime().markedForDeleteAt());

        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneMarker)tombstoneMarker).closeIsInclusive(false));
        assertEquals(3, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());
        assertFalse(partition.hasNext());
    }

    @Test
    public void test2DeleteRangeWithInclusiveOverlapingBoundCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 <= ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(3)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 3 WHERE pk = ? AND ck1 = ? AND ck2 >= ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected:  {"table kind":"REGULAR","partition":{"key":["0"],"position":11},"rows":[
        // 	{"type":"range_tombstone_bound",
        //	     "start":{"type":"inclusive","clustering":[0,"*"],
        //		 		  "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //				  				   "local_delete_time":"2025-05-17T09:12:27Z"}}},
        //	{"type":"range_tombstone_boundary",
        //		"start":{"type":"inclusive","clustering":[0,0],
        //				 "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000003Z",
        //				 				  "local_delete_time":"2025-05-17T09:12:27Z"}},
        //		"end":{"type":"exclusive","clustering":[0,0],
        //				"deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //				                 "local_delete_time":"2025-05-17T09:12:27Z"}}},
        //	{"type":"range_tombstone_bound",
        //		"end":{"type":"inclusive","clustering":[0,"*"],
        //			   "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000003Z",
        //			   					"local_delete_time":"2025-05-17T09:12:27Z"}}}]}

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(partition.staticRow().isEmpty());
        Unfiltered tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneBoundMarker)tombstoneMarker).openIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());
        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneMarker)tombstoneMarker).openIsInclusive(false));
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).closeIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundaryMarker)tombstoneMarker).endDeletionTime().markedForDeleteAt());
        assertEquals(3, ((RangeTombstoneBoundaryMarker)tombstoneMarker).startDeletionTime().markedForDeleteAt());

        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneMarker)tombstoneMarker).closeIsInclusive(false));
        assertEquals(3, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());
        assertFalse(partition.hasNext());
    }

    @Test
    public void test2DeleteRangeWithOverlapingBoundAndSameTimestampCompaction() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, sc1 bigint static, sc2 int static, ck1 bigint, ck2 int, c1 bigint, c2 int, PRIMARY KEY(pk, ck1, ck2))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 <= ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(3)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 >= ?;",
                Long.valueOf(0), //pk
                Long.valueOf(0), //ck1
                Integer.valueOf(0)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected:  {"table kind":"REGULAR","partition":{"key":["0"],"position":11},"rows":[
        // 	{"type":"range_tombstone_bound",
        //	     "start":{"type":"inclusive","clustering":[0,"*"],
        //		 		  "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //				  				   "local_delete_time":"2025-05-17T09:12:27Z"}}},
        //	{"type":"range_tombstone_bound",
        //		"end":{"type":"inclusive","clustering":[0,"*"],
        //			   "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //			   					"local_delete_time":"2025-05-17T09:12:27Z"}}}]}

        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(partition.staticRow().isEmpty());
        Unfiltered tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneBoundMarker)tombstoneMarker).openIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());

        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneMarker)tombstoneMarker).closeIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());
        assertFalse(partition.hasNext());
    }

    @Test
    public void testWrite1RowAndRangeDeleteCompaction() throws Throwable
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
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 < ?;",
                Long.valueOf(0), //pk
                Long.valueOf(11), //ck1
                Integer.valueOf(22)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected: {"table kind":"REGULAR","partition":{"key":["0"],"position":31},"rows":
        //   [{"type":"static_block","position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000001Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000001Z"}]},
        //   {"type":"range_tombstone_bound","start":
        //      {"type":"inclusive","clustering":[11,"*"],
        //       "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //       "local_delete_time":"2025-04-18T10:29:02Z"}}},
        //   {"type":"range_tombstone_bound","end":
        //      {"type":"exclusive","clustering":[11,22],
        //       "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //       "local_delete_time":"2025-04-18T10:29:02Z"}}}]}
        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());

        Unfiltered tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneBoundMarker)tombstoneMarker).openIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());

        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).closeIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());
        assertFalse(partition.hasNext());
    }

    @Test
    public void testWrite1RowAndRangeDeleteAndPKDeleteCompaction() throws Throwable
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

        // Delete RT
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 = ? AND ck2 < ?;",
                Long.valueOf(0), //pk
                Long.valueOf(11), //ck1
                Integer.valueOf(22)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete PK which should remove the RT and row
        execute("DELETE FROM " + table + "  using timestamp 3 WHERE pk = ?;",
                Long.valueOf(0) //pk
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);
        // Expected: {"table kind":"REGULAR","partition":{"key":["0"],"position":27,"deletion_info":{"marked_deleted":"2025-01-14T11:20:37.220Z","local_delete_time":"2025-01-14T11:20:37Z"}},"rows":[]}
        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(!partition.hasNext());
        assertTrue(!partition.partitionLevelDeletion().isLive());
        assertEquals(3, partition.partitionLevelDeletion().markedForDeleteAt());
        assertTrue(partition.staticRow().isEmpty());
        assertFalse(partition.hasNext());
    }

    @Test
    public void testWrite1RowAndPKDeleteAndRangeDeleteCompaction() throws Throwable
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

        // Delete PK
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ?;",
                Long.valueOf(0) //pk
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // Delete RT, which is later than the PK delete
        execute("DELETE FROM " + table + "  using timestamp 3 WHERE pk = ? AND ck1 = ? AND ck2 < ?;",
                Long.valueOf(0), //pk
                Long.valueOf(11), //ck1
                Integer.valueOf(22)  //ck2
        );

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);


        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        verifyAndPrint(cfs, sstable);

        // {"table kind":"REGULAR","partition":
        //      {"key":["0"],"position":27,
        //              "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //              "local_delete_time":"2025-04-18T12:22:21Z"}},
        //       "rows":[
        //       {"type":"range_tombstone_bound","start":
        //          {"type":"inclusive","clustering":[11,"*"],
        //           "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000003Z","local_delete_time":"2025-04-18T12:22:21Z"}}},
        //       {"type":"range_tombstone_bound","end":
        //          {"type":"exclusive","clustering":[11,22],
        //          "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000003Z","local_delete_time":"2025-04-18T12:22:21Z"}}}]}
        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(!partition.partitionLevelDeletion().isLive());
        assertEquals(2, partition.partitionLevelDeletion().markedForDeleteAt());
        assertTrue(partition.staticRow().isEmpty());

        Unfiltered tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(((RangeTombstoneBoundMarker)tombstoneMarker).openIsInclusive(false));
        assertEquals(3, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());

        tombstoneMarker = partition.next();
        assertTrue(tombstoneMarker.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).isBoundary());
        assertTrue(!((RangeTombstoneMarker)tombstoneMarker).closeIsInclusive(false));
        assertEquals(3, ((RangeTombstoneBoundMarker)tombstoneMarker).deletionTime().markedForDeleteAt());
        assertFalse(partition.hasNext());
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
        execute("DELETE FROM " + table + "  using timestamp 2 WHERE pk = ? AND ck1 > ?;",
                Long.valueOf(0), //pk
                Long.valueOf(10) //ck1
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

        // {"table kind":"REGULAR","partition":{"key":["0"],"position":31}, "rows":[
        // {"type":"static_block","position":31,"cells":[{"name":"sc1","value":112,"tstamp":"1970-01-01T00:00:00.000003Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000003Z"}]},
        // {"type":"range_tombstone_bound","start":
        //      {"type":"exclusive","clustering":[10,"*"],
        //       "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //       "local_delete_time":"2025-04-18T12:15:58Z"}}},
        // {"type": "row",
        //      "position":48,"clustering":[11,21],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000003Z"},
        //      "cells":[{"name":"c1","value":12},{"name":"c2","value":2}]},
        // {"type":"range_tombstone_bound","end":
        //      {"type":"inclusive",
        //       "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000002Z",
        //       "local_delete_time":"2025-04-18T12:15:58Z"}}}]}
        UnfilteredRowIterator partition = sstable.getScanner().next();
        assertTrue(partition.hasNext());
        assertTrue(partition.partitionLevelDeletion().isLive());
        assertTrue(!partition.staticRow().isEmpty());

        Unfiltered unfiltered = partition.next();
        assertTrue(unfiltered.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)unfiltered).isBoundary());
        assertTrue(!((RangeTombstoneBoundMarker)unfiltered).openIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)unfiltered).deletionTime().markedForDeleteAt());

        unfiltered = partition.next();
        assertTrue(unfiltered.isRow());
        assertTrue(!((Row)unfiltered).isEmpty());
        assertEquals(3, ((Row)unfiltered).primaryKeyLivenessInfo().timestamp());

        unfiltered = partition.next();
        assertTrue(unfiltered.isRangeTombstoneMarker());
        assertTrue(!((RangeTombstoneMarker)unfiltered).isBoundary());
        assertTrue(((RangeTombstoneMarker)unfiltered).closeIsInclusive(false));
        assertEquals(2, ((RangeTombstoneBoundMarker)unfiltered).deletionTime().markedForDeleteAt());
        assertFalse(partition.hasNext());
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
            execute("DELETE FROM " + table + " using timestamp ? WHERE pk = ? AND ck1 = ? AND ck2 < ?;",
                    Long.valueOf(timestamp + i), // timestamp
                    Long.valueOf(i), //pk
                    Long.valueOf(2), //ck1
                    Integer.valueOf(3)  //ck2
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
        // {"table kind":"REGULAR","partition":{"key":["2"],"position":31},
        //  "rows":[{"type":"static_block","position":31,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000006Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000006Z"}]},
        //          {"type":"row","position":31,"clustering":[1,1],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000006Z"},"cells":[{"name":"c1","value":2},{"name":"c2","value":2}]},
        //          {"type":"range_tombstone_bound","start":
        //              {"type":"inclusive","clustering":[2,"*"],
        //               "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000014Z","local_delete_time":"2025-04-18T12:42:52Z"}}},
        //          {"type":"range_tombstone_bound","end":
        //              {"type":"exclusive","clustering":[2,3],
        //               "deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000014Z","local_delete_time":"2025-04-18T12:42:52Z"}}},
        //          {"type":"row","position":100,"clustering":[3,3],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000008Z"},"cells":[{"name":"c1","value":2},{"name":"c2","value":2}]}]}
        //{"table kind":"REGULAR","partition":{"key":["3"],"position":163},
        // "rows":[{"type":"static_block","position":163,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000009Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000009Z"}]},{"type":"row","position":163,"clustering":[1,1],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000009Z"},"cells":[{"name":"c1","value":3},{"name":"c2","value":3}]},{"type":"row","position":194,"clustering":[2,2],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000010Z"},"cells":[{"name":"c1","value":3},{"name":"c2","value":3}]},{"type":"row","position":225,"clustering":[3,3],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000011Z"},"cells":[{"name":"c1","value":3},{"name":"c2","value":3}]}]}
        //{"table kind":"REGULAR","partition":{"key":["0"],"position":288},"rows":[
        // {"type":"static_block","position":288,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00Z"}]},{"type":"row","position":288,"clustering":[1,1],"liveness_info":{"tstamp":"1970-01-01T00:00:00Z"},"cells":[{"name":"c1","value":0},{"name":"c2","value":0}]},{"type":"range_tombstone_bound","start":{"type":"inclusive","clustering":[2,"*"],"deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000012Z","local_delete_time":"2025-04-18T12:42:52Z"}}},{"type":"range_tombstone_bound","end":{"type":"exclusive","clustering":[2,3],"deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000012Z","local_delete_time":"2025-04-18T12:42:52Z"}}},{"type":"row","position":357,"clustering":[3,3],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000002Z"},"cells":[{"name":"c1","value":0},{"name":"c2","value":0}]}]}
        //{"table kind":"REGULAR","partition":{"key":["5"],"position":405},"rows":[
        // {"type":"row","position":405,"clustering":[11,21],"deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000017Z","local_delete_time":"2025-04-18T12:42:52Z"},"cells":[]}]}
        //{"table kind":"REGULAR","partition":{"key":["1"],"position":456},"rows":[
        // {"type":"static_block","position":456,"cells":[{"name":"sc1","value":111,"tstamp":"1970-01-01T00:00:00.000003Z"},{"name":"sc2","value":222,"tstamp":"1970-01-01T00:00:00.000003Z"}]},{"type":"row","position":456,"clustering":[1,1],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000003Z"},"cells":[{"name":"c1","value":1},{"name":"c2","value":1}]},{"type":"row","position":487,"clustering":[2,2],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000004Z"},"cells":[{"name":"c1","value":1},{"name":"c2","value":1}]},{"type":"row","position":518,"clustering":[3,3],"liveness_info":{"tstamp":"1970-01-01T00:00:00.000005Z"},"cells":[{"name":"c1","value":1},{"name":"c2","value":1}]}]}
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
                    assertTrue("pk="+pk,row.isRow());

                    row = partition.next();
                    assertTrue("pk="+pk,!row.isEmpty());
                    assertTrue("pk="+pk,row.isRangeTombstoneMarker());

                    row = partition.next();
                    assertTrue("pk="+pk,!row.isEmpty());
                    assertTrue("pk="+pk,row.isRangeTombstoneMarker());

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
