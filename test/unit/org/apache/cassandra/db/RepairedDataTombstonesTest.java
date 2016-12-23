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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Collections;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractRow;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepairedDataTombstonesTest extends CQLTester
{
    @Test
    public void compactionTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        // insert a live row to make sure that the sstables are not dropped (we test dropping in compactionDropExpiredSSTableTest() below)
        execute("insert into %s (id, id2, t) values (999,999,'live')");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        execute("insert into %s (id, id2, t) values (999,999,'live')");
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        Thread.sleep(1000);
        // at this point we have 2 sstables, one repaired and one unrepaired. Both sstables contain expired tombstones, but we should only drop the tombstones from the repaired sstable.
        getCurrentColumnFamilyStore().forceMajorCompaction();
        verifyIncludingPurgeable();
        verify2IncludingPurgeable(1);
        assertEquals(2, Iterables.size(getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE)));
    }

    @Test
    public void compactionDropExpiredSSTableTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        Thread.sleep(1000);
        getCurrentColumnFamilyStore().forceMajorCompaction();
        verifyIncludingPurgeable();
        verify2IncludingPurgeable(1);
        assertEquals(1, Iterables.size(getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE)));
        assertFalse(getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next().isRepaired());

    }

    @Test
    public void readTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        for (int i = 0; i < 10; i++)
        {
            execute("update %s set t2=null where id=? and id2=?", 123, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("update %s set t2=null where id=? and id2=?", 123, i);
        }
        flush();
        // allow gcgrace to properly expire:
        Thread.sleep(1000);
        // make sure we only see the unrepaired tombstones, the other ones are expired and can be purged
        verify();
        verify2(123);
    }

    @Test
    public void readOnlyUnrepairedTest() throws Throwable
    {
        // make sure we keep all tombstones if we only have unrepaired data
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        for (int i = 10; i < 20; i++)
        {
            execute("update %s set t2=null where id=? and id2=?", 123, i);
        }
        flush();

        // allow gcgrace to properly expire:
        Thread.sleep(1000);
        verifyIncludingPurgeable();
        verify2IncludingPurgeable(123);
    }


    @Test
    public void readTestRowTombstones() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        Thread.sleep(1000);
        verify();
        verify2(1);
    }

    @Test
    public void readTestPartitionTombstones() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=?", i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=?", i);
        }
        flush();

        Thread.sleep(1000);
        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        int partitionsFound = 0;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            while (iterator.hasNext())
            {
                partitionsFound++;
                try (UnfilteredRowIterator rowIter = iterator.next())
                {
                    int val = ByteBufferUtil.toInt(rowIter.partitionKey().getKey());
                    assertTrue("val=" + val, val >= 10 && val < 20);
                }
            }
        }
        assertEquals(10, partitionsFound);
    }

    @Test
    public void readTestOldUnrepaired() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=1 and id2=?", i);
        }
        flush();
        SSTableReader oldSSTable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=1 and id2=?", i);
        }
        flush();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            if (sstable != oldSSTable)
                repair(getCurrentColumnFamilyStore(), sstable);
        Thread.sleep(2000);
        for (int i = 20; i < 30; i++)
        {
            execute("delete from %s where id=1 and id2=?", i);
        }
        flush();

        Thread.sleep(2000);
        // we will keep all tombstones since the oldest tombstones are unrepaired:
        verify(30, 0, 30, false);
        verify2(1, 30, 0, 30, false);
    }

    private void verify()
    {
        verify(10, 10, 20, false);
    }

    private void verifyIncludingPurgeable()
    {
        verify(10, 10, 20, true);
    }

    private void verify(int expectedRows, int minVal, int maxVal, boolean includePurgeable)
    {
        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        int foundRows = 0;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator =
             includePurgeable ? cmd.queryStorage(getCurrentColumnFamilyStore(), executionController) :
                                cmd.executeLocally(executionController))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator rowIter = iterator.next())
                {
                    if (!rowIter.partitionKey().equals(Util.dk(ByteBufferUtil.bytes(999)))) // partition key 999 is 'live' and used to avoid sstables from being dropped
                    {
                        while (rowIter.hasNext())
                        {
                            AbstractRow row = (AbstractRow) rowIter.next();
                            for (int i = 0; i < row.clustering().size(); i++)
                            {
                                foundRows++;
                                int val = ByteBufferUtil.toInt(row.clustering().get(i));
                                assertTrue("val=" + val, val >= minVal && val < maxVal);
                            }
                        }
                    }
                }
            }
        }
        assertEquals(expectedRows, foundRows);
    }

    private void verify2(int key)
    {
        verify2(key, 10, 10, 20, false);
    }

    private void verify2IncludingPurgeable(int key)
    {
        verify2(key, 10, 10, 20, true);
    }

    private void verify2(int key, int expectedRows, int minVal, int maxVal, boolean includePurgeable)
    {
        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore(), Util.dk(ByteBufferUtil.bytes(key))).build();
        int foundRows = 0;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator =
             includePurgeable ? cmd.queryStorage(getCurrentColumnFamilyStore(), executionController) :
                                cmd.executeLocally(executionController))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator rowIter = iterator.next())
                {
                    while (rowIter.hasNext())
                    {
                        AbstractRow row = (AbstractRow) rowIter.next();
                        for (int i = 0; i < row.clustering().size(); i++)
                        {
                            foundRows++;
                            int val = ByteBufferUtil.toInt(row.clustering().get(i));
                            assertTrue("val=" + val, val >= minVal && val < maxVal);
                        }
                    }
                }
            }
        }
        assertEquals(expectedRows, foundRows);
    }

    public static void repair(ColumnFamilyStore cfs, SSTableReader sstable) throws IOException
    {
        sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, 1);
        sstable.reloadSSTableMetadata();
        cfs.getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(sstable));
    }
}
