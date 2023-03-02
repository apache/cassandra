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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SingleSSTableLCSTaskTest extends CQLTester
{
    @Test
    public void basicTest() throws Throwable
    {
        createTable("create table %s (id int primary key, t text) with compaction = {'class':'LeveledCompactionStrategy','single_sstable_uplevel':true}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        execute("insert into %s (id, t) values (1, 'meep')");
        Util.flush(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.COMPACTION))
        {
            if (txn != null)
            {
                SingleSSTableLCSTask task = new SingleSSTableLCSTask(cfs, txn, 2);
                task.executeInternal(null);
            }
        }
        assertEquals(1, cfs.getLiveSSTables().size());
        cfs.getLiveSSTables().forEach(s -> assertEquals(2, s.getSSTableLevel()));
        // make sure compaction strategy is notified:
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getUnrepairedUnsafe().first();
        for (int i = 0; i < lcs.manifest.getLevelCount(); i++)
        {
            if (i == 2)
                assertEquals(1, lcs.getLevelSize(i));
            else
                assertEquals(0, lcs.getLevelSize(i));
        }
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
    }

    @Test
    public void compactionTest() throws Throwable
    {
        compactionTestHelper(true);
    }

    @Test
    public void uplevelDisabledTest() throws Throwable
    {
        compactionTestHelper(false);
    }

    private void compactionTestHelper(boolean singleSSTUplevel) throws Throwable
    {
        createTable("create table %s (id int, id2 int, t blob, primary key (id, id2))" +
                    "with compaction = {'class':'LeveledCompactionStrategy', 'single_sstable_uplevel':" + singleSSTUplevel + ", 'sstable_size_in_mb':'1', 'max_threshold':'1000'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        byte[] b = new byte[10 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b);
        for (int i = 0; i < 5000; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                execute("insert into %s (id, id2, t) values (?, ?, ?)", i, j, value);
            }
            if (i % 100 == 0)
                Util.flush(cfs);
        }
        // now we have a bunch of data in L0, first compaction will be a normal one, containing all sstables:
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getUnrepairedUnsafe().first();
        AbstractCompactionTask act = lcs.getNextBackgroundTask(0);
        act.execute(ActiveCompactionsTracker.NOOP);

        // now all sstables are laid out non-overlapping in L1, this means that the rest of the compactions
        // will be single sstable ones, make sure that we use SingleSSTableLCSTask if singleSSTUplevel is true:
        while (lcs.getEstimatedRemainingTasks() > 0)
        {
            act = lcs.getNextBackgroundTask(0);
            assertEquals(singleSSTUplevel, act instanceof SingleSSTableLCSTask);
            act.execute(ActiveCompactionsTracker.NOOP);
        }
        assertEquals(0, lcs.getLevelSize(0));
        int l1size = lcs.getLevelSize(1);
        // this should be 10, but it might vary a bit depending on partition sizes etc
        assertTrue(l1size >= 8 && l1size <= 12);
        assertTrue(lcs.getLevelSize(2) > 0);
    }

    @Test
    public void corruptMetadataTest() throws Throwable
    {
        createTable("create table %s (id int primary key, t text) with compaction = {'class':'LeveledCompactionStrategy','single_sstable_uplevel':true}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        execute("insert into %s (id, t) values (1, 'meep')");
        Util.flush(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try(FileChannel fc = sstable.descriptor.fileFor(Components.STATS).newReadWriteChannel())
        {
            fc.position(0);
            fc.write(ByteBufferUtil.bytes(StringUtils.repeat('z', 2)));
        }
        boolean gotException = false;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.COMPACTION))
        {
            if (txn != null)
            {
                SingleSSTableLCSTask task = new SingleSSTableLCSTask(cfs, txn, 2);
                task.executeInternal(null);
            }
        }
        catch (Throwable t)
        {
            gotException = true;
        }
        assertTrue(gotException);
        assertEquals(1, cfs.getLiveSSTables().size());
        for (SSTableReader sst : cfs.getLiveSSTables())
            assertEquals(0, sst.getSSTableMetadata().sstableLevel);
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getUnrepairedUnsafe().first();
        assertEquals(1, lcs.getLevelSize(0));
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
    }
}
