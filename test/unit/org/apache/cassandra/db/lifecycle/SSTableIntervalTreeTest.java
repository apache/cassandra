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

package org.apache.cassandra.db.lifecycle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.Interval;

import static java.util.Collections.singleton;

public class SSTableIntervalTreeTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        MockSchema.cleanup();
    }

    @Test
    public void testSSTableIntervalTreeUpdateCount()
    {
        DatabaseDescriptor.setAddSSTableReaderForIntervalTreeEnabled(true);
        ColumnFamilyStore cfs = MockSchema.newCFS();
        SSTableReader sstable = MockSchema.sstable(0, cfs);
        List<SSTableReader> allSSTables = new ArrayList<>();
        allSSTables.add(sstable);

        // init tree has 1 sstable
        SSTableIntervalTree tree = SSTableIntervalTree.build(singleton(MockSchema.sstable(0, cfs)));
        Assert.assertEquals(1, tree.intervalCount());
        Assert.assertEquals(0, tree.updateCount());

        // suppose we insert 105 sstables, do check
        tree = addSSTablesAndCheck(105, cfs, tree, allSSTables, 1);
        Assert.assertEquals(5, tree.updateCount());
        Assert.assertEquals(allSSTables.size(), tree.intervalCount());

        // on update
        sstable = allSSTables.get(0);
        Map<SSTableReader, SSTableReader> replacementMap = new HashMap<>();
        SSTableReader sstable2 = MockSchema.sstable(0, cfs);
        replacementMap.put(sstable, sstable2);
        tree = tree.copyAndReplaceSSTables(replacementMap);
        allSSTables.set(0, sstable2);
        // got replaced by sstable2
        Assert.assertTrue(tree.search(Interval.create(sstable2.first, sstable2.last)).stream().anyMatch(x -> x == sstable2));
        // update count inheriated
        Assert.assertEquals(5, tree.updateCount());
        Assert.assertEquals(allSSTables.size(), tree.intervalCount());

        // on any rebuild the update count will be reset
        tree = SSTableIntervalTree.build(allSSTables);
        Assert.assertEquals(0, tree.updateCount());
        Assert.assertEquals(allSSTables.size(), tree.intervalCount());
    }

    static SSTableIntervalTree addSSTablesAndCheck(int cnt, ColumnFamilyStore cfs, SSTableIntervalTree tree, List<SSTableReader> allSSTables, int id)
    {
        for (int i = 0; i < cnt; i++)
        {
            int prevUpdates = tree.updateCount();
            SSTableReader sstable = MockSchema.sstable(id++, cfs);
            allSSTables.add(sstable);
            tree = tree.copyAndAddSSTables(allSSTables, singleton(sstable));
            Assert.assertEquals(allSSTables.size(), tree.intervalCount());
            // expect the first MAX_INTERVALS_ADDED_BEFORE_REBUILD - 1 intervals were inserted
            // after MAX_INTERVALS_ADDED_BEFORE_REBUILD insertions this should fall back to rebuild method
            if (prevUpdates == SSTableIntervalTree.MAX_INTERVALS_ADDED_BEFORE_REBUILD - 1)
                Assert.assertEquals(0, tree.updateCount());
            else
                Assert.assertEquals(prevUpdates + 1, tree.updateCount());
        }
        return tree;
    }
}
