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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionsCQLTest extends CQLTester
{

    public static final int SLEEP_TIME = 5000;

    @Test
    public void testTriggerMinorCompactionSTCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testTriggerMinorCompactionLCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY) WITH compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':1, 'fanout_size':5};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }



    @Test
    public void testTriggerMinorCompactionDTCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY) WITH compaction = {'class':'DateTieredCompactionStrategy', 'min_threshold':2};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1') using timestamp 1000"); // same timestamp = same window = minor compaction triggered
        flush();
        execute("insert into %s (id) values ('1') using timestamp 1000");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testTriggerMinorCompactionTWCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY) WITH compaction = {'class':'TimeWindowCompactionStrategy', 'min_threshold':2};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }


    @Test
    public void testTriggerNoMinorCompactionSTCSDisabled() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':false};");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, false);
    }

    @Test
    public void testTriggerMinorCompactionSTCSNodetoolEnabled() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':false};");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        getCurrentColumnFamilyStore().enableAutoCompaction();
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());

        // Alter keyspace replication settings to force compaction strategy reload and check strategy is still enabled
        execute("alter keyspace "+keyspace()+" with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        getCurrentColumnFamilyStore().getCompactionStrategyManager().maybeReloadDiskBoundaries();
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());

        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testTriggerNoMinorCompactionSTCSNodetoolDisabled() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':true};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        getCurrentColumnFamilyStore().disableAutoCompaction();
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, false);
    }

    @Test
    public void testTriggerNoMinorCompactionSTCSAlterTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':true};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("ALTER TABLE %s WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false}");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, false);
    }

    @Test
    public void testTriggerMinorCompactionSTCSAlterTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':false};");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("ALTER TABLE %s WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': 2, 'enabled': true}");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testSetLocalCompactionStrategy() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "DateTieredCompactionStrategy");
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), DateTieredCompactionStrategy.class));
        // Invalidate disk boundaries to ensure that boundary invalidation will not cause the old strategy to be reloaded
        getCurrentColumnFamilyStore().invalidateDiskBoundaries();
        // altering something non-compaction related
        execute("ALTER TABLE %s WITH gc_grace_seconds = 1000");
        // should keep the local compaction strat
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), DateTieredCompactionStrategy.class));
        // Alter keyspace replication settings to force compaction strategy reload
        execute("alter keyspace "+keyspace()+" with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        // should keep the local compaction strat
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), DateTieredCompactionStrategy.class));
        // altering a compaction option
        execute("ALTER TABLE %s WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':3}");
        // will use the new option
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), SizeTieredCompactionStrategy.class));
    }

    @Test
    public void testSetLocalCompactionStrategyDisable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "DateTieredCompactionStrategy");
        localOptions.put("enabled", "false");
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        localOptions.clear();
        localOptions.put("class", "DateTieredCompactionStrategy");
        // localOptions.put("enabled", "true"); - this is default!
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
    }


    @Test
    public void testSetLocalCompactionStrategyEnable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "DateTieredCompactionStrategy");

        getCurrentColumnFamilyStore().disableAutoCompaction();
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());

        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
    }



    @Test(expected = IllegalArgumentException.class)
    public void testBadLocalCompactionStrategyOptions()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class","SizeTieredCompactionStrategy");
        localOptions.put("sstable_size_in_mb","1234"); // not for STCS
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
    }

     @Test(expected = IllegalArgumentException.class)
     public void testBadProvidesTombstoneOption()
     {
         createTable("CREATE TABLE %s (id text PRIMARY KEY)");
         Map<String, String> localOptions = new HashMap<>();
         localOptions.put("class","SizeTieredCompactionStrategy");
         localOptions.put("provide_overlapping_tombstones","IllegalValue");

         getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
     }
     @Test
     public void testProvidesTombstoneOptionverifiation()
     {
         createTable("CREATE TABLE %s (id text PRIMARY KEY)");
         Map<String, String> localOptions = new HashMap<>();
         localOptions.put("class","SizeTieredCompactionStrategy");
         localOptions.put("provide_overlapping_tombstones","row");

         getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
         assertEquals(CompactionParams.TombstoneOption.ROW, getCurrentColumnFamilyStore().getCompactionStrategyManager().getCompactionParams().tombstoneOption());
     }

    @Test
    public void testAbortNotifications() throws Throwable
    {
        createTable("create table %s (id int primary key, x blob) with compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':1}");
        Random r = new Random();
        byte [] b = new byte[100 * 1024];
        for (int i = 0; i < 1000; i++)
        {
            r.nextBytes(b);
            execute("insert into %s (id, x) values (?, ?)", i, ByteBuffer.wrap(b));
        }
        getCurrentColumnFamilyStore().forceBlockingFlush();
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 1000; i++)
        {
            r.nextBytes(b);
            execute("insert into %s (id, x) values (?, ?)", i, ByteBuffer.wrap(b));
        }
        getCurrentColumnFamilyStore().forceBlockingFlush();

        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) getCurrentColumnFamilyStore().getCompactionStrategyManager().getStrategies().get(1).get(0);
        LeveledCompactionTask lcsTask;
        while (true)
        {
            lcsTask = (LeveledCompactionTask) lcs.getNextBackgroundTask(0);
            if (lcsTask != null)
            {
                lcsTask.execute(null);
                break;
            }
            Thread.sleep(1000);
        }
        // now all sstables are non-overlapping in L1 - we need them to be in L2:
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            lcs.removeSSTable(sstable);
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 2);
            sstable.reloadSSTableMetadata();
            lcs.addSSTable(sstable);
        }

        for (int i = 0; i < 1000; i++)
        {
            r.nextBytes(b);
            execute("insert into %s (id, x) values (?, ?)", i, ByteBuffer.wrap(b));
        }
        getCurrentColumnFamilyStore().forceBlockingFlush();
        // now we have a bunch of sstables in L2 and one in L0 - bump the L0 one to L1:
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            if (sstable.getSSTableLevel() == 0)
            {
                lcs.removeSSTable(sstable);
                sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1);
                sstable.reloadSSTableMetadata();
                lcs.addSSTable(sstable);
            }
        }
        // at this point we have a single sstable in L1, and a bunch of sstables in L2 - a background compaction should
        // trigger an L1 -> L2 compaction which we abort after creating 5 sstables - this notifies LCS that MOVED_START
        // sstables have been removed.
        try
        {
            AbstractCompactionTask task = new NotifyingCompactionTask((LeveledCompactionTask) lcs.getNextBackgroundTask(0));
            task.execute(null);
            fail("task should throw exception");
        }
        catch (Exception ignored)
        {
            // ignored
        }

        lcsTask = (LeveledCompactionTask) lcs.getNextBackgroundTask(0);
        try
        {
            assertNotNull(lcsTask);
        }
        finally
        {
            if (lcsTask != null)
                lcsTask.transaction.abort();
        }
    }

    private static class NotifyingCompactionTask extends LeveledCompactionTask
    {
        public NotifyingCompactionTask(LeveledCompactionTask task)
        {
            super(task.cfs, task.transaction, task.getLevel(), task.gcBefore, task.getLevel(), false);
        }

        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                              Directories directories,
                                                              LifecycleTransaction txn,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new MaxSSTableSizeWriter(cfs, directories, txn, nonExpiredSSTables, 1 << 20, 1)
            {
                int switchCount = 0;
                public void switchCompactionLocation(Directories.DataDirectory directory)
                {
                    switchCount++;
                    if (switchCount > 5)
                        throw new RuntimeException("Throw after a few sstables have had their starts moved");
                    super.switchCompactionLocation(directory);
                }
            };
        }
    }

    public boolean verifyStrategies(CompactionStrategyManager manager, Class<? extends AbstractCompactionStrategy> expected)
    {
        boolean found = false;
        for (List<AbstractCompactionStrategy> strategies : manager.getStrategies())
        {
            if (!strategies.stream().allMatch((strategy) -> strategy.getClass().equals(expected)))
                return false;
            found = true;
        }
        return found;
    }

    private void waitForMinor(String keyspace, String cf, long maxWaitTime, boolean shouldFind) throws Throwable
    {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < maxWaitTime)
        {
            UntypedResultSet res = execute("SELECT * FROM system.compaction_history");
            for (UntypedResultSet.Row r : res)
            {
                if (r.getString("keyspace_name").equals(keyspace) && r.getString("columnfamily_name").equals(cf))
                    if (shouldFind)
                        return;
                    else
                        fail("Found minor compaction");
            }
            Thread.sleep(100);
        }
        if (shouldFind)
            fail("No minor compaction triggered in "+maxWaitTime+"ms");
    }
}
