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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertFalse;
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
