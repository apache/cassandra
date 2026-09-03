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

package org.apache.cassandra.db.memtable;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InheritingClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.apache.cassandra.db.memtable.AbstractShardedMemtable.SHARDS_OPTION;
import static org.apache.cassandra.db.memtable.ShardedSkipListMemtable.LOCKING_OPTION;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link Memtable#limitsConcurrentWritesTo} must be true only for memtables that serialize writes per shard, and
 * only when the shard count is within the limit asked about.
 */
public class MemtableConcurrentWriteLimitTest extends CQLTester
{
    private static final int SHARDS = 4;

    // Overrides CQLTester.setUpClass so the memtable configurations are registered before the server is prepared
    @BeforeClass
    public static void setUpClass()
    {
        prePrepareServer();

        LinkedHashMap<String, InheritingClass> memtableConfig = new LinkedHashMap<>();
        memtableConfig.put("skiplist", new InheritingClass(null, SkipListMemtable.class.getName(), Map.of()));
        memtableConfig.put("trie", new InheritingClass(null, TrieMemtable.class.getName(), Map.of(SHARDS_OPTION, String.valueOf(SHARDS))));
        memtableConfig.put("sharded", new InheritingClass(null, ShardedSkipListMemtable.class.getName(), Map.of(SHARDS_OPTION, String.valueOf(SHARDS))));
        memtableConfig.put("sharded_locking", new InheritingClass(null, ShardedSkipListMemtable.class.getName(), Map.of(SHARDS_OPTION, String.valueOf(SHARDS), LOCKING_OPTION, "true")));
        DatabaseDescriptor.getRawConfig().memtable = new Config.MemtableOptions();
        DatabaseDescriptor.getRawConfig().memtable.configurations = memtableConfig;

        prepareServer();
    }

    @Test
    public void unshardedMemtableDoesNotLimitWriters()
    {
        assertFalse(memtableFor("skiplist").limitsConcurrentWritesTo(Integer.MAX_VALUE));
    }

    @Test
    public void shardedSkipListWithoutLockingDoesNotLimitWriters()
    {
        assertFalse(memtableFor("sharded").limitsConcurrentWritesTo(Integer.MAX_VALUE));
    }

    @Test
    public void trieMemtableLimitsWritersToShardCount()
    {
        assertLimitsToShardCount(memtableFor("trie"));
    }

    @Test
    public void lockingShardedSkipListLimitsWritersToShardCount()
    {
        assertLimitsToShardCount(memtableFor("sharded_locking"));
    }

    private void assertLimitsToShardCount(Memtable memtable)
    {
        // the memtable may get fewer shards than requested, e.g. if local ranges cannot be split that finely
        int shards = getCurrentColumnFamilyStore().localRangeSplits(SHARDS).shardCount();
        assertTrue(memtable.limitsConcurrentWritesTo(shards));
        assertTrue(memtable.limitsConcurrentWritesTo(shards + 1));
        assertFalse(memtable.limitsConcurrentWritesTo(shards - 1));
    }

    private Memtable memtableFor(String memtableConfig)
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH memtable = '" + memtableConfig + '\'');
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        return cfs.getTracker().getView().getCurrentMemtable();
    }
}
