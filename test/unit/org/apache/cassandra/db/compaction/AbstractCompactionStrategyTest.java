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

import java.util.Collections;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;

public class AbstractCompactionStrategyTest
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String LCS_TABLE = "LCS_TABLE";
    private static final String STCS_TABLE = "STCS_TABLE";
    private static final String TWCS_TABLE = "TWCS_TABLE";

    @BeforeClass
    public static void loadData() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, LCS_TABLE)
                                                .compaction(CompactionParams.lcs(Collections.emptyMap())),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STCS_TABLE)
                                                .compaction(CompactionParams.stcs(Collections.emptyMap())),
                                    SchemaLoader.standardCFMD(KEYSPACE1, TWCS_TABLE)
                                                .compaction(CompactionParams.create(TimeWindowCompactionStrategy.class, Collections.emptyMap())));
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(LCS_TABLE).disableAutoCompaction();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(STCS_TABLE).disableAutoCompaction();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(TWCS_TABLE).disableAutoCompaction();
    }

    @After
    public void tearDown()
    {

        Keyspace.open(KEYSPACE1).getColumnFamilyStore(LCS_TABLE).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(STCS_TABLE).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(TWCS_TABLE).truncateBlocking();
    }

    @Test(timeout=30000)
    public void testGetNextBackgroundTaskDoesNotBlockLCS()
    {
        testGetNextBackgroundTaskDoesNotBlock(LCS_TABLE);
    }

    @Test(timeout=30000)
    public void testGetNextBackgroundTaskDoesNotBlockSTCS()
    {
        testGetNextBackgroundTaskDoesNotBlock(STCS_TABLE);
    }

    @Test(timeout=30000)
    public void testGetNextBackgroundTaskDoesNotBlockTWCS()
    {
        testGetNextBackgroundTaskDoesNotBlock(TWCS_TABLE);
    }

    public void testGetNextBackgroundTaskDoesNotBlock(String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(table);
        AbstractCompactionStrategy strategy = cfs.getCompactionStrategyManager().getStrategies().get(1).get(0);

        // Add 4 sstables
        for (int i = 1; i <= 4; i++)
        {
            insertKeyAndFlush(table, i);
        }

        // Check they are returned on the next background task
        try (LifecycleTransaction txn = strategy.getNextBackgroundTask(FBUtilities.nowInSeconds()).transaction)
        {
            Assert.assertEquals(cfs.getLiveSSTables(), txn.originals());
        }

        // now remove sstables on the tracker, to simulate a concurrent transaction
        cfs.getTracker().removeUnsafe(cfs.getLiveSSTables());

        // verify the compaction strategy will return null
        Assert.assertNull(strategy.getNextBackgroundTask(FBUtilities.nowInSeconds()));
    }


    private static void insertKeyAndFlush(String table, int key)
    {
        long timestamp = System.currentTimeMillis();
        DecoratedKey dk = Util.dk(String.format("%03d", key));
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(table);
        new RowUpdateBuilder(cfs.metadata(), timestamp, dk.getKey())
        .clustering(String.valueOf(key))
        .add("val", "val")
        .build()
        .applyUnsafe();
        Util.flush(cfs);
    }
}
