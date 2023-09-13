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

package org.apache.cassandra.index.sai.cql;

import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexParallelBuildTest extends SAITester
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT)" +
                                                        "  WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";

    private static final int PAGE_SIZE = 5000;

    @BeforeClass
    public static void setupCQLTester()
    {
        Config conf = DatabaseDescriptor.loadConfig();
        conf.num_tokens = 16;
        conf.incremental_backups = false;
        DatabaseDescriptor.daemonInitialization(() -> conf);
    }

    @Before
    public void setup()
    {
        requireNetwork();
    }

    @After
    public void resetCountersAndInjections()
    {
        Injections.deleteAll();
    }

    @Test
    public void testParallelIndexBuild() throws Throwable
    {
        // populate 10 sstables
        createTable(CREATE_TABLE_TEMPLATE);
        int sstable = 10;
        int rowsPerSSTable = 100;
        int key = 0;
        for (int s = 0; s < sstable; s++)
        {
            for (int i = 0; i < rowsPerSSTable; i++)
            {
                execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '0')", Integer.toString(key++));
            }
            flush();
        }

        // leave sstable files on disk
        getCurrentColumnFamilyStore().getTracker().unloadSSTables();
        assertTrue(getCurrentColumnFamilyStore().getLiveSSTables().isEmpty());

        // create indexes
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForIndexQueryable();
        assertTrue(getCurrentColumnFamilyStore().getLiveSSTables().isEmpty());

        Injections.Counter parallelBuildCounter = Injections.newCounter("IndexParallelBuildCounter")
                                                            .add(InvokePointBuilder.newInvokePoint().onClass("org.apache.cassandra.index.sai.StorageAttachedIndex$StorageAttachedIndexBuildingSupport")
                                                                                   .onMethod("getParallelIndexBuildTasks"))
                                                            .build();

        Injections.inject(parallelBuildCounter);
        assertEquals(0, parallelBuildCounter.get());

        // reload on-disk sstables and wait for parallel index build
        getCurrentColumnFamilyStore().loadNewSSTables();
        waitForAssert(() -> {
            assertEquals(getCurrentColumnFamilyStore().getLiveSSTables().size(), sstable);
            StorageAttachedIndex sai = (StorageAttachedIndex) Iterables.getOnlyElement(getCurrentColumnFamilyStore().indexManager.listIndexes());
            assertEquals(sai.getIndexContext().getView().getIndexes().size(), sstable);
        });

        // verify parallel index build is invoked once
        assertEquals(1, parallelBuildCounter.get());

        // verify index can be read
        assertRowCount(sstable * rowsPerSSTable, "SELECT id1 FROM %s WHERE v1>=0");
    }

    private void assertRowCount(int count, String query)
    {
        assertEquals(count,
                     sessionNet(getDefaultVersion())
                     .execute(new SimpleStatement(formatQuery(query))
                              .setFetchSize(PAGE_SIZE)).all().size());
    }
}

