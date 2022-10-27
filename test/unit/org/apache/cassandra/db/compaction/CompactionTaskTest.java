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

import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;

public class CompactionTaskTest
{
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();
        CFMetaData table = CFMetaData.compile("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "ks");
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1), table);

        cfs = Schema.instance.getColumnFamilyStoreInstance(table.cfId);
    }

    @Before
    public void setUp() throws Exception
    {
        cfs.getCompactionStrategyManager().enable();
        cfs.truncateBlocking();
    }

    @Test
    public void testOfflineCompaction()
    {
        cfs.getCompactionStrategyManager().disable();
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (1, 1);");
        cfs.forceBlockingFlush();
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (2, 2);");
        cfs.forceBlockingFlush();
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (3, 3);");
        cfs.forceBlockingFlush();
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (4, 4);");
        cfs.forceBlockingFlush();

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        Assert.assertEquals(4, sstables.size());

        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.COMPACTION, sstables))
        {
            Assert.assertEquals(4, txn.tracker.getView().liveSSTables().size());
            CompactionTask task = new CompactionTask(cfs, txn, 1000);
            task.execute(null);

            // Check that new SSTable was not released
            Assert.assertEquals(1, txn.tracker.getView().liveSSTables().size());
            SSTableReader newSSTable = txn.tracker.getView().liveSSTables().iterator().next();
            Assert.assertNotNull(newSSTable.tryRef());
        }
        finally
        {
            // SSTables were compacted offline; CFS didn't notice that, so we have to remove them manually
            cfs.getTracker().removeUnsafe(sstables);
        }
    }
}