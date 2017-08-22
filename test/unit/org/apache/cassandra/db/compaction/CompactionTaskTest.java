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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Transactional;

public class CompactionTaskTest
{
    private static TableMetadata cfm;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "coordinatorsessiontest").build();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
    }

    @Before
    public void setUp() throws Exception
    {
        cfs.getCompactionStrategyManager().enable();
        cfs.truncateBlocking();
    }

    @Test
    public void compactionInterruption() throws Exception
    {
        cfs.getCompactionStrategyManager().disable();
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (1, 1);");
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (2, 2);");
        cfs.forceBlockingFlush();
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (3, 3);");
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (4, 4);");
        cfs.forceBlockingFlush();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        Assert.assertEquals(2, sstables.size());

        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        Assert.assertNotNull(txn);
        CompactionTask task = new CompactionTask(cfs, txn, 0);
        Assert.assertNotNull(task);
        cfs.getCompactionStrategyManager().pause();
        try
        {
            task.execute(CompactionManager.instance.getMetrics());
            Assert.fail("Expected CompactionInterruptedException");
        }
        catch (CompactionInterruptedException e)
        {
            // expected
        }
        Assert.assertEquals(Transactional.AbstractTransactional.State.ABORTED, txn.state());
    }

    private static void mutateRepaired(SSTableReader sstable, long repairedAt, UUID pendingRepair) throws IOException
    {
        sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, repairedAt, pendingRepair);
        sstable.reloadSSTableMetadata();
    }

    /**
     * If we try to create a compaction task that will mix
     * repaired/unrepaired/pending repair sstables, it should fail
     */
    @Test
    public void mixedSSTableFailure() throws Exception
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

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        Assert.assertEquals(4, sstables.size());

        SSTableReader unrepaired = sstables.get(0);
        SSTableReader repaired = sstables.get(1);
        SSTableReader pending1 = sstables.get(2);
        SSTableReader pending2 = sstables.get(3);

        mutateRepaired(repaired, FBUtilities.nowInSeconds(), ActiveRepairService.NO_PENDING_REPAIR);
        mutateRepaired(pending1, ActiveRepairService.UNREPAIRED_SSTABLE, UUIDGen.getTimeUUID());
        mutateRepaired(pending2, ActiveRepairService.UNREPAIRED_SSTABLE, UUIDGen.getTimeUUID());

        LifecycleTransaction txn = null;
        List<SSTableReader> toCompact = new ArrayList<>(sstables);
        for (int i=0; i<sstables.size(); i++)
        {
            try
            {
                txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
                Assert.assertNotNull(txn);
                CompactionTask task = new CompactionTask(cfs, txn, 0);
                Assert.fail("Expected IllegalArgumentException");
            }
            catch (IllegalArgumentException e)
            {
                // expected
            }
            finally
            {
                if (txn != null)
                    txn.abort();
            }
            Collections.rotate(toCompact, 1);
        }
    }
}
