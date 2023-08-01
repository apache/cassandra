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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Transactional;

import static java.lang.String.format;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class CompactionTaskTest
{
    private static TableMetadata cfm;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "ks").build();
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
    public void testTaskIdIsPersistedInCompactionHistory()
    {
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (1, 1);");
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (2, 2);");
        Util.flush(cfs);
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (3, 3);");
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (4, 4);");
        Util.flush(cfs);
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        Assert.assertEquals(2, sstables.size());

        TimeUUID id;

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION))
        {
            id = txn.opId();
            CompactionTask task = new CompactionTask(cfs, txn, 0);
            task.execute(CompactionManager.instance.active);
        }

        UntypedResultSet rows = QueryProcessor.executeInternal(format("SELECT id FROM system.%s where id = %s",
                                                                      SystemKeyspace.COMPACTION_HISTORY,
                                                                      id.toString()));

        Assert.assertNotNull(rows);
        Assert.assertFalse(rows.isEmpty());

        UntypedResultSet.Row one = rows.one();
        TimeUUID persistedId = one.getTimeUUID("id");

        Assert.assertEquals(id, persistedId);
    }

    @Test
    public void compactionInterruption() throws Exception
    {
        cfs.getCompactionStrategyManager().disable();
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (1, 1);");
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (2, 2);");
        Util.flush(cfs);
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (3, 3);");
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (4, 4);");
        Util.flush(cfs);
        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        Assert.assertEquals(2, sstables.size());

        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        Assert.assertNotNull(txn);
        CompactionTask task = new CompactionTask(cfs, txn, 0);
        Assert.assertNotNull(task);
        cfs.getCompactionStrategyManager().pause();
        try
        {
            task.execute(CompactionManager.instance.active);
            Assert.fail("Expected CompactionInterruptedException");
        }
        catch (CompactionInterruptedException e)
        {
            // expected
        }
        Assert.assertEquals(Transactional.AbstractTransactional.State.ABORTED, txn.state());
    }

    private static void mutateRepaired(SSTableReader sstable, long repairedAt, TimeUUID pendingRepair, boolean isTransient) throws IOException
    {
        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, repairedAt, pendingRepair, isTransient);
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
        Util.flush(cfs);
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (2, 2);");
        Util.flush(cfs);
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (3, 3);");
        Util.flush(cfs);
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (4, 4);");
        Util.flush(cfs);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        Assert.assertEquals(4, sstables.size());

        SSTableReader unrepaired = sstables.get(0);
        SSTableReader repaired = sstables.get(1);
        SSTableReader pending1 = sstables.get(2);
        SSTableReader pending2 = sstables.get(3);

        mutateRepaired(repaired, FBUtilities.nowInSeconds(), ActiveRepairService.NO_PENDING_REPAIR, false);
        mutateRepaired(pending1, UNREPAIRED_SSTABLE, nextTimeUUID(), false);
        mutateRepaired(pending2, UNREPAIRED_SSTABLE, nextTimeUUID(), false);

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

    @Test
    public void testOfflineCompaction()
    {
        cfs.getCompactionStrategyManager().disable();
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (1, 1);");
        Util.flush(cfs);
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (2, 2);");
        Util.flush(cfs);
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (3, 3);");
        Util.flush(cfs);
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, v) VALUES (4, 4);");
        Util.flush(cfs);

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
    
    @Test
    public void testMajorCompactTask()
    {
        //major compact without range/pk specified 
        CompactionTasks compactionTasks = cfs.getCompactionStrategyManager().getMaximalTasks(Integer.MAX_VALUE, false, OperationType.MAJOR_COMPACTION);
        Assert.assertTrue(compactionTasks.stream().allMatch(task -> task.compactionType.equals(OperationType.MAJOR_COMPACTION)));
    }
}
