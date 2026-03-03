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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.TestableLifecycleTransaction;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.concurrent.Transactional;

import static java.lang.String.format;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class CompactionTaskTest
{
    private static TableMetadata cfm;
    private static ColumnFamilyStore cfs;

    private static TableMetadata gcGraceCfm;
    private static ColumnFamilyStore gcGraceCfs;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "ks").build();
        gcGraceCfm = CreateTableStatement.parse("CREATE TABLE tbl2 (k INT PRIMARY KEY, col1 INT, col2 INT, col3 INT, data TEXT) WITH gc_grace_seconds = 0", "ks").build();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1), cfm, gcGraceCfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
        gcGraceCfs = Schema.instance.getColumnFamilyStoreInstance(gcGraceCfm.id);
    }

    @Before
    public void setUp() throws Exception
    {
        cfs.getCompactionStrategyManager().enable();
        cfs.truncateBlocking();

        gcGraceCfs.getCompactionStrategyManager().enable();
        gcGraceCfs.truncateBlocking();
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

    /**
     * Test that even some SSTables are fully expired, we can still select and reference them
     * while they are part of compaction.
     *
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-19776>CASSANDRA-19776</a>
     */
    @Test
    public void testFullyExpiredSSTablesAreNotReleasedPrematurely()
    {
        Assert.assertEquals(0, gcGraceCfs.getLiveSSTables().size());
        gcGraceCfs.getCompactionStrategyManager().disable();

        // Use large SSTables (10+ MiB) so that switching to new output SSTables happens during
        // compaction. Without large enough SSTables, the output fits in one SSTable and no switch happens
        int numKeys = 5000;
        String data = new String(new char[2048]).replace('\0', 'x'); // ~2KB padding per row

        // Similar technique to get fully expired SSTables as in TTLExpiryTest#testAggressiveFullyExpired
        // SSTable 1 (will be fully expired - superseded by SSTable 2)
        for (int k = 0; k < numKeys; k++)
        {
            QueryProcessor.executeInternal("INSERT INTO ks.tbl2 (k, col1, data) VALUES (?, 1, ?) USING TIMESTAMP 1 AND TTL 1", k, data);
            QueryProcessor.executeInternal("INSERT INTO ks.tbl2 (k, col2, data) VALUES (?, 1, ?) USING TIMESTAMP 3 AND TTL 1", k, data);
        }
        Util.flush(gcGraceCfs);

        // SSTable 2 (will be fully expired - superseded by SSTables 3 and 4)
        for (int k = 0; k < numKeys; k++)
        {
            QueryProcessor.executeInternal("INSERT INTO ks.tbl2 (k, col1, data) VALUES (?, 1, ?) USING TIMESTAMP 2 AND TTL 1", k, data);
            QueryProcessor.executeInternal("INSERT INTO ks.tbl2 (k, col2, data) VALUES (?, 1, ?) USING TIMESTAMP 5 AND TTL 1", k, data);
        }
        Util.flush(gcGraceCfs);

        Set<SSTableReader> toBeObsolete = new HashSet<>(gcGraceCfs.getLiveSSTables());
        Assert.assertEquals(2, toBeObsolete.size());

        // SSTable 3 (not fully expired)
        for (int k = 0; k < numKeys; k++)
        {
            QueryProcessor.executeInternal("INSERT INTO ks.tbl2 (k, col1, data) VALUES (?, 1, ?) USING TIMESTAMP 4 AND TTL 1", k, data);
            QueryProcessor.executeInternal("INSERT INTO ks.tbl2 (k, col3, data) VALUES (?, 1, ?) USING TIMESTAMP 7 AND TTL 1", k, data);
        }
        Util.flush(gcGraceCfs);

        // SSTable 4 (not fully expired - col3 has longer TTL)
        for (int k = 0; k < numKeys; k++)
        {
            QueryProcessor.executeInternal("INSERT INTO ks.tbl2 (k, col3, data) VALUES (?, 1, ?) USING TIMESTAMP 6 AND TTL 3", k, data);
            QueryProcessor.executeInternal("INSERT INTO ks.tbl2 (k, col2, data) VALUES (?, 1, ?) USING TIMESTAMP 8 AND TTL 1", k, data);
        }
        Util.flush(gcGraceCfs);

        Set<SSTableReader> sstables = gcGraceCfs.getLiveSSTables();
        Assert.assertEquals(4, sstables.size());

        // Enable preemptive opening so that SSTableRewriter.switchWriter() calls checkpoint().
        int originalPreemptiveOpenInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(2);

        // collector of stacktraces to later check if checkpoint was called in switchWriter
        final List<StackTraceElement[]> stacks = new ArrayList<>();

        try
        {
            AtomicInteger checkpointCount = new AtomicInteger(0);

            // Hook into transaction's checkpoint and doCommit methods to verify that after
            // checkpointing, all SSTables (including fully expired ones) remain referenceable.
            // We use TestableLifecycleTransaction because in cassandra-5.0, AbstractCompactionTask.transaction
            // is LifecycleTransaction (concrete), not ILifecycleTransaction (interface), so
            // WrappedLifecycleTransaction cannot be used with CompactionTask directly.
            LifecycleTransaction txn = new TestableLifecycleTransaction(gcGraceCfs.getTracker(), OperationType.COMPACTION, sstables)
            {
                @Override
                public void checkpoint()
                {
                    stacks.add(Thread.currentThread().getStackTrace());

                    for (SSTableReader r : toBeObsolete)
                        Assert.assertTrue(this.isObsolete(r));

                    assertAllSSTablesAreReferenceable();

                    super.checkpoint();

                    // This is the critical assertion: after checkpoint(), all SSTables in the
                    // CANONICAL view must still be referenceable. Before the fix, fully expired
                    // SSTables would lose their references here, causing selectAndReference() to
                    // spin loop (CASSANDRA-19776).
                    assertAllSSTablesAreReferenceable();

                    checkpointCount.incrementAndGet();
                }

                @Override
                public Throwable doCommit(Throwable accumulate)
                {
                    assertAllSSTablesAreReferenceable();
                    return super.doCommit(accumulate);
                }

                private void assertAllSSTablesAreReferenceable()
                {
                    // This simulates what EstimatedPartitionCount metric and similar code paths do.
                    // It is crucial that tryRef does not return null; a null result means some SSTables
                    // are not referenceable, which would cause selectAndReference() to spin loop.
                    ColumnFamilyStore.ViewFragment view = gcGraceCfs.select(View.selectFunction(SSTableSet.CANONICAL));
                    Refs<SSTableReader> refs = Refs.tryRef(view.sstables);
                    Assert.assertNotNull("Some SSTables in CANONICAL view are not referenceable (CASSANDRA-19776)", refs);
                    refs.close();
                }
            };

            // Use MaxSSTableSizeWriter with a small max size (2 MiB) to force output sstable switches during compaction.
            long maxSSTableSize = 2L * 1024 * 1024; // 2 MiB
            CompactionTask task = new CompactionTask(gcGraceCfs, txn, FBUtilities.nowInSeconds() + 2)
            {
                @Override
                public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                      Directories directories,
                                                                      LifecycleTransaction transaction,
                                                                      Set<SSTableReader> nonExpiredSSTables)
                {
                    return new MaxSSTableSizeWriter(cfs, directories, transaction, nonExpiredSSTables,
                                                    maxSSTableSize, 0);
                }
            };

            try (CompactionController compactionController = task.getCompactionController(txn.originals()))
            {
                Set<SSTableReader> fullyExpiredSSTables = compactionController.getFullyExpiredSSTables();
                Assert.assertEquals(2, fullyExpiredSSTables.size());
                task.execute(null);
            }

            // Verify that checkpoint was called more than once, proving that output sstable switching
            // happened during compaction. Without MaxSSTableSizeWriter and large enough SSTables,
            // checkpoint would only be called at the end, which would not exercise the CASSANDRA-19776 fix.
            Assert.assertTrue("Expected checkpoint() to be called more than once during compaction, but was called "
                              + checkpointCount.get() + " time(s). Output sstable switching did not occur.",
                              checkpointCount.get() > 1);
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpenInterval);
        }

        Assert.assertFalse(stacks.isEmpty());

        boolean checkpointCalledInSSTableRewriter = false;

        for (int i = 0; i < stacks.size(); i++)
        {
            for (StackTraceElement element : stacks.get(i))
            {
                if (element.getClassName().equals(SSTableRewriter.class.getName())
                    && (element.getMethodName().equals("switchWriter")
                        || element.getMethodName().equals("maybeReopenEarly")))
                {
                    checkpointCalledInSSTableRewriter = true;
                    break;
                }
            }
        }

        Assert.assertTrue(checkpointCalledInSSTableRewriter);
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
}
