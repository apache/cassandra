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

package org.apache.cassandra.db.repair;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

public class IgnoreRepairedAtTest
{
    static InetAddressAndPort local;

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    String ks;
    static final String tbl = "tbl";

    TableMetadata cfm;
    ColumnFamilyStore cfs;

    SSTableReader repairedSSTable;

    @BeforeClass
    public static void setupClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        local = InetAddressAndPort.getByName("127.0.0.1");
        ActiveRepairService.instance.consistent.local.start();
    }

    @Before
    public void init() throws Throwable
    {
        StorageService.instance.setIgnoreRepairedatEnabled(false);
        ks = "ks_" + System.currentTimeMillis();
        cfm = CreateTableStatement.parse(String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT)", ks, tbl), ks).build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
        assert cfs != null;

        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        // create 10 sstables, 10 rows per SSTable with TTL=1s
        makeSSTablesWithTTL(10, cfs, 10, 1);
        assertEquals(cfs.getLiveSSTables().size(), 10);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getSSTableMetadata().repairedAt);
        }
        // mutate repairAt for 1 SSTables
        repairedSSTable = cfs.getLiveSSTables().iterator().next();
        mutateRepairedAt(repairedSSTable, 1000);
        assertTrue(repairedSSTable.isRepaired());
    }

    @Test
    public void testCompactionIgnoresRepairedAtDisabled() throws Throwable
    {
        // regular path
        assertFalse(StorageService.instance.getIgnoreRepairedatEnabled());

        // trigger a compaction: should throw exception
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION))
        {
            assertNotNull(txn);
            CompactionTask task = new CompactionTask(cfs, txn, 0);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected, unrepaired and repaired can't be compacted together by default
        }
    }

    @Test
    public void testCompactionIgnoresRepairedAtEnabled() throws Throwable
    {
        // set ignore repairedAt
        StorageService.instance.setIgnoreRepairedatEnabled(true);
        // this SSTable should be considered unrepaired
        assertFalse(repairedSSTable.isRepaired());

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION))
        {
            assertNotNull(txn);
            CompactionTask task = new CompactionTask(cfs, txn, 0);
            task.execute(null);

            // Check that new SSTable was not released
            assertEquals(1, txn.tracker.getView().liveSSTables().size());
            SSTableReader newSSTable = txn.tracker.getView().liveSSTables().iterator().next();
            assertNotNull(newSSTable.tryRef());

            // Check that new SSTable should have been reset to unrepaired
            assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, txn.tracker.getView().liveSSTables().iterator().next().getRepairedAt());
        }
        catch (IllegalArgumentException e)
        {
            fail("Got IllegalArgumentException");
        }
    }

    @Test
    public void testSSTablesAreSeparatedWithRepairedAt() throws Throwable
    {
        // regular path
        assertFalse(StorageService.instance.getIgnoreRepairedatEnabled());

        // wait enough to force single compaction
        TimeUnit.SECONDS.sleep(5);

        // start compaction
        cfs.enableAutoCompaction();
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
        do
        {
            TimeUnit.SECONDS.sleep(1);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);
        cfs.disableAutoCompaction();

        // should have repaired and unrepaired timestamps
        assertEquals(2, cfs.getLiveSSTables().size());
        assertTrue(cfs.getLiveSSTables().stream().anyMatch(ts -> ts.getRepairedAt() == ActiveRepairService.UNREPAIRED_SSTABLE));
        assertTrue(cfs.getLiveSSTables().stream().anyMatch(ts -> ts.getRepairedAt() != ActiveRepairService.UNREPAIRED_SSTABLE));

        assertFalse(CompactionManager.instance.isCompacting(Collections.singleton(cfs), (sstable) -> true));
        // major comapction will still separate them
        cfs.forceMajorCompaction(false);
        do
        {
            TimeUnit.SECONDS.sleep(1);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);

        assertEquals(2, cfs.getLiveSSTables().size());
        assertTrue(cfs.getLiveSSTables().stream().anyMatch(ts -> ts.getRepairedAt() == ActiveRepairService.UNREPAIRED_SSTABLE));
        assertTrue(cfs.getLiveSSTables().stream().anyMatch(ts -> ts.getRepairedAt() != ActiveRepairService.UNREPAIRED_SSTABLE));
    }

    @Test
    public void testSSTableNotSeparatedWhenRepairedAtIgnored() throws Throwable
    {
        StorageService.instance.setIgnoreRepairedatEnabled(true);
        assertTrue(StorageService.instance.getIgnoreRepairedatEnabled());

        // ensure at least 2 SSTables with mixed repaired timestamp
        assertTrue(cfs.getLiveSSTables().size() >= 2);
        assertTrue(cfs.getLiveSSTables().stream().anyMatch(ts -> ts.getRepairedAt() == ActiveRepairService.UNREPAIRED_SSTABLE));
        assertTrue(cfs.getLiveSSTables().stream().anyMatch(ts -> ts.getRepairedAt() != ActiveRepairService.UNREPAIRED_SSTABLE));

        // Here we recreate cfs to mock restarting of Cassandra (reload SSTables from directory)
        // After restart Cassandra should not separate SSTables to different holders because we
        // ignore repairedAt, and they'll all grouped to repaired holder
        cfs = new ColumnFamilyStore(cfs.keyspace, cfs.name, Util.newSeqGen(10), cfs.metadata, cfs.getDirectories(), true, false, false);

        // wait enough to force single compaction
        TimeUnit.SECONDS.sleep(5);

        // start compaction
        cfs.enableAutoCompaction();
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
        do
        {
            TimeUnit.SECONDS.sleep(1);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);
        cfs.disableAutoCompaction();

        assertEquals(1, cfs.getLiveSSTables().size());
        // new SSTables should have unrepaired status
        assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, cfs.getLiveSSTables().iterator().next().getRepairedAt());
    }

    private void mutateRepairedAt(SSTableReader sstable, long newRepairedAt) throws Throwable
    {
        Set<SSTableReader> changed = new HashSet<>();
        Descriptor descriptor = sstable.descriptor;
        descriptor.getMetadataSerializer().mutateRepairMetadata(descriptor, newRepairedAt, null, false);
        sstable.reloadSSTableMetadata();
        changed.add(sstable);
        cfs.getTracker().notifySSTableRepairedStatusChanged(changed);
    }

    private void makeSSTablesWithTTL(int num, ColumnFamilyStore cfs, int rowsPerSSTable, int ttl)
    {
        for (int i = 0; i < num; i++)
        {
            int val = i * rowsPerSSTable;  // multiplied to prevent ranges from overlapping
            for (int j = 0; j < rowsPerSSTable; j++)
                QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?) USING TTL ?",
                                                             ks,
                                                             cfs.getTableName()), val + j, val + j, ttl);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }
        Assert.assertEquals(num, cfs.getLiveSSTables().size());
    }
}
