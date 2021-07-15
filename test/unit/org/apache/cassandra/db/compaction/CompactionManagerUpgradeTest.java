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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CompactionManagerUpgradeTest
{
    private static final String KS_PREFIX = "Keyspace1";
    private static final String TABLE_PREFIX = "CF_STANDARD";

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KS_PREFIX,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS_PREFIX, TABLE_PREFIX)
                                                .compaction(CompactionParams.stcs(Collections.emptyMap())));
    }

    @Before
    public void setUp() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        cfs.truncateBlocking();
    }


    @Test
    public void testAutomaticUpgradeConcurrency() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(1);

        // latch to block CompactionManager.BackgroundCompactionCandidate#maybeRunUpgradeTask
        // inside the currentlyBackgroundUpgrading check - with max_concurrent_auto_upgrade_tasks = 1 this will make
        // sure that BackgroundCompactionCandidate#maybeRunUpgradeTask returns false until the latch has been counted down
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger upgradeTaskCount = new AtomicInteger(0);
        MockCFSForCSM mock = new MockCFSForCSM(cfs, latch, upgradeTaskCount);

        CompactionManager.BackgroundCompactionCandidate r = CompactionManager.instance.getBackgroundCompactionCandidate(mock);

        // basic idea is that we start a thread which will be able to get in to the currentlyBackgroundUpgrading-guarded
        // code in CompactionManager, then we try to run a bunch more of the upgrade tasks which should return false
        // due to the currentlyBackgroundUpgrading count being >= max_concurrent_auto_upgrade_tasks
        Thread t = new Thread(r::maybeRunUpgradeTask);
        t.start();
        Thread.sleep(100); // let the thread start and grab the task
        assertEquals(1, CompactionManager.instance.currentlyBackgroundUpgrading.get());
        assertNull(r.maybeRunUpgradeTask());
        assertNull(r.maybeRunUpgradeTask());
        latch.countDown();
        t.join();
        assertEquals(1, upgradeTaskCount.get()); // we should only call findUpgradeSSTableTask once when concurrency = 1
        assertEquals(0, CompactionManager.instance.currentlyBackgroundUpgrading.get());

        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
    }

    @Test
    public void testAutomaticUpgradeConcurrency2() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(2);
        // latch to block CompactionManager.BackgroundCompactionCandidate#maybeRunUpgradeTask
        // inside the currentlyBackgroundUpgrading check - with max_concurrent_auto_upgrade_tasks = 1 this will make
        // sure that BackgroundCompactionCandidate#maybeRunUpgradeTask returns false until the latch has been counted down
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger upgradeTaskCount = new AtomicInteger();
        MockCFSForCSM mock = new MockCFSForCSM(cfs, latch, upgradeTaskCount);

        CompactionManager.BackgroundCompactionCandidate r = CompactionManager.instance.getBackgroundCompactionCandidate(mock);

        // basic idea is that we start 2 threads who will be able to get in to the currentlyBackgroundUpgrading-guarded
        // code in CompactionManager, then we try to run a bunch more of the upgrade task which should return false
        // due to the currentlyBackgroundUpgrading count being >= max_concurrent_auto_upgrade_tasks
        Thread t = new Thread(r::maybeRunUpgradeTask);
        t.start();
        Thread t2 = new Thread(r::maybeRunUpgradeTask);
        t2.start();
        Thread.sleep(100); // let the threads start and grab the task
        assertEquals(2, CompactionManager.instance.currentlyBackgroundUpgrading.get());
        assertNull(r.maybeRunUpgradeTask());
        assertNull(r.maybeRunUpgradeTask());
        assertNull(r.maybeRunUpgradeTask());
        assertEquals(2, CompactionManager.instance.currentlyBackgroundUpgrading.get());
        latch.countDown();
        t.join();
        t2.join();
        assertEquals(2, upgradeTaskCount.get());
        assertEquals(0, CompactionManager.instance.currentlyBackgroundUpgrading.get());

        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(1);
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
    }

    private static class MockCFSForCSM extends ColumnFamilyStore
    {
        private final CountDownLatch latch;
        private final AtomicInteger upgradeTaskCount;

        private MockCFSForCSM(ColumnFamilyStore cfs, CountDownLatch latch, AtomicInteger upgradeTaskCount)
        {
            super(cfs.keyspace, cfs.name, 10, cfs.metadata, cfs.getDirectories(), true, false, false);
            this.latch = latch;
            this.upgradeTaskCount = upgradeTaskCount;
        }

        @Override
        public Set<SSTableReader> getLiveSSTables()
        {
            try
            {
                latch.await();
                upgradeTaskCount.incrementAndGet();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }

            return Collections.emptySet();
        }
    }
}
