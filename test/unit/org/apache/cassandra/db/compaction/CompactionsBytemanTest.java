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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(BMUnitRunner.class)
public class CompactionsBytemanTest extends CQLTester
{
    /*
    Return false for the first time hasAvailableDiskSpace is called. i.e first SSTable is too big
    Create 5 SSTables. After compaction, there should be 2 left - 1 as the 9 SStables which were merged,
    and the other the SSTable that was 'too large' and failed the hasAvailableDiskSpace check
     */
    @Test
    @BMRules(rules = { @BMRule(name = "One SSTable too big for remaining disk space test",
    targetClass = "Directories",
    targetMethod = "hasDiskSpaceForCompactionsAndStreams",
    condition = "not flagged(\"done\")",
    action = "flag(\"done\"); return false;") } )
    public void testSSTableNotEnoughDiskSpaceForCompactionGetsDropped() throws Throwable
    {
        createLowGCGraceTable();
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (int i = 0; i < 5; i++)
        {
            createPossiblyExpiredSSTable(cfs, false);
        }
        assertEquals(5, getCurrentColumnFamilyStore().getLiveSSTables().size());
        cfs.forceMajorCompaction(false);
        assertEquals(2, getCurrentColumnFamilyStore().getLiveSSTables().size());
        dropTable("DROP TABLE %s");
    }

    /*
    Always return false for hasAvailableDiskSpace. i.e node has no more space
    Create 2 expired SSTables and 1 long lived one. After compaction, there should only be 1 left,
    as the 2 expired ones would have been compacted away.
     */
    @Test
    @BMRules(rules = { @BMRule(name = "No disk space with expired SSTables test",
    targetClass = "Directories",
    targetMethod = "hasDiskSpaceForCompactionsAndStreams",
    action = "return false;") } )
    public void testExpiredSSTablesStillGetDroppedWithNoDiskSpace() throws Throwable
    {
        createLowGCGraceTable();
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        createPossiblyExpiredSSTable(cfs, true);
        createPossiblyExpiredSSTable(cfs, true);
        createPossiblyExpiredSSTable(cfs, false);
        assertEquals(3, cfs.getLiveSSTables().size());
        Thread.sleep(TimeUnit.SECONDS.toMillis((long)1.5)); // give some time to expire.
        cfs.forceMajorCompaction(false);
        assertEquals(1, cfs.getLiveSSTables().size());
        dropTable("DROP TABLE %s");
    }

    /*
    Always return false for hasAvailableDiskSpace. i.e node has no more space
    Create 2 SSTables. Compaction will not succeed and will throw Runtime Exception
     */
    @Test(expected = RuntimeException.class)
    @BMRules(rules = { @BMRule(name = "No disk space with expired SSTables test",
    targetClass = "Directories",
    targetMethod = "hasDiskSpaceForCompactionsAndStreams",
    action = "return false;") } )
    public void testRuntimeExceptionWhenNoDiskSpaceForCompaction() throws Throwable
    {
        createLowGCGraceTable();
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        createPossiblyExpiredSSTable(cfs, false);
        createPossiblyExpiredSSTable(cfs, false);
        cfs.forceMajorCompaction(false);
        dropTable("DROP TABLE %s");
    }

    @Test
    @BMRule(name = "Delay background compaction task future check",
            targetClass = "CompactionManager",
            targetMethod = "submitBackground",
            targetLocation = "AT INVOKE java.util.concurrent.Future.isCancelled",
            condition = "!$cfs.getKeyspaceName().contains(\"system\")",
            action = "Thread.sleep(5000)")
    public void testCompactingCFCounting() throws Throwable
    {
        createTable("CREATE TABLE %s (k INT, c INT, v INT, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.enableAutoCompaction();

        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 1, 1);
        Util.spinAssertEquals(true, () -> CompactionManager.instance.compactingCF.count(cfs) == 0, 5);
        Util.flush(cfs);

        Util.spinAssertEquals(true, () -> CompactionManager.instance.compactingCF.count(cfs) == 0, 5);
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
        assertEquals(0, CompactionManager.instance.compactingCF.count(cfs));
    }

    private void createPossiblyExpiredSSTable(final ColumnFamilyStore cfs, final boolean expired) throws Throwable
    {
        if (expired)
        {
            execute("INSERT INTO %s (id, val) values (1, 'expired') USING TTL 1");
            Thread.sleep(TimeUnit.SECONDS.toMillis((long)1.5));
        }
        else
        {
            execute("INSERT INTO %s (id, val) values (2, 'immortal')");
        }
        Util.flush(cfs);
    }

    private void createLowGCGraceTable(){
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text) with compaction = {'class':'SizeTieredCompactionStrategy', 'enabled': 'false'} AND gc_grace_seconds=0");
    }

    @Test
    @BMRule(name = "Stop all compactions",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "AT INVOKE getCompactionAwareWriter",
    action = "$ci.stop()")
    public void testStopUserDefinedCompactionRepaired() throws Throwable
    {
        testStopCompactionRepaired((cfs) -> {
            Collection<Descriptor> files = cfs.getLiveSSTables().stream().map(s -> s.descriptor).collect(Collectors.toList());
            FBUtilities.waitOnFuture(CompactionManager.instance.submitUserDefined(cfs, files, CompactionManager.NO_GC));
        });
    }

    @Test
    @BMRule(name = "Stop all compactions",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "AT INVOKE getCompactionAwareWriter",
    action = "$ci.stop()")
    public void testStopSubRangeCompactionRepaired() throws Throwable
    {
        testStopCompactionRepaired((cfs) -> {
            Collection<Range<Token>> ranges = Collections.singleton(new Range<>(cfs.getPartitioner().getMinimumToken(),
                                                                                cfs.getPartitioner().getMaximumToken()));
            CompactionManager.instance.forceCompactionForTokenRange(cfs, ranges);
        });
    }

    public void testStopCompactionRepaired(Consumer<ColumnFamilyStore> compactionRunner) throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k INT, c INT, v INT, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = Keyspace.open(CQLTester.KEYSPACE).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                execute("insert into %s (k, c, v) values (?, ?, ?)", i, j, i*j);
            }
            Util.flush(cfs);
        }
        cfs.getCompactionStrategyManager().mutateRepaired(cfs.getLiveSSTables(), System.currentTimeMillis(), null, false);
        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                execute("insert into %s (k, c, v) values (?, ?, ?)", i, j, i*j);
            }
            Util.flush(cfs);
        }

        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        assertTrue(CompactionManager.instance.active.getCompactions().stream().noneMatch(h -> h.getCompactionInfo().getTableMetadata().equals(cfs.metadata)));

        try
        {
            compactionRunner.accept(cfs);
            fail("compaction should fail");
        }
        catch (RuntimeException t)
        {
            if (!Throwables.isCausedBy(t, CompactionInterruptedException.class::isInstance))
                throw t;
            //expected
        }

        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        assertTrue(CompactionManager.instance.active.getCompactions().stream().noneMatch(h -> h.getCompactionInfo().getTableMetadata().equals(cfs.metadata)));

    }
}
