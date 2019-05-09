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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(BMUnitRunner.class)
public class CompactionsBytemanTest extends CQLTester
{
    @Test
    @BMRule(name = "Delay background compaction task future check",
            targetClass = "CompactionManager",
            targetMethod = "submitBackground",
            targetLocation = "AT INVOKE java.util.concurrent.Future.isCancelled",
            condition = "!$cfs.keyspace.getName().contains(\"system\")",
            action = "Thread.sleep(1000)")
    public void testCompactingCFCounting() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k INT, c INT, v INT, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = Keyspace.open(CQLTester.KEYSPACE).getColumnFamilyStore(table);
        cfs.enableAutoCompaction();

        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 1, 1);
        assertEquals(0, CompactionManager.instance.compactingCF.count(cfs));
        cfs.forceBlockingFlush();

        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
        assertEquals(0, CompactionManager.instance.compactingCF.count(cfs));
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
            cfs.forceBlockingFlush();
        }
        setRepaired(cfs, cfs.getLiveSSTables());
        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                execute("insert into %s (k, c, v) values (?, ?, ?)", i, j, i*j);
            }
            cfs.forceBlockingFlush();
        }

        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        assertTrue(CompactionMetrics.getCompactions().stream().noneMatch(h -> h.getCompactionInfo().getCFMetaData().equals(cfs.metadata)));

        try
        {
            compactionRunner.accept(cfs);
            fail("compaction should fail");
        }
        catch (RuntimeException t)
        {
            if (!(t.getCause().getCause() instanceof CompactionInterruptedException))
                throw t;
            //expected
        }

        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        assertTrue(CompactionMetrics.getCompactions().stream().noneMatch(h -> h.getCompactionInfo().getCFMetaData().equals(cfs.metadata)));

    }

    private void setRepaired(ColumnFamilyStore cfs, Iterable<SSTableReader> sstables) throws IOException
    {
        Set<SSTableReader> changed = new HashSet<>();
        for (SSTableReader sstable: sstables)
        {
            sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, System.currentTimeMillis());
            sstable.reloadSSTableMetadata();
            changed.add(sstable);
        }
        cfs.getTracker().notifySSTableRepairedStatusChanged(changed);
    }
}
