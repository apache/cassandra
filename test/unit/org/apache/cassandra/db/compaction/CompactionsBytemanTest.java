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

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;

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
    targetMethod = "hasAvailableDiskSpace",
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
    targetMethod = "hasAvailableDiskSpace",
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
    targetMethod = "hasAvailableDiskSpace",
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
            condition = "!$cfs.keyspace.getName().contains(\"system\")",
            action = "Thread.sleep(1000)")
    public void testCompactingCFCounting() throws Throwable
    {
        createTable("CREATE TABLE %s (k INT, c INT, v INT, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.enableAutoCompaction();

        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 1, 1);
        assertEquals(0, CompactionManager.instance.compactingCF.count(cfs));
        cfs.forceBlockingFlush();

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
        cfs.forceBlockingFlush();
    }

    private void createLowGCGraceTable(){
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text) with compaction = {'class':'SizeTieredCompactionStrategy', 'enabled': 'false'} AND gc_grace_seconds=0");
    }
}