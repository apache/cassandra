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

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;

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
}
