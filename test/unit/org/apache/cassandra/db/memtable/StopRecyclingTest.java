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

package org.apache.cassandra.db.memtable;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.junit.Assert.assertTrue;

/**
 * Test that verifies stopRecycling is called when a memtable is flushed.
 */
@RunWith(BMUnitRunner.class)
public class StopRecyclingTest extends CQLTester
{
    public static AtomicInteger stopRecyclingCalls = new AtomicInteger(0);

    public static void countStopRecycling()
    {
        stopRecyclingCalls.getAndIncrement();
    }

    @Test
    @BMRule(name = "Count stopRecycling calls",
            targetClass = "InMemoryBaseTrie",
            targetMethod = "stopRecycling",
            action = "org.apache.cassandra.db.memtable.StopRecyclingTest.countStopRecycling()")
    public void testStopRecyclingCalledOnFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int) WITH memtable = 'trie'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertTrue(cfs.getCurrentMemtable() instanceof TrieMemtable);

        for (int i = 0; i < 1000; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, i);

        // The memtable must release its recycling state once all writes to it have completed. Every shard of the
        // memtable does this for its trie.
        int initialCount = stopRecyclingCalls.get();
        flush();
        assertTrue("stopRecycling should have been called at least once during flush",
                   stopRecyclingCalls.get() > initialCount);
    }
}
