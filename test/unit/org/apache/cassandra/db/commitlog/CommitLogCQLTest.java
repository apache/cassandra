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

package org.apache.cassandra.db.commitlog;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;

public class CommitLogCQLTest extends CQLTester
{
    @Test
    public void testTruncateSegmentDiscard() throws Throwable
    {
        String otherTable = createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));");

        createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));");
        execute("INSERT INTO %s (idx, data) VALUES (?, ?)", 15, Integer.toString(15));
        flush();

        // We write something in different table to advance the commit log position. Current table remains clean.
        executeFormattedQuery(String.format("INSERT INTO %s.%s (idx, data) VALUES (?, ?)", keyspace(), otherTable), 16, Integer.toString(16));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assert cfs.getTracker().getView().getCurrentMemtable().isClean();
        // Calling switchMemtable directly applies Flush even though memtable is empty. This can happen with some races
        // (flush with recycling by segment manager). It should still tell commitlog that the memtable's region is clean.
        // CASSANDRA-12436
        cfs.switchMemtable(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        execute("INSERT INTO %s (idx, data) VALUES (?, ?)", 15, Integer.toString(17));

        Collection<CommitLogSegment> active = new ArrayList<>(CommitLog.instance.segmentManager.getActiveSegments());
        CommitLog.instance.forceRecycleAllSegments();

        // If one of the previous segments remains, it wasn't clean.
        active.retainAll(CommitLog.instance.segmentManager.getActiveSegments());
        assert active.isEmpty();
    }
    
    @Test
    public void testSwitchMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        
        AtomicBoolean shouldStop = new AtomicBoolean(false);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<>();
        
        final String stmt = String.format("INSERT INTO %s.%s (idx, data) VALUES(?, ?)", KEYSPACE, currentTable());
        for (int i = 0; i < 10; ++i)
        {
            threads.add(new Thread("" + i)
            {
                public void run()
                {
                    try
                    {
                        while (!shouldStop.get())
                        {
                            for (int i = 0; i < 50; i++)
                            {
                                QueryProcessor.executeInternal(stmt, i, Integer.toString(i));
                            }
                            cfs.dumpMemtable();
                        }
                    }
                    catch (Throwable t)
                    {
                        errors.add(t);
                        shouldStop.set(true);
                    }
                }
            });
        }

        for (Thread t : threads)
            t.start();

        Thread.sleep(15_000);
        shouldStop.set(true);
        
        for (Thread t : threads)
            t.join();

        if (!errors.isEmpty())
        {
            StringBuilder sb = new StringBuilder();
            for(Throwable error: errors)
            {
                sb.append("Got error during memtable switching:\n");
                sb.append(error.getMessage() + "\n");
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream ps = new PrintStream(os);
                error.printStackTrace(ps);
                sb.append(os.toString("UTF-8"));
            }
            Assert.fail(sb.toString());
        }
    }
}
