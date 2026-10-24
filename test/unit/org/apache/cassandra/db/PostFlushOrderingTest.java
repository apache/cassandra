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

package org.apache.cassandra.db;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** A stalled flush delays its own table's later flush futures, but not other tables' (CASSANDRA-19597). */
@RunWith(BMUnitRunner.class)
@BMRule(name = "stall flush",
        targetClass = "org.apache.cassandra.db.ColumnFamilyStore$Flush",
        targetMethod = "flushMemtable",
        targetLocation = "AT ENTRY",
        action = "org.apache.cassandra.db.PostFlushOrderingTest.maybeStall($1)")
public class PostFlushOrderingTest extends CQLTester
{
    private static volatile String stallTable;
    private static volatile CountDownLatch stalled;
    private static volatile CountDownLatch release;

    public static void maybeStall(Memtable memtable)
    {
        if (!memtable.cfs.name.equals(stallTable))
            return;
        stallTable = null;
        stalled.countDown();
        Uninterruptibles.awaitUninterruptibly(release);
    }

    @Before
    public void reset()
    {
        stallTable = null;
        stalled = new CountDownLatch(1);
        release = new CountDownLatch(1);
    }

    @After
    public void unstall()
    {
        release.countDown();
    }

    @Test
    public void otherTable() throws Throwable
    {
        ColumnFamilyStore slow = table();
        ColumnFamilyStore fast = table();
        ListenableFuture<?> stalledFlush = stall(slow);

        write(fast);
        fast.forceFlush().get(30, TimeUnit.SECONDS);
        fast.forceFlush().get(30, TimeUnit.SECONDS); // clean memtable: waitForFlushes

        assertFalse(stalledFlush.isDone());
        release.countDown();
        stalledFlush.get(30, TimeUnit.SECONDS);
    }

    @Test
    public void sameTable() throws Throwable
    {
        ColumnFamilyStore cfs = table();
        ListenableFuture<?> first = stall(cfs);

        write(cfs);
        ListenableFuture<?> second = cfs.forceFlush();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (cfs.getLiveSSTables().isEmpty() && System.nanoTime() < deadline)
            Thread.sleep(10);
        assertEquals(1, cfs.getLiveSSTables().size());
        assertFalse(second.isDone());

        release.countDown();
        first.get(30, TimeUnit.SECONDS);
        second.get(30, TimeUnit.SECONDS);
        assertEquals(2, cfs.getLiveSSTables().size());
    }

    private ColumnFamilyStore table()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)"));
    }

    private static ListenableFuture<?> stall(ColumnFamilyStore cfs) throws InterruptedException
    {
        write(cfs);
        stallTable = cfs.name;
        ListenableFuture<?> flush = cfs.forceFlush();
        assertTrue(stalled.await(30, TimeUnit.SECONDS));
        return flush;
    }

    private static void write(ColumnFamilyStore cfs)
    {
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", cfs.keyspace.getName(), cfs.name));
    }
}
