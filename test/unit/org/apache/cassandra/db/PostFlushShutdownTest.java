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
import org.junit.Test;
import org.junit.runner.RunWith;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;

import static org.junit.Assert.assertTrue;

/**
 * Shutting down the post-flush executor neither waits on a flush in progress nor strands flush futures (CASSANDRA-19597).
 * Its own class: the executor is static and cannot be restarted.
 */
@RunWith(BMUnitRunner.class)
@BMRule(name = "stall flush",
        targetClass = "org.apache.cassandra.db.ColumnFamilyStore$Flush",
        targetMethod = "flushMemtable",
        targetLocation = "AT ENTRY",
        action = "org.apache.cassandra.db.PostFlushShutdownTest.maybeStall($1)")
public class PostFlushShutdownTest extends CQLTester
{
    private static volatile String stallTable;
    private static final CountDownLatch stalled = new CountDownLatch(1);
    private static final CountDownLatch release = new CountDownLatch(1);

    public static void maybeStall(Memtable memtable)
    {
        if (!memtable.cfs.name.equals(stallTable))
            return;
        stallTable = null;
        stalled.countDown();
        Uninterruptibles.awaitUninterruptibly(release);
    }

    @After
    public void unstall()
    {
        release.countDown();
    }

    @Test
    public void shutdown() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)"));
        write(cfs);
        stallTable = cfs.name;
        ListenableFuture<?> stalledFlush = cfs.forceFlush();
        assertTrue(stalled.await(30, TimeUnit.SECONDS));

        long start = System.nanoTime();
        ColumnFamilyStore.shutdownPostFlushExecutor();
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));

        release.countDown();
        stalledFlush.get(30, TimeUnit.SECONDS);

        write(cfs);
        cfs.forceFlush().get(30, TimeUnit.SECONDS);
        cfs.forceFlush().get(30, TimeUnit.SECONDS); // clean memtable: waitForFlushes
    }

    private static void write(ColumnFamilyStore cfs)
    {
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", cfs.keyspace.getName(), cfs.name));
    }
}
