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

package org.apache.cassandra.index.sai.plan;

import java.util.concurrent.ForkJoinPool;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.IndexSearchResultIterator;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

/**
 * Test to cover edge cases related to memtable flush during query execution.
 */
public class FlushIndexWhileQueryingTest extends SAITester
{
    @Test
    public void testFlushDuringEqualityQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, x int)");

        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'sai'");

        execute("INSERT INTO %s (k, x) VALUES (?, ?)", "a", 0);
        execute("INSERT INTO %s (k, x) VALUES (?, ?)", "b", 0);
        execute("INSERT INTO %s (k, x) VALUES (?, ?)", "c", 1);

        // We use a barrier to trigger flush at precisely the right time
        InvokePointBuilder initialInvokePoint = InvokePointBuilder.newInvokePoint()
                                                           .onClass(QueryViewBuilder.class)
                                                           .onMethod("build")
                                                           .atExit();
        Injections.Barrier initialBarrier = Injections.newBarrier("pause_query", 2, false)
                                                      .add(initialInvokePoint)
                                                      .build();
        InvokePointBuilder secondInvokePoint = InvokePointBuilder.newInvokePoint()
                                                           .onClass(IndexSearchResultIterator.class)
                                                           .onMethod("build")
                                                           .atEntry();
        Injections.Barrier secondBarrier = Injections.newBarrier("resume_query", 2, false)
                                                     .add(secondInvokePoint)
                                                     .build();

        Injections.inject(initialBarrier, secondBarrier);

        // Flush in a separate thread to allow the query to run concurrently
        ForkJoinPool.commonPool().submit(() -> {
            try
            {
                initialBarrier.arrive();
                flush();
                secondBarrier.arrive();
            }
            catch (InterruptedException t)
            {
                throw new RuntimeException(t);
            }
        });

        assertRowCount(execute("SELECT k FROM %s WHERE x = 0"), 2);
        assertEquals("Confirm that we hit the barrier (helps in case method name changed)", 0, initialBarrier.getCount());
        assertEquals("Confirm that we hit the barrier (helps in case method name changed)", 0, secondBarrier.getCount());
    }
}
