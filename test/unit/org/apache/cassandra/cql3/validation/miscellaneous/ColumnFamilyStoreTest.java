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

package org.apache.cassandra.cql3.validation.miscellaneous;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.schema.IndexMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ColumnFamilyStoreTest extends CQLTester
{
    @Test
    public void testFailing2iFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int)");
        createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(value) USING 'org.apache.cassandra.cql3.validation.miscellaneous.ColumnFamilyStoreTest$BrokenCustom2I'");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, i);

        try
        {
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }
        catch (Throwable t)
        {
            // ignore
        }

        // Make sure there's no flush running
        waitFor(() -> ((JMXEnabledThreadPoolExecutor) ColumnFamilyStore.flushExecutor).getActiveCount() == 0,
                TimeUnit.SECONDS.toMillis(5));

        // SSTables remain uncommitted.
        assertEquals(1, getCurrentColumnFamilyStore().getDirectories().getDirectoryForNewSSTables().listFiles().length);
    }

    public void waitFor(Supplier<Boolean> condition, long timeout)
    {
        long start = System.currentTimeMillis();
        while(true)
        {
            if (condition.get())
                return;

            assertTrue("Timeout ocurred while waiting for condition",
                       System.currentTimeMillis() - start < timeout);
        }
    }

    // Used for index creation above
    public static class BrokenCustom2I extends StubIndex
    {
        public BrokenCustom2I(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public Callable<?> getBlockingFlushTask()
        {
            throw new RuntimeException();
        }
    }
}
