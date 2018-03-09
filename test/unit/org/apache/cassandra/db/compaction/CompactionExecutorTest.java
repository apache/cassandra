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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CompactionExecutorTest
{
    static Throwable testTaskThrowable = null;
    private static class TestTaskExecutor extends CompactionManager.CompactionExecutor
    {
        @Override
        public void afterExecute(Runnable r, Throwable t)
        {
            if (t == null)
            {
                t = DebuggableThreadPoolExecutor.extractThrowable(r);
            }
            testTaskThrowable = t;
        }
        @Override
        protected void beforeExecute(Thread t, Runnable r)
        {
        }
    }
    private CompactionManager.CompactionExecutor executor;

    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        executor = new TestTaskExecutor();
    }

    @After
    public void destroy() throws Exception
    {
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    public void testFailedRunnable() throws Exception
    {
        testTaskThrowable = null;
        Future<?> tt = executor.submitIfRunning(
            () -> { assert false : "testFailedRunnable"; }
            , "compactionExecutorTest");

        while (!tt.isDone())
            Thread.sleep(10);
        assertNotNull(testTaskThrowable);
        assertEquals(testTaskThrowable.getMessage(), "testFailedRunnable");
    }

    @Test
    public void testFailedCallable() throws Exception
    {
        testTaskThrowable = null;
        Future<?> tt = executor.submitIfRunning(
            () -> { assert false : "testFailedCallable"; return 1; }
            , "compactionExecutorTest");

        while (!tt.isDone())
            Thread.sleep(10);
        assertNotNull(testTaskThrowable);
        assertEquals(testTaskThrowable.getMessage(), "testFailedCallable");
    }

    @Test
    public void testExceptionRunnable() throws Exception
    {
        testTaskThrowable = null;
        Future<?> tt = executor.submitIfRunning(
        () -> { throw new RuntimeException("testExceptionRunnable"); }
        , "compactionExecutorTest");

        while (!tt.isDone())
            Thread.sleep(10);
        assertNotNull(testTaskThrowable);
        assertEquals(testTaskThrowable.getMessage(), "testExceptionRunnable");
    }
}
