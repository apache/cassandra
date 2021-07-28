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

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CompactionExecutorTest
{
    static Throwable testTaskThrowable = null;
    private CompactionManager.CompactionExecutor executor;

    @Before
    public void setup()
    {
        executor = new CompactionManager.CompactionExecutor(new ExecutorFactory.Default(null, null, (thread, throwable) -> {
            if (throwable != null)
                testTaskThrowable = throwable;
        }), 1, "test", Integer.MAX_VALUE);
    }

    @After
    public void destroy() throws Exception
    {
        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
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
