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

package org.apache.cassandra.streaming;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamReceiveTaskTest extends CQLTester
{
    @Mock
    StreamSession session;
    TableId table;
    StreamReceiveTask task;

    @Before
    public void setup()
    {
        initMocks(this);
        String name = createTableName();
        createTable(KEYSPACE, "CREATE TABLE IF NOT EXISTS %s (id int primary key, val int)", name);
        table = currentTableMetadata().id;
        when(session.streamOperation()).thenReturn(StreamOperation.BOOTSTRAP);
        task = new StreamReceiveTask(session, table, 1, 1, new CountDownLatch.Sync(1));
    }

    @Test
    public void TestAwaitCommitLatchTimesOut() throws InterruptedException, ExecutionException
    {
        StreamReceiveTask.OnCompletionRunnable runnable = new StreamReceiveTask.OnCompletionRunnable(task);
        DatabaseDescriptor.setStreamingCommitLatchTimeout(new DurationSpec.LongSecondsBound("0s"));
        long startCount = StreamingMetrics.commitLatchTimeout.getCount();

        try
        {
            executeWithTimeout(() -> {
                runnable.awaitCommitLatch();
                return null;
            }, 10, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            fail("awaitCommitLatch did not exit upon await timeout");
        }

        assertEquals(startCount + 1, StreamingMetrics.commitLatchTimeout.getCount());
    }

    @Test
    public void TestAwaitCommitLatchWaitsForLatch() throws InterruptedException, ExecutionException
    {
        StreamReceiveTask.OnCompletionRunnable runnable = new StreamReceiveTask.OnCompletionRunnable(task);
        DatabaseDescriptor.setStreamingCommitLatchTimeout(new DurationSpec.LongSecondsBound("1s"));
        long startCount = StreamingMetrics.commitLatchTimeout.getCount();

        try
        {
            executeWithTimeout(() -> {
                runnable.awaitCommitLatch();
                return null;
            }, 10, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            // expected
        }

        assertEquals(startCount, StreamingMetrics.commitLatchTimeout.getCount());
    }

    @Test
    public void TestAwaitCommitLatchExitsUponLatchRelease() throws InterruptedException, ExecutionException
    {
        StreamReceiveTask.OnCompletionRunnable runnable = new StreamReceiveTask.OnCompletionRunnable(task);
        DatabaseDescriptor.setStreamingCommitLatchTimeout(new DurationSpec.LongSecondsBound("1s"));
        long startCount = StreamingMetrics.commitLatchTimeout.getCount();
        task.commitLatch.decrement();

        try
        {
            executeWithTimeout(() -> {
                runnable.awaitCommitLatch();
                return null;
            }, 10, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            fail("awaitCommitLatch did not exit upon releasing the latch");
        }

        assertEquals(startCount, StreamingMetrics.commitLatchTimeout.getCount());
    }

    private void executeWithTimeout(Callable<Void> function, long timeout, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Void> future = executor.submit(function);

        future.get(timeout, unit);
    }
}
