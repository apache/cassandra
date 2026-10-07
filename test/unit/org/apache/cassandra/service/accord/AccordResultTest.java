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

package org.apache.cassandra.service.accord;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.primitives.Keys;
import accord.topology.TopologyMismatch;
import accord.utils.async.AsyncResults;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * @author LLM
 */
public class AccordResultTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void droppedTableTopologyMismatchCompletesResult()
    {
        TableId droppedTable = TableId.generate();
        Assert.assertNull(Schema.instance.getTableMetadata(droppedTable));
        PartitionKey key = new PartitionKey(droppedTable, DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(1)));
        AccordResult<Object> result = new AccordResult<>(null, Keys.of(key), new NoopBookkeeping(), 0, Long.MAX_VALUE, true, null);
        AtomicBoolean listenerCalled = new AtomicBoolean();
        result.addCallback((success, failure) -> listenerCalled.set(true));

        AsyncResults.SettableResult<Object> coordinate = new AsyncResults.SettableResult<>();
        coordinate.invoke(result);

        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        Thread thread = Thread.currentThread();
        Thread.UncaughtExceptionHandler previous = thread.getUncaughtExceptionHandler();
        try
        {
            thread.setUncaughtExceptionHandler((ignore, failure) -> uncaught.set(failure));
            coordinate.setFailure(new TopologyMismatch("table was dropped"));
        }
        finally
        {
            thread.setUncaughtExceptionHandler(previous);
        }

        Assert.assertNull(uncaught.get());
        Assert.assertTrue(result.isDone());
        Assert.assertTrue(listenerCalled.get());
        Assert.assertTrue(result.cause() instanceof InvalidRequestException);
        try
        {
            result.awaitAndGet();
            Assert.fail("Expected InvalidRequestException");
        }
        catch (InvalidRequestException expected)
        {
        }
    }

    private static final class NoopBookkeeping extends RequestBookkeeping
    {
        NoopBookkeeping()
        {
            super(ThrowsExceptionType.READ);
        }

        @Override void markFailure() {}
        @Override void markTimeout() {}
        @Override void markPreempted() {}
        @Override void markRetryDifferentSystem() {}
        @Override void markTopologyMismatch() {}
        @Override void markElapsedNanos(long nanos) {}
    }
}
