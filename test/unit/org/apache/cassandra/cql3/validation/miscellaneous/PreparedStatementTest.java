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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class PreparedStatementTest extends CQLTester
{
    private static final int NUM_THREADS = 50;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final CountDownLatch finishLatch = new CountDownLatch(NUM_THREADS);

    @Test
    public void testPreparedStatementStaysInCache() throws Throwable
    {
        execute("CREATE TABLE " + KEYSPACE + ".test_fullyqualified(a int primary key, b int)");

        ClientState state = ClientState.forInternalCalls();
        Assert.assertEquals(0, QueryProcessor.instance.getPreparedStatements().size());
        final ResultMessage.Prepared[] preparedSelect = new ResultMessage.Prepared[NUM_THREADS];
        AtomicBoolean preparedStatementPresentInCache = new AtomicBoolean(true);
        for (int i = 0; i < NUM_THREADS; i++)
        {
            int threadId = i;
            Thread thread = new Thread(() -> {
                try
                {
                    // Wait until the start signal is given
                    startLatch.await();

                    // Code to be executed in each thread
                    preparedSelect[threadId] = QueryProcessor.instance.prepare(
                    String.format("SELECT b FROM %s.test_fullyqualified where a = 10", KEYSPACE), state);
                    Assert.assertNotNull(preparedSelect[threadId].statementId);
                    if(!QueryProcessor.instance.getPreparedStatements().containsKey(preparedSelect[threadId].statementId))
                    {
                        preparedStatementPresentInCache.set(false);
                    }
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
                finally
                {
                    // Signal that this thread has finished
                    finishLatch.countDown();
                }
                Assert.fail();
            });
            thread.start();
        }

        // Signal all threads to start
        startLatch.countDown();

        // Wait for all threads to finish
        finishLatch.await();
        Assert.assertTrue(preparedStatementPresentInCache.get());
    }
}
