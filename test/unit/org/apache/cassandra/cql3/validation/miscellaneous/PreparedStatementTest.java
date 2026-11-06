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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

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

    /**
     * CASSANDRA-17693: Using the same named bind variable for columns of incompatible types
     * should be rejected during preparation.
     *
     * For example: INSERT INTO t (id, col_text, col_int) VALUES (:id, :a, :a)
     * where col_text is text and col_int is int — the :a variable cannot be both types.
     */
    @Test
    public void testReusedNamedBindVariableWithIncompatibleTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, col_text text, col_int int)");

        String query = formatQuery("INSERT INTO %s (id, col_text, col_int) VALUES (:id, :a, :a)");
        ClientState state = ClientState.forInternalCalls();

        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected InvalidRequestException for bind variable :a used with incompatible types text and int");
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue("Error should mention the conflicting bind variable",
                              e.getMessage().contains("'a'"));
            Assert.assertTrue("Error should mention incompatible types",
                              e.getMessage().contains("incompatible types"));
        }
    }

    /**
     * CASSANDRA-17693: Reusing a named bind variable for columns of the same type should still be accepted.
     */
    @Test
    public void testReusedNamedBindVariableWithCompatibleTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, col_text1 text, col_text2 text)");

        String query = formatQuery("INSERT INTO %s (id, col_text1, col_text2) VALUES (:id, :a, :a)");
        ClientState state = ClientState.forInternalCalls();

        // Should succeed — :a is used for two text columns, which is compatible
        ResultMessage.Prepared prepared = QueryProcessor.instance.prepare(query, state);
        Assert.assertNotNull(prepared);
    }

    /**
     * CASSANDRA-17693: When a named bind variable is reused across 3+ columns and only the third
     * conflicts, preparation should still be rejected.
     */
    @Test
    public void testReusedNamedBindVariableThirdColumnConflicts() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, col_text1 text, col_text2 text, col_int int)");

        String query = formatQuery("INSERT INTO %s (id, col_text1, col_text2, col_int) VALUES (:id, :a, :a, :a)");
        ClientState state = ClientState.forInternalCalls();

        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected InvalidRequestException for bind variable :a used with incompatible types");
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue("Error should mention the conflicting bind variable",
                              e.getMessage().contains("'a'"));
            Assert.assertTrue("Error should mention incompatible types",
                              e.getMessage().contains("incompatible types"));
        }
    }

    /**
     * CASSANDRA-17693: When multiple named bind variables are used and only one pair conflicts,
     * the conflicting pair should be rejected without affecting the valid ones.
     */
    @Test
    public void testMultipleBindVariablesOnlyOneConflicts() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, col_text1 text, col_text2 text, col_int int)");

        // :b is used for compatible columns (text, text), but :a is used for incompatible columns (text, int)
        String query = formatQuery("INSERT INTO %s (id, col_text1, col_int, col_text2) VALUES (:id, :a, :a, :b)");
        ClientState state = ClientState.forInternalCalls();

        try
        {
            QueryProcessor.instance.prepare(query, state);
            Assert.fail("Expected InvalidRequestException for bind variable :a used with incompatible types");
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue("Error should mention the conflicting bind variable",
                              e.getMessage().contains("'a'"));
        }
    }

    /**
     * CASSANDRA-17693: Reusing a named bind variable for ascii and text columns should be accepted,
     * since text (UTF8Type) is compatible with ascii (AsciiType).
     */
    @Test
    public void testReusedNamedBindVariableWithAsciiAndText() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, col_ascii ascii, col_text text)");

        String query = formatQuery("INSERT INTO %s (id, col_text, col_ascii) VALUES (:id, :a, :a)");
        ClientState state = ClientState.forInternalCalls();

        // Should succeed — text is compatible with ascii
        ResultMessage.Prepared prepared = QueryProcessor.instance.prepare(query, state);
        Assert.assertNotNull(prepared);
    }
}
