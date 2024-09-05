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

import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class PreparedStatementTest extends CQLTester
{
    @Test
    public void testReproduceRaceInPrepapre() throws Throwable
    {
        execute("CREATE TABLE " + KEYSPACE + ".test_fullyqualified_withks(a int primary key, b int)");
        execute("CREATE TABLE " + KEYSPACE_PER_TEST + ".test_fullyqualified_withks(a int primary key, b int)");

        ClientState state = ClientState.forInternalCalls();
        Assert.assertEquals(0, QueryProcessor.getPreparedStatementsCache().size());
        final ResultMessage.Prepared[] preparedSelect = new ResultMessage.Prepared[2];

        CompletableFuture<Void> thread1 = CompletableFuture.runAsync(() -> {
            preparedSelect[0] = QueryProcessor.instance.prepare(
            String.format("SELECT b FROM %s.test_fullyqualified_withks where a = 10", KEYSPACE), state,
            0, 10000, 30000);
            Assert.assertNotNull(preparedSelect[0].statementId);
        });

        CompletableFuture<Void> thread2 = CompletableFuture.runAsync(() -> {
            preparedSelect[1] = QueryProcessor.instance.prepare(
            String.format("SELECT b FROM %s.test_fullyqualified_withks where a = 10", KEYSPACE), state,
            5000, 0, 0);
            QueryProcessor.getPreparedStatementsCache().containsKey(preparedSelect[1].statementId);
            for (int i=0; i<30; i++)
            {
                Assert.assertNotNull(preparedSelect[1].statementId);
                if (!QueryProcessor.getPreparedStatementsCache().containsKey(preparedSelect[1].statementId))
                {
                    Assert.fail("The prepared map is missing already a valid statement, this is unexpected!!!! " + i);
                }
                System.out.println("The statement is present in cache: " + i);
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        });
        thread2.get();
        thread1.get();
    }
}
