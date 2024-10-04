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

package org.apache.cassandra.distributed.test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.CoordinatorHelper;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.transport.Dispatcher;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;

public class BatchStoreVerbHandlerTest extends TestBaseImpl
{

    @Test
    public void batchTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .start())
        {
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + 3 + "}");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));");

            AtomicLong dropCountBefore = new AtomicLong();


            // test case when the message is NOT expired
            cluster.get(1).runOnInstance(() -> {
                dropCountBefore.set(MessagingService.instance().getDroppedMessages().get(Verb.BATCH_STORE_REQ.toString()));
                new TestScenario(false)
                .assertWillSpeculate();
            });
            cluster.get(2).runOnInstance(() -> {
                long dropCountAfterForLegit = MessagingService.instance().getDroppedMessages().get(Verb.BATCH_STORE_REQ.toString());
                assertEquals(0, dropCountAfterForLegit - dropCountBefore.get());
            });


            // test case when the message is expired
            cluster.get(1).runOnInstance(() -> {
                dropCountBefore.set(MessagingService.instance().getDroppedMessages().get(Verb.BATCH_STORE_REQ.toString()));
                new TestScenario(true)
                .assertWillSpeculate();
            });
            cluster.get(2).runOnInstance(() -> {
                long dropCountAfterForExpired = MessagingService.instance().getDroppedMessages().get(Verb.BATCH_STORE_REQ.toString());
                assertEquals(1, dropCountAfterForExpired - dropCountBefore.get());
            });

        }
    }

    private static class TestScenario
    {
        boolean expired;
        TestScenario(boolean expired){
            this.expired = expired;
        }

        private void assertWillSpeculate()
        {
            String batch = "BEGIN BATCH\n" +
                            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 100);\n" +
                            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (2, 2, 200);\n" +
                            "APPLY BATCH;";

            try
            {
                CoordinatorHelper.unsafeExecuteInternal(batch,
                                                        ConsistencyLevel.QUORUM,
                                                        ConsistencyLevel.QUORUM,
                                                        new Dispatcher.RequestTime(expired ? System.nanoTime() - TimeUnit.SECONDS.toNanos(60) : System.nanoTime() + TimeUnit.SECONDS.toNanos(60)));
            }
            catch (WriteTimeoutException e)
            {
            }
        }
    }

}
