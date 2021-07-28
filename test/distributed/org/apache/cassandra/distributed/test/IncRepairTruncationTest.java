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

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.utils.concurrent.Condition;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.net.Verb;


import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.IMessageFilters.Matcher;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.insert;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

public class IncRepairTruncationTest extends TestBaseImpl
{
    @Test
    public void testTruncateDuringIncRepair() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newFixedThreadPool(3);
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.with(GOSSIP)
                                                                      .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");

            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            // mark everything repaired
            cluster.get(1).nodetoolResult("repair", KEYSPACE, "tbl").asserts().success();

            /*
            make sure we are out-of-sync to make node2 stream data to node1:
             */
            cluster.get(2).executeInternal("insert into "+KEYSPACE+".tbl (id, t) values (5, 5)");
            cluster.get(2).flush(KEYSPACE);
            /*
            start repair:
            block streaming from 2 -> 1 until truncation below has executed
             */
            BlockMessage node2Streaming = new BlockMessage();
            cluster.filters().inbound().verbs(Verb.VALIDATION_RSP.id).from(2).to(1).messagesMatching(node2Streaming).drop();

            /*
            block truncation on node2:
             */
            BlockMessage node2Truncation = new BlockMessage();
            cluster.filters().inbound().verbs(Verb.TRUNCATE_REQ.id).from(1).to(2).messagesMatching(node2Truncation).drop();

            Future<NodeToolResult> repairResult = es.submit(() -> cluster.get(1).nodetoolResult("repair", KEYSPACE, "tbl"));

            Future<?> truncationFuture = es.submit(() -> {
                try
                {
                    /*
                    wait for streaming message to sent before truncating, to make sure we have a mismatch to make us stream later
                     */
                    node2Streaming.gotMessage.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                cluster.coordinator(1).execute("TRUNCATE "+KEYSPACE+".tbl", ConsistencyLevel.ALL);
            });

            node2Truncation.gotMessage.await();
            // make sure node1 finishes truncation, removing its files
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                while (!cfs.getLiveSSTables().isEmpty())
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            });

            /* let repair finish, streaming files from 2 -> 1 */
            node2Streaming.allowMessage.signalAll();

            /* and the repair should fail: */
            repairResult.get().asserts().failure();

            /*
            and let truncation finish on node2
             */
            node2Truncation.allowMessage.signalAll();
            truncationFuture.get();

            /* wait for truncation to remove files on node2 */
            cluster.get(2).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                while (!cfs.getLiveSSTables().isEmpty())
                {
                    System.out.println(cfs.getLiveSSTables());
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                }
            });

            cluster.get(1).nodetoolResult("repair", "-vd", KEYSPACE, "tbl").asserts().success().notificationContains("Repair preview completed successfully");
        }
        finally
        {
            es.shutdown();
        }
    }

    private static class BlockMessage implements Matcher
    {
        private final Condition gotMessage = newOneTimeCondition();
        private final Condition allowMessage = newOneTimeCondition();

        public boolean matches(int from, int to, IMessage message)
        {
            gotMessage.signalAll();
            try
            {
                allowMessage.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            return false;
        }
    }
}
