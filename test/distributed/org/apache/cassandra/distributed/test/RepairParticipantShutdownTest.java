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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertNotNull;

/**
 * This test has been authored entirely by LLM.
 *
 * A participant that shuts down while the coordinator is waiting for its merkle tree must fail the repair.
 */
public class RepairParticipantShutdownTest extends TestBaseImpl
{
    @Test
    public void repairTerminatesWhenParticipantShutsDownDuringValidation() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, t int)"));
            for (int i = 0; i < 10; i++)
                cluster.get(1).executeInternal(withKeyspace("insert into %s.tbl (id, t) values (?, ?)"), i, i);
            cluster.forEach(node -> node.flush(KEYSPACE));

            // node2 will acknowledge the validation request but its merkle tree never arrives - the same state the
            // coordinator is in when a participant dies in the middle of a validation, without having to win the race
            cluster.filters().verbs(Verb.VALIDATION_RSP.id).from(2).to(1).drop();

            AtomicReference<NodeToolResult> result = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            Thread repair = new Thread(() -> {
                try { result.set(cluster.get(1).nodetoolResult("repair", "-full", KEYSPACE)); }
                finally { done.countDown(); }
            }, "repair");
            repair.setDaemon(true);
            repair.start();

            // once node2 has the validation request, take it away cleanly
            cluster.get(1).logs().watchFor("VALIDATION_REQ received by /127.0.0.2");
            cluster.get(2).shutdown().get();

            if (!done.await(2, TimeUnit.MINUTES))
                throw new AssertionError("nodetool repair did not terminate after its only other replica shut down " +
                                         "while a validation was outstanding");
            assertNotNull(result.get());
            result.get().asserts().failure();
        }
    }
}
