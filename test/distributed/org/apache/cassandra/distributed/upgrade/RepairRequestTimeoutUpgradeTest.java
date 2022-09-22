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

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;

import org.apache.cassandra.distributed.api.Feature;

import static org.apache.cassandra.net.Verb.VALIDATION_REQ;

public class RepairRequestTimeoutUpgradeTest extends UpgradeTestBase
{
    @Test
    public void simpleUpgradeWithNetworkAndGossipTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP).set("repair_request_timeout_in_ms", 1000))
        .upgrades(v40, v41)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            for (int i = 0; i < 10; i++)
                cluster.get(i % 2 + 1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES ("+i+", 1, 1)");
            cluster.forEach(i -> i.flush(KEYSPACE));
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            cluster.filters().verbs(VALIDATION_REQ.id).drop();
            cluster.get(2).nodetoolResult("repair", KEYSPACE, "-full").asserts().failure();
            cluster.filters().reset();
            for (int i = 10; i < 20; i++)
                cluster.get(i % 2 + 1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES ("+i+", 1, 1)");
            cluster.forEach(i -> i.flush(KEYSPACE));
            cluster.get(1).nodetoolResult("repair", KEYSPACE, "-full").asserts().success();
        }).run();
    }
}
