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

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;

import java.util.function.Consumer;

public class Pre40MessageFilterTest extends UpgradeTestBase
{
    public void reserializePre40RequestPaxosTest(Consumer<IInstanceConfig> configConsumer) throws Throwable
    {
        new UpgradeTestBase.TestCase()
        .nodes(2)
        .withConfig(configConsumer)
        .nodesToUpgrade(1)
        .upgradesToCurrentFrom(v40)
        .setup((cluster) -> {
            cluster.filters().outbound().allVerbs().messagesMatching((f,t,m) -> false).drop();
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl(pk,ck,v) VALUES (1, 1, 1) IF NOT EXISTS",
                                           ConsistencyLevel.QUORUM,
                                           1);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            cluster.coordinator(node).execute("UPDATE " + KEYSPACE + ".tbl SET v = ? WHERE pk = ? AND ck = ?  IF v = ?",
                                              ConsistencyLevel.QUORUM,
                                              2, 1, 1, 1);
        }).run();
    }

    @Test
    public void reserializePre40RequestPaxosWithoutNetworkTest() throws Throwable
    {
        reserializePre40RequestPaxosTest(config -> {});
    }

    @Test
    public void reserializePre40RequestPaxosWithNetworkTest() throws Throwable
    {
        reserializePre40RequestPaxosTest(config -> config.with(Feature.NETWORK, Feature.GOSSIP));
    }
}
