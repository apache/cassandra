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

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.upgrade.UpgradeTestBase.v50;

public class ClusterMetadata2iUpgradeTest extends TestBaseImpl
{
    @Test
    public void upgradeIndexIsNotBuiltTest() throws Throwable
    {
        new UpgradeTestBase.TestCase()
        .nodes(1)
        .nodesToUpgrade(1)
        .withConfig((cfg) -> cfg.with(Feature.GOSSIP))
        .upgradesToCurrentFrom(v50)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.schemaChange(withKeyspace("create index iii on %s.tbl (ck)"));
            cluster.schemaChange(withKeyspace("create index iii2 on %s.tbl (v)"));
            for (int i = 0; i < 1000; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v) values (?, ?, ?)"), ConsistencyLevel.ALL, i, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));
            cluster.forEach(i -> i.executeInternal("truncate system.\"IndexInfo\""));
        })
        .runAfterClusterUpgrade((cluster) -> {
        }).run();
    }
}
