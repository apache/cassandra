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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.net.Verb.FINALIZE_COMMIT_MSG;
import static org.assertj.core.api.Assertions.assertThat;

public class IncRepairCoordinatorErrorTest extends TestBaseImpl
{
    @Test
    public void errorTest() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, x int)"));
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, x) values (?, ?)"), ConsistencyLevel.ALL, i, i);

            cluster.filters().inbound()
                   .to(3)
                   .messagesMatching((from, to, msg) -> msg.verb() == FINALIZE_COMMIT_MSG.id).drop();
            cluster.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
            assertThat(cluster.get(1).logs().watchFor("Removing completed session .* with state FINALIZED").getResult()).isNotEmpty();

            TimeUUID result = (TimeUUID) cluster.get(1).executeInternal("select parent_id from system_distributed.repair_history")[0][0];
            cluster.get(3).runOnInstance(() -> {
                ActiveRepairService.instance().failSession(result.toString(), true);
            });
        }
    }
}
