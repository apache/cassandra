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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.RING_DELAY;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class MoveTest extends TestBaseImpl
{
    private static final String KEYSPACE = "move_test_ks";
    private static final String TABLE = "tbl";
    private static final String KS_TBL = KEYSPACE + '.' + TABLE;

    static
    {
        RING_DELAY.setLong(5000);
    }

    private void move(boolean forwards) throws Throwable
    {
        // TODO: fails with vnode enabled
        try (Cluster cluster = Cluster.build(4)
                                      .withConfig(config -> config.set("paxos_variant", "v2_without_linearizable_reads").with(NETWORK).with(GOSSIP))
                                      .withoutVNodes()
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KS_TBL + " (k int, c int, v int, primary key (k, c));");
            for (int i=0; i<30; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KS_TBL + " (k, c, v) VALUES (?, 1, 1) IF NOT EXISTS",
                                               ConsistencyLevel.SERIAL, ConsistencyLevel.ALL, i);
            }

            List<String> initialTokens = new ArrayList<>();
            for (int i=0; i<cluster.size(); i++)
            {
                String token = cluster.get(i + 1).callsOnInstance(() -> Iterables.getOnlyElement(StorageService.instance.getLocalTokens()).toString()).call();
                initialTokens.add(token);
            }
            Assert.assertEquals(Lists.newArrayList("-4611686018427387905",
                                                   "-3",
                                                   "4611686018427387899",
                                                   "9223372036854775801"), initialTokens);

            NodeToolResult result = cluster.get(forwards ? 2 : 3).nodetoolResult("move", "2305843009213693949");
            Assert.assertTrue(result.toString(), result.getRc() == 0);
        }
    }

    @Test
    public void moveBack() throws Throwable
    {
        move(false);
    }

    @Test
    public void moveForwards() throws Throwable
    {
        move(true);
    }

}
