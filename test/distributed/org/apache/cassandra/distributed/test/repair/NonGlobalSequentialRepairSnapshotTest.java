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

package org.apache.cassandra.distributed.test.repair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken.keyForToken;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class NonGlobalSequentialRepairSnapshotTest extends TestBaseImpl
{
    @Test
    public void nonGlobalSequentialRepairRepairsAllRanges() throws IOException
    {
        // Ring: node1 = token 1000, node2 = 2000, node3 = 3000, replication factor 2.
        // node1 owns two ranges, each shared with a different node:
        // (3000, 1000]  shared with node2 -> row A goes here (token 500)
        // (2000, 3000]  shared with node3 -> row B goes here (token 2500)
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.set("num_tokens", 1)
                                                             .set("initial_token", Long.toString(c.num() * 1000L))
                                                             .with(GOSSIP)
                                                             .with(NETWORK))
                                           .start(), 2))
        {
            ByteBuffer keyA = keyForToken(500);
            ByteBuffer keyB = keyForToken(2500);

            assertEquals(Map.of("1000", "127.0.0.1:7012", "2000", "127.0.0.2:7012", "3000", "127.0.0.3:7012"),
                         cluster.get(1).callOnInstance(() -> StorageService.instance.getTokenToEndpointWithPortMap()));
            assertEquals(Set.of("(3000,1000]", "(2000,3000]"),
                         cluster.get(1).callOnInstance(() -> StorageService.instance.getLocalReplicas(KEYSPACE).ranges().stream().map(Object::toString).collect(Collectors.toSet())));

            String table = KEYSPACE + ".tbl";
            cluster.schemaChange("CREATE TABLE " + table + " (k blob PRIMARY KEY, v int)");
            cluster.get(1).nodetoolResult("disableautocompaction", KEYSPACE, "tbl").asserts().success();

            cluster.get(1).executeInternal("INSERT INTO " + table + " (k, v) VALUES (?, 1)", keyA);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(1).executeInternal("INSERT INTO " + table + " (k, v) VALUES (?, 2)", keyB);
            cluster.get(1).flush(KEYSPACE);

            cluster.get(1).nodetoolResult("repair", "-full", "-dcpar", "-local", KEYSPACE, "tbl").asserts().success();

            // Repair said it succeeded, so node1's rows must now be on its partners.
            assertEquals(1, cluster.get(2).executeInternal("SELECT * FROM " + table + " WHERE k = ?", keyA).length);
            assertEquals(1, cluster.get(3).executeInternal("SELECT * FROM " + table + " WHERE k = ?", keyB).length);
        }
    }
}
